package jobs

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const MaxLines = 250000

const jobQueueCapacity = 100

var (
	ErrInvalidLineCount = errors.New("lines must be between 1 and 250000")
	ErrJobNotFound      = errors.New("job not found")
	ErrQueueFull        = errors.New("job queue is full")
	ErrShuttingDown     = errors.New("service is shutting down")
)

type PDFGenerator interface {
	Generate(lines int) ([]byte, error)
}

// JobStore holds job metadata. Generated PDF bytes remain on the filesystem.
// The basic methods intentionally match Store so the in-memory implementation
// remains a fast test double. CreateQueued and ClaimQueued provide the atomic
// queue operations needed by a persistent store.
type JobStore interface {
	Create(Job) bool
	Delete(string)
	Get(string) (Job, bool)
	MarkProcessing(string) bool
	MarkCompleted(string, string) bool
	MarkFailed(string, string) bool
	CreateQueued(context.Context, Job, int) (bool, error)
	ClaimQueued(context.Context) (Job, bool, error)
}

// Store holds metadata only. The generated PDF remains on the filesystem.
type Store struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewStore() *Store {
	return &Store{jobs: make(map[string]Job)}
}

func (s *Store) Create(job Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[job.ID]; exists {
		return false
	}
	s.jobs[job.ID] = job
	return true
}

func (s *Store) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
}

func (s *Store) Get(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

func (s *Store) update(id string, update func(*Job)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return false
	}
	update(&job)
	job.UpdatedAt = time.Now().UTC()
	s.jobs[id] = job
	return true
}

func (s *Store) MarkProcessing(id string) bool {
	return s.update(id, func(job *Job) { job.Status = StatusProcessing })
}

func (s *Store) MarkCompleted(id, path string) bool {
	return s.update(id, func(job *Job) {
		job.Status = StatusCompleted
		job.FilePath = path
		job.Error = ""
	})
}

func (s *Store) MarkFailed(id, message string) bool {
	return s.update(id, func(job *Job) {
		job.Status = StatusFailed
		job.Error = message
	})
}

// CreateQueued creates a job only when fewer than capacity jobs are queued.
// Holding the mutex for both operations mirrors the transactional Postgres
// implementation and keeps the in-memory store useful in unit tests.
func (s *Store) CreateQueued(_ context.Context, job Job, capacity int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queued := 0
	for _, existing := range s.jobs {
		if existing.Status == StatusQueued {
			queued++
		}
	}
	if queued >= capacity {
		return false, ErrQueueFull
	}
	if _, exists := s.jobs[job.ID]; exists {
		return false, nil
	}
	s.jobs[job.ID] = job
	return true, nil
}

// ClaimQueued atomically transitions one queued job to processing.
func (s *Store) ClaimQueued(_ context.Context) (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, job := range s.jobs {
		if job.Status != StatusQueued {
			continue
		}
		job.Status = StatusProcessing
		job.UpdatedAt = time.Now().UTC()
		s.jobs[id] = job
		return job, true, nil
	}
	return Job{}, false, nil
}

type Service struct {
	pdfGenerator PDFGenerator
	// store is retained for the existing in-memory tests. Service operations
	// use jobStore so production can use any JobStore implementation.
	store      *Store
	jobStore   JobStore
	storageDir string
	logger     *slog.Logger

	mu        sync.Mutex
	accepting bool
	stopping  bool
	wake      chan struct{}
	stop      chan struct{}
	jobs      sync.WaitGroup
}

func NewService(pdfGenerator PDFGenerator, storageDir string, workers int) *Service {
	store := NewStore()
	return newService(pdfGenerator, storageDir, workers, store, store)
}

// NewServiceWithStore constructs a service backed by store. It is used by the
// server with Postgres while NewService continues to use Store for unit tests.
func NewServiceWithStore(pdfGenerator PDFGenerator, storageDir string, workers int, store JobStore) *Service {
	return newService(pdfGenerator, storageDir, workers, nil, store)
}

func newService(pdfGenerator PDFGenerator, storageDir string, workers int, memoryStore *Store, store JobStore) *Service {
	if workers < 1 {
		panic("workers must be at least 1")
	}
	if store == nil {
		panic("job store must not be nil")
	}

	service := &Service{
		pdfGenerator: pdfGenerator,
		store:        memoryStore,
		jobStore:     store,
		storageDir:   storageDir,
		logger:       slog.Default(),
		accepting:    true,
		wake:         make(chan struct{}, 1),
		stop:         make(chan struct{}),
	}
	for range workers {
		go service.worker()
	}
	return service
}

func (s *Service) CreateJob(lines int) (Job, error) {
	if lines < 1 || lines > MaxLines {
		return Job{}, ErrInvalidLineCount
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accepting {
		return Job{}, ErrShuttingDown
	}

	for {
		id, err := newID()
		if err != nil {
			return Job{}, fmt.Errorf("generate job ID: %w", err)
		}
		now := time.Now().UTC()
		job := Job{
			ID:        id,
			Lines:     lines,
			Status:    StatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		}
		created, err := s.jobStore.CreateQueued(context.Background(), job, jobQueueCapacity)
		if errors.Is(err, ErrQueueFull) {
			return Job{}, ErrQueueFull
		}
		if err != nil {
			return Job{}, fmt.Errorf("store queued job: %w", err)
		}
		if !created {
			continue
		}
		s.signalWorker()
		s.logger.Info("job created",
			slog.String("job_id", job.ID),
			slog.Int("lines", job.Lines),
		)
		return job, nil
	}
}

func (s *Service) GetJob(id string) (Job, error) {
	job, ok := s.jobStore.Get(id)
	if !ok {
		return Job{}, ErrJobNotFound
	}
	return job, nil
}

func (s *Service) StopAccepting() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accepting {
		return
	}
	s.accepting = false
	s.stopping = true
	close(s.stop)
}

// Wait blocks until the jobs this service's workers are running finish, or ctx
// ends. Call it after StopAccepting: only then are no further jobs claimed.
func (s *Service) Wait(ctx context.Context) error {
	// StopAccepting and worker Add calls share this lock. Once stopping is set,
	// no worker can add new work after this point, so Wait observes a stable
	// WaitGroup counter.
	s.mu.Lock()
	done := make(chan struct{})
	go func() {
		s.jobs.Wait()
		close(done)
	}()
	s.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) run(job Job) {
	processingStarted := time.Now()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("job panic recovered",
				slog.String("job_id", job.ID),
				slog.Any("panic", recovered),
			)
			s.markFailed(job.ID, processingStarted)
		}
	}()

	processingStarted = time.Now()
	s.logger.Info("job state transition",
		slog.String("job_id", job.ID),
		slog.String("status", string(StatusProcessing)),
	)

	document, err := s.pdfGenerator.Generate(job.Lines)
	if err != nil {
		s.logger.Error("job PDF generation failed",
			slog.String("job_id", job.ID),
			slog.Any("error", err),
		)
		s.markFailed(job.ID, processingStarted)
		return
	}
	path, err := s.writePDF(job.ID, document)
	if err != nil {
		s.logger.Error("job PDF storage failed",
			slog.String("job_id", job.ID),
			slog.Any("error", err),
		)
		s.markFailed(job.ID, processingStarted)
		return
	}
	if !s.jobStore.MarkCompleted(job.ID, path) {
		s.logger.Error("job completed transition failed",
			slog.String("job_id", job.ID),
			slog.String("error", "job not found"),
		)
		s.markFailed(job.ID, processingStarted)
		return
	}
	s.logger.Info("job state transition",
		slog.String("job_id", job.ID),
		slog.String("status", string(StatusCompleted)),
		slog.Duration("generation_duration", time.Since(processingStarted)),
		slog.Int("pdf_size_bytes", len(document)),
	)
}

func (s *Service) worker() {
	var tick <-chan time.Time
	var ticker *time.Ticker
	// In-memory jobs are always signalled by CreateJob. Postgres also polls so
	// a newly started process discovers jobs persisted by an earlier process.
	if s.store == nil {
		ticker = time.NewTicker(200 * time.Millisecond)
		tick = ticker.C
		defer ticker.Stop()
	} else {
		// Keep direct Store setup deterministic for unit tests, while allowing a
		// shutdown to wake every idle in-memory worker.
		select {
		case <-s.wake:
		case <-s.stop:
			return
		}
	}
	for {
		// Holding mu across the stopping check and Add prevents StopAccepting
		// from racing a new Add with Wait.
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return
		}
		s.jobs.Add(1)
		s.mu.Unlock()

		job, claimed, err := s.jobStore.ClaimQueued(context.Background())
		if err != nil {
			s.logger.Error("claim queued job", slog.Any("error", err))
		} else if claimed {
			s.run(job)
			s.jobs.Done()
			continue
		}
		s.jobs.Done()

		select {
		case <-tick:
		case <-s.wake:
		case <-s.stop:
			return
		}
	}
}

func (s *Service) markFailed(id string, processingStarted time.Time) {
	if !s.jobStore.MarkFailed(id, "job failed") {
		s.logger.Error("job failed transition failed",
			slog.String("job_id", id),
			slog.String("error", "job not found"),
		)
		return
	}
	s.logger.Info("job state transition",
		slog.String("job_id", id),
		slog.String("status", string(StatusFailed)),
		slog.Duration("generation_duration", time.Since(processingStarted)),
	)
}

func (s *Service) signalWorker() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Service) writePDF(id string, document []byte) (string, error) {
	file, err := os.CreateTemp(s.storageDir, id+"-*.tmp")
	if err != nil {
		return "", err
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)

	if _, err := file.Write(document); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}

	finalPath := filepath.Join(s.storageDir, id+".pdf")
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return "", err
	}
	return finalPath, nil
}

func newID() (string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const length = 10
	const limit = byte(256 / len(alphabet) * len(alphabet))

	id := make([]byte, length)
	for position := 0; position < length; {
		var random [1]byte
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", err
		}
		if random[0] >= limit {
			continue
		}
		id[position] = alphabet[int(random[0])%len(alphabet)]
		position++
	}
	return string(id), nil
}
