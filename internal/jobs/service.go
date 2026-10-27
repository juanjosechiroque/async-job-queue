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
// Store and the Postgres implementation share the same errors and queue
// semantics so either can back the same Service behavior.
type JobStore interface {
	Get(context.Context, string) (Job, error)
	MarkCompleted(context.Context, string, string) error
	MarkFailed(context.Context, string, string) error
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

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	return job, nil
}

func (s *Store) update(ctx context.Context, id string, update func(*Job)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	update(&job)
	job.UpdatedAt = time.Now().UTC()
	s.jobs[id] = job
	return nil
}

func (s *Store) MarkCompleted(ctx context.Context, id, path string) error {
	return s.update(ctx, id, func(job *Job) {
		job.Status = StatusCompleted
		job.FilePath = path
		job.Error = ""
	})
}

func (s *Store) MarkFailed(ctx context.Context, id, message string) error {
	return s.update(ctx, id, func(job *Job) {
		job.Status = StatusFailed
		job.Error = message
	})
}

// CreateQueued creates a job only when fewer than capacity jobs are queued.
// Holding the mutex for both operations mirrors the transactional Postgres
// implementation and keeps the in-memory store useful in unit tests.
func (s *Store) CreateQueued(ctx context.Context, job Job, capacity int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
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
func (s *Store) ClaimQueued(ctx context.Context) (Job, bool, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var selectedID string
	var selected Job
	for id, job := range s.jobs {
		if job.Status == StatusQueued && (selectedID == "" || job.CreatedAt.Before(selected.CreatedAt) || (job.CreatedAt.Equal(selected.CreatedAt) && id < selectedID)) {
			selectedID = id
			selected = job
		}
	}
	if selectedID == "" {
		return Job{}, false, nil
	}
	selected.Status = StatusProcessing
	selected.UpdatedAt = time.Now().UTC()
	s.jobs[selectedID] = selected
	return selected, true, nil
}

type Service struct {
	pdfGenerator PDFGenerator
	jobStore     JobStore
	storageDir   string
	logger       *slog.Logger
	ctx          context.Context
	cancel       context.CancelFunc

	mu        sync.Mutex
	accepting bool
	stopping  bool
	wake      chan struct{}
	jobs      sync.WaitGroup
}

func NewService(pdfGenerator PDFGenerator, storageDir string, workers int, store JobStore) *Service {
	if workers < 1 {
		panic("workers must be at least 1")
	}
	if store == nil {
		panic("job store must not be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{
		pdfGenerator: pdfGenerator,
		jobStore:     store,
		storageDir:   storageDir,
		logger:       slog.Default(),
		ctx:          ctx,
		cancel:       cancel,
		accepting:    true,
		wake:         make(chan struct{}, 1),
	}
	for range workers {
		go service.worker()
	}
	return service
}

func (s *Service) CreateJob(ctx context.Context, lines int) (Job, error) {
	if lines < 1 || lines > MaxLines {
		return Job{}, ErrInvalidLineCount
	}

	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		return Job{}, ErrShuttingDown
	}
	s.mu.Unlock()

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
		created, err := s.jobStore.CreateQueued(ctx, job, jobQueueCapacity)
		if errors.Is(err, ErrQueueFull) {
			return Job{}, ErrQueueFull
		}
		if err != nil {
			return Job{}, fmt.Errorf("store queued job: %w", err)
		}
		if !created {
			continue
		}
		// StopAccepting may run after the accepting check and before this insert.
		// Such a job remains queued for another process to claim at the next start.
		s.signalWorker()
		s.logger.Info("job created",
			slog.String("job_id", job.ID),
			slog.Int("lines", job.Lines),
		)
		return job, nil
	}
}

func (s *Service) GetJob(ctx context.Context, id string) (Job, error) {
	job, err := s.jobStore.Get(ctx, id)
	if err != nil {
		return Job{}, fmt.Errorf("get job %s: %w", id, err)
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
	s.cancel()
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
	if err := s.jobStore.MarkCompleted(context.WithoutCancel(s.ctx), job.ID, path); err != nil {
		s.logger.Error("job completed transition failed",
			slog.String("job_id", job.ID),
			slog.Any("error", err),
		)
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
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
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

		job, claimed, err := s.jobStore.ClaimQueued(s.ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				s.logger.Error("claim queued job", slog.Any("error", err))
			}
		} else if claimed {
			s.run(job)
			s.jobs.Done()
			continue
		}
		s.jobs.Done()

		select {
		case <-ticker.C:
		case <-s.wake:
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) markFailed(id string, processingStarted time.Time) {
	if err := s.jobStore.MarkFailed(context.WithoutCancel(s.ctx), id, "job failed"); err != nil {
		s.logger.Error("job failed transition failed",
			slog.String("job_id", id),
			slog.Any("error", err),
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
