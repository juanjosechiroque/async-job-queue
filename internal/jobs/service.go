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

var (
	ErrInvalidLineCount = errors.New("lines must be between 1 and 250000")
	ErrJobNotFound      = errors.New("job not found")
	ErrShuttingDown     = errors.New("service is shutting down")
)

type PDFGenerator interface {
	Generate(lines int) ([]byte, error)
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

type Service struct {
	pdfGenerator PDFGenerator
	store        *Store
	storageDir   string
	logger       *slog.Logger

	mu        sync.Mutex
	accepting bool
	jobs      sync.WaitGroup
}

func NewService(pdfGenerator PDFGenerator, storageDir string) *Service {
	return &Service{
		pdfGenerator: pdfGenerator,
		store:        NewStore(),
		storageDir:   storageDir,
		logger:       slog.Default(),
		accepting:    true,
	}
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
		if !s.store.Create(job) {
			continue
		}
		s.logger.Info("job created",
			slog.String("job_id", job.ID),
			slog.Int("lines", job.Lines),
		)
		s.jobs.Add(1)
		go s.run(job.ID, lines)
		return job, nil
	}
}

func (s *Service) GetJob(id string) (Job, error) {
	job, ok := s.store.Get(id)
	if !ok {
		return Job{}, ErrJobNotFound
	}
	return job, nil
}

func (s *Service) StopAccepting() {
	s.mu.Lock()
	s.accepting = false
	s.mu.Unlock()
}

func (s *Service) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.jobs.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) run(id string, lines int) {
	defer s.jobs.Done()
	processingStarted := time.Now()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("job panic recovered",
				slog.String("job_id", id),
				slog.Any("panic", recovered),
			)
			s.markFailed(id, processingStarted)
		}
	}()

	if !s.store.MarkProcessing(id) {
		s.logger.Error("job processing transition failed",
			slog.String("job_id", id),
			slog.String("error", "job not found"),
		)
		return
	}
	processingStarted = time.Now()
	s.logger.Info("job state transition",
		slog.String("job_id", id),
		slog.String("status", string(StatusProcessing)),
	)

	document, err := s.pdfGenerator.Generate(lines)
	if err != nil {
		s.logger.Error("job PDF generation failed",
			slog.String("job_id", id),
			slog.Any("error", err),
		)
		s.markFailed(id, processingStarted)
		return
	}
	path, err := s.writePDF(id, document)
	if err != nil {
		s.logger.Error("job PDF storage failed",
			slog.String("job_id", id),
			slog.Any("error", err),
		)
		s.markFailed(id, processingStarted)
		return
	}
	if !s.store.MarkCompleted(id, path) {
		s.logger.Error("job completed transition failed",
			slog.String("job_id", id),
			slog.String("error", "job not found"),
		)
		s.markFailed(id, processingStarted)
		return
	}
	s.logger.Info("job state transition",
		slog.String("job_id", id),
		slog.String("status", string(StatusCompleted)),
		slog.Duration("generation_duration", time.Since(processingStarted)),
		slog.Int("pdf_size_bytes", len(document)),
	)
}

func (s *Service) markFailed(id string, processingStarted time.Time) {
	if !s.store.MarkFailed(id, "job failed") {
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
