// Package postgres provides a Postgres-backed jobs.JobStore.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juanjosechiroque/async-job-queue/internal/jobs"
)

const operationTimeout = 5 * time.Second

// Store persists job metadata in Postgres. PDFs themselves remain local files.
type Store struct {
	pool *pgxpool.Pool
}

// New opens the pool, verifies connectivity, and applies pending migrations.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// CreateQueued serializes the count-and-insert operation with an advisory
// transaction lock so concurrent processes cannot exceed queue capacity.
func (s *Store) CreateQueued(ctx context.Context, job jobs.Job, capacity int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin create queued transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// This fixed application-scoped key only serializes job creation, not claims.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(584493819274)); err != nil {
		return false, fmt.Errorf("lock queue for create: %w", err)
	}
	var queued int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status = $1", jobs.StatusQueued).Scan(&queued); err != nil {
		return false, fmt.Errorf("count queued jobs: %w", err)
	}
	if queued >= capacity {
		return false, jobs.ErrQueueFull
	}
	command, err := tx.Exec(ctx, `
		INSERT INTO jobs (id, lines, status, file_path, error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.Lines, job.Status, job.FilePath, job.Error, job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("insert queued job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit queued job: %w", err)
	}
	return command.RowsAffected() == 1, nil
}

func (s *Store) Get(parent context.Context, id string) (jobs.Job, error) {
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	job, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT id, lines, status, file_path, error, created_at, updated_at
		FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("get job %s: %w", id, err)
	}
	return job, nil
}

func (s *Store) MarkCompleted(parent context.Context, id, path string) error {
	return s.update(parent, id, `
		UPDATE jobs SET status = $1, file_path = $2, error = '', updated_at = $3
		WHERE id = $4`, jobs.StatusCompleted, path, time.Now().UTC())
}

func (s *Store) MarkFailed(parent context.Context, id, message string) error {
	return s.update(parent, id, `
		UPDATE jobs SET status = $1, error = $2, updated_at = $3
		WHERE id = $4`, jobs.StatusFailed, message, time.Now().UTC())
}

// ClaimQueued uses SKIP LOCKED so one job can only be claimed by one worker,
// even when multiple service instances share this database.
func (s *Store) ClaimQueued(ctx context.Context) (jobs.Job, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	job, err := scanJob(tx.QueryRow(ctx, `
		SELECT id, lines, status, file_path, error, created_at, updated_at
		FROM jobs
		WHERE status = $1
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, jobs.StatusQueued))
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return jobs.Job{}, false, fmt.Errorf("commit empty claim transaction: %w", err)
		}
		return jobs.Job{}, false, nil
	}
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("select queued job: %w", err)
	}

	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status = $1, updated_at = $2 WHERE id = $3`, jobs.StatusProcessing, now, job.ID); err != nil {
		return jobs.Job{}, false, fmt.Errorf("mark claimed job processing: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, false, fmt.Errorf("commit claimed job: %w", err)
	}
	job.Status = jobs.StatusProcessing
	job.UpdatedAt = now
	return job, true, nil
}

func (s *Store) update(parent context.Context, id, query string, values ...any) error {
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	values = append(values, id)
	command, err := s.pool.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("update job %s: %w", id, err)
	}
	if command.RowsAffected() == 0 {
		return jobs.ErrJobNotFound
	}
	return nil
}

func (s *Store) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, operationTimeout)
}

type rowScanner interface {
	Scan(...any) error
}

func scanJob(row rowScanner) (jobs.Job, error) {
	var job jobs.Job
	err := row.Scan(&job.ID, &job.Lines, &job.Status, &job.FilePath, &job.Error, &job.CreatedAt, &job.UpdatedAt)
	return job, err
}
