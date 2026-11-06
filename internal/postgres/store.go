// Package postgres provides a Postgres-backed jobs.JobStore.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juanjosechiroque/async-job-queue/internal/jobs"
)

//go:embed schema.sql
var schema string

const operationTimeout = 5 * time.Second

// Store persists job metadata in Postgres. PDFs themselves remain local files.
type Store struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// New opens the pool, verifies connectivity, and applies the embedded schema.
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
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, logger: slog.Default()}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Create(job jobs.Job) bool {
	ctx, cancel := s.operationContext()
	defer cancel()
	command, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (id, lines, status, file_path, error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.Lines, job.Status, job.FilePath, job.Error, job.CreatedAt, job.UpdatedAt)
	if err != nil {
		s.logError("create job", err)
		return false
	}
	return command.RowsAffected() == 1
}

// CreateQueued serializes the count-and-insert operation with an advisory
// transaction lock so concurrent processes cannot exceed queue capacity.
func (s *Store) CreateQueued(ctx context.Context, job jobs.Job, capacity int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	// This fixed application-scoped key only serializes job creation, not claims.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(584493819274)); err != nil {
		return false, err
	}
	var queued int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status = $1", jobs.StatusQueued).Scan(&queued); err != nil {
		return false, err
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
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return command.RowsAffected() == 1, nil
}

func (s *Store) Delete(id string) {
	ctx, cancel := s.operationContext()
	defer cancel()
	if _, err := s.pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", id); err != nil {
		s.logError("delete job", err)
	}
}

func (s *Store) Get(id string) (jobs.Job, bool) {
	ctx, cancel := s.operationContext()
	defer cancel()
	job, err := scanJob(s.pool.QueryRow(ctx, `
		SELECT id, lines, status, file_path, error, created_at, updated_at
		FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, false
	}
	if err != nil {
		s.logError("get job", err)
		return jobs.Job{}, false
	}
	return job, true
}

func (s *Store) MarkProcessing(id string) bool {
	return s.update(id, jobs.StatusProcessing, "", "", false, false)
}

func (s *Store) MarkCompleted(id, path string) bool {
	return s.update(id, jobs.StatusCompleted, path, "", true, true)
}

func (s *Store) MarkFailed(id, message string) bool {
	return s.update(id, jobs.StatusFailed, "", message, false, true)
}

// ClaimQueued uses SKIP LOCKED so one job can only be claimed by one worker,
// even when multiple service instances share this database.
func (s *Store) ClaimQueued(ctx context.Context) (jobs.Job, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return jobs.Job{}, false, err
	}
	defer tx.Rollback(ctx)

	job, err := scanJob(tx.QueryRow(ctx, `
		SELECT id, lines, status, file_path, error, created_at, updated_at
		FROM jobs
		WHERE status = $1
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, jobs.StatusQueued))
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return jobs.Job{}, false, err
		}
		return jobs.Job{}, false, nil
	}
	if err != nil {
		return jobs.Job{}, false, err
	}

	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status = $1, updated_at = $2 WHERE id = $3`, jobs.StatusProcessing, now, job.ID); err != nil {
		return jobs.Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, false, err
	}
	job.Status = jobs.StatusProcessing
	job.UpdatedAt = now
	return job, true, nil
}

func (s *Store) update(id string, status jobs.JobStatus, path, message string, setPath, setError bool) bool {
	ctx, cancel := s.operationContext()
	defer cancel()
	now := time.Now().UTC()
	query := `UPDATE jobs SET status = $1, updated_at = $2`
	arguments := []any{status, now}
	if setPath {
		query += `, file_path = $3`
		arguments = append(arguments, path)
	}
	if setError {
		query += fmt.Sprintf(", error = $%d", len(arguments)+1)
		arguments = append(arguments, message)
	}
	query += fmt.Sprintf(" WHERE id = $%d", len(arguments)+1)
	arguments = append(arguments, id)
	command, err := s.pool.Exec(ctx, query, arguments...)
	if err != nil {
		s.logError("update job", err)
		return false
	}
	return command.RowsAffected() == 1
}

func (s *Store) operationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), operationTimeout)
}

func (s *Store) logError(operation string, err error) {
	s.logger.Error("Postgres job store operation failed", slog.String("operation", operation), slog.Any("error", err))
}

type rowScanner interface {
	Scan(...any) error
}

func scanJob(row rowScanner) (jobs.Job, error) {
	var job jobs.Job
	err := row.Scan(&job.ID, &job.Lines, &job.Status, &job.FilePath, &job.Error, &job.CreatedAt, &job.UpdatedAt)
	return job, err
}
