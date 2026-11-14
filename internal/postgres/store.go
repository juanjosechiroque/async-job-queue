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
	var active int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status IN ('queued', 'processing')").Scan(&active); err != nil {
		return false, fmt.Errorf("count active jobs: %w", err)
	}
	if active >= capacity {
		return false, jobs.ErrQueueFull
	}
	var availableAt any
	if !job.AvailableAt.IsZero() {
		availableAt = job.AvailableAt
	}
	command, err := tx.Exec(ctx, `
		INSERT INTO jobs (id, lines, status, file_path, error, created_at, updated_at, available_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8::timestamptz, now()))
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.Lines, job.Status, job.FilePath, job.Error, job.CreatedAt, job.UpdatedAt, availableAt)
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
		SELECT id, lines, status, file_path, error, created_at, updated_at, available_at, lease_expires_at, attempts, claim_version
		FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("get job %s: %w", id, err)
	}
	return job, nil
}

func (s *Store) MarkCompleted(parent context.Context, id string, version int64, path string) error {
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	command, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'completed', file_path = $3, error = '', updated_at = now(), lease_expires_at = NULL
		WHERE id = $1 AND status = 'processing' AND claim_version = $2 AND lease_expires_at > now()`, id, version, path)
	if err != nil {
		return fmt.Errorf("complete job %s: %w", id, err)
	}
	if command.RowsAffected() == 0 {
		return jobs.ErrJobNotFound
	}
	return nil
}

func (s *Store) FailAttempt(parent context.Context, id string, version int64, message string) (jobs.JobStatus, error) {
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	var status jobs.JobStatus
	err := s.pool.QueryRow(ctx, `UPDATE jobs SET
		status = CASE WHEN attempts < $4 THEN 'queued' ELSE 'dead' END,
		error = $3, updated_at = now(), lease_expires_at = NULL,
		available_at = CASE WHEN attempts < $4 THEN now() + (CASE WHEN attempts = 1 THEN $5::int ELSE $6::int END) * interval '1 second' ELSE now() END
		WHERE id = $1 AND status = 'processing' AND claim_version = $2 AND lease_expires_at > now()
		RETURNING status`, id, version, message, jobs.MaxAttempts, int(jobs.RetryDelay(1)/time.Second), int(jobs.RetryDelay(2)/time.Second)).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", jobs.ErrJobNotFound
	}
	if err != nil {
		return "", fmt.Errorf("fail job %s attempt: %w", id, err)
	}
	return status, nil
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
		SELECT id, lines, status, file_path, error, created_at, updated_at, available_at, lease_expires_at, attempts, claim_version
		FROM jobs
		WHERE status = $1 AND available_at <= now()
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

	if err := tx.QueryRow(ctx, `UPDATE jobs SET status = $1, updated_at = now(), lease_expires_at = now() + $3 * interval '1 second',
		attempts = attempts + 1, claim_version = claim_version + 1, error = ''
		WHERE id = $2 RETURNING updated_at, lease_expires_at, attempts, claim_version`, jobs.StatusProcessing, job.ID, int(jobs.LeaseDuration/time.Second)).Scan(&job.UpdatedAt, &job.LeaseExpiresAt, &job.Attempts, &job.ClaimVersion); err != nil {
		return jobs.Job{}, false, fmt.Errorf("mark claimed job processing: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, false, fmt.Errorf("commit claimed job: %w", err)
	}
	job.Status = jobs.StatusProcessing
	return job, true, nil
}

func (s *Store) ExtendLease(parent context.Context, id string, version int64) (bool, error) {
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	command, err := s.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() + $3 * interval '1 second', updated_at = now()
		WHERE id = $1 AND status = 'processing' AND claim_version = $2 AND lease_expires_at > now()`, id, version, int(jobs.LeaseDuration/time.Second))
	if err != nil {
		return false, fmt.Errorf("extend job %s lease: %w", id, err)
	}
	return command.RowsAffected() == 1, nil
}

func (s *Store) ReapExpired(parent context.Context) ([]jobs.JobTransition, error) {
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	rows, err := s.pool.Query(ctx, `UPDATE jobs SET
		status = CASE WHEN attempts < $1 THEN 'queued' ELSE 'dead' END,
		error = 'lease expired', lease_expires_at = NULL, updated_at = now(),
		available_at = CASE WHEN attempts < $1 THEN now() + (CASE WHEN attempts = 1 THEN $2::int ELSE $3::int END) * interval '1 second' ELSE now() END
		WHERE id IN (SELECT id FROM jobs WHERE status = 'processing'
			AND lease_expires_at <= now() FOR UPDATE SKIP LOCKED)
		RETURNING id, status`, jobs.MaxAttempts, int(jobs.RetryDelay(1)/time.Second), int(jobs.RetryDelay(2)/time.Second))
	if err != nil {
		return nil, fmt.Errorf("reap expired jobs: %w", err)
	}
	defer rows.Close()
	var transitions []jobs.JobTransition
	for rows.Next() {
		var transition jobs.JobTransition
		if err := rows.Scan(&transition.ID, &transition.Status); err != nil {
			return nil, fmt.Errorf("scan reaped job: %w", err)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read reaped jobs: %w", err)
	}
	return transitions, nil
}

func (s *Store) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, operationTimeout)
}

type rowScanner interface {
	Scan(...any) error
}

func scanJob(row rowScanner) (jobs.Job, error) {
	var job jobs.Job
	err := row.Scan(&job.ID, &job.Lines, &job.Status, &job.FilePath, &job.Error, &job.CreatedAt, &job.UpdatedAt, &job.AvailableAt, &job.LeaseExpiresAt, &job.Attempts, &job.ClaimVersion)
	return job, err
}
