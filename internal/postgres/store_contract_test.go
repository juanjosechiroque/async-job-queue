package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/juanjosechiroque/async-job-queue/internal/jobs"
)

func TestJobStoreContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runJobStoreContract(t, jobs.NewStore()) })
	t.Run("postgres", func(t *testing.T) {
		store := newIntegrationStore(t, isolatedDatabaseURL(t))
		runJobStoreContract(t, store)
	})
}

func runJobStoreContract(t *testing.T, store jobs.JobStore) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.Get(ctx, "missing000"); !errors.Is(err, jobs.ErrJobNotFound) {
		t.Fatalf("Get missing = %v", err)
	}
	now := time.Now().UTC()
	for _, job := range []jobs.Job{
		{ID: "contract01", Lines: 1, Status: jobs.StatusQueued, CreatedAt: now, UpdatedAt: now},
		{ID: "contract02", Lines: 1, Status: jobs.StatusQueued, CreatedAt: now.Add(time.Second), UpdatedAt: now},
	} {
		created, err := store.CreateQueued(ctx, job, 2)
		if err != nil || !created {
			t.Fatalf("CreateQueued(%s) = (%t, %v)", job.ID, created, err)
		}
	}
	if _, err := store.CreateQueued(ctx, jobs.Job{ID: "contract03", Status: jobs.StatusQueued}, 2); !errors.Is(err, jobs.ErrQueueFull) {
		t.Fatalf("full queue error = %v", err)
	}
	first, claimed, err := store.ClaimQueued(ctx)
	if err != nil || !claimed || first.ID != "contract01" || first.Status != jobs.StatusProcessing || first.LeaseExpiresAt == nil {
		t.Fatalf("first claim = (%+v, %t, %v)", first, claimed, err)
	}
	if remaining := time.Until(*first.LeaseExpiresAt); remaining < 25*time.Second || remaining > 35*time.Second {
		t.Fatalf("initial lease remaining = %v", remaining)
	}
	active, err := store.ExtendLease(ctx, first.ID, first.ClaimVersion)
	if err != nil || !active {
		t.Fatalf("ExtendLease processing = (%t, %v)", active, err)
	}
	if transitions, err := store.ReapExpired(ctx); err != nil || len(transitions) != 0 {
		t.Fatalf("ReapExpired live jobs = (%v, %v)", transitions, err)
	}
	if err := store.MarkCompleted(ctx, first.ID, first.ClaimVersion, "file.pdf"); err != nil {
		t.Fatal(err)
	}
	completed, err := store.Get(ctx, first.ID)
	if err != nil || completed.Status != jobs.StatusCompleted || completed.FilePath != "file.pdf" || completed.LeaseExpiresAt != nil {
		t.Fatalf("completed job = (%+v, %v)", completed, err)
	}
	if active, err := store.ExtendLease(ctx, first.ID, first.ClaimVersion); err != nil || active {
		t.Fatalf("ExtendLease completed = (%t, %v)", active, err)
	}
	second, claimed, err := store.ClaimQueued(ctx)
	if err != nil || !claimed || second.ID != "contract02" {
		t.Fatalf("second claim = (%+v, %t, %v)", second, claimed, err)
	}
	for attempt := 1; attempt <= jobs.MaxAttempts; attempt++ {
		status, err := store.FailAttempt(ctx, second.ID, second.ClaimVersion, "internal detail")
		if err != nil {
			t.Fatal(err)
		}
		if attempt < jobs.MaxAttempts {
			if status != jobs.StatusQueued {
				t.Fatalf("retry status = %q", status)
			}
			if _, claimed, err := store.ClaimQueued(ctx); err != nil || claimed {
				t.Fatalf("claimed retry before delay = (%t, %v)", claimed, err)
			}
			time.Sleep(jobs.RetryDelay(attempt) + 20*time.Millisecond)
			var claimed bool
			second, claimed, err = store.ClaimQueued(ctx)
			if err != nil || !claimed || second.ID != "contract02" {
				t.Fatalf("retry claim = (%+v, %t, %v)", second, claimed, err)
			}
		} else if status != jobs.StatusDead {
			t.Fatalf("final status = %q", status)
		}
	}
	dead, err := store.Get(ctx, second.ID)
	if err != nil || dead.Status != jobs.StatusDead || dead.Error != "internal detail" || dead.LeaseExpiresAt != nil {
		t.Fatalf("dead job = (%+v, %v)", dead, err)
	}
	if _, claimed, err := store.ClaimQueued(ctx); err != nil || claimed {
		t.Fatalf("empty claim = (%t, %v)", claimed, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Get(canceled, first.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get canceled = %v", err)
	}
}
