package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/juanjosechiroque/async-job-queue/internal/jobs"
)

func TestStoreClaimQueuedDoesNotDuplicateClaims(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping Postgres integration test")
	}

	store, err := New(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(store.Close)

	var existingQueued int
	if err := store.pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE status = $1", jobs.StatusQueued).Scan(&existingQueued); err != nil {
		t.Fatalf("count queued jobs: %v", err)
	}
	if existingQueued != 0 {
		t.Skip("Postgres integration test requires a database without pre-existing queued jobs")
	}

	const jobCount = 32
	ids := make(map[string]struct{}, jobCount)
	now := time.Now().UTC()
	for index := range jobCount {
		id := fmt.Sprintf("itclaim%03d", index)
		ids[id] = struct{}{}
		store.Delete(id)
		if !store.Create(jobs.Job{
			ID:        id,
			Lines:     1,
			Status:    jobs.StatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		}) {
			t.Fatalf("Create(%q) = false, want true", id)
		}
	}
	t.Cleanup(func() {
		for id := range ids {
			store.Delete(id)
		}
	})

	const claimants = 8
	start := make(chan struct{})
	claimed := make(chan string, jobCount)
	errs := make(chan error, claimants)
	var workers sync.WaitGroup
	for range claimants {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for {
				job, found, err := store.ClaimQueued(context.Background())
				if err != nil {
					errs <- err
					return
				}
				if !found {
					return
				}
				claimed <- job.ID
			}
		}()
	}
	close(start)
	workers.Wait()
	close(claimed)
	close(errs)

	for err := range errs {
		t.Errorf("ClaimQueued() error = %v", err)
	}

	seen := make(map[string]bool, jobCount)
	for id := range claimed {
		if _, expected := ids[id]; !expected {
			t.Errorf("ClaimQueued() claimed unexpected job %q", id)
			continue
		}
		if seen[id] {
			t.Errorf("job %q was claimed more than once", id)
		}
		seen[id] = true
	}
	if got := len(seen); got != jobCount {
		t.Errorf("unique claimed jobs = %d, want %d", got, jobCount)
	}
}

func TestServiceShutdownWaitsOnlyForWorkClaimedByItsWorkers(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ensureNoQueuedJobs(t, databaseURL)

	storeA := newIntegrationStore(t, databaseURL)
	storeB := newIntegrationStore(t, databaseURL)
	var ids []string
	t.Cleanup(func() {
		for _, id := range ids {
			storeA.Delete(id)
		}
	})

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	serviceA := jobs.NewServiceWithStore(blockingGenerator{started: started, release: release}, t.TempDir(), 1, storeA)

	first, err := serviceA.CreateJob(1)
	if err != nil {
		t.Fatalf("A.CreateJob() first job error = %v", err)
	}
	ids = append(ids, first.ID)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("A did not start its first job")
	}
	second, err := serviceA.CreateJob(1)
	if err != nil {
		t.Fatalf("A.CreateJob() second job error = %v", err)
	}
	third, err := serviceA.CreateJob(1)
	if err != nil {
		t.Fatalf("A.CreateJob() third job error = %v", err)
	}
	ids = append(ids, second.ID, third.ID)

	serviceB := jobs.NewServiceWithStore(immediateGenerator{}, t.TempDir(), 1, storeB)
	waitForCompletedJob(t, serviceB, second.ID)
	waitForCompletedJob(t, serviceB, third.ID)
	stopAndWait(t, serviceB, time.Second)

	close(release)
	serviceA.StopAccepting()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := serviceA.Wait(ctx); err != nil {
		t.Fatalf("A.Wait() error = %v, want nil", err)
	}
}

func TestNewServiceClaimsJobQueuedByStoppedInstance(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ensureNoQueuedJobs(t, databaseURL)

	storeA := newIntegrationStore(t, databaseURL)
	storeB := newIntegrationStore(t, databaseURL)
	var ids []string
	t.Cleanup(func() {
		for _, id := range ids {
			storeA.Delete(id)
		}
	})

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	serviceA := jobs.NewServiceWithStore(blockingGenerator{started: started, release: release}, t.TempDir(), 1, storeA)
	first, err := serviceA.CreateJob(1)
	if err != nil {
		t.Fatalf("A.CreateJob() first job error = %v", err)
	}
	ids = append(ids, first.ID)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("A did not start its first job")
	}
	queued, err := serviceA.CreateJob(1)
	if err != nil {
		t.Fatalf("A.CreateJob() queued job error = %v", err)
	}
	ids = append(ids, queued.ID)

	serviceA.StopAccepting()
	close(release)
	stopAndWait(t, serviceA, time.Second)

	leftBehind, err := serviceA.GetJob(queued.ID)
	if err != nil {
		t.Fatalf("A.GetJob(%q) error = %v", queued.ID, err)
	}
	if leftBehind.Status != jobs.StatusQueued {
		t.Fatalf("A.GetJob(%q) status = %q, want %q", queued.ID, leftBehind.Status, jobs.StatusQueued)
	}

	serviceB := jobs.NewServiceWithStore(immediateGenerator{}, t.TempDir(), 1, storeB)
	waitForCompletedJob(t, serviceB, queued.ID)
	stopAndWait(t, serviceB, time.Second)
}

type blockingGenerator struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (g blockingGenerator) Generate(int) ([]byte, error) {
	g.started <- struct{}{}
	<-g.release
	return []byte("%PDF-test"), nil
}

type immediateGenerator struct{}

func (immediateGenerator) Generate(int) ([]byte, error) {
	return []byte("%PDF-test"), nil
}

func integrationDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping Postgres integration test")
	}
	return databaseURL
}

func newIntegrationStore(t *testing.T, databaseURL string) *Store {
	t.Helper()
	store, err := New(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// Register Close before row cleanup. t.Cleanup runs in LIFO order, so the
	// later cleanup can delete rows while the pool remains open.
	t.Cleanup(store.Close)
	return store
}

func ensureNoQueuedJobs(t *testing.T, databaseURL string) {
	t.Helper()
	store := newIntegrationStore(t, databaseURL)
	var queued int
	if err := store.pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE status = $1", jobs.StatusQueued).Scan(&queued); err != nil {
		t.Fatalf("count queued jobs: %v", err)
	}
	if queued != 0 {
		t.Skip("Postgres integration test requires a database without pre-existing queued jobs")
	}
}

func waitForTerminalJob(t *testing.T, service *jobs.Service, id string) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := service.GetJob(id)
		if err != nil {
			t.Fatalf("GetJob(%q) error = %v", id, err)
		}
		if job.Status == jobs.StatusCompleted || job.Status == jobs.StatusFailed {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %q did not complete or fail before timeout", id)
	return jobs.Job{}
}

func waitForCompletedJob(t *testing.T, service *jobs.Service, id string) {
	t.Helper()
	job := waitForTerminalJob(t, service, id)
	if job.Status != jobs.StatusCompleted {
		t.Fatalf("GetJob(%q) status = %q, want %q", id, job.Status, jobs.StatusCompleted)
	}
}

func stopAndWait(t *testing.T, service *jobs.Service, timeout time.Duration) {
	t.Helper()
	service.StopAccepting()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}
