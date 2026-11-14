package jobs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testPDFGenerator struct {
	document []byte
	err      error
	started  chan<- struct{}
	release  <-chan struct{}
}

type flakyPDFGenerator struct{ calls atomic.Int32 }

func (g *flakyPDFGenerator) Generate(int) ([]byte, error) {
	if g.calls.Add(1) == 1 {
		return nil, errors.New("temporary generation failure")
	}
	return []byte("%PDF-retry"), nil
}

type panicOncePDFGenerator struct{ calls atomic.Int32 }

func (g *panicOncePDFGenerator) Generate(int) ([]byte, error) {
	if g.calls.Add(1) == 1 {
		panic("temporary panic")
	}
	return []byte("%PDF-retry"), nil
}

func TestServiceRetriesRecoveredPanic(t *testing.T) {
	generator := &panicOncePDFGenerator{}
	service := newTestService(generator, t.TempDir(), 1)
	created, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForTerminalJob(t, service, created.ID)
	if completed.Status != StatusCompleted || completed.Attempts != 2 || generator.calls.Load() != 2 {
		t.Fatalf("retried panic = %+v, generator calls = %d", completed, generator.calls.Load())
	}
}

func TestServiceRetriesGeneratorFailure(t *testing.T) {
	generator := &flakyPDFGenerator{}
	service := newTestService(generator, t.TempDir(), 1)
	created, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForTerminalJob(t, service, created.ID)
	if completed.Status != StatusCompleted || completed.Attempts != 2 || generator.calls.Load() != 2 {
		t.Fatalf("retried job = %+v, generator calls = %d", completed, generator.calls.Load())
	}
	if !strings.HasSuffix(completed.FilePath, "-attempt-2.pdf") {
		t.Fatalf("second attempt file = %q", completed.FilePath)
	}
}

func TestServiceRetriesStorageFailure(t *testing.T) {
	storagePath := filepath.Join(t.TempDir(), "storage")
	if err := os.WriteFile(storagePath, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewStore()
	service := NewService(testPDFGenerator{document: []byte("%PDF-retry")}, storagePath, 1, store)
	created, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		job, err := store.Get(context.Background(), created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == StatusQueued && job.Attempts == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	job, err := store.Get(context.Background(), created.ID)
	if err != nil || job.Attempts != 1 || job.Status != StatusQueued {
		t.Fatalf("first write failure = (%+v, %v)", job, err)
	}
	if err := os.Remove(storagePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(storagePath, 0o755); err != nil {
		t.Fatal(err)
	}
	completed := waitForTerminalJob(t, service, created.ID)
	if completed.Status != StatusCompleted || completed.Attempts != 2 {
		t.Fatalf("retried write = %+v", completed)
	}
}

func TestMemoryStoreRejectsStaleClaimAfterRetry(t *testing.T) {
	store := NewStore()
	now := time.Now().UTC()
	if created, err := store.CreateQueued(context.Background(), Job{ID: "stale00001", Status: StatusQueued, CreatedAt: now}, 1); err != nil || !created {
		t.Fatalf("create = (%t, %v)", created, err)
	}
	first, claimed, err := store.ClaimQueued(context.Background())
	if err != nil || !claimed {
		t.Fatalf("first claim = (%+v, %t, %v)", first, claimed, err)
	}
	status, err := store.FailAttempt(context.Background(), first.ID, first.ClaimVersion, "first failed")
	if err != nil || status != StatusQueued {
		t.Fatalf("failure = (%q, %v)", status, err)
	}
	store.mu.Lock()
	job := store.jobs[first.ID]
	job.AvailableAt = time.Now().Add(-time.Second)
	store.jobs[first.ID] = job
	store.mu.Unlock()
	second, claimed, err := store.ClaimQueued(context.Background())
	if err != nil || !claimed || second.ClaimVersion <= first.ClaimVersion {
		t.Fatalf("second claim = (%+v, %t, %v)", second, claimed, err)
	}
	if active, err := store.ExtendLease(context.Background(), first.ID, first.ClaimVersion); err != nil || active {
		t.Fatalf("stale heartbeat = (%t, %v)", active, err)
	}
	if err := store.MarkCompleted(context.Background(), first.ID, first.ClaimVersion, "stale.pdf"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("stale completion = %v", err)
	}
	if err := store.MarkCompleted(context.Background(), second.ID, second.ClaimVersion, "current.pdf"); err != nil {
		t.Fatal(err)
	}
}

func newTestService(generator PDFGenerator, storageDir string, workers int) *Service {
	return NewService(generator, storageDir, workers, NewStore())
}

func (g testPDFGenerator) Generate(int) ([]byte, error) {
	if g.started != nil {
		g.started <- struct{}{}
	}
	if g.release != nil {
		<-g.release
	}
	return g.document, g.err
}

func TestServiceCompletesJobAndStoresPDF(t *testing.T) {
	storageDir := t.TempDir()
	service := newTestService(testPDFGenerator{document: []byte("%PDF-test")}, storageDir, 1)

	created, err := service.CreateJob(context.Background(), MaxLines)
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if created.Status != StatusQueued {
		t.Fatalf("CreateJob() status = %q, want %q", created.Status, StatusQueued)
	}
	if !validID(created.ID) {
		t.Fatalf("CreateJob() ID = %q, want a 10-character Base62 ID", created.ID)
	}

	waitForTerminalJob(t, service, created.ID)

	completed, err := service.GetJob(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if completed.Status != StatusCompleted {
		t.Fatalf("GetJob() status = %q, want %q", completed.Status, StatusCompleted)
	}
	if got, want := completed.FilePath, filepath.Join(storageDir, created.ID+"-attempt-1.pdf"); got != want {
		t.Fatalf("FilePath = %q, want %q", got, want)
	}
	document, err := os.ReadFile(completed.FilePath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if got, want := string(document), "%PDF-test"; got != want {
		t.Fatalf("stored PDF = %q, want %q", got, want)
	}
}

func TestServiceMarksGeneratorFailures(t *testing.T) {
	service := newTestService(testPDFGenerator{err: errors.New("generator failed")}, t.TempDir(), 1)
	created, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	waitForTerminalJob(t, service, created.ID)

	failed, err := service.GetJob(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if failed.Status != StatusDead || failed.Error != "generator failed" {
		t.Fatalf("GetJob() = status %q, error %q; want failed job", failed.Status, failed.Error)
	}
}

func TestServiceRejectsLineCountAboveMaximum(t *testing.T) {
	service := newTestService(testPDFGenerator{}, t.TempDir(), 1)
	if _, err := service.CreateJob(context.Background(), MaxLines+1); !errors.Is(err, ErrInvalidLineCount) {
		t.Fatalf("CreateJob() error = %v, want %v", err, ErrInvalidLineCount)
	}
}

func TestServiceLimitsConcurrentProcessing(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	service := newTestService(testPDFGenerator{
		document: []byte("%PDF-test"),
		started:  started,
		release:  release,
	}, t.TempDir(), 1)

	first, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatalf("CreateJob() first job error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	second, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatalf("CreateJob() second job error = %v", err)
	}
	select {
	case <-started:
		t.Fatal("second job started while the only worker was busy")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second job did not start after the worker was released")
	}
	waitForTerminalJob(t, service, first.ID)
	waitForTerminalJob(t, service, second.ID)
}

func TestServiceRejectsJobsWhenQueueIsFull(t *testing.T) {
	started := make(chan struct{}, jobQueueCapacity+1)
	release := make(chan struct{})
	service := newTestService(testPDFGenerator{
		document: []byte("%PDF-test"),
		started:  started,
		release:  release,
	}, t.TempDir(), 1)

	blocking, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatalf("CreateJob() blocking job error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking job did not start")
	}
	queued := make([]Job, 0, jobQueueCapacity-1)
	for range jobQueueCapacity - 1 {
		job, err := service.CreateJob(context.Background(), 1)
		if err != nil {
			t.Fatalf("CreateJob() queued job error = %v", err)
		}
		queued = append(queued, job)
	}
	if _, err := service.CreateJob(context.Background(), 1); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("CreateJob() error = %v, want %v", err, ErrQueueFull)
	}

	close(release)
	waitForTerminalJob(t, service, blocking.ID)
	for _, job := range queued {
		waitForTerminalJob(t, service, job.ID)
	}
}

func TestServiceConcurrentCreateJobsRespectsQueueCapacity(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service := newTestService(testPDFGenerator{
		document: []byte("%PDF-test"),
		started:  started,
		release:  release,
	}, t.TempDir(), 1)
	defer func() {
		close(release)
		service.StopAccepting()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Wait(ctx); err != nil {
			t.Errorf("Wait() error = %v", err)
		}
	}()

	if _, err := service.CreateJob(context.Background(), 1); err != nil {
		t.Fatalf("CreateJob() blocking job error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking job did not start")
	}

	const callers = jobQueueCapacity * 2
	start := make(chan struct{})
	var creators sync.WaitGroup
	var accepted atomic.Int32
	var rejected atomic.Int32
	var unexpected atomic.Int32
	for range callers {
		creators.Add(1)
		go func() {
			defer creators.Done()
			<-start
			_, err := service.CreateJob(context.Background(), 1)
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrQueueFull):
				rejected.Add(1)
			default:
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	creators.Wait()
	if got := accepted.Load(); got != jobQueueCapacity-1 {
		t.Errorf("accepted concurrent creates = %d, want %d", got, jobQueueCapacity-1)
	}
	if got := rejected.Load(); got != callers-(jobQueueCapacity-1) {
		t.Errorf("queue-full errors = %d, want %d", got, callers-(jobQueueCapacity-1))
	}
	if got := unexpected.Load(); got != 0 {
		t.Errorf("unexpected CreateJob errors = %d", got)
	}
}

func TestServiceCreateConcurrentWithStopAccepting(t *testing.T) {
	service := newTestService(testPDFGenerator{document: []byte("%PDF-test")}, t.TempDir(), 2)
	const callers = 64
	start := make(chan struct{})
	var creators sync.WaitGroup
	var unexpected atomic.Int32
	for range callers {
		creators.Add(1)
		go func() {
			defer creators.Done()
			<-start
			_, err := service.CreateJob(context.Background(), 1)
			if err != nil && !errors.Is(err, ErrShuttingDown) && !errors.Is(err, ErrQueueFull) {
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	service.StopAccepting()
	creators.Wait()
	if got := unexpected.Load(); got != 0 {
		t.Errorf("unexpected concurrent CreateJob errors = %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestServiceLeavesQueuedJobsAfterStopAccepting(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service := newTestService(testPDFGenerator{
		document: []byte("%PDF-test"),
		started:  started,
		release:  release,
	}, t.TempDir(), 1)

	first, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatalf("CreateJob() first job error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	second, err := service.CreateJob(context.Background(), 1)
	if err != nil {
		t.Fatalf("CreateJob() second job error = %v", err)
	}
	service.StopAccepting()
	if _, err := service.CreateJob(context.Background(), 1); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("CreateJob() after StopAccepting() error = %v, want %v", err, ErrShuttingDown)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	firstJob, err := service.GetJob(context.Background(), first.ID)
	if err != nil {
		t.Fatalf("GetJob(%q) error = %v", first.ID, err)
	}
	if firstJob.Status != StatusCompleted {
		t.Fatalf("GetJob(%q) status = %q, want %q", first.ID, firstJob.Status, StatusCompleted)
	}
	secondJob, err := service.GetJob(context.Background(), second.ID)
	if err != nil {
		t.Fatalf("GetJob(%q) error = %v", second.ID, err)
	}
	if secondJob.Status != StatusQueued {
		t.Fatalf("GetJob(%q) status = %q, want %q", second.ID, secondJob.Status, StatusQueued)
	}
}

func waitForTerminalJob(t *testing.T, service *Service, id string) Job {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		job, err := service.GetJob(context.Background(), id)
		if err != nil {
			t.Fatalf("GetJob(%q) error = %v", id, err)
		}
		if job.Status == StatusCompleted || job.Status == StatusDead {
			return job
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %q did not complete or fail before timeout", id)
	return Job{}
}

func TestNewIDUsesBase62(t *testing.T) {
	id, err := newID()
	if err != nil {
		t.Fatalf("newID() error = %v", err)
	}
	if len(id) != 10 || !validID(id) || strings.ContainsAny(id, "-_/") {
		t.Fatalf("newID() = %q, want a 10-character Base62 ID", id)
	}
}

func TestMemoryStoreErrorsContextAndClaimOrder(t *testing.T) {
	store := NewStore()
	base := time.Now().UTC()
	for _, job := range []Job{
		{ID: "later00001", Status: StatusQueued, CreatedAt: base.Add(time.Second)},
		{ID: "older00002", Status: StatusQueued, CreatedAt: base},
		{ID: "older00001", Status: StatusQueued, CreatedAt: base},
	} {
		if created, err := store.CreateQueued(context.Background(), job, jobQueueCapacity); err != nil || !created {
			t.Fatalf("CreateQueued(%q) = (%t, %v), want (true, nil)", job.ID, created, err)
		}
	}

	for _, wantID := range []string{"older00001", "older00002", "later00001"} {
		job, claimed, err := store.ClaimQueued(context.Background())
		if err != nil || !claimed || job.ID != wantID {
			t.Fatalf("ClaimQueued() = (%q, %t, %v), want (%q, true, nil)", job.ID, claimed, err, wantID)
		}
	}
	if _, err := store.Get(context.Background(), "missing000"); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("Get(missing) error = %v, want ErrJobNotFound", err)
	}
	if err := store.MarkCompleted(context.Background(), "missing000", 1, "missing.pdf"); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("MarkCompleted(missing) error = %v, want ErrJobNotFound", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(ctx, "older00001"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get(canceled) error = %v, want context.Canceled", err)
	}
}
