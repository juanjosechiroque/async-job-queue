package jobs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testPDFGenerator struct {
	document []byte
	err      error
	started  chan<- struct{}
	release  <-chan struct{}
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
	service := NewService(testPDFGenerator{document: []byte("%PDF-test")}, storageDir, 1)

	created, err := service.CreateJob(MaxLines)
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

	completed, err := service.GetJob(created.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if completed.Status != StatusCompleted {
		t.Fatalf("GetJob() status = %q, want %q", completed.Status, StatusCompleted)
	}
	if got, want := completed.FilePath, filepath.Join(storageDir, created.ID+".pdf"); got != want {
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
	service := NewService(testPDFGenerator{err: errors.New("generator failed")}, t.TempDir(), 1)
	created, err := service.CreateJob(1)
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	waitForTerminalJob(t, service, created.ID)

	failed, err := service.GetJob(created.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if failed.Status != StatusFailed || failed.Error != "job failed" {
		t.Fatalf("GetJob() = status %q, error %q; want failed job", failed.Status, failed.Error)
	}
}

func TestServiceRejectsLineCountAboveMaximum(t *testing.T) {
	service := NewService(testPDFGenerator{}, t.TempDir(), 1)
	if _, err := service.CreateJob(MaxLines + 1); !errors.Is(err, ErrInvalidLineCount) {
		t.Fatalf("CreateJob() error = %v, want %v", err, ErrInvalidLineCount)
	}
}

func TestServiceLimitsConcurrentProcessing(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	service := NewService(testPDFGenerator{
		document: []byte("%PDF-test"),
		started:  started,
		release:  release,
	}, t.TempDir(), 1)

	first, err := service.CreateJob(1)
	if err != nil {
		t.Fatalf("CreateJob() first job error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	second, err := service.CreateJob(1)
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
	service := NewService(testPDFGenerator{
		document: []byte("%PDF-test"),
		started:  started,
		release:  release,
	}, t.TempDir(), 1)

	blocking, err := service.CreateJob(1)
	if err != nil {
		t.Fatalf("CreateJob() blocking job error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking job did not start")
	}
	queued := make([]Job, 0, jobQueueCapacity)
	for range jobQueueCapacity {
		job, err := service.CreateJob(1)
		if err != nil {
			t.Fatalf("CreateJob() queued job error = %v", err)
		}
		queued = append(queued, job)
	}
	if _, err := service.CreateJob(1); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("CreateJob() error = %v, want %v", err, ErrQueueFull)
	}

	close(release)
	waitForTerminalJob(t, service, blocking.ID)
	for _, job := range queued {
		waitForTerminalJob(t, service, job.ID)
	}
}

func TestServiceLeavesQueuedJobsAfterStopAccepting(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service := NewService(testPDFGenerator{
		document: []byte("%PDF-test"),
		started:  started,
		release:  release,
	}, t.TempDir(), 1)

	first, err := service.CreateJob(1)
	if err != nil {
		t.Fatalf("CreateJob() first job error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	second, err := service.CreateJob(1)
	if err != nil {
		t.Fatalf("CreateJob() second job error = %v", err)
	}
	service.StopAccepting()
	if _, err := service.CreateJob(1); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("CreateJob() after StopAccepting() error = %v, want %v", err, ErrShuttingDown)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	firstJob, err := service.GetJob(first.ID)
	if err != nil {
		t.Fatalf("GetJob(%q) error = %v", first.ID, err)
	}
	if firstJob.Status != StatusCompleted {
		t.Fatalf("GetJob(%q) status = %q, want %q", first.ID, firstJob.Status, StatusCompleted)
	}
	secondJob, err := service.GetJob(second.ID)
	if err != nil {
		t.Fatalf("GetJob(%q) error = %v", second.ID, err)
	}
	if secondJob.Status != StatusQueued {
		t.Fatalf("GetJob(%q) status = %q, want %q", second.ID, secondJob.Status, StatusQueued)
	}
}

func waitForTerminalJob(t *testing.T, service *Service, id string) Job {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		job, err := service.GetJob(id)
		if err != nil {
			t.Fatalf("GetJob(%q) error = %v", id, err)
		}
		if job.Status == StatusCompleted || job.Status == StatusFailed {
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
