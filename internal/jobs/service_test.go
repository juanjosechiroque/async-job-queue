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
	service := NewService(testPDFGenerator{document: []byte("%PDF-test")}, storageDir)

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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

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
	service := NewService(testPDFGenerator{err: errors.New("generator failed")}, t.TempDir())
	created, err := service.CreateJob(1)
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	failed, err := service.GetJob(created.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if failed.Status != StatusFailed || failed.Error != "job failed" {
		t.Fatalf("GetJob() = status %q, error %q; want failed job", failed.Status, failed.Error)
	}
}

func TestServiceRejectsLineCountAboveMaximum(t *testing.T) {
	service := NewService(testPDFGenerator{}, t.TempDir())
	if _, err := service.CreateJob(MaxLines + 1); !errors.Is(err, ErrInvalidLineCount) {
		t.Fatalf("CreateJob() error = %v, want %v", err, ErrInvalidLineCount)
	}
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
