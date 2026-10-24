package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type erroringJobService struct {
	err error
}

func (s erroringJobService) CreateJob(int) (Job, error) { return Job{}, s.err }
func (s erroringJobService) GetJob(string) (Job, error) { return Job{}, s.err }

func TestHandlerCreateJob(t *testing.T) {
	service := NewService(testPDFGenerator{document: []byte("%PDF-test")}, t.TempDir(), 1)
	handler := NewHandler(service)

	recorder := serveRequest(handler, http.MethodPost, "/jobs", `{"lines":1}`)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("POST /jobs status = %d, want %d", recorder.Code, http.StatusAccepted)
	}

	response := decodeJSON[createJobResponse](t, recorder)
	if !validID(response.JobID) {
		t.Fatalf("POST /jobs jobId = %q, want a 10-character Base62 ID", response.JobID)
	}
	if response.Status != StatusQueued {
		t.Fatalf("POST /jobs status = %q, want %q", response.Status, StatusQueued)
	}
	if want := downloadURL(response.JobID); response.DownloadURL != want {
		t.Fatalf("POST /jobs downloadUrl = %q, want %q", response.DownloadURL, want)
	}

	waitForJobs(t, service)
}

func TestHandlerCreateJobRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		message string
	}{
		{
			name:    "invalid JSON",
			body:    `{"lines":`,
			message: "request body must be valid JSON",
		},
		{
			name:    "unknown JSON field",
			body:    `{"lines":1,"extra":true}`,
			message: "request body must be valid JSON",
		},
		{
			name:    "zero lines",
			body:    `{"lines":0}`,
			message: ErrInvalidLineCount.Error(),
		},
		{
			name:    "line count above maximum",
			body:    `{"lines":250001}`,
			message: ErrInvalidLineCount.Error(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := NewHandler(NewService(testPDFGenerator{}, t.TempDir(), 1))
			recorder := serveRequest(handler, http.MethodPost, "/jobs", test.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("POST /jobs status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
			assertJSONError(t, recorder, test.message)
		})
	}
}

func TestHandlerCreateJobRejectsFullQueue(t *testing.T) {
	handler := NewHandler(erroringJobService{err: ErrQueueFull})
	recorder := serveRequest(handler, http.MethodPost, "/jobs", `{"lines":1}`)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST /jobs status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	assertJSONError(t, recorder, "job queue is full")
}

func TestHandlerRejectsWrongMethodOnJobs(t *testing.T) {
	handler := NewHandler(NewService(testPDFGenerator{}, t.TempDir(), 1))
	recorder := serveRequest(handler, http.MethodGet, "/jobs", "")

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /jobs status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
	assertJSONError(t, recorder, "method not allowed")
}

func TestHandlerRejectsWrongMethodOnJobRoutes(t *testing.T) {
	handler := NewHandler(NewService(testPDFGenerator{}, t.TempDir(), 1))

	tests := []struct {
		name string
		path string
	}{
		{name: "read job", path: "/jobs/abcdefghij"},
		{name: "download file", path: "/jobs/abcdefghij/file"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := serveRequest(handler, http.MethodPost, test.path, "")
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Fatalf("POST %s status = %d, want %d", test.path, recorder.Code, http.StatusMethodNotAllowed)
			}
			assertJSONError(t, recorder, "method not allowed")
		})
	}
}

func TestHandlerReadJob(t *testing.T) {
	t.Run("unknown job", func(t *testing.T) {
		handler := NewHandler(NewService(testPDFGenerator{}, t.TempDir(), 1))
		recorder := serveRequest(handler, http.MethodGet, "/jobs/abcdefghij", "")

		if recorder.Code != http.StatusNotFound {
			t.Fatalf("GET /jobs/{id} status = %d, want %d", recorder.Code, http.StatusNotFound)
		}
		assertJSONError(t, recorder, "job not found")
	})

	t.Run("completed job", func(t *testing.T) {
		service := NewService(testPDFGenerator{document: []byte("%PDF-test")}, t.TempDir(), 1)
		handler := NewHandler(service)
		created := createJob(t, handler)
		waitForJobs(t, service)

		recorder := serveRequest(handler, http.MethodGet, "/jobs/"+created.JobID, "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET /jobs/{id} status = %d, want %d", recorder.Code, http.StatusOK)
		}
		response := decodeJSON[map[string]json.RawMessage](t, recorder)
		if got := decodeRawString(t, response["downloadUrl"]); got != downloadURL(created.JobID) {
			t.Fatalf("GET /jobs/{id} downloadUrl = %q, want %q", got, downloadURL(created.JobID))
		}
		if _, exists := response["error"]; exists {
			t.Fatalf("GET /jobs/{id} response unexpectedly contains error: %s", recorder.Body.String())
		}
	})

	t.Run("unexpected service error", func(t *testing.T) {
		handler := NewHandler(erroringJobService{err: errors.New("store unavailable")})
		recorder := serveRequest(handler, http.MethodGet, "/jobs/abcdefghij", "")

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("GET /jobs/{id} status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
		assertJSONError(t, recorder, "could not read job")
	})

	t.Run("failed job", func(t *testing.T) {
		service := NewService(testPDFGenerator{err: errors.New("generator failed")}, t.TempDir(), 1)
		handler := NewHandler(service)
		created := createJob(t, handler)
		waitForJobs(t, service)

		recorder := serveRequest(handler, http.MethodGet, "/jobs/"+created.JobID, "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET /jobs/{id} status = %d, want %d", recorder.Code, http.StatusOK)
		}
		response := decodeJSON[readJobResponse](t, recorder)
		if response.Status != StatusFailed || response.Error != "job failed" {
			t.Fatalf("GET /jobs/{id} = status %q, error %q; want failed job", response.Status, response.Error)
		}
		if response.DownloadURL != "" {
			t.Fatalf("GET /jobs/{id} downloadUrl = %q, want empty", response.DownloadURL)
		}
	})
}

func TestHandlerDownloadFile(t *testing.T) {
	t.Run("queued job", func(t *testing.T) {
		service := NewService(testPDFGenerator{}, t.TempDir(), 1)
		const id = "QueuedID01"
		if created := service.store.Create(Job{ID: id, Status: StatusQueued}); !created {
			t.Fatal("could not create queued job")
		}

		recorder := serveRequest(NewHandler(service), http.MethodGet, "/jobs/"+id+"/file", "")
		if recorder.Code != http.StatusConflict {
			t.Fatalf("GET /jobs/{id}/file status = %d, want %d", recorder.Code, http.StatusConflict)
		}
		assertJSONError(t, recorder, "job is queued")
	})

	t.Run("processing job", func(t *testing.T) {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		service := NewService(testPDFGenerator{
			document: []byte("%PDF-test"),
			started:  started,
			release:  release,
		}, t.TempDir(), 1)
		handler := NewHandler(service)
		created := createJob(t, handler)
		t.Cleanup(func() {
			close(release)
			waitForJobs(t, service)
		})

		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("generator did not start")
		}

		recorder := serveRequest(handler, http.MethodGet, "/jobs/"+created.JobID+"/file", "")
		if recorder.Code != http.StatusConflict {
			t.Fatalf("GET /jobs/{id}/file status = %d, want %d", recorder.Code, http.StatusConflict)
		}
		assertJSONError(t, recorder, "job is still being processed")
	})

	t.Run("completed job", func(t *testing.T) {
		service := NewService(testPDFGenerator{document: []byte("%PDF-test")}, t.TempDir(), 1)
		handler := NewHandler(service)
		created := createJob(t, handler)
		waitForJobs(t, service)

		recorder := serveRequest(handler, http.MethodGet, "/jobs/"+created.JobID+"/file", "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET /jobs/{id}/file status = %d, want %d", recorder.Code, http.StatusOK)
		}
		if got := recorder.Header().Get("Content-Type"); got != "application/pdf" {
			t.Fatalf("GET /jobs/{id}/file Content-Type = %q, want %q", got, "application/pdf")
		}
		if got := recorder.Header().Get("Content-Disposition"); got == "" {
			t.Fatal("GET /jobs/{id}/file missing Content-Disposition header")
		}
		if !bytes.HasPrefix(recorder.Body.Bytes(), []byte("%PDF")) {
			t.Fatalf("GET /jobs/{id}/file body = %q, want prefix %%PDF", recorder.Body.Bytes())
		}
	})

	t.Run("completed job with missing file on disk", func(t *testing.T) {
		service := NewService(testPDFGenerator{document: []byte("%PDF-test")}, t.TempDir(), 1)
		handler := NewHandler(service)
		created := createJob(t, handler)
		waitForJobs(t, service)

		job, err := service.GetJob(created.JobID)
		if err != nil {
			t.Fatalf("GetJob() error = %v", err)
		}
		if err := os.Remove(job.FilePath); err != nil {
			t.Fatalf("os.Remove() error = %v", err)
		}

		recorder := serveRequest(handler, http.MethodGet, "/jobs/"+created.JobID+"/file", "")
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("GET /jobs/{id}/file status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
		assertJSONError(t, recorder, "could not retrieve PDF")
	})

	t.Run("unexpected service error", func(t *testing.T) {
		handler := NewHandler(erroringJobService{err: errors.New("store unavailable")})
		recorder := serveRequest(handler, http.MethodGet, "/jobs/abcdefghij/file", "")

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("GET /jobs/{id}/file status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
		assertJSONError(t, recorder, "could not read job")
	})

	t.Run("failed job", func(t *testing.T) {
		service := NewService(testPDFGenerator{err: errors.New("generator failed")}, t.TempDir(), 1)
		handler := NewHandler(service)
		created := createJob(t, handler)
		waitForJobs(t, service)

		recorder := serveRequest(handler, http.MethodGet, "/jobs/"+created.JobID+"/file", "")
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("GET /jobs/{id}/file status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
		assertJSONError(t, recorder, "job failed")
	})

	t.Run("unknown job", func(t *testing.T) {
		handler := NewHandler(NewService(testPDFGenerator{}, t.TempDir(), 1))
		recorder := serveRequest(handler, http.MethodGet, "/jobs/abcdefghij/file", "")

		if recorder.Code != http.StatusNotFound {
			t.Fatalf("GET /jobs/{id}/file status = %d, want %d", recorder.Code, http.StatusNotFound)
		}
		assertJSONError(t, recorder, "job not found")
	})
}

func TestHandlerRejectsMalformedAndUnknownRoutes(t *testing.T) {
	handler := NewHandler(NewService(testPDFGenerator{}, t.TempDir(), 1))

	for _, path := range []string{
		"/jobs/short",
		"/jobs/abcde!ghij",
		"/jobs/abcdefghij/unknown",
	} {
		t.Run(path, func(t *testing.T) {
			recorder := serveRequest(handler, http.MethodGet, path, "")
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("GET %s status = %d, want %d", path, recorder.Code, http.StatusNotFound)
			}
			assertJSONError(t, recorder, "not found")
		})
	}
}

func createJob(t *testing.T, handler http.Handler) createJobResponse {
	t.Helper()
	recorder := serveRequest(handler, http.MethodPost, "/jobs", `{"lines":1}`)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("POST /jobs status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	return decodeJSON[createJobResponse](t, recorder)
}

func serveRequest(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func waitForJobs(t *testing.T, service *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func assertJSONError(t *testing.T, recorder *httptest.ResponseRecorder, want string) {
	t.Helper()
	response := decodeJSON[map[string]string](t, recorder)
	if got := response["error"]; got != want {
		t.Fatalf("error response = %q, want %q", got, want)
	}
}

func decodeJSON[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode JSON response %q: %v", recorder.Body.String(), err)
	}
	return value
}

func decodeRawString(t *testing.T, value json.RawMessage) string {
	t.Helper()
	var result string
	if err := json.Unmarshal(value, &result); err != nil {
		t.Fatalf("decode JSON string %q: %v", value, err)
	}
	return result
}
