package jobs

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
)

type JobService interface {
	CreateJob(lines int) (Job, error)
	GetJob(id string) (Job, error)
}

type Handler struct {
	service JobService
}

func NewHandler(service JobService) *Handler {
	return &Handler{service: service}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/jobs" {
		h.createJob(w, r)
		return
	}

	const prefix = "/jobs/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		writeJSONError(w, http.StatusNotFound, "not found")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, prefix), "/")
	if len(parts) == 1 && validID(parts[0]) {
		h.readJob(w, r, parts[0])
		return
	}
	if len(parts) == 2 && parts[1] == "file" && validID(parts[0]) {
		h.downloadFile(w, r, parts[0])
		return
	}
	writeJSONError(w, http.StatusNotFound, "not found")
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var request GenerateRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		writeJSONError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	job, err := h.service.CreateJob(request.Lines)
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidLineCount):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, ErrShuttingDown):
			writeJSONError(w, http.StatusServiceUnavailable, "service is shutting down")
		case errors.Is(err, ErrQueueFull):
			writeJSONError(w, http.StatusServiceUnavailable, "job queue is full")
		default:
			writeJSONError(w, http.StatusInternalServerError, "could not create job")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, createJobResponse{
		JobID:       job.ID,
		Status:      StatusQueued,
		DownloadURL: downloadURL(job.ID),
	})
}

func (h *Handler) readJob(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	job, err := h.service.GetJob(id)
	if errors.Is(err, ErrJobNotFound) {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "could not read job")
		return
	}

	response := readJobResponse{JobID: job.ID, Status: job.Status}
	if job.Status == StatusCompleted {
		response.DownloadURL = downloadURL(job.ID)
	}
	if job.Status == StatusFailed {
		response.Error = "job failed"
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) downloadFile(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	job, err := h.service.GetJob(id)
	if errors.Is(err, ErrJobNotFound) {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "could not read job")
		return
	}

	switch job.Status {
	case StatusQueued:
		writeJSONError(w, http.StatusConflict, "job is queued")
	case StatusProcessing:
		writeJSONError(w, http.StatusConflict, "job is still being processed")
	case StatusFailed:
		writeJSONError(w, http.StatusInternalServerError, "job failed")
	case StatusCompleted:
		file, err := os.Open(job.FilePath)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "could not retrieve PDF")
			return
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || info.IsDir() {
			writeJSONError(w, http.StatusInternalServerError, "could not retrieve PDF")
			return
		}
		w.Header().Set("Content-Type", "application/pdf")
		w.Header().Set("Content-Disposition", "attachment; filename=\""+id+".pdf\"")
		http.ServeContent(w, r, id+".pdf", info.ModTime(), file)
	default:
		writeJSONError(w, http.StatusInternalServerError, "could not retrieve PDF")
	}
}

func validID(id string) bool {
	if len(id) != 10 {
		return false
	}
	for _, character := range id {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func downloadURL(id string) string {
	return "/jobs/" + id + "/file"
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
