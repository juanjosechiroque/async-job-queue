package job

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

type JobService interface {
	GeneratePDF(lines int) ([]byte, error)
}

type Handler struct {
	service JobService
}

func NewHandler(service JobService) *Handler {
	return &Handler{service: service}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var request GenerateRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&request); err != nil {
		writeJSONError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	pdf, err := h.service.GeneratePDF(request.Lines)
	if err != nil {
		if errors.Is(err, ErrInvalidLineCount) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeJSONError(w, http.StatusInternalServerError, "could not generate PDF")
		return
	}

	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", "attachment; filename=job-lines-"+strconv.Itoa(request.Lines)+".pdf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(pdf)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
