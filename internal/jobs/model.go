package jobs

import "time"

type GenerateRequest struct {
	Lines int `json:"lines"`
}

type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusDead       JobStatus = "dead"
)

type Job struct {
	ID             string
	Lines          int
	Status         JobStatus
	FilePath       string
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	AvailableAt    time.Time
	LeaseExpiresAt *time.Time
	Attempts       int
	ClaimVersion   int64
}

type createJobResponse struct {
	JobID       string    `json:"jobId"`
	Status      JobStatus `json:"status"`
	DownloadURL string    `json:"downloadUrl"`
}

type readJobResponse struct {
	JobID       string    `json:"jobId"`
	Status      JobStatus `json:"status"`
	DownloadURL string    `json:"downloadUrl,omitempty"`
	Error       string    `json:"error,omitempty"`
}
