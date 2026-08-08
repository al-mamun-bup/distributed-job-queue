package http

import (
	"encoding/json"
	"time"

	"hopper/internal/domain"
	"hopper/internal/usecase/port"
)

// CreateJobRequest is the POST /v1/jobs request body. The idempotency key,
// if any, travels via the Idempotency-Key header, not the body.
type CreateJobRequest struct {
	Queue       string          `json:"queue" validate:"required"`
	Payload     json.RawMessage `json:"payload" validate:"required"`
	MaxAttempts int             `json:"max_attempts" validate:"omitempty,gt=0"`
	RunAt       *time.Time      `json:"run_at,omitempty"`
}

// JobResponse is the wire representation of a job. Handlers map to this
// explicitly - a domain.Job is never returned directly.
type JobResponse struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	State          string          `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

func toJobResponse(j domain.Job) JobResponse {
	return JobResponse{
		ID:             j.ID,
		Queue:          string(j.Queue),
		Payload:        json.RawMessage(j.Payload),
		State:          string(j.State),
		Attempts:       j.Attempts,
		MaxAttempts:    j.MaxAttempts,
		RunAt:          j.RunAt,
		LeaseExpiresAt: j.LeaseExpiresAt,
		WorkerID:       j.WorkerID,
		IdempotencyKey: j.IdempotencyKey,
		LastError:      j.LastError,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		CompletedAt:    j.CompletedAt,
	}
}

// ListJobsResponse is the paginated GET /v1/jobs response body.
type ListJobsResponse struct {
	Jobs   []JobResponse `json:"jobs"`
	Total  int64         `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

// QueueStatsResponse is the GET /v1/queues/:queue/stats response body.
type QueueStatsResponse struct {
	Queue     string    `json:"queue"`
	Pending   int64     `json:"pending"`
	Running   int64     `json:"running"`
	Succeeded int64     `json:"succeeded"`
	Dead      int64     `json:"dead"`
	Total     int64     `json:"total"`
	UpdatedAt time.Time `json:"updated_at"`
}

func toQueueStatsResponse(s port.QueueStats) QueueStatsResponse {
	return QueueStatsResponse{
		Queue:     s.Queue,
		Pending:   s.Pending,
		Running:   s.Running,
		Succeeded: s.Succeeded,
		Dead:      s.Dead,
		Total:     s.Total,
		UpdatedAt: s.UpdatedAt,
	}
}
