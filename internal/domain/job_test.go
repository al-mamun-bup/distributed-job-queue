package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewJob(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tests := []struct {
		name        string
		id          string
		queue       Queue
		maxAttempts int
		wantErr     bool
	}{
		{
			name:        "valid job",
			id:          "job-1",
			queue:       "default",
			maxAttempts: 5,
			wantErr:     false,
		},
		{
			name:        "missing id",
			id:          "",
			queue:       "default",
			maxAttempts: 5,
			wantErr:     true,
		},
		{
			name:        "missing queue",
			id:          "job-1",
			queue:       "",
			maxAttempts: 5,
			wantErr:     true,
		},
		{
			name:        "invalid max attempts",
			id:          "job-1",
			queue:       "default",
			maxAttempts: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job, err := NewJob(tt.id, tt.queue, []byte("{}"), tt.maxAttempts, now)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if tt.wantErr {
				return
			}

			if job.State != JobStatePending {
				t.Fatalf("expected pending, got %s", job.State)
			}
			if job.Attempts != 0 {
				t.Fatalf("expected attempts 0, got %d", job.Attempts)
			}
		})
	}
}

func TestJobTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T) *Job
		action  func(*Job) error
		want    JobState
		wantErr bool
	}{
		{
			name: "start pending job",
			setup: func(t *testing.T) *Job {
				t.Helper()
				job, err := NewJob("job-1", "default", []byte("{}"), 5, time.Now().UTC())
				if err != nil {
					t.Fatalf("creating job: %v", err)
				}
				return job
			},
			action: func(job *Job) error { return job.Start() },
			want:   JobStateRunning,
		},
		{
			name: "cannot start running job",
			setup: func(t *testing.T) *Job {
				t.Helper()
				job, err := NewJob("job-1", "default", []byte("{}"), 5, time.Now().UTC())
				if err != nil {
					t.Fatalf("creating job: %v", err)
				}
				if err := job.Start(); err != nil {
					t.Fatalf("starting job: %v", err)
				}
				return job
			},
			action:  func(job *Job) error { return job.Start() },
			want:    JobStateRunning,
			wantErr: true,
		},
		{
			name: "mark succeeded from running",
			setup: func(t *testing.T) *Job {
				t.Helper()
				job, err := NewJob("job-1", "default", []byte("{}"), 5, time.Now().UTC())
				if err != nil {
					t.Fatalf("creating job: %v", err)
				}
				if err := job.Start(); err != nil {
					t.Fatalf("starting job: %v", err)
				}
				return job
			},
			action: func(job *Job) error { return job.MarkSucceeded() },
			want:   JobStateSucceeded,
		},
		{
			name: "cannot mark succeeded from pending",
			setup: func(t *testing.T) *Job {
				t.Helper()
				job, err := NewJob("job-1", "default", []byte("{}"), 5, time.Now().UTC())
				if err != nil {
					t.Fatalf("creating job: %v", err)
				}
				return job
			},
			action:  func(job *Job) error { return job.MarkSucceeded() },
			want:    JobStatePending,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job := tt.setup(t)
			err := tt.action(job)

			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if job.State != tt.want {
				t.Fatalf("expected state %s, got %s", tt.want, job.State)
			}
		})
	}
}

func TestMarkFailed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		maxAttempts   int
		startAttempts int
		wantState     JobState
		wantCanRetry  bool
	}{
		{
			name:          "failed but retryable",
			maxAttempts:   5,
			startAttempts: 1,
			wantState:     JobStatePending,
			wantCanRetry:  true,
		},
		{
			name:          "failed and exhausted",
			maxAttempts:   2,
			startAttempts: 2,
			wantState:     JobStateDead,
			wantCanRetry:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job, err := NewJob("job-1", "default", []byte("{}"), tt.maxAttempts, time.Now().UTC())
			if err != nil {
				t.Fatalf("creating job: %v", err)
			}
			job.State = JobStateRunning
			job.Attempts = tt.startAttempts

			failErr := errors.New("downstream failed")
			if err := job.MarkFailed(failErr); err != nil {
				t.Fatalf("marking failed: %v", err)
			}

			if job.State != tt.wantState {
				t.Fatalf("expected state %s, got %s", tt.wantState, job.State)
			}
			if job.LastError != failErr.Error() {
				t.Fatalf("expected last error %q, got %q", failErr.Error(), job.LastError)
			}
			if got := job.CanRetry(); got != tt.wantCanRetry {
				t.Fatalf("expected can retry %t, got %t", tt.wantCanRetry, got)
			}
		})
	}
}

func TestMarkFailedRequiresRunningAndError(t *testing.T) {
	t.Parallel()

	job, err := NewJob("job-1", "default", []byte("{}"), 5, time.Now().UTC())
	if err != nil {
		t.Fatalf("creating job: %v", err)
	}

	if err := job.MarkFailed(nil); err == nil {
		t.Fatalf("expected error for nil failure")
	}

	if err := job.MarkFailed(errors.New("boom")); err == nil {
		t.Fatalf("expected invalid transition error")
	}
}

func TestRetryFromDead(t *testing.T) {
	t.Parallel()

	job, err := NewJob("job-1", "default", []byte("{}"), 2, time.Now().UTC())
	if err != nil {
		t.Fatalf("creating job: %v", err)
	}
	job.State = JobStateDead
	job.Attempts = 2
	job.LastError = "failed hard"

	if err := job.RetryFromDead(); err != nil {
		t.Fatalf("retrying from dead: %v", err)
	}

	if job.State != JobStatePending {
		t.Fatalf("expected pending, got %s", job.State)
	}
	if job.Attempts != 0 {
		t.Fatalf("expected attempts reset, got %d", job.Attempts)
	}
	if job.LastError != "" {
		t.Fatalf("expected last error cleared, got %q", job.LastError)
	}
}
