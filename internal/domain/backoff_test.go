package domain

import (
	"testing"
	"time"
)

func TestComputeNextRunAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	policy := RetryPolicy{
		BaseDelay: 10 * time.Second,
		MaxDelay:  1 * time.Minute,
		Jitter:    0.2,
	}

	tests := []struct {
		name         string
		attempts     int
		jitterFactor float64
		wantDelay    time.Duration
	}{
		{
			name:         "attempt one min jitter",
			attempts:     1,
			jitterFactor: 0.0,
			wantDelay:    8 * time.Second,
		},
		{
			name:         "attempt two no jitter shift",
			attempts:     2,
			jitterFactor: 0.5,
			wantDelay:    20 * time.Second,
		},
		{
			name:         "attempt three max jitter",
			attempts:     3,
			jitterFactor: 1.0,
			wantDelay:    48 * time.Second,
		},
		{
			name:         "cap at max delay",
			attempts:     10,
			jitterFactor: 0.5,
			wantDelay:    1 * time.Minute,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ComputeNextRunAt(now, tt.attempts, policy, tt.jitterFactor)
			if err != nil {
				t.Fatalf("computing backoff: %v", err)
			}

			want := now.Add(tt.wantDelay)
			if !got.Equal(want) {
				t.Fatalf("expected %v, got %v", want, got)
			}
		})
	}
}

func TestComputeNextRunAtInvalidInput(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name         string
		policy       RetryPolicy
		jitterFactor float64
	}{
		{
			name: "base delay <= 0",
			policy: RetryPolicy{
				BaseDelay: 0,
				MaxDelay:  time.Second,
				Jitter:    0.2,
			},
			jitterFactor: 0.5,
		},
		{
			name: "max delay < base delay",
			policy: RetryPolicy{
				BaseDelay: 2 * time.Second,
				MaxDelay:  time.Second,
				Jitter:    0.2,
			},
			jitterFactor: 0.5,
		},
		{
			name: "invalid policy jitter",
			policy: RetryPolicy{
				BaseDelay: time.Second,
				MaxDelay:  2 * time.Second,
				Jitter:    1.2,
			},
			jitterFactor: 0.5,
		},
		{
			name: "invalid jitter factor",
			policy: RetryPolicy{
				BaseDelay: time.Second,
				MaxDelay:  2 * time.Second,
				Jitter:    0.2,
			},
			jitterFactor: 1.2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ComputeNextRunAt(now, 1, tt.policy, tt.jitterFactor); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestJobNextRunAt(t *testing.T) {
	t.Parallel()

	job := Job{Attempts: 3}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	policy := RetryPolicy{
		BaseDelay: 10 * time.Second,
		MaxDelay:  time.Minute,
		Jitter:    0.2,
	}

	got, err := job.NextRunAt(now, policy, 0.5)
	if err != nil {
		t.Fatalf("computing next run at: %v", err)
	}

	want := now.Add(40 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
