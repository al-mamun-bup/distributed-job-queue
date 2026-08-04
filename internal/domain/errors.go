package domain

import "errors"

var (
	// ErrInvalidTransition indicates an unsupported job state change.
	ErrInvalidTransition = errors.New("invalid job state transition")
	// ErrJobNotFound indicates a missing job record.
	ErrJobNotFound = errors.New("job not found")
	// ErrInvalidRetryConfig indicates invalid retry/backoff configuration.
	ErrInvalidRetryConfig = errors.New("invalid retry config")
	// ErrInvalidJitterFactor indicates a jitter factor outside [0, 1].
	ErrInvalidJitterFactor = errors.New("invalid jitter factor")
)
