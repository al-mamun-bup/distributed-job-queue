package domain

import (
	"fmt"
	"time"
)

// ComputeNextRunAt returns now plus capped exponential backoff with jitter.
//
// attempts is the already-consumed attempt count for the failed execution.
// jitterFactor must be in [0,1] and is supplied by the caller to keep this pure.
// The jitter multiplier is in [1-jitter, 1+jitter].
func ComputeNextRunAt(now time.Time, attempts int, policy RetryPolicy, jitterFactor float64) (time.Time, error) {
	if policy.BaseDelay <= 0 {
		return time.Time{}, fmt.Errorf("%w: base delay must be > 0", ErrInvalidRetryConfig)
	}
	if policy.MaxDelay < policy.BaseDelay {
		return time.Time{}, fmt.Errorf("%w: max delay must be >= base delay", ErrInvalidRetryConfig)
	}
	if policy.Jitter < 0 || policy.Jitter > 1 {
		return time.Time{}, fmt.Errorf("%w: jitter must be in [0,1]", ErrInvalidRetryConfig)
	}
	if jitterFactor < 0 || jitterFactor > 1 {
		return time.Time{}, fmt.Errorf("%w: jitter factor must be in [0,1]", ErrInvalidJitterFactor)
	}

	delay := policy.BaseDelay
	for i := 1; i < attempts; i++ {
		if delay >= policy.MaxDelay {
			delay = policy.MaxDelay
			break
		}
		next := delay * 2
		if next > policy.MaxDelay {
			delay = policy.MaxDelay
			break
		}
		delay = next
	}

	span := 2 * policy.Jitter
	multiplier := (1 - policy.Jitter) + (span * jitterFactor)
	jittered := time.Duration(float64(delay) * multiplier)

	if jittered < 0 {
		jittered = 0
	}

	return now.Add(jittered), nil
}
