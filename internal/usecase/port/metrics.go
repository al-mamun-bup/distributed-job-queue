package port

import "time"

// MetricsRecorder records observability signals for job lifecycle events.
// Defined here, the consumer (usecase/job), rather than alongside the
// Prometheus collectors that implement it - the usecase layer must not
// import infrastructure/metrics. Implementations must be safe for
// concurrent use.
type MetricsRecorder interface {
	// JobEnqueued records a job accepted by Enqueue.
	JobEnqueued(queue string)
	// JobCompleted records a finished processing attempt: result is one of
	// "succeeded", "retried", "dead".
	JobCompleted(queue, result string, duration time.Duration)
	// ClaimBatch records the size of a batch returned by the claim query.
	ClaimBatch(queue string, size int)
	// LeasesReclaimed records leases the reaper returned to pending.
	LeasesReclaimed(count int)
}
