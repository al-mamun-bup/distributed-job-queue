// Package metrics defines Prometheus collectors.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"hopper/internal/usecase/port"
)

// Metrics holds every Prometheus collector hopper exposes and implements
// port.MetricsRecorder, so usecase/job can record signals without importing
// this package.
type Metrics struct {
	Registry *prometheus.Registry

	jobsEnqueuedTotal    *prometheus.CounterVec
	jobsCompletedTotal   *prometheus.CounterVec
	jobDurationSeconds   *prometheus.HistogramVec
	claimBatchSize       *prometheus.HistogramVec
	leasesReclaimedTotal prometheus.Counter
}

var _ port.MetricsRecorder = (*Metrics)(nil)

// New creates a fresh registry with all hopper collectors plus the standard
// Go/process collectors registered.
func New() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		jobsEnqueuedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopper_jobs_enqueued_total",
			Help: "Total jobs accepted by Enqueue, per queue.",
		}, []string{"queue"}),
		jobsCompletedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hopper_jobs_completed_total",
			Help: "Total job processing attempts that finished, by outcome (succeeded, retried, dead).",
		}, []string{"queue", "result"}),
		jobDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hopper_job_duration_seconds",
			Help:    "Job handler execution duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue", "result"}),
		claimBatchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hopper_claim_batch_size",
			Help:    "Number of jobs returned per non-empty claim query call.",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100},
		}, []string{"queue"}),
		leasesReclaimedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hopper_leases_reclaimed_total",
			Help: "Total expired leases returned to pending by the reaper.",
		}),
	}

	m.Registry.MustRegister(
		m.jobsEnqueuedTotal,
		m.jobsCompletedTotal,
		m.jobDurationSeconds,
		m.claimBatchSize,
		m.leasesReclaimedTotal,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

func (m *Metrics) JobEnqueued(queue string) {
	m.jobsEnqueuedTotal.WithLabelValues(queue).Inc()
}

func (m *Metrics) JobCompleted(queue, result string, duration time.Duration) {
	m.jobsCompletedTotal.WithLabelValues(queue, result).Inc()
	m.jobDurationSeconds.WithLabelValues(queue, result).Observe(duration.Seconds())
}

func (m *Metrics) ClaimBatch(queue string, size int) {
	m.claimBatchSize.WithLabelValues(queue).Observe(float64(size))
}

func (m *Metrics) LeasesReclaimed(count int) {
	if count <= 0 {
		return
	}
	m.leasesReclaimedTotal.Add(float64(count))
}

// RegisterQueueDepth wires the hopper_queue_depth gauge, backed by a
// repository query run on every /metrics scrape rather than a background
// refresh loop, so it's always accurate without extra polling machinery.
func (m *Metrics) RegisterQueueDepth(repository port.JobRepository) {
	m.Registry.MustRegister(newQueueDepthCollector(repository))
}

type queueDepthCollector struct {
	repository port.JobRepository
	desc       *prometheus.Desc
}

func newQueueDepthCollector(repository port.JobRepository) *queueDepthCollector {
	return &queueDepthCollector{
		repository: repository,
		desc: prometheus.NewDesc(
			"hopper_queue_depth",
			"Current number of jobs per queue and state.",
			[]string{"queue", "state"},
			nil,
		),
	}
}

func (c *queueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *queueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	depths, err := c.repository.QueueDepths(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.desc, fmt.Errorf("collecting queue depth: %w", err))
		return
	}

	for _, d := range depths {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(d.Count), d.Queue, d.State)
	}
}

// Serve exposes registry on /metrics until ctx is done, then shuts down
// within a short grace period. Used by processes without their own HTTP
// server (the worker) to make their metrics scrapable.
func Serve(ctx context.Context, registry *prometheus.Registry, port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	serveErrCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrCh <- fmt.Errorf("serving metrics: %w", err)
			return
		}
		serveErrCh <- nil
	}()

	select {
	case err := <-serveErrCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutting down metrics server: %w", err)
	}
	return nil
}
