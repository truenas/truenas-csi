package driver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/status"
)

const (
	// metricsNamespace prefixes every metric the driver publishes. Metric names are
	// a public interface that dashboards and alerts are written against, so extend
	// this set rather than renaming what is already in it.
	metricsNamespace = "truenas_csi"

	// MetricsPath is the HTTP path the Prometheus endpoint is served on.
	MetricsPath = "/metrics"

	// metricsReadHeaderTimeout bounds how long a scraper may take to send its
	// request headers.
	metricsReadHeaderTimeout = 5 * time.Second

	// metricsShutdownTimeout bounds the wait for in-flight scrapes on shutdown.
	metricsShutdownTimeout = 5 * time.Second
)

// Volume operations counted by protocol. Snapshot deletion is absent on purpose:
// a snapshot ID alone does not say which protocol backs its volume, and resolving
// it would cost an API call on every delete.
const (
	volumeOperationCreate   = "create"
	volumeOperationDelete   = "delete"
	volumeOperationExpand   = "expand"
	volumeOperationSnapshot = "snapshot"
)

const (
	operationStatusSuccess = "success"
	operationStatusError   = "error"

	// protocolUnknown labels an operation on a volume whose metadata could not be
	// read, which happens when TrueNAS no longer has the dataset.
	protocolUnknown = "unknown"
)

// operationStatus reduces an error to a label value. The gRPC metrics already
// carry the status code, so repeating it here would multiply series for no gain.
func operationStatus(err error) string {
	if err != nil {
		return operationStatusError
	}
	return operationStatusSuccess
}

// metricsDurationBuckets spans the range CSI calls actually take: a cached query
// answers in milliseconds, while creating or deleting a volume waits on ZFS and can
// run for minutes. The client library's default buckets stop at ten seconds, which
// would collapse every slow provisioning call into the overflow bucket and hide the
// latency this endpoint exists to show.
var metricsDurationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// Metrics holds the driver's Prometheus collectors and the registry that exposes
// them. The registry is the driver's own rather than the client library's global
// default, so no dependency can register into this endpoint or collide with it.
type Metrics struct {
	registry *prometheus.Registry

	grpcRequests *prometheus.CounterVec
	grpcDuration *prometheus.HistogramVec
	volumeOps    *prometheus.CounterVec
	apiCalls     *prometheus.CounterVec
	apiDuration  *prometheus.HistogramVec
}

// NewMetrics creates the driver's collectors. Metrics are always collected; only
// serving them over HTTP is optional, so an operator can enable the endpoint
// without the driver having to be restarted into a different instrumentation mode.
func NewMetrics() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		grpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "grpc_requests_total",
			Help:      "Total CSI gRPC requests served, by method and resulting gRPC status code.",
		}, []string{"method", "code"}),
		grpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "grpc_request_duration_seconds",
			Help:      "Time taken to serve each CSI gRPC request, by method.",
			Buckets:   metricsDurationBuckets,
		}, []string{"method"}),
		volumeOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "volume_operations_total",
			Help:      "Volume operations attempted, by storage protocol, operation and outcome. Requests rejected before a protocol was resolved are counted only in grpc_requests_total.",
		}, []string{"protocol", "operation", "status"}),
		apiCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "api_calls_total",
			Help:      "Calls made to the TrueNAS API, by JSON-RPC method and outcome.",
		}, []string{"method", "status"}),
		apiDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "api_call_duration_seconds",
			Help:      "Time each TrueNAS API call took, including any wait for a reconnect, by JSON-RPC method.",
			Buckets:   metricsDurationBuckets,
		}, []string{"method"}),
	}

	m.registry.MustRegister(
		m.grpcRequests,
		m.grpcDuration,
		m.volumeOps,
		m.apiCalls,
		m.apiDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// RegisterConnectionState publishes the TrueNAS API connection metrics. Both are
// read from the client at scrape time, so no state change has to be pushed into a
// collector and the reported value can never be stale.
func (m *Metrics) RegisterConnectionState(connected func() bool, reconnects func() uint64) error {
	err := m.registry.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "api_connected",
		Help:      "Whether the TrueNAS API WebSocket is currently connected (1) or not (0).",
	}, func() float64 {
		if connected() {
			return 1
		}
		return 0
	}))
	if err != nil {
		return fmt.Errorf("failed to register the connection gauge: %w", err)
	}

	err = m.registry.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "api_reconnects_total",
		Help:      "Total successful reconnections to the TrueNAS API after a failed or lost connection.",
	}, func() float64 {
		return float64(reconnects())
	}))
	if err != nil {
		return fmt.Errorf("failed to register the reconnect counter: %w", err)
	}

	return nil
}

// RecordGRPCCall records the outcome and duration of one CSI call. Both labels come
// from a fixed set, which keeps cardinality bounded; per-volume values must never
// become labels, since a label value that grows with the cluster grows Prometheus
// memory with it.
func (m *Metrics) RecordGRPCCall(method string, err error, duration time.Duration) {
	if m == nil {
		return
	}
	m.grpcRequests.WithLabelValues(method, status.Code(err).String()).Inc()
	m.grpcDuration.WithLabelValues(method).Observe(duration.Seconds())
}

// RecordVolumeOperation records the outcome of one volume operation. The gRPC
// metrics already show which calls failed; this shows whether the failures follow
// a protocol, which is what separates a broken iSCSI portal from a broken
// appliance.
func (m *Metrics) RecordVolumeOperation(protocol, operation string, err error) {
	if m == nil {
		return
	}
	if protocol == "" {
		protocol = protocolUnknown
	}
	m.volumeOps.WithLabelValues(protocol, operation, operationStatus(err)).Inc()
}

// RecordAPICall records one call to the TrueNAS API. It satisfies
// client.CallObserver, so the client package reports through it without depending
// on any metrics library. The JSON-RPC method comes from a fixed set of calls the
// driver makes, so it is safe as a label.
func (m *Metrics) RecordAPICall(method string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.apiCalls.WithLabelValues(method, operationStatus(err)).Inc()
	m.apiDuration.WithLabelValues(method).Observe(duration.Seconds())
}

// Handler serves the metrics endpoint. Only the metrics path is routed: profiling
// handlers stay out of a driver that runs privileged, and a health path here would
// be confused with the liveness probe sidecar's own endpoint on another port.
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(MetricsPath, promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	return mux
}

// StartServer binds addr and serves the metrics endpoint until ctx is done,
// returning the bound address.
//
// A bind failure is reported to the caller rather than handled here, but it must
// never be treated as fatal: the node plugin shares the host's network namespace,
// where a port already in use is entirely plausible, and losing a monitoring
// endpoint must not stop a node from mounting volumes.
func (m *Metrics) StartServer(ctx context.Context, addr string, log logr.Logger) (net.Addr, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	server := &http.Server{
		Handler:           m.Handler(),
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(err, "Metrics endpoint stopped serving")
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.V(LogLevelDebug).Info("Metrics endpoint did not shut down cleanly", "error", err)
		}
	}()

	return listener.Addr(), nil
}
