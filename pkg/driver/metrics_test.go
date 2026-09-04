package driver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metric names are what dashboards and alerts are written against, so a rename is
// a breaking change for anyone scraping the endpoint. This pins the published set.
func TestMetrics_PublishedNames(t *testing.T) {
	m := NewMetrics()
	if err := m.RegisterConnectionState(func() bool { return true }, func() uint64 { return 0 }); err != nil {
		t.Fatalf("RegisterConnectionState() = %v, want nil", err)
	}

	// Counters and histograms only appear in a scrape once observed.
	m.RecordGRPCCall("/csi.v1.Controller/CreateVolume", nil, time.Second)

	m.RecordVolumeOperation(ProtocolISCSI, volumeOperationCreate, nil)
	m.RecordAPICall("pool.dataset.create", time.Second, nil)

	body := scrapeMetrics(t, m)

	for _, name := range []string{
		"truenas_csi_grpc_requests_total",
		"truenas_csi_grpc_request_duration_seconds",
		"truenas_csi_volume_operations_total",
		"truenas_csi_api_calls_total",
		"truenas_csi_api_call_duration_seconds",
		"truenas_csi_api_connected",
		"truenas_csi_api_reconnects_total",
		// The Go and process collectors are part of what the endpoint promises.
		"go_goroutines",
		"process_start_time_seconds",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("scrape is missing %q", name)
		}
	}
}

// The gRPC status code has to reach the counter as a label, otherwise a scrape
// cannot tell a cluster that is provisioning volumes from one that is failing every
// call, which is the whole point of the endpoint.
func TestMetrics_RecordGRPCCall(t *testing.T) {
	m := NewMetrics()

	m.RecordGRPCCall("/csi.v1.Controller/CreateVolume", nil, 2*time.Second)
	m.RecordGRPCCall("/csi.v1.Controller/CreateVolume", status.Error(codes.Aborted, "in progress"), time.Second)
	m.RecordGRPCCall("/csi.v1.Node/NodeStageVolume", errors.New("plain error"), time.Second)

	tests := []struct {
		method string
		code   string
		want   float64
	}{
		{"/csi.v1.Controller/CreateVolume", "OK", 1},
		{"/csi.v1.Controller/CreateVolume", "Aborted", 1},
		// A non-status error is still a failure and must not be counted as OK.
		{"/csi.v1.Node/NodeStageVolume", "Unknown", 1},
	}

	for _, tt := range tests {
		got := testutil.ToFloat64(m.grpcRequests.WithLabelValues(tt.method, tt.code))
		if got != tt.want {
			t.Errorf("requests{method=%q,code=%q} = %v, want %v", tt.method, tt.code, got, tt.want)
		}
	}

	if got := testutil.CollectAndCount(m.grpcDuration); got != 2 {
		t.Errorf("duration series = %d, want one per method", got)
	}
}

// Failures that follow one protocol are what separate a broken iSCSI portal from a
// broken appliance, so the protocol has to survive as a label.
func TestMetrics_RecordVolumeOperation(t *testing.T) {
	m := NewMetrics()

	m.RecordVolumeOperation(ProtocolISCSI, volumeOperationCreate, nil)
	m.RecordVolumeOperation(ProtocolISCSI, volumeOperationCreate, errors.New("portal unreachable"))
	m.RecordVolumeOperation(ProtocolNFS, volumeOperationCreate, nil)
	m.RecordVolumeOperation(ProtocolNVMeOF, volumeOperationExpand, nil)
	m.RecordVolumeOperation(ProtocolNFS, volumeOperationSnapshot, nil)
	// A volume TrueNAS no longer knows about has no resolvable protocol.
	m.RecordVolumeOperation("", volumeOperationDelete, nil)

	tests := []struct {
		protocol  string
		operation string
		status    string
		want      float64
	}{
		{ProtocolISCSI, volumeOperationCreate, operationStatusSuccess, 1},
		{ProtocolISCSI, volumeOperationCreate, operationStatusError, 1},
		{ProtocolNFS, volumeOperationCreate, operationStatusSuccess, 1},
		{ProtocolNVMeOF, volumeOperationExpand, operationStatusSuccess, 1},
		{ProtocolNFS, volumeOperationSnapshot, operationStatusSuccess, 1},
		{protocolUnknown, volumeOperationDelete, operationStatusSuccess, 1},
		// Nothing should have leaked across protocols.
		{ProtocolNFS, volumeOperationCreate, operationStatusError, 0},
	}

	for _, tt := range tests {
		got := testutil.ToFloat64(m.volumeOps.WithLabelValues(tt.protocol, tt.operation, tt.status))
		if got != tt.want {
			t.Errorf("volume_operations{protocol=%q,operation=%q,status=%q} = %v, want %v",
				tt.protocol, tt.operation, tt.status, got, tt.want)
		}
	}
}

// A slow or failing TrueNAS call is the usual cause of a stuck PVC, and it is
// invisible in the gRPC metrics whenever the driver retries and still succeeds.
func TestMetrics_RecordAPICall(t *testing.T) {
	m := NewMetrics()

	m.RecordAPICall("pool.dataset.create", 2*time.Second, nil)
	m.RecordAPICall("pool.dataset.create", 30*time.Second, errors.New("timeout"))
	m.RecordAPICall("iscsi.target.query", 100*time.Millisecond, nil)

	if got := testutil.ToFloat64(m.apiCalls.WithLabelValues("pool.dataset.create", operationStatusSuccess)); got != 1 {
		t.Errorf("successful dataset creates = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.apiCalls.WithLabelValues("pool.dataset.create", operationStatusError)); got != 1 {
		t.Errorf("failed dataset creates = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.apiDuration); got != 2 {
		t.Errorf("duration series = %d, want one per method", got)
	}
}

// The connection metrics are read at scrape time so a scrape can never report a
// connection state the client left behind.
func TestMetrics_ConnectionStateIsReadAtScrapeTime(t *testing.T) {
	m := NewMetrics()

	connected := false
	reconnects := uint64(0)
	if err := m.RegisterConnectionState(
		func() bool { return connected },
		func() uint64 { return reconnects },
	); err != nil {
		t.Fatalf("RegisterConnectionState() = %v, want nil", err)
	}

	if body := scrapeMetrics(t, m); !strings.Contains(body, "truenas_csi_api_connected 0") {
		t.Error("a disconnected client should scrape as 0")
	}

	connected = true
	reconnects = 3

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "truenas_csi_api_connected 1") {
		t.Error("a connected client should scrape as 1")
	}
	if !strings.Contains(body, "truenas_csi_api_reconnects_total 3") {
		t.Errorf("reconnect count did not reach the scrape:\n%s", body)
	}
}

// Only the metrics path is served. A driver that runs privileged must not expose
// profiling handlers, and a second health path would be confused with the liveness
// probe sidecar's endpoint.
func TestMetrics_ServesOnlyTheMetricsPath(t *testing.T) {
	m := NewMetrics()
	base := serveMetrics(t, m)

	resp, err := http.Get(base + MetricsPath)
	if err != nil {
		t.Fatalf("GET %s: %v", MetricsPath, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s = %d, want 200", MetricsPath, resp.StatusCode)
	}

	for _, path := range []string{"/debug/pprof/", "/healthz", "/"} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, resp.StatusCode)
		}
	}
}

// A metrics port that cannot be bound must be reported rather than crashing the
// driver: the node plugin shares the host network namespace, where a conflict is
// plausible, and volumes still have to mount there.
func TestMetrics_StartServerReportsBindFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy a port: %v", err)
	}
	defer occupied.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := NewMetrics().StartServer(ctx, occupied.Addr().String(), logr.Discard()); err == nil {
		t.Fatal("StartServer() on a port already in use should report an error")
	}
}

// Cancelling the driver's context has to take the listener with it, or a restart in
// the same network namespace would find its own port occupied.
func TestMetrics_StartServerStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	addr, err := NewMetrics().StartServer(ctx, "127.0.0.1:0", logr.Discard())
	if err != nil {
		t.Fatalf("StartServer() = %v, want nil", err)
	}

	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := http.Get("http://" + addr.String() + MetricsPath); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("the metrics endpoint is still serving after the context was cancelled")
}

// A driver with no metrics address must not open a listener, and the interceptor
// must survive a driver built without collectors.
func TestMetrics_DisabledByDefault(t *testing.T) {
	if (&DriverConfig{}).MetricsAddr != "" {
		t.Error("metrics should be disabled unless an address is configured")
	}

	// A driver built without collectors (as tests do) must not panic on any of the
	// recording paths, which run from the interceptor and the controller handlers.
	var absent *Metrics
	absent.RecordGRPCCall("/csi.v1.Identity/Probe", nil, time.Second)
	absent.RecordVolumeOperation(ProtocolNFS, volumeOperationCreate, nil)
	absent.RecordAPICall("pool.dataset.query", time.Second, nil)
}

// scrapeMetrics renders the registry the way a Prometheus scrape would.
func scrapeMetrics(t *testing.T, m *Metrics) string {
	t.Helper()

	resp, err := http.Get(serveMetrics(t, m) + MetricsPath)
	if err != nil {
		t.Fatalf("failed to scrape metrics: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read the scrape: %v", err)
	}
	return string(body)
}

// serveMetrics starts the endpoint on a loopback port and returns its base URL.
func serveMetrics(t *testing.T, m *Metrics) string {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addr, err := m.StartServer(ctx, "127.0.0.1:0", logr.Discard())
	if err != nil {
		t.Fatalf("StartServer() = %v, want nil", err)
	}
	return "http://" + addr.String()
}
