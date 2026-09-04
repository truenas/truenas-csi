package client

import (
	"sync"
	"testing"
	"time"
)

// observation is one call reported to a CallObserver.
type observation struct {
	method   string
	duration time.Duration
	failed   bool
}

// recordingObserver returns an observer and the slice it appends to. Calls can be
// concurrent, so access is guarded.
func recordingObserver() (CallObserver, *[]observation, *sync.Mutex) {
	var mu sync.Mutex
	var seen []observation
	return func(method string, duration time.Duration, err error) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, observation{method: method, duration: duration, failed: err != nil})
	}, &seen, &mu
}

// The observer is how the driver measures TrueNAS API calls without this package
// knowing anything about metrics. Every call has to be reported, including the ones
// that fail, since a failing appliance is what the metric exists to reveal.
func TestCallObserver_ReportsEveryCall(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse("test.ok", MockResponse{Result: "ok"})
	mock.SetResponse("test.fail", MockResponse{
		Error: &RPCError{Code: -32001, Message: "dataset does not exist"},
	})

	observer, seen, mu := recordingObserver()

	client := New(Config{
		URL:          mock.URL,
		APIKey:       "test-api-key",
		CallTimeout:  testTimeout,
		PingInterval: 1 * time.Hour,
		CallObserver: observer,
	})
	if err := client.Connect(testContext(t)); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	var result string
	if err := client.Call(testContext(t), "test.ok", nil, &result); err != nil {
		t.Fatalf("test.ok = %v, want nil", err)
	}
	if err := client.Call(testContext(t), "test.fail", nil, &result); err == nil {
		t.Fatal("test.fail should have returned an error")
	}

	mu.Lock()
	defer mu.Unlock()

	// Connect performs its own calls (auth, version), so filter to the two above.
	var ok, failed int
	for _, o := range *seen {
		switch o.method {
		case "test.ok":
			ok++
			if o.failed {
				t.Error("a successful call was reported as failed")
			}
		case "test.fail":
			failed++
			if !o.failed {
				t.Error("a failed call was reported as successful")
			}
		}
		if o.duration < 0 {
			t.Errorf("%s reported a negative duration", o.method)
		}
	}

	if ok != 1 {
		t.Errorf("test.ok observed %d times, want 1", ok)
	}
	if failed != 1 {
		t.Errorf("test.fail observed %d times, want 1", failed)
	}
}

// A client with no observer configured must behave exactly as before.
func TestCallObserver_OptionalByDefault(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse("test.method", MockResponse{Result: "ok"})

	client := connectTestClient(t, mock)

	var result string
	if err := client.Call(testContext(t), "test.method", nil, &result); err != nil {
		t.Fatalf("Call() = %v, want nil", err)
	}
	if result != "ok" {
		t.Errorf("result = %q, want %q", result, "ok")
	}
}
