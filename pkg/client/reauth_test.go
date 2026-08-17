package client

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// notAuthenticatedError mirrors what TrueNAS returns once a session is no longer
// authenticated: a generic method-call code with the errno carried in the data.
func notAuthenticatedError() *RPCError {
	return &RPCError{
		Code:    -32001,
		Message: "Method call error",
		Data:    json.RawMessage(`{"error": 207, "errname": "ENOTAUTHENTICATED", "reason": "[ENOTAUTHENTICATED] Not authenticated"}`),
	}
}

func TestIsNotAuthenticatedError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"non-RPC error", errors.New("boom"), false},
		{"appliance payload", notAuthenticatedError(), true},
		{
			name: "errno only",
			err:  &RPCError{Code: -32001, Message: "Method call error", Data: json.RawMessage(`{"error": 207}`)},
			want: true,
		},
		{
			name: "errname only",
			err:  &RPCError{Code: -32001, Message: "Method call error", Data: json.RawMessage(`{"errname": "ENOTAUTHENTICATED"}`)},
			want: true,
		},
		{
			name: "message fallback when data is absent",
			err:  &RPCError{Code: -32001, Message: "[ENOTAUTHENTICATED] Not authenticated"},
			want: true,
		},
		{
			name: "different errno is not an auth failure",
			err:  &RPCError{Code: -32001, Message: "Method call error", Data: json.RawMessage(`{"error": 2, "errname": "ENOENT"}`)},
			want: false,
		},
		{
			name: "connection lost is not an auth failure",
			err:  &RPCError{Code: rpcErrCodeConnectionLost, Message: "connection lost"},
			want: false,
		},
		{
			name: "unparsable data falls back to the message",
			err:  &RPCError{Code: -32001, Message: "Method call error", Data: json.RawMessage(`not json`)},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNotAuthenticatedError(tt.err); got != tt.want {
				t.Errorf("IsNotAuthenticatedError() = %v, want %v", got, tt.want)
			}
		})
	}
}

// newReauthTestClient returns a client that reconnects quickly, so the retry path
// completes well inside the test timeout.
func newReauthTestClient(t *testing.T, mock *MockTrueNASServer) *Client {
	t.Helper()
	client := New(Config{
		URL:          mock.URL,
		APIKey:       "test-api-key",
		CallTimeout:  testTimeout,
		PingInterval: 1 * time.Hour, // exercise the Call path, not the ping loop
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
	})
	if err := client.Connect(testContext(t)); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// An expired session leaves the socket healthy, so the client must notice the
// error itself, reconnect to re-authenticate, and retry the call.
func TestCall_ReconnectsWhenSessionExpired(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	var calls atomic.Int32
	mock.SetResponseFunc(func(method string, _ json.RawMessage) MockResponse {
		if method != "pool.dataset.query" {
			return MockResponse{Result: nil}
		}
		if calls.Add(1) == 1 {
			return MockResponse{Error: notAuthenticatedError()}
		}
		return MockResponse{Result: []string{"tank"}}
	})

	client := newReauthTestClient(t, mock)
	connectionsBefore := mock.ConnectionCount()

	var result []string
	if err := client.Call(testContext(t), "pool.dataset.query", []any{}, &result); err != nil {
		t.Fatalf("Call should have recovered from the expired session, got: %v", err)
	}

	if len(result) != 1 || result[0] != "tank" {
		t.Errorf("result = %v, want [tank]", result)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("method was called %d times, want 2 (initial + retry)", got)
	}
	if mock.ConnectionCount() <= connectionsBefore {
		t.Errorf("client did not reconnect: connection count stayed at %d", mock.ConnectionCount())
	}
}

// A key that is genuinely rejected must surface instead of retrying forever.
func TestCall_NotAuthenticatedRetriesOnlyOnce(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	var calls atomic.Int32
	mock.SetResponseFunc(func(method string, _ json.RawMessage) MockResponse {
		if method == "pool.dataset.query" {
			calls.Add(1)
			return MockResponse{Error: notAuthenticatedError()}
		}
		return MockResponse{Result: nil}
	})

	client := newReauthTestClient(t, mock)

	err := client.Call(testContext(t), "pool.dataset.query", []any{}, nil)
	if err == nil {
		t.Fatal("expected the persistent auth error to be returned")
	}
	if !IsNotAuthenticatedError(err) {
		t.Errorf("expected a not-authenticated error, got: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("method was called %d times, want 2 (initial + single retry)", got)
	}
}
