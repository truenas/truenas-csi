package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// MockResponse represents a configurable response for a specific RPC method.
type MockResponse struct {
	Result any       // The result to return (will be JSON marshaled)
	Error  *RPCError // The error to return (if non-nil, Result is ignored)
}

// RecordedRequest represents a request that was received by the mock server.
type RecordedRequest struct {
	Method string
	Params json.RawMessage
}

// MockTrueNASServer provides a mock WebSocket server for testing.
type MockTrueNASServer struct {
	Server *httptest.Server
	URL    string

	// responses maps method names to their mock responses
	responses map[string]MockResponse
	// responseFunc allows dynamic response generation
	responseFunc func(method string, params json.RawMessage) MockResponse

	// requests records all received requests
	requests []RecordedRequest

	// apiKey is the expected API key for authentication
	apiKey string

	// mu protects responses and requests
	mu sync.RWMutex

	// authFailure forces authentication to fail
	authFailure bool

	// connectionCount tracks number of connections
	connectionCount int
}

// NewMockTrueNASServer creates a new mock TrueNAS WebSocket server.
func NewMockTrueNASServer() *MockTrueNASServer {
	m := &MockTrueNASServer{
		responses: make(map[string]MockResponse),
		apiKey:    "test-api-key",
	}

	server := httptest.NewServer(http.HandlerFunc(m.handleWebSocket))
	m.Server = server
	// Convert http:// to ws://
	m.URL = "ws" + strings.TrimPrefix(server.URL, "http")

	return m
}

// SetAPIKey sets the expected API key for authentication.
func (m *MockTrueNASServer) SetAPIKey(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apiKey = key
}

// SetAuthFailure sets whether authentication should fail.
func (m *MockTrueNASServer) SetAuthFailure(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.authFailure = fail
}

// SetResponse sets a mock response for a specific method.
func (m *MockTrueNASServer) SetResponse(method string, resp MockResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[method] = resp
}

// SetResponseFunc sets a function to dynamically generate responses.
func (m *MockTrueNASServer) SetResponseFunc(fn func(method string, params json.RawMessage) MockResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responseFunc = fn
}

// GetRequests returns all recorded requests.
func (m *MockTrueNASServer) GetRequests() []RecordedRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]RecordedRequest, len(m.requests))
	copy(result, m.requests)
	return result
}

// GetRequestsByMethod returns all recorded requests for a specific method.
func (m *MockTrueNASServer) GetRequestsByMethod(method string) []RecordedRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []RecordedRequest
	for _, req := range m.requests {
		if req.Method == method {
			result = append(result, req)
		}
	}
	return result
}

// ClearRequests clears all recorded requests.
func (m *MockTrueNASServer) ClearRequests() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = nil
}

// ConnectionCount returns the number of connections made to the server.
func (m *MockTrueNASServer) ConnectionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connectionCount
}

// Close shuts down the mock server.
func (m *MockTrueNASServer) Close() {
	m.Server.Close()
}

// handleWebSocket handles incoming WebSocket connections.
func (m *MockTrueNASServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Serve the supported-versions preflight like real TrueNAS does, without
	// counting it as a WebSocket connection.
	if r.URL.Path == apiVersionsPath {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]string{MinAPIVersion})
		return
	}

	m.mu.Lock()
	m.connectionCount++
	m.mu.Unlock()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Handle messages
	for {
		var req request
		if err := wsjson.Read(r.Context(), conn, &req); err != nil {
			return
		}

		// Record the request (except auth)
		if req.Method != "auth.login_with_api_key" {
			m.mu.Lock()
			paramsJSON, _ := json.Marshal(req.Params)
			m.requests = append(m.requests, RecordedRequest{
				Method: req.Method,
				Params: paramsJSON,
			})
			m.mu.Unlock()
		}

		// Generate response
		resp := m.generateResponse(req)
		if err := wsjson.Write(r.Context(), conn, resp); err != nil {
			return
		}
	}
}

// generateResponse creates a response for the given request.
func (m *MockTrueNASServer) generateResponse(req request) response {
	m.mu.RLock()
	defer m.mu.RUnlock()

	resp := response{
		ID:      req.ID,
		JSONRPC: jsonRPCVersion,
	}

	// Handle authentication
	if req.Method == "auth.login_with_api_key" {
		if m.authFailure {
			resp.Error = &RPCError{Code: -1, Message: "Authentication failed"}
			return resp
		}

		// Check API key
		var params []string
		if paramsBytes, err := json.Marshal(req.Params); err == nil {
			if err := json.Unmarshal(paramsBytes, &params); err == nil && len(params) > 0 {
				if params[0] == m.apiKey {
					resp.Result, _ = json.Marshal(true)
					return resp
				}
			}
		}
		resp.Result, _ = json.Marshal(false)
		return resp
	}

	// Handle ping
	if req.Method == "core.ping" {
		resp.Result, _ = json.Marshal("pong")
		return resp
	}

	// Check for dynamic response function first
	if m.responseFunc != nil {
		paramsJSON, _ := json.Marshal(req.Params)
		mockResp := m.responseFunc(req.Method, paramsJSON)
		if mockResp.Error != nil {
			resp.Error = mockResp.Error
		} else if mockResp.Result != nil {
			resp.Result, _ = json.Marshal(mockResp.Result)
		}
		return resp
	}

	// Check for configured response
	if mockResp, ok := m.responses[req.Method]; ok {
		if mockResp.Error != nil {
			resp.Error = mockResp.Error
		} else if mockResp.Result != nil {
			resp.Result, _ = json.Marshal(mockResp.Result)
		}
		return resp
	}

	// Default: return empty result
	resp.Result, _ = json.Marshal(nil)
	return resp
}

// Common mock responses for reuse across tests

// MockDataset returns a mock dataset response.
func MockDataset(id, name, pool string, used, available, refQuota int64) map[string]any {
	return map[string]any{
		"id":         id,
		"name":       name,
		"pool":       pool,
		"type":       "FILESYSTEM",
		"mountpoint": "/" + strings.ReplaceAll(id, "/", "/"),
		"used":       map[string]any{"parsed": float64(used)},
		"available":  map[string]any{"parsed": float64(available)},
		"refquota":   map[string]any{"parsed": float64(refQuota)},
	}
}

// MockZVOL returns a mock ZVOL response.
func MockZVOL(id, name, pool string, volsize int64) map[string]any {
	return map[string]any{
		"id":      id,
		"name":    name,
		"pool":    pool,
		"type":    "VOLUME",
		"volsize": map[string]any{"parsed": float64(volsize)},
	}
}

// MockNFSShare returns a mock NFS share response.
func MockNFSShare(id int, path, comment string, hosts, networks []string) NFSShare {
	return NFSShare{
		ID:       id,
		Path:     path,
		Comment:  comment,
		Hosts:    hosts,
		Networks: networks,
		Enabled:  true,
	}
}

// MockISCSITarget returns a mock iSCSI target response.
func MockISCSITarget(id int, name, alias string) ISCSITarget {
	return ISCSITarget{
		ID:    id,
		Name:  name,
		Alias: alias,
		Mode:  "ISCSI",
		Groups: []ISCSITargetGroup{
			{Portal: 1},
		},
	}
}

// MockISCSIExtent returns a mock iSCSI extent response.
func MockISCSIExtent(id int, name, disk string, blocksize int) ISCSIExtent {
	return ISCSIExtent{
		ID:        id,
		Name:      name,
		Type:      "DISK",
		Disk:      disk,
		BlockSize: blocksize,
		Enabled:   true,
	}
}

// MockISCSITargetExtent returns a mock iSCSI target-extent association response.
func MockISCSITargetExtent(id, targetID, extentID, lunID int) ISCSITargetExtent {
	return ISCSITargetExtent{
		ID:     id,
		Target: targetID,
		Extent: extentID,
		LunID:  lunID,
	}
}

// MockSnapshot returns a mock snapshot response.
func MockSnapshot(id, dataset, name string) Snapshot {
	return Snapshot{
		ID:      id,
		Dataset: dataset,
		Name:    name,
	}
}

// MockPool returns a mock pool response.
func MockPool(id int, name string, size, allocated, free int64) Pool {
	return Pool{
		ID:        id,
		Name:      name,
		Status:    "ONLINE",
		Healthy:   true,
		Size:      size,
		Allocated: allocated,
		Free:      free,
	}
}

// MockISCSIAuth returns a mock iSCSI auth response.
func MockISCSIAuth(id, tag int, user, secret string) ISCSIAuth {
	return ISCSIAuth{
		ID:     id,
		Tag:    tag,
		User:   user,
		Secret: secret,
	}
}

// MockSnapshotTask returns a mock snapshot task response.
func MockSnapshotTask(id int, dataset string, lifetimeValue int, lifetimeUnit string) SnapshotTask {
	return SnapshotTask{
		ID:            id,
		Dataset:       dataset,
		LifetimeValue: lifetimeValue,
		LifetimeUnit:  lifetimeUnit,
		Enabled:       true,
	}
}

// MockZFSResource returns a mock ZFS resource response for GetAvailableSpace.
func MockZFSResource(name string, available int64) ZFSResource {
	return ZFSResource{
		Name: name,
		Pool: name,
		Type: "pool",
		Properties: map[string]ZFSProperty{
			"available": {
				Raw:   "",
				Value: float64(available),
			},
		},
	}
}
