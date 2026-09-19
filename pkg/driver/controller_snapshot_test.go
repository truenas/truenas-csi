package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/truenas/truenas-csi/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type snapshotRPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// Exercise the controller through the real client, including JSON-RPC error
// wrapping and the snapshot deletion options sent to TrueNAS.
func snapshotTestController(t *testing.T, handle func(snapshotRPCRequest) (any, *client.RPCError)) (*ControllerServer, func() []snapshotRPCRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []snapshotRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/versions" {
			_ = json.NewEncoder(w).Encode([]string{client.MinAPIVersion})
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept WebSocket: %v", err)
			return
		}
		defer conn.CloseNow()
		for {
			var req snapshotRPCRequest
			if err := wsjson.Read(r.Context(), conn, &req); err != nil {
				return
			}
			var result any = true
			var rpcErr *client.RPCError
			if req.Method != "auth.login_with_api_key" {
				mu.Lock()
				requests = append(requests, req)
				mu.Unlock()
				result, rpcErr = handle(req)
			}
			response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
			if rpcErr != nil {
				response["error"] = rpcErr
			} else {
				response["result"] = result
			}
			if err := wsjson.Write(r.Context(), conn, response); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	c := client.New(client.Config{
		URL:          "ws" + strings.TrimPrefix(server.URL, "http"),
		APIKey:       "test-api-key",
		CallTimeout:  5 * time.Second,
		PingInterval: time.Hour,
	})
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("connect client: %v", err)
	}
	controller := NewControllerServer(&Driver{client: c, log: logr.Discard()})
	return controller, func() []snapshotRPCRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]snapshotRPCRequest(nil), requests...)
	}
}

func assertDeferredSnapshotDelete(t *testing.T, requests []snapshotRPCRequest, snapshotID string) {
	t.Helper()
	var deletes int
	for _, req := range requests {
		if req.Method != "pool.snapshot.delete" {
			continue
		}
		deletes++
		var params []json.RawMessage
		if err := json.Unmarshal(req.Params, &params); err != nil || len(params) != 2 {
			t.Fatalf("snapshot deletion parameters = %s, error = %v", req.Params, err)
		}
		var id string
		var options client.SnapshotDeleteOptions
		if err := json.Unmarshal(params[0], &id); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(params[1], &options); err != nil {
			t.Fatal(err)
		}
		if id != snapshotID || !options.Defer || options.Recursive {
			t.Errorf("snapshot deletion = (%q, %+v), want (%q, defer=true, recursive=false)", id, options, snapshotID)
		}
	}
	if deletes != 1 {
		t.Errorf("snapshot deletion calls = %d, want 1", deletes)
	}
}

func TestDeleteSnapshot_ErrorHandling(t *testing.T) {
	tests := []struct {
		name   string
		rpcErr *client.RPCError
		want   codes.Code
	}{
		{name: "deleted", want: codes.OK},
		{name: "already absent", rpcErr: &client.RPCError{Code: -6, Message: "Snapshot not found"}, want: codes.OK},
		{name: "nested not found", rpcErr: &client.RPCError{
			Code: -32001, Message: "CallError", Data: json.RawMessage(`{"type":"InstanceNotFound","reason":"Snapshot does not exist"}`),
		}, want: codes.OK},
		{name: "permission denied", rpcErr: &client.RPCError{Code: -13, Message: "Permission denied"}, want: codes.Internal},
		{name: "deletion rejected", rpcErr: &client.RPCError{Code: -22, Message: "Snapshot deletion failed"}, want: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, requests := snapshotTestController(t, func(req snapshotRPCRequest) (any, *client.RPCError) {
				if req.Method != "pool.snapshot.delete" {
					t.Errorf("unexpected method %q", req.Method)
				}
				return true, tt.rpcErr
			})
			resp, err := controller.DeleteSnapshot(t.Context(), &csi.DeleteSnapshotRequest{SnapshotId: "tank/source@snapshot"})
			if status.Code(err) != tt.want {
				t.Fatalf("DeleteSnapshot error = %v, want code %v", err, tt.want)
			}
			if tt.want == codes.OK && resp == nil {
				t.Fatal("successful deletion returned no response")
			}
			if tt.want != codes.OK && (resp != nil || !strings.Contains(err.Error(), tt.rpcErr.Message)) {
				t.Fatalf("deletion failure lost its cause: response=%v error=%v", resp, err)
			}
			assertDeferredSnapshotDelete(t, requests(), "tank/source@snapshot")
		})
	}
}

func TestDeleteSnapshot_DisconnectedClient(t *testing.T) {
	controller, _ := snapshotTestController(t, func(req snapshotRPCRequest) (any, *client.RPCError) {
		t.Errorf("unexpected API call after close: %s", req.Method)
		return nil, nil
	})
	_ = controller.driver.Client().Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	resp, err := controller.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "tank/source@snapshot"})
	if resp != nil || status.Code(err) != codes.Internal {
		t.Fatalf("disconnected DeleteSnapshot = (%v, %v), want an error", resp, err)
	}
}

func TestCloneVolume_TemporarySnapshotCleanup(t *testing.T) {
	const snapshotID = "tank/source@csi-clone-test"
	tests := []struct {
		name        string
		cloneFails  bool
		deleteFails bool
	}{
		{name: "successful clone"},
		{name: "failed clone", cloneFails: true},
		{name: "cleanup failure after success", deleteFails: true},
		{name: "cleanup failure preserves clone error", cloneFails: true, deleteFails: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, requests := snapshotTestController(t, func(req snapshotRPCRequest) (any, *client.RPCError) {
				switch req.Method {
				case "pool.dataset.get_instance":
					var params []json.RawMessage
					if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
						t.Errorf("invalid dataset query: %s", req.Params)
						return nil, &client.RPCError{Code: -22, Message: "Invalid dataset query"}
					}
					var path string
					if err := json.Unmarshal(params[0], &path); err != nil {
						t.Error(err)
					}
					return map[string]any{"id": path, "type": "FILESYSTEM", "mountpoint": "/mnt/" + path}, nil
				case "pool.snapshot.create":
					return map[string]any{"id": snapshotID}, nil
				case "pool.snapshot.clone":
					if tt.cloneFails {
						return nil, &client.RPCError{Code: -22, Message: "clone failed"}
					}
					return nil, nil
				case "pool.snapshot.delete":
					if tt.deleteFails {
						return nil, &client.RPCError{Code: -13, Message: "cleanup denied"}
					}
					return true, nil
				case "sharing.nfs.create":
					return map[string]any{"id": 1, "path": "/mnt/tank/clone"}, nil
				default:
					t.Errorf("unexpected method %q", req.Method)
					return nil, &client.RPCError{Code: -32601, Message: "Unexpected method"}
				}
			})
			var logs bytes.Buffer
			controller.driver.log = funcr.New(func(prefix, args string) {
				logs.WriteString(prefix + args + "\n")
			}, funcr.Options{})
			source := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "tank/source"},
			}}
			req := &csi.CreateVolumeRequest{
				Name: "clone", CapacityRange: &csi.CapacityRange{}, VolumeContentSource: source,
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			resp, err := controller.createVolumeFromSource(ctx, req, "tank/clone", "tank/clone", ProtocolNFS, map[string]string{})
			if tt.cloneFails {
				if resp != nil || status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "clone failed") {
					t.Fatalf("clone failure = (%v, %v), want the original clone error", resp, err)
				}
			} else if err != nil || resp == nil || resp.Volume.VolumeId != "tank/clone" {
				t.Fatalf("clone result = (%v, %v), want the created volume", resp, err)
			}
			assertDeferredSnapshotDelete(t, requests(), snapshotID)
			if tt.deleteFails && (!strings.Contains(logs.String(), snapshotID) || !strings.Contains(logs.String(), "cleanup denied")) {
				t.Errorf("cleanup failure was not logged with its snapshot ID: %s", logs.String())
			}
		})
	}
}
