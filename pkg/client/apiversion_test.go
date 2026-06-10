package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResolveAPIURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		version string
		want    string
		wantErr bool
	}{
		{"replaces current", "wss://10.0.0.1/api/current", "v25.10.0", "wss://10.0.0.1/api/v25.10.0", false},
		{"replaces older version", "wss://host/api/v25.04.0", "v25.10.0", "wss://host/api/v25.10.0", false},
		{"appends to bare host", "wss://host", "v25.10.0", "wss://host/api/v25.10.0", false},
		{"preserves port", "wss://host:8443/api/current", "v25.10.0", "wss://host:8443/api/v25.10.0", false},
		{"drops query/fragment", "wss://host/api/current?x=1#y", "v25.10.0", "wss://host/api/v25.10.0", false},
		{"missing host", "/api/current", "v25.10.0", "", true},
		{"empty", "", "v25.10.0", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveAPIURL(tt.raw, tt.version)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %q", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("resolveAPIURL(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestVersionSupported(t *testing.T) {
	supported := []string{"v25.04.0", "v25.04.1", "v25.10.0"}
	if !versionSupported(supported, "v25.10.0") {
		t.Error("expected v25.10.0 to be supported")
	}
	if versionSupported(supported, "v26.0.0") {
		t.Error("did not expect v26.0.0 to be supported")
	}
	if versionSupported(nil, "v25.10.0") {
		t.Error("empty list should support nothing")
	}
}

// newVersionsServer returns an httptest server that serves the given versions
// list at /api/versions, plus the ws:// URL pointing at it.
func newVersionsServer(t *testing.T, versions []string, status int) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != apiVersionsPath {
			http.NotFound(w, r)
			return
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(versions)
	}))
	t.Cleanup(srv.Close)
	// srv.URL is http://127.0.0.1:port; convert to ws:// so the client maps it back to http.
	wsURL := "ws://" + strings.TrimPrefix(srv.URL, "http://")
	return srv, wsURL
}

func TestFetchSupportedAPIVersions(t *testing.T) {
	want := []string{"v25.04.0", "v25.04.1", "v25.04.2", "v25.10.0"}
	_, wsURL := newVersionsServer(t, want, http.StatusOK)

	got, err := fetchSupportedAPIVersions(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFetchSupportedAPIVersions_HTTPError(t *testing.T) {
	_, wsURL := newVersionsServer(t, nil, http.StatusInternalServerError)
	if _, err := fetchSupportedAPIVersions(context.Background(), wsURL, nil); err == nil {
		t.Fatal("expected error on HTTP 500")
	}
}

func TestVerifyAndPinAPIVersion(t *testing.T) {
	wantSuffix := apiPathPrefix + apiVersionCurrent // always connect to /api/current

	t.Run("min supported -> connects to current", func(t *testing.T) {
		_, wsURL := newVersionsServer(t, []string{"v25.04.0", MinAPIVersion}, http.StatusOK)
		c := New(Config{URL: wsURL})
		if err := c.verifyAndPinAPIVersion(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.HasSuffix(c.config.URL, wantSuffix) {
			t.Errorf("URL = %q, want suffix %q", c.config.URL, wantSuffix)
		}
	})

	t.Run("below minimum fails fast", func(t *testing.T) {
		_, wsURL := newVersionsServer(t, []string{"v25.04.0", "v25.04.1"}, http.StatusOK)
		c := New(Config{URL: wsURL})
		err := c.verifyAndPinAPIVersion(context.Background())
		if err == nil {
			t.Fatal("expected ErrUnsupportedAPIVersion")
		}
		if !errors.Is(err, ErrUnsupportedAPIVersion) {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("fetch failure proceeds with current", func(t *testing.T) {
		// Point at a closed port so the GET fails; verify it still pins to current.
		srv, wsURL := newVersionsServer(t, nil, http.StatusOK)
		srv.Close() // close immediately so the request fails
		c := New(Config{URL: wsURL})
		if err := c.verifyAndPinAPIVersion(context.Background()); err != nil {
			t.Fatalf("expected nil (graceful fallback), got %v", err)
		}
		if !strings.HasSuffix(c.config.URL, wantSuffix) {
			t.Errorf("URL not pinned on fallback: %q", c.config.URL)
		}
	})
}
