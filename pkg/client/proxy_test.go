package client

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// connectProxy is a minimal HTTP CONNECT proxy that records the hosts it tunnels to.
type connectProxy struct {
	*httptest.Server
	mu    sync.Mutex
	hosts []string
}

func newConnectProxy(t *testing.T) *connectProxy {
	t.Helper()
	p := &connectProxy{}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
			return
		}
		p.mu.Lock()
		p.hosts = append(p.hosts, r.Host)
		p.mu.Unlock()

		upstream, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		_, _ = client.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
		go func() { _, _ = io.Copy(upstream, buffered); upstream.Close() }()
		go func() { _, _ = io.Copy(client, upstream); client.Close() }()
	}))
	t.Cleanup(p.Close)
	return p
}

func (p *connectProxy) tunnels() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.hosts...)
}

// Both requests the client makes to TrueNAS, the supported-versions check and the
// WebSocket, must go through the proxy: on a cluster whose egress has to, a direct
// connection simply fails.
func TestConnect_ThroughProxy(t *testing.T) {
	mock := NewMockTrueNASTLSServer()
	defer mock.Close()
	proxy := newConnectProxy(t)

	tlsConfig, err := NewTLSConfig(false, writeCABundle(t, mock))
	if err != nil {
		t.Fatalf("NewTLSConfig() = %v", err)
	}
	proxyURL, _ := url.Parse(proxy.URL)
	c := New(Config{
		URL:         mock.URL,
		APIKey:      "test-api-key",
		TLSConfig:   tlsConfig,
		CallTimeout: testTimeout,
		Proxy:       http.ProxyURL(proxyURL),
	})
	defer c.Close()

	if err := c.Connect(testContext(t)); err != nil {
		t.Fatalf("Connect() through the proxy = %v", err)
	}

	target := strings.TrimPrefix(mock.URL, "wss://")
	tunnels := proxy.tunnels()
	if len(tunnels) < 2 {
		t.Fatalf("proxy tunneled %v, want the versions check and the WebSocket", tunnels)
	}
	for _, host := range tunnels {
		if host != target {
			t.Errorf("proxy tunneled to %s, want %s", host, target)
		}
	}
}

// The connect log names the proxy, which is how to tell whether NO_PROXY applied,
// but must never print the proxy's credentials.
func TestProxyForRedactsCredentials(t *testing.T) {
	withCredentials, _ := url.Parse("http://user:secret@proxy.example:3128")

	c := New(Config{URL: "wss://truenas.example", Proxy: http.ProxyURL(withCredentials)})
	if got := c.proxyFor(c.config.URL); strings.Contains(got, "secret") || !strings.Contains(got, "proxy.example:3128") {
		t.Errorf("proxyFor() = %q, want the proxy without its password", got)
	}

	direct := New(Config{URL: "wss://truenas.example", Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }})
	if got := direct.proxyFor(direct.config.URL); got != "direct" {
		t.Errorf("proxyFor() without a proxy = %q, want %q", got, "direct")
	}
}
