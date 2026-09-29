package client

import (
	"context"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeCABundle writes the mock server's certificate as a PEM CA bundle.
func writeCABundle(t *testing.T, mock *MockTrueNASServer) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca-bundle.crt")
	block := &pem.Block{Type: "CERTIFICATE", Bytes: mock.Server.Certificate().Raw}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("failed to write CA bundle: %v", err)
	}
	return path
}

func TestNewTLSConfig(t *testing.T) {
	mock := NewMockTrueNASTLSServer()
	defer mock.Close()
	bundle := writeCABundle(t, mock)

	notPEM := filepath.Join(t.TempDir(), "not-pem.crt")
	if err := os.WriteFile(notPEM, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	t.Run("insecure ignores the bundle", func(t *testing.T) {
		cfg, err := NewTLSConfig(true, filepath.Join(t.TempDir(), "missing.crt"))
		if err != nil || !cfg.InsecureSkipVerify {
			t.Fatalf("NewTLSConfig(true, missing) = %+v, %v; want InsecureSkipVerify", cfg, err)
		}
	})
	t.Run("no bundle verifies against the system", func(t *testing.T) {
		cfg, err := NewTLSConfig(false, "")
		if err != nil || cfg.InsecureSkipVerify || cfg.RootCAs != nil {
			t.Fatalf("NewTLSConfig(false, \"\") = %+v, %v; want system roots", cfg, err)
		}
	})
	t.Run("bundle is added to the pool", func(t *testing.T) {
		cfg, err := NewTLSConfig(false, bundle)
		if err != nil || cfg.RootCAs == nil {
			t.Fatalf("NewTLSConfig(false, bundle) = %+v, %v; want RootCAs", cfg, err)
		}
	})
	t.Run("missing bundle", func(t *testing.T) {
		if _, err := NewTLSConfig(false, filepath.Join(t.TempDir(), "missing.crt")); err == nil {
			t.Fatal("NewTLSConfig with a missing bundle succeeded")
		}
	})
	t.Run("bundle without certificates", func(t *testing.T) {
		_, err := NewTLSConfig(false, notPEM)
		if err == nil || !strings.Contains(err.Error(), "no PEM certificates") {
			t.Fatalf("NewTLSConfig with a non-PEM file = %v, want a no-certificates error", err)
		}
	})
}

// A certificate from a CA outside the system store verifies once its bundle is
// trusted.
func TestConnect_TrustsCABundle(t *testing.T) {
	mock := NewMockTrueNASTLSServer()
	defer mock.Close()

	tlsConfig, err := NewTLSConfig(false, writeCABundle(t, mock))
	if err != nil {
		t.Fatalf("NewTLSConfig() = %v", err)
	}
	c := New(Config{URL: mock.URL, APIKey: "test-api-key", TLSConfig: tlsConfig, CallTimeout: testTimeout})
	defer c.Close()

	if err := c.Connect(testContext(t)); err != nil {
		t.Fatalf("Connect() with the CA bundle = %v", err)
	}
}

// A certificate that cannot be verified must end the first connect with a clear
// error, rather than leave it retrying forever: retries cannot fix it.
func TestConnect_FailsFastOnCertificateError(t *testing.T) {
	mock := NewMockTrueNASTLSServer()
	defer mock.Close()

	bundle := writeCABundle(t, mock)
	tests := []struct {
		name   string
		url    string
		bundle string
	}{
		{"untrusted issuer", mock.URL, ""},
		// The certificate is trusted but issued for other names than the host.
		{"host not in the certificate", strings.Replace(mock.URL, "127.0.0.1", "localhost", 1), bundle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tlsConfig, err := NewTLSConfig(false, tt.bundle)
			if err != nil {
				t.Fatalf("NewTLSConfig() = %v", err)
			}
			c := New(Config{URL: tt.url, APIKey: "test-api-key", TLSConfig: tlsConfig, CallTimeout: testTimeout})
			defer c.Close()

			// Far longer than a fail-fast connect takes, so a retrying one times out.
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			start := time.Now()
			err = c.Connect(ctx)

			if !errors.Is(err, ErrCertificateVerification) {
				t.Fatalf("Connect() = %v, want ErrCertificateVerification", err)
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Errorf("Connect() took %s, want it to give up without retrying", elapsed)
			}
		})
	}
}
