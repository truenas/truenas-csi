package main

import (
	"testing"

	"github.com/truenas/truenas-csi/pkg/driver"
)

// The deployment manifests configure metrics through the environment rather than
// the flag, because an older driver image ignores an unknown environment variable
// but exits on an unknown flag. Both paths have to work, with the flag winning when
// an operator passes it explicitly.
func TestLoadEnvConfig_MetricsAddr(t *testing.T) {
	tests := []struct {
		name string
		flag string
		env  string
		want string
	}{
		{"disabled by default", "", "", ""},
		{"from the environment", "", ":8080", ":8080"},
		{"from the flag", ":9090", "", ":9090"},
		{"flag wins over the environment", ":9090", ":8080", ":9090"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TRUENAS_URL", "wss://truenas.example")
			t.Setenv("TRUENAS_API_KEY", "key")
			t.Setenv("TRUENAS_DEFAULT_POOL", "tank")
			t.Setenv("TRUENAS_METRICS_ADDR", tt.env)

			config := &driver.DriverConfig{MetricsAddr: tt.flag}
			if err := loadEnvConfig(config); err != nil {
				t.Fatalf("loadEnvConfig() = %v, want nil", err)
			}
			if config.MetricsAddr != tt.want {
				t.Errorf("MetricsAddr = %q, want %q", config.MetricsAddr, tt.want)
			}
		})
	}
}
