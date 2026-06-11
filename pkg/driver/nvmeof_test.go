package driver

import (
	"context"
	"os/exec"
	"testing"

	"github.com/go-logr/logr"
)

// recordedCmd captures an invocation of nvmeExecCommand.
type recordedCmd struct {
	name string
	args []string
}

// mockNVMeExec replaces nvmeExecCommand with a fake that records invocations and
// returns a command that exits 0. Restored automatically at test cleanup.
func mockNVMeExec(t *testing.T) *[]recordedCmd {
	t.Helper()
	var calls []recordedCmd
	orig := nvmeExecCommand
	nvmeExecCommand = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		calls = append(calls, recordedCmd{name: name, args: args})
		return exec.CommandContext(ctx, "true")
	}
	t.Cleanup(func() { nvmeExecCommand = orig })
	return &calls
}

func hasFlag(args []string, flag, val string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == val {
			return true
		}
	}
	return false
}

func hasArg(args []string, a string) bool {
	for _, v := range args {
		if v == a {
			return true
		}
	}
	return false
}

func TestParseNVMeOFConfig(t *testing.T) {
	publish := map[string]string{
		PublishContextNVMeSubNQN:    "nqn.2011-06.com.truenas:csi-pvc-abc",
		PublishContextNVMePortAddr:  "10.0.0.136",
		PublishContextNVMePortSvcID: "4420",
		PublishContextNVMeNSUUID:    "e1f2a3b4-0000-1111-2222-333344445555",
		// transport intentionally omitted to exercise the default
	}
	volume := map[string]string{
		paramNVMeOFHostNQN:       "nqn.2014-08.org.nvmexpress:uuid:node-a",
		paramNVMeOFDHCHAPKey:     "DHHC-1:01:key:",
		paramNVMeOFDHCHAPCtrlKey: "DHHC-1:01:ctrl:",
	}

	cfg := parseNVMeOFConfig(publish, volume)
	if cfg.SubNQN != "nqn.2011-06.com.truenas:csi-pvc-abc" {
		t.Errorf("SubNQN = %q", cfg.SubNQN)
	}
	if cfg.PortAddr != "10.0.0.136" || cfg.PortSvcID != "4420" {
		t.Errorf("portal = %q:%q", cfg.PortAddr, cfg.PortSvcID)
	}
	if cfg.Transport != defaultNVMeOFTransport {
		t.Errorf("Transport = %q, want default %q", cfg.Transport, defaultNVMeOFTransport)
	}
	if cfg.NamespaceUUID != "e1f2a3b4-0000-1111-2222-333344445555" {
		t.Errorf("NamespaceUUID = %q", cfg.NamespaceUUID)
	}
	if cfg.HostNQN != "nqn.2014-08.org.nvmexpress:uuid:node-a" {
		t.Errorf("HostNQN = %q", cfg.HostNQN)
	}
	if cfg.DHCHAPKey != "DHHC-1:01:key:" || cfg.DHCHAPCtrlKey != "DHHC-1:01:ctrl:" {
		t.Errorf("dhchap = %q / %q", cfg.DHCHAPKey, cfg.DHCHAPCtrlKey)
	}
}

func TestNVMeControllerForDevice(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"/dev/nvme0n1", "/dev/nvme0"},
		{"/dev/nvme10n2", "/dev/nvme10"},
		{"/dev/nvme0n1p1", ""}, // partition suffix is not a plain namespace device
		{"/dev/sda", ""},
		{"/dev/nvme0", ""},
		{"", ""},
	}
	for _, tt := range tests {
		if got := nvmeControllerForDevice(tt.in); got != tt.want {
			t.Errorf("nvmeControllerForDevice(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestIsAllDigits(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"0", true},
		{"123", true},
		{"", false},
		{"1a", false},
		{"n1", false},
	}
	for _, tt := range tests {
		if got := isAllDigits(tt.in); got != tt.want {
			t.Errorf("isAllDigits(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestNVMeByIDCandidates(t *testing.T) {
	// Mixed-case UUID yields both the original and a lowercased candidate.
	got := nvmeByIDCandidates("ABCdef-12")
	if len(got) != 2 {
		t.Fatalf("expected 2 candidates, got %v", got)
	}
	if got[0] != nvmeByIDPrefix+"ABCdef-12" {
		t.Errorf("candidate[0] = %q", got[0])
	}
	if got[1] != nvmeByIDPrefix+"abcdef-12" {
		t.Errorf("candidate[1] = %q", got[1])
	}

	// Already-lowercase UUID yields a single candidate.
	if got := nvmeByIDCandidates("abc-12"); len(got) != 1 {
		t.Errorf("expected 1 candidate for lowercase uuid, got %v", got)
	}
}

func TestNVMeConnectorPath(t *testing.T) {
	p := nvmeConnectorPath("tank/k8s/pvc-abc")
	if got := p[len(p)-len(nvmeConnectorExt):]; got != nvmeConnectorExt {
		t.Errorf("connector path %q does not end with %q", p, nvmeConnectorExt)
	}
	// Slashes from the volume ID must be sanitized out of the filename.
	base := p[len(connectorDir)+1:]
	if containsSlash(base) {
		t.Errorf("connector filename %q contains a slash", base)
	}
}

func containsSlash(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return true
		}
	}
	return false
}

func TestNVMeConnectArgs_WithDHCHAP(t *testing.T) {
	calls := mockNVMeExec(t)
	h := &NVMeOFHandler{log: logr.Discard()}

	cfg := &NVMeOFConfig{
		SubNQN:        "nqn.x:vol",
		PortAddr:      "10.0.0.136",
		PortSvcID:     "4420",
		Transport:     "tcp",
		HostNQN:       "nqn.host",
		DHCHAPKey:     "DHHC-1:01:key:",
		DHCHAPCtrlKey: "DHHC-1:01:ctrl:",
	}
	if err := h.nvmeConnect(context.Background(), cfg); err != nil {
		t.Fatalf("nvmeConnect: %v", err)
	}

	if len(*calls) != 1 || (*calls)[0].name != "nvme" {
		t.Fatalf("expected one nvme call, got %+v", *calls)
	}
	args := (*calls)[0].args
	if len(args) == 0 || args[0] != "connect" {
		t.Fatalf("expected connect subcommand, got %v", args)
	}
	for _, fv := range [][2]string{
		{"-t", "tcp"}, {"-n", "nqn.x:vol"}, {"-a", "10.0.0.136"},
		{"-s", "4420"}, {"-l", nvmeCtrlLossTmo}, {"--hostnqn", "nqn.host"},
		{"--dhchap-secret", "DHHC-1:01:key:"}, {"--dhchap-ctrl-secret", "DHHC-1:01:ctrl:"},
	} {
		if !hasFlag(args, fv[0], fv[1]) {
			t.Errorf("missing %s %s in args %v", fv[0], fv[1], args)
		}
	}
}

func TestNVMeConnectArgs_NoAuth(t *testing.T) {
	calls := mockNVMeExec(t)
	h := &NVMeOFHandler{log: logr.Discard()}

	cfg := &NVMeOFConfig{SubNQN: "nqn.x:vol", PortAddr: "10.0.0.136", PortSvcID: "4420", Transport: "tcp"}
	if err := h.nvmeConnect(context.Background(), cfg); err != nil {
		t.Fatalf("nvmeConnect: %v", err)
	}

	args := (*calls)[0].args
	for _, flag := range []string{"--hostnqn", "--dhchap-secret", "--dhchap-ctrl-secret"} {
		if hasArg(args, flag) {
			t.Errorf("did not expect %s in no-auth args %v", flag, args)
		}
	}
}

func TestNVMeDisconnectArgs(t *testing.T) {
	calls := mockNVMeExec(t)
	h := &NVMeOFHandler{log: logr.Discard()}

	if err := h.nvmeDisconnect(context.Background(), "nqn.x:vol"); err != nil {
		t.Fatalf("nvmeDisconnect: %v", err)
	}
	args := (*calls)[0].args
	if !(len(args) >= 1 && args[0] == "disconnect") || !hasFlag(args, "-n", "nqn.x:vol") {
		t.Errorf("unexpected disconnect args %v", args)
	}
}

func TestNVMeRescanNamespace(t *testing.T) {
	calls := mockNVMeExec(t)
	h := &NVMeOFHandler{log: logr.Discard()}

	if err := h.nvmeRescanNamespace(context.Background(), "/dev/nvme3n1"); err != nil {
		t.Fatalf("nvmeRescanNamespace: %v", err)
	}
	args := (*calls)[0].args
	if len(args) != 2 || args[0] != "ns-rescan" || args[1] != "/dev/nvme3" {
		t.Errorf("expected [ns-rescan /dev/nvme3], got %v", args)
	}

	// A non-namespace device path is rejected without invoking nvme.
	*calls = nil
	if err := h.nvmeRescanNamespace(context.Background(), "/dev/sda"); err == nil {
		t.Error("expected error for non-NVMe device path")
	}
	if len(*calls) != 0 {
		t.Errorf("expected no nvme call for invalid device, got %v", *calls)
	}
}

func TestLoadKernelModules(t *testing.T) {
	calls := mockNVMeExec(t)
	h := &NVMeOFHandler{log: logr.Discard()}

	if err := h.loadKernelModules(context.Background()); err != nil {
		t.Fatalf("loadKernelModules: %v", err)
	}
	if len(*calls) != 2 {
		t.Fatalf("expected 2 modprobe calls, got %v", *calls)
	}
	for i, mod := range []string{"nvme_tcp", "nvme_fabrics"} {
		c := (*calls)[i]
		if c.name != "modprobe" || len(c.args) != 1 || c.args[0] != mod {
			t.Errorf("call %d = %s %v, want modprobe %s", i, c.name, c.args, mod)
		}
	}
}

func TestLoadConnector_Missing(t *testing.T) {
	h := &NVMeOFHandler{log: logr.Discard()}
	if info := h.loadConnector("nonexistent-volume-xyz"); info != nil {
		t.Errorf("expected nil for missing connector, got %+v", info)
	}
}
