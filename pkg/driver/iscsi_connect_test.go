package driver

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
)

const (
	connectTestIQN    = "iqn.2005-10.org.freenas.ctl:csi-pvc-abc"
	connectTestPortal = "10.0.0.1:3260"
)

// iscsiadmCalls connects with the connector buildConnector makes for config, against
// a stand-in iscsiadm that records its arguments, and returns one line per call.
// The connect itself fails once no device appears, which is past everything this
// looks at.
func iscsiadmCalls(t *testing.T, config *ISCSIConfig) []string {
	t.Helper()
	dir := t.TempDir()
	logFile := filepath.Join(dir, "calls")
	script := "#!/bin/sh\n" +
		"echo \"$@\" >> " + logFile + "\n" +
		"case \"$*\" in *'-m iface'*) echo 'iface.transport_name = tcp';; esac\n"
	if err := os.WriteFile(filepath.Join(dir, "iscsiadm"), []byte(script), 0o755); err != nil {
		t.Fatalf("failed to write the stand-in iscsiadm: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	config.TargetIQN, config.TargetPortal = connectTestIQN, connectTestPortal
	connector := (&ISCSIHandler{log: logr.Discard()}).buildConnector("pvc-abc", config)
	connector.RetryCount, connector.CheckInterval = 1, 1
	_, _ = connector.Connect()

	out, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("iscsiadm was never called: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

func indexOf(calls []string, want string) int {
	return slices.IndexFunc(calls, func(c string) bool { return strings.Contains(c, want) })
}

// A CHAP target rejects a login that presents no credentials. The node record must
// carry them, mutual ones included, before the login.
func TestISCSIConnect_WritesSessionCHAPBeforeLogin(t *testing.T) {
	calls := iscsiadmCalls(t, &ISCSIConfig{
		CHAPUsername:   "openshift",
		CHAPPassword:   "testtesttest1",
		CHAPUsernameIn: "openshift-target",
		CHAPPasswordIn: "testtesttest2",
	})

	node := "-m node -T " + connectTestIQN + " -p " + connectTestPortal
	create := indexOf(calls, node+" -I default -o new")
	update := indexOf(calls, node+" -o update -n node.session.auth.authmethod -v CHAP")
	login := indexOf(calls, node+" -l")
	if create < 0 || update < 0 || login < 0 || !(create < update && update < login) {
		t.Fatalf("want the node record created, given session CHAP, then logged in; iscsiadm calls:\n%s", strings.Join(calls, "\n"))
	}

	for _, want := range []string{
		"-n node.session.auth.username -v openshift ",
		"-n node.session.auth.password -v testtesttest1",
		"-n node.session.auth.username_in -v openshift-target",
		"-n node.session.auth.password_in -v testtesttest2",
	} {
		if !strings.Contains(calls[update], want) {
			t.Errorf("session CHAP update lacks %q: %s", want, calls[update])
		}
	}
	assertNoDiscovery(t, calls)
}

// Without CHAP the node record is still created directly, not through discovery.
func TestISCSIConnect_WithoutCHAPCreatesTheRecordDirectly(t *testing.T) {
	calls := iscsiadmCalls(t, &ISCSIConfig{})

	node := "-m node -T " + connectTestIQN + " -p " + connectTestPortal
	create := indexOf(calls, node+" -I default -o new")
	login := indexOf(calls, node+" -l")
	if create < 0 || login < 0 || create > login {
		t.Fatalf("want the node record created, then logged in; iscsiadm calls:\n%s", strings.Join(calls, "\n"))
	}
	if i := indexOf(calls, "node.session.auth"); i >= 0 {
		t.Errorf("session CHAP set for a volume without it: %s", calls[i])
	}
	assertNoDiscovery(t, calls)
}

// SendTargets discovery fails on an appliance that requires discovery
// authentication, which TrueNAS turns on for every initiator once any auth entry
// asks for it, so the node must never depend on it.
func assertNoDiscovery(t *testing.T, calls []string) {
	t.Helper()
	for _, c := range calls {
		if strings.Contains(c, "discovery") {
			t.Errorf("iscsiadm ran discovery: %s", c)
		}
	}
}
