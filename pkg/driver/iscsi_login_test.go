package driver

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

// slowISCSIAdm is a stand-in iscsiadm on PATH with just enough state for a login:
// a login takes loginDelay and exits with loginExit, and a session exists once one
// has succeeded (exit 0 or 15). Every call is recorded, one line each.
type slowISCSIAdm struct {
	dir        string
	loginDelay string // seconds, as sleep takes them
	loginExit  int
	updateExit int
}

func (f slowISCSIAdm) install(t *testing.T) {
	t.Helper()
	script := `#!/bin/sh
dir="` + f.dir + `"
echo "$@" >> "$dir/calls"
case "$*" in
  *"-m iface"*) echo 'iface.transport_name = tcp' ;;
  *"-m session"*)
    if [ -f "$dir/session" ]; then cat "$dir/session"; else echo 'iscsiadm: No active sessions.' >&2; exit 21; fi ;;
  *"-o update"*) exit ` + strconv.Itoa(f.updateExit) + ` ;;
  *" -l"*)
    sleep ` + f.loginDelay + `
    code=` + strconv.Itoa(f.loginExit) + `
    if [ "$code" = 0 ] || [ "$code" = 15 ]; then
      echo "tcp: [1] 10.0.0.1:3260,1 ` + connectTestIQN + ` (non-flash)" > "$dir/session"
    fi
    if [ "$code" = 24 ]; then echo 'iscsiadm: Login failed to authenticate with target' >&2; fi
    exit "$code" ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(f.dir, "iscsiadm"), []byte(script), 0o755); err != nil {
		t.Fatalf("failed to write the stand-in iscsiadm: %v", err)
	}
	t.Setenv("PATH", f.dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func (f slowISCSIAdm) calls(t *testing.T) []string {
	t.Helper()
	out, err := os.ReadFile(filepath.Join(f.dir, "calls"))
	if err != nil {
		return nil
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

func loginTestConfig() *ISCSIConfig {
	return &ISCSIConfig{TargetPortal: connectTestPortal, TargetIQN: connectTestIQN}
}

func countCalls(calls []string, want string) int {
	n := 0
	for _, c := range calls {
		if strings.Contains(c, want) {
			n++
		}
	}
	return n
}

// Reproduces the report: a login that takes longer than csi-lib-iscsi's fixed 3
// seconds fails every time on its own, and succeeds once the driver logs in first.
func TestEnsureISCSISession_OutlastsTheLibraryLimit(t *testing.T) {
	h := &ISCSIHandler{log: logr.Discard()}

	t.Run("csi-lib-iscsi alone gives up", func(t *testing.T) {
		f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "4"}
		f.install(t)
		connector := h.buildConnector("pvc-abc", loginTestConfig())
		connector.RetryCount, connector.CheckInterval = 1, 1
		_, err := connector.Connect()
		if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
			t.Fatalf("Connect() with a 4-second login = %v, want the library's 3-second timeout", err)
		}
	})

	t.Run("logging in first", func(t *testing.T) {
		f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "4"}
		f.install(t)
		if err := h.ensureISCSISession(context.Background(), loginTestConfig(), iscsiLoginTimeout); err != nil {
			t.Fatalf("ensureISCSISession() = %v", err)
		}

		// csi-lib-iscsi then finds the session and does not log in again. Its
		// connect still fails here, on waiting for a device the stand-in never makes.
		connector := h.buildConnector("pvc-abc", loginTestConfig())
		connector.RetryCount, connector.CheckInterval = 1, 1
		_, _ = connector.Connect()
		if n := countCalls(f.calls(t), " -l"); n != 1 {
			t.Errorf("logins = %d, want 1: csi-lib-iscsi logged in again; calls:\n%s", n, strings.Join(f.calls(t), "\n"))
		}
	})
}

func TestEnsureISCSISession_ReusesAnExistingSession(t *testing.T) {
	f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "0"}
	f.install(t)
	// Listed by address although the portal below is a host name.
	if err := os.WriteFile(filepath.Join(f.dir, "session"), []byte("tcp: [1] 10.0.0.1:3260,1 "+connectTestIQN+" (non-flash)\n"), 0o600); err != nil {
		t.Fatalf("failed to write the session: %v", err)
	}

	config := loginTestConfig()
	config.TargetPortal = "truenas.internal:3260"
	if err := (&ISCSIHandler{log: logr.Discard()}).ensureISCSISession(context.Background(), config, iscsiLoginTimeout); err != nil {
		t.Fatalf("ensureISCSISession() = %v", err)
	}
	if n := countCalls(f.calls(t), " -l") + countCalls(f.calls(t), "-o new"); n != 0 {
		t.Errorf("logged in again over an existing session; calls:\n%s", strings.Join(f.calls(t), "\n"))
	}
}

func TestEnsureISCSISession_Failures(t *testing.T) {
	h := &ISCSIHandler{log: logr.Discard()}

	t.Run("login that does not finish", func(t *testing.T) {
		f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "3"}
		f.install(t)
		err := h.ensureISCSISession(context.Background(), loginTestConfig(), time.Second)
		if err == nil || !strings.Contains(err.Error(), "did not finish within") {
			t.Fatalf("ensureISCSISession() = %v, want a login timeout naming the time it took", err)
		}
		// The login may still finish; its record has to be there for the next try.
		if n := countCalls(f.calls(t), "-o delete"); n != 0 {
			t.Errorf("the node record was deleted after a timed-out login")
		}
	})

	t.Run("login refused", func(t *testing.T) {
		f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "0", loginExit: 24}
		f.install(t)
		err := h.ensureISCSISession(context.Background(), loginTestConfig(), iscsiLoginTimeout)
		if err == nil || !strings.Contains(err.Error(), "failed to authenticate") {
			t.Fatalf("ensureISCSISession() = %v, want iscsiadm's reason", err)
		}
	})

	t.Run("login already done in the background", func(t *testing.T) {
		f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "0", loginExit: 15}
		f.install(t)
		if err := h.ensureISCSISession(context.Background(), loginTestConfig(), iscsiLoginTimeout); err != nil {
			t.Errorf("ensureISCSISession() with the session already up = %v, want success", err)
		}
	})
}

func TestEnsureISCSISession_CHAP(t *testing.T) {
	config := loginTestConfig()
	config.CHAPUsername, config.CHAPPassword = "openshift", "testtesttest1"
	config.CHAPUsernameIn, config.CHAPPasswordIn = "openshift-target", "testtesttest2"
	h := &ISCSIHandler{log: logr.Discard()}

	t.Run("credentials are set before the login", func(t *testing.T) {
		f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "0"}
		f.install(t)
		if err := h.ensureISCSISession(context.Background(), config, iscsiLoginTimeout); err != nil {
			t.Fatalf("ensureISCSISession() = %v", err)
		}
		calls := f.calls(t)
		update, login := indexOf(calls, "-o update"), indexOf(calls, " -l")
		if update < 0 || login < 0 || update > login {
			t.Fatalf("want the CHAP update before the login; calls:\n%s", strings.Join(calls, "\n"))
		}
		for _, want := range []string{"node.session.auth.username -v openshift ", "node.session.auth.password_in -v testtesttest2"} {
			if !strings.Contains(calls[update], want) {
				t.Errorf("CHAP update lacks %q: %s", want, calls[update])
			}
		}
	})

	t.Run("a failed update keeps the secrets out of the error", func(t *testing.T) {
		f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "0", updateExit: 1}
		f.install(t)
		err := h.ensureISCSISession(context.Background(), config, iscsiLoginTimeout)
		if err == nil {
			t.Fatal("ensureISCSISession() succeeded with the CHAP update failing")
		}
		if strings.Contains(err.Error(), "testtesttest") {
			t.Errorf("error exposes a CHAP secret: %v", err)
		}
	})
}

// Stage has to log in before handing over to csi-lib-iscsi. Without that, the
// 4-second login here would end in the library's 3-second timeout.
func TestStage_LogsInBeforeTheLibrary(t *testing.T) {
	useTempConnectorDir(t)
	f := slowISCSIAdm{dir: t.TempDir(), loginDelay: "4"}
	f.install(t)

	h := &ISCSIHandler{log: logr.Discard()}
	_, err := h.Stage(context.Background(), &StageRequest{
		VolumeID:    "tank/pvc-abc",
		StagingPath: filepath.Join(t.TempDir(), "staging"),
		PublishContext: map[string]string{
			PublishContextTargetPortal: connectTestPortal,
			PublishContextTargetIQN:    connectTestIQN,
			PublishContextLUN:          "0",
		},
		IsBlockVolume: true,
	})

	// The stand-in never makes a device, so staging still fails, but past the login.
	if err == nil || strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("Stage() = %v, want it past the login and waiting on the device", err)
	}
	if n := countCalls(f.calls(t), " -l"); n != 1 {
		t.Errorf("logins = %d, want 1; calls:\n%s", n, strings.Join(f.calls(t), "\n"))
	}
}
