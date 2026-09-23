package sanity

import (
	"context"
	"os"
	"testing"
	"time"

	"k8s.io/klog/v2/textlogger"

	"github.com/truenas/truenas-csi/pkg/client"
)

// The iSCSI CHAP sanity tests run the full suite against a real TrueNAS, and every
// NodeStageVolume in them logs into a target that requires CHAP, so a node that does
// not present the credentials fails the run. The sanity suite runs once per process,
// so run each test on its own:
//
//	sudo -E go test ./test/sanity -run '^TestSanityISCSICHAP$' -v
//	sudo -E go test ./test/sanity -run '^TestSanityISCSIMutualCHAP$' -v
//
// With TRUENAS_TEST_ISCSI_DISCOVERY_AUTH=true, any iSCSI sanity test first adds an
// auth entry that turns on mutual discovery authentication for the whole appliance,
// and removes it afterwards, to show that volumes do not depend on discovery. Don't
// set it against an appliance where other initiators rely on discovery.

// CHAP secrets must be 12 to 16 characters, and a peer secret must differ from the
// secret it answers.
const (
	chapTestUser       = "csi-sanity"
	chapTestSecret     = "csisanitysecr1"
	chapTestPeerUser   = "csi-sanity-target"
	chapTestPeerSecret = "csisanitypeer2"

	discoveryAuthTestUser       = "csi-sanity-disc"
	discoveryAuthTestSecret     = "csisanitydisc1"
	discoveryAuthTestPeerUser   = "csi-sanity-discp"
	discoveryAuthTestPeerSecret = "csisanitydisc2"

	discoveryAuthEnv = "TRUENAS_TEST_ISCSI_DISCOVERY_AUTH"
)

// TestSanityISCSICHAP runs the iSCSI sanity suite with one-way CHAP.
func TestSanityISCSICHAP(t *testing.T) {
	runISCSISanity(t, map[string]string{
		"protocol":         "iscsi",
		"iscsi.chapUser":   chapTestUser,
		"iscsi.chapSecret": chapTestSecret,
	})
}

// TestSanityISCSIMutualCHAP runs the iSCSI sanity suite with mutual CHAP, where the
// target must also authenticate to the node. Every volume gets mutual credentials,
// which TrueNAS used to refuse for all but the first.
func TestSanityISCSIMutualCHAP(t *testing.T) {
	runISCSISanity(t, map[string]string{
		"protocol":             "iscsi",
		"iscsi.chapUser":       chapTestUser,
		"iscsi.chapSecret":     chapTestSecret,
		"iscsi.chapPeerUser":   chapTestPeerUser,
		"iscsi.chapPeerSecret": chapTestPeerSecret,
	})
}

// withISCSIDiscoveryAuth turns on mutual discovery authentication on the appliance
// for the rest of the test, when discoveryAuthEnv asks for it.
func withISCSIDiscoveryAuth(t *testing.T) {
	if os.Getenv(discoveryAuthEnv) != "true" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c := client.New(client.Config{
		URL:                os.Getenv("TRUENAS_URL"),
		APIKey:             os.Getenv("TRUENAS_API_KEY"),
		InsecureSkipVerify: os.Getenv("TRUENAS_INSECURE_SKIP_VERIFY") == "true",
		Logger:             textlogger.NewLogger(textlogger.NewConfig()),
	})
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to TrueNAS: %v", err)
	}

	tag, err := c.GetNextISCSIAuthTag(ctx)
	if err != nil {
		t.Fatalf("Failed to pick an iSCSI auth tag: %v", err)
	}
	auth, err := c.CreateISCSIAuth(ctx, &client.ISCSIAuthCreateOptions{
		Tag:           tag,
		User:          discoveryAuthTestUser,
		Secret:        discoveryAuthTestSecret,
		PeerUser:      discoveryAuthTestPeerUser,
		PeerSecret:    discoveryAuthTestPeerSecret,
		DiscoveryAuth: client.ISCSIAuthMethodCHAPMutual,
	})
	if err != nil {
		t.Fatalf("Failed to turn on discovery authentication: %v", err)
	}
	t.Logf("Discovery authentication is on for this test (auth entry %d)", auth.ID)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := c.DeleteISCSIAuth(ctx, auth.ID); err != nil {
			t.Errorf("Failed to remove discovery auth entry %d, delete it by hand: %v", auth.ID, err)
		}
		_ = c.Close()
	})
}
