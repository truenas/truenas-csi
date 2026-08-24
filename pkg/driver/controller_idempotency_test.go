package driver

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// A repeated CreateVolume for a snapshot- or volume-backed request must echo the
// content source. The external-provisioner rejects a source-backed request answered
// without one and calls DeleteVolume to clean up, which deletes the clone it just
// asked for while the CO is already publishing it.
func TestIdempotentVolumeResponse_EchoesContentSource(t *testing.T) {
	snapshotSource := &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "tank/vol@snap-1"},
		},
	}
	volumeSource := &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "tank/pvc-source"},
		},
	}

	tests := []struct {
		name   string
		source *csi.VolumeContentSource
	}{
		{"snapshot source", snapshotSource},
		{"volume source", volumeSource},
		{"no source", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			volumeContext := map[string]string{PublishContextProtocol: ProtocolISCSI}
			resp := idempotentVolumeResponse("tank/pvc-abc", 5*GiB, volumeContext, tt.source)

			if resp.Volume.VolumeId != "tank/pvc-abc" {
				t.Errorf("VolumeId = %q, want %q", resp.Volume.VolumeId, "tank/pvc-abc")
			}
			if resp.Volume.CapacityBytes != 5*GiB {
				t.Errorf("CapacityBytes = %d, want %d", resp.Volume.CapacityBytes, 5*GiB)
			}
			if resp.Volume.VolumeContext[PublishContextProtocol] != ProtocolISCSI {
				t.Errorf("VolumeContext = %v, want the protocol preserved", resp.Volume.VolumeContext)
			}

			if tt.source == nil {
				if resp.Volume.ContentSource != nil {
					t.Errorf("ContentSource = %v, want none for a sourceless request", resp.Volume.ContentSource)
				}
				return
			}

			if resp.Volume.ContentSource == nil {
				t.Fatal("ContentSource is missing, the provisioner would delete this volume")
			}
			if got, want := resp.Volume.ContentSource.GetSnapshot().GetSnapshotId(), tt.source.GetSnapshot().GetSnapshotId(); got != want {
				t.Errorf("snapshot ID = %q, want %q", got, want)
			}
			if got, want := resp.Volume.ContentSource.GetVolume().GetVolumeId(), tt.source.GetVolume().GetVolumeId(); got != want {
				t.Errorf("source volume ID = %q, want %q", got, want)
			}
		})
	}
}
