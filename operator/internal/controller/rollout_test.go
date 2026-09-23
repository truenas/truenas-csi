package controller

import (
	"context"
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

func rolloutTestCR() *csiv1alpha1.TrueNASCSI {
	return &csiv1alpha1.TrueNASCSI{
		ObjectMeta: metav1.ObjectMeta{Name: "truenas"},
		Spec: csiv1alpha1.TrueNASCSISpec{
			TrueNASURL:        "wss://truenas.example.com",
			CredentialsSecret: "truenas-credentials",
			DefaultPool:       "tank",
			NFSServer:         "10.0.0.1",
			ISCSIPortal:       "10.0.0.1:3260",
			NVMeOFPortal:      "10.0.0.1:4420",
		},
	}
}

// Every value the containers read from the ConfigMap has to move the hash, or
// changing it in the CR would leave the pods running on the old one.
func TestHashWorkloadConfigFollowsEveryConfigMapKey(t *testing.T) {
	apiKey := []byte("1-abcdef")
	base := configMapData(rolloutTestCR())
	baseHash := hashWorkloadConfig(base, apiKey)

	if again := hashWorkloadConfig(configMapData(rolloutTestCR()), apiKey); again != baseHash {
		t.Fatalf("hash of an unchanged configuration moved: %s then %s", baseHash, again)
	}

	for key := range base {
		t.Run(key, func(t *testing.T) {
			changed := maps.Clone(base)
			changed[key] += "-changed"
			if hashWorkloadConfig(changed, apiKey) == baseHash {
				t.Errorf("changing %q did not change the hash", key)
			}
		})
	}
}

func TestHashWorkloadConfigFollowsAPIKey(t *testing.T) {
	config := configMapData(rolloutTestCR())
	if hashWorkloadConfig(config, []byte("1-old")) == hashWorkloadConfig(config, []byte("1-new")) {
		t.Error("rotating the API key did not change the hash")
	}
}

// Quoting keeps a value from standing in for a separator. Written unquoted, these
// two configurations serialize to the same bytes.
func TestHashWorkloadConfigSeparatesValues(t *testing.T) {
	a := map[string]string{"iscsiPortal": "x\nnfsServer=y", "nfsServer": "z"}
	b := map[string]string{"iscsiPortal": "x", "nfsServer": "y\nnfsServer=z"}
	if hashWorkloadConfig(a, nil) == hashWorkloadConfig(b, nil) {
		t.Error("two different configurations hashed the same")
	}
}

func TestPodTemplateAnnotations(t *testing.T) {
	const restartedAt = "kubectl.kubernetes.io/restartedAt"
	existing := map[string]string{
		restartedAt:          "2026-09-23T00:00:00Z",
		ConfigHashAnnotation: "stale",
	}

	got := podTemplateAnnotations(existing, "fresh")

	if got[ConfigHashAnnotation] != "fresh" {
		t.Errorf("%s = %q, want %q", ConfigHashAnnotation, got[ConfigHashAnnotation], "fresh")
	}
	// Dropping this would undo a manual rollout restart and roll the pods again.
	if got[restartedAt] != existing[restartedAt] {
		t.Errorf("%s = %q, want it kept as %q", restartedAt, got[restartedAt], existing[restartedAt])
	}
	if existing[ConfigHashAnnotation] != "stale" {
		t.Error("the template's existing annotations were modified in place")
	}

	if got := podTemplateAnnotations(nil, "fresh"); got[ConfigHashAnnotation] != "fresh" {
		t.Errorf("on a new template %s = %q, want %q", ConfigHashAnnotation, got[ConfigHashAnnotation], "fresh")
	}
}

func TestRequestsForCredentialsSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := csiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to build scheme: %v", err)
	}

	defaultNamespace := rolloutTestCR()
	defaultNamespace.Name = "default-namespace"

	customNamespace := rolloutTestCR()
	customNamespace.Name = "custom-namespace"
	customNamespace.Spec.Namespace = "storage"

	otherSecret := rolloutTestCR()
	otherSecret.Name = "other-secret"
	otherSecret.Spec.CredentialsSecret = "another-key"

	r := &TrueNASCSIReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(defaultNamespace, customNamespace, otherSecret).Build(),
		Scheme: scheme,
	}

	tests := []struct {
		name      string
		namespace string
		secret    string
		want      []string
	}{
		{"secret in the default namespace", CSINamespace, "truenas-credentials", []string{"default-namespace"}},
		{"secret in a custom namespace", "storage", "truenas-credentials", []string{"custom-namespace"}},
		{"secret with another name", CSINamespace, "another-key", []string{"other-secret"}},
		{"unrelated secret", CSINamespace, "something-else", nil},
		{"same name in another namespace", "elsewhere", "truenas-credentials", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: tt.secret, Namespace: tt.namespace}}

			var want []reconcile.Request
			for _, name := range tt.want {
				want = append(want, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
			}

			got := r.requestsForCredentialsSecret(context.Background(), secret)
			if len(got) != len(want) {
				t.Fatalf("requests = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("requests = %v, want %v", got, want)
				}
			}
		})
	}
}
