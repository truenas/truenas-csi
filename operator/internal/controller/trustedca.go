package controller

import (
	"context"
	"fmt"
	"path"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

// spec.trustedCA reaches the driver as a file: the ConfigMap key is mounted into
// the controller and node containers, both of which connect to TrueNAS, and
// EnvCABundle names the file. The driver reads it once at startup, so its
// contents are part of the config hash, and a change to them rolls the pods.

// trustedCAKey returns the ConfigMap key spec.trustedCA reads.
func trustedCAKey(ref *csiv1alpha1.ConfigMapKeyReference) string {
	if ref.Key != "" {
		return ref.Key
	}
	return DefaultTrustedCAKey
}

// trustedCAVolumes returns the volume carrying spec.trustedCA, or none when unset.
func trustedCAVolumes(csi *csiv1alpha1.TrueNASCSI) []corev1.Volume {
	ref := csi.Spec.TrustedCA
	if ref == nil {
		return nil
	}
	return []corev1.Volume{{
		Name: VolumeTrustedCA,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
				Items:                []corev1.KeyToPath{{Key: trustedCAKey(ref), Path: TrustedCAFileName}},
			},
		},
	}}
}

// trustedCAVolumeMounts returns the driver container's mount for spec.trustedCA.
func trustedCAVolumeMounts(csi *csiv1alpha1.TrueNASCSI) []corev1.VolumeMount {
	if csi.Spec.TrustedCA == nil {
		return nil
	}
	return []corev1.VolumeMount{{Name: VolumeTrustedCA, MountPath: TrustedCAMountDir, ReadOnly: true}}
}

// trustedCAEnvVars returns the variable pointing the driver at the mounted bundle.
func trustedCAEnvVars(csi *csiv1alpha1.TrueNASCSI) []corev1.EnvVar {
	if csi.Spec.TrustedCA == nil {
		return nil
	}
	return []corev1.EnvVar{{Name: EnvCABundle, Value: path.Join(TrustedCAMountDir, TrustedCAFileName)}}
}

// trustedCABundle returns the certificates spec.trustedCA names, or nil when it is
// unset. A missing ConfigMap or key is an error, so the resource reports it rather
// than leaving the pods unable to start.
func (r *TrueNASCSIReconciler) trustedCABundle(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) ([]byte, error) {
	ref := csi.Spec.TrustedCA
	if ref == nil {
		return nil, nil
	}

	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: ref.Name, Namespace: getNamespace(csi)}
	if err := r.Get(ctx, key, cm); err != nil {
		return nil, fmt.Errorf("get trusted CA ConfigMap %s: %w", key, err)
	}

	caKey := trustedCAKey(ref)
	if data, ok := cm.Data[caKey]; ok && data != "" {
		return []byte(data), nil
	}
	if data, ok := cm.BinaryData[caKey]; ok && len(data) > 0 {
		return data, nil
	}
	return nil, fmt.Errorf("trusted CA ConfigMap %s has no key %q", key, caKey)
}

// requestsForTrustedCA maps a ConfigMap to the TrueNASCSI resources that trust
// the certificates in it, so a changed bundle rolls the pods promptly.
func (r *TrueNASCSIReconciler) requestsForTrustedCA(ctx context.Context, cm client.Object) []reconcile.Request {
	return r.requestsReferencing(ctx, cm, func(csi *csiv1alpha1.TrueNASCSI) string {
		if csi.Spec.TrustedCA == nil {
			return ""
		}
		return csi.Spec.TrustedCA.Name
	})
}
