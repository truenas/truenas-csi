package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

// The CSI containers take their TrueNAS settings from the ConfigMap and the API
// key from the credentials Secret, both through environment variables. Those are
// resolved once, when a container starts, so updating either object leaves the
// running pods on the old values until something changes their template. The
// config hash annotation is that change: it moves exactly when the values do.

// workloadConfigHash returns the config hash for the CSI workloads of csi.
func (r *TrueNASCSIReconciler) workloadConfigHash(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) (string, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: csi.Spec.CredentialsSecret, Namespace: getNamespace(csi)}
	if err := r.Get(ctx, key, secret); err != nil {
		return "", fmt.Errorf("get credentials secret %s: %w", key, err)
	}
	caBundle, err := r.trustedCABundle(ctx, csi)
	if err != nil {
		return "", err
	}
	return hashWorkloadConfig(configMapData(csi), secret.Data[CredentialsSecretKey], caBundle), nil
}

// hashWorkloadConfig fingerprints the ConfigMap data, API key and trusted CA
// bundle the CSI containers read. It depends only on the values, so re-applying an
// unchanged configuration hashes the same and rolls nothing. A SHA-256 of the API
// key reveals nothing usable about the key, only whether it changed.
func hashWorkloadConfig(config map[string]string, apiKey, caBundle []byte) string {
	h := sha256.New()
	// Keys are written in sorted order so equal configuration always hashes the
	// same, and values are quoted so none can pass for a separator.
	for _, key := range slices.Sorted(maps.Keys(config)) {
		fmt.Fprintf(h, "%s=%q\n", key, config[key])
	}
	fmt.Fprintf(h, "%s=%q\n", CredentialsSecretKey, apiKey)
	if len(caBundle) > 0 {
		fmt.Fprintf(h, "%s=%q\n", EnvCABundle, caBundle)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// podTemplateAnnotations returns the pod template annotations with the config hash
// set. Annotations already on the template are kept: kubectl rollout restart works
// by adding one, and dropping it would roll the pods a second time.
func podTemplateAnnotations(existing map[string]string, configHash string) map[string]string {
	annotations := make(map[string]string, len(existing)+1)
	maps.Copy(annotations, existing)
	annotations[ConfigHashAnnotation] = configHash
	return annotations
}

// requestsForCredentialsSecret maps a Secret to the TrueNASCSI resources that read
// their API key from it, so rotating the key rolls the pods promptly rather than at
// the next periodic reconcile.
func (r *TrueNASCSIReconciler) requestsForCredentialsSecret(ctx context.Context, secret client.Object) []reconcile.Request {
	return r.requestsReferencing(ctx, secret, func(csi *csiv1alpha1.TrueNASCSI) string {
		return csi.Spec.CredentialsSecret
	})
}

// requestsReferencing maps obj to the TrueNASCSI resources whose driver namespace
// it is in and that name it through referenced.
func (r *TrueNASCSIReconciler) requestsReferencing(ctx context.Context, obj client.Object, referenced func(*csiv1alpha1.TrueNASCSI) string) []reconcile.Request {
	list := &csiv1alpha1.TrueNASCSIList{}
	if err := r.List(ctx, list); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list TrueNASCSI resources for a change to an object they reference",
			"object", client.ObjectKeyFromObject(obj))
		return nil
	}

	var requests []reconcile.Request
	for i := range list.Items {
		csi := &list.Items[i]
		if referenced(csi) == obj.GetName() && getNamespace(csi) == obj.GetNamespace() {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: csi.Name}})
		}
	}
	return requests
}
