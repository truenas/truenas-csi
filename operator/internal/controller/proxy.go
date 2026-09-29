package controller

import (
	"maps"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

// With spec.useClusterProxy, the operator publishes its own proxy settings in the
// driver ConfigMap, and the driver containers read them back into HTTP_PROXY,
// HTTPS_PROXY and NO_PROXY. They are in the config hash like every other setting,
// so a changed cluster proxy rolls the pods once OLM restarts the operator with it.

// desiredConfigMapData returns everything the operator publishes in the driver
// ConfigMap: the settings from the resource and, when it asks for them, the
// operator's proxy settings.
func (r *TrueNASCSIReconciler) desiredConfigMapData(csi *csiv1alpha1.TrueNASCSI) map[string]string {
	data := configMapData(csi)
	if csi.Spec.UseClusterProxy {
		maps.Copy(data, r.proxyConfigMapData())
	}
	return data
}

// proxyConfigMapData returns the operator's proxy variables under their ConfigMap
// keys, leaving out any that are unset. Each is read in upper case, then in lower
// case, the same way Go's HTTP client reads them.
func (r *TrueNASCSIReconciler) proxyConfigMapData() map[string]string {
	getenv := r.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}

	data := map[string]string{}
	for _, v := range proxyVariables {
		value := getenv(v.env)
		if value == "" {
			value = getenv(strings.ToLower(v.env))
		}
		if value != "" {
			data[v.key] = value
		}
	}
	return data
}

// proxyEnvVars returns the driver containers' proxy variables. They are always
// present and optional, so a key missing from the ConfigMap leaves its variable
// unset, and turning spec.useClusterProxy on or off changes only the ConfigMap.
func proxyEnvVars() []corev1.EnvVar {
	vars := make([]corev1.EnvVar, 0, len(proxyVariables))
	for _, v := range proxyVariables {
		vars = append(vars, configMapEnvVar(v.env, ConfigMapName, v.key, true))
	}
	return vars
}
