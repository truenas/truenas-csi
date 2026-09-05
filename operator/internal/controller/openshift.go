package controller

import (
	"context"
	"os"
	"fmt"
	"path/filepath"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// This function provides a reconciler method to check if the Operator is running on OpenShift.
// This is simply a wrapper around the checkOpenShift function, which can be used in other contexts as well.
func (r *TrueNASCSIReconciler) checkOpenShift(ctx context.Context) (bool, error) {
	return checkOpenShift(ctx)
}

func checkOpenShift(ctx context.Context) (bool, error) {
	log := logf.FromContext(ctx)
	var runsOnOpenShift bool

	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := filepath.Join(
			os.Getenv("HOME"), ".kube", "config",
		)
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return runsOnOpenShift, err
		}
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err == nil {
		_, err := discoveryClient.ServerVersion()
		if err == nil {
			apiGroup, _, err := discoveryClient.ServerGroupsAndResources()
			if err == nil {
				for i := 0; i < len(apiGroup); i++ {
					if apiGroup[i].Name == "config.openshift.io" {
						log.V(1).Info("OpenShift detected", "apiGroup", apiGroup[i].Name)
						runsOnOpenShift = true
					}
				}
			}
		}
	}
	return runsOnOpenShift, nil
}

// sccGVK is the GroupVersionKind for OpenShift SecurityContextConstraints.
var sccGVK = schema.GroupVersionKind{Group: "security.openshift.io", Version: "v1", Kind: "SecurityContextConstraints"}

// sccDefinition describes an SCC the operator manages for one CSI workload.
type sccDefinition struct {
	name      string
	component string
	fields    map[string]any
}

// sccDefinitions returns the SecurityContextConstraints the CSI workloads need,
// with each SCC granting only this driver's ServiceAccount in the given namespace.
// The node SCC is privileged (hostPath/hostNetwork/privileged mount operations);
// the controller SCC is unprivileged but RunAsAny, because the controller pods run
// as a fixed non-root UID that falls outside a namespace's OpenShift-assigned UID
// range and is therefore rejected by the default restricted-v2 SCC.
func sccDefinitions(namespace string) []sccDefinition {
	saUser := func(sa string) []any {
		return []any{fmt.Sprintf("system:serviceaccount:%s:%s", namespace, sa)}
	}
	// Strategies that leave the mapped user/SELinux/groups unconstrained so the
	// workloads' own securityContext is honored.
	common := func(m map[string]any, sa string) map[string]any {
		m["runAsUser"] = map[string]any{"type": "RunAsAny"}
		m["seLinuxContext"] = map[string]any{"type": "MustRunAs"}
		m["fsGroup"] = map[string]any{"type": "RunAsAny"}
		m["supplementalGroups"] = map[string]any{"type": "RunAsAny"}
		m["users"] = saUser(sa)
		return m
	}

	return []sccDefinition{
		{
			name:      NodeSCCName,
			component: "node",
			fields: common(map[string]any{
				"allowPrivilegedContainer": true,
				"allowHostIPC":             true,
				"allowHostNetwork":         true,
				"allowHostPID":             true,
				"allowHostPorts":           true,
				"allowHostDirVolumePlugin": true,
				"allowedCapabilities":      []any{"SYS_ADMIN"},
				"volumes":                  []any{"configMap", "downwardAPI", "emptyDir", "hostPath", "persistentVolumeClaim", "projected", "secret"},
			}, NodeServiceAccount),
		},
		{
			name:      ControllerSCCName,
			component: "controller",
			fields: common(map[string]any{
				"allowPrivilegedContainer": false,
				"allowHostNetwork":         false,
				"allowHostPID":             false,
				"allowHostPorts":           false,
				"allowHostDirVolumePlugin": false,
				"volumes":                  []any{"configMap", "downwardAPI", "emptyDir", "projected", "secret"},
			}, ControllerServiceAccount),
		},
	}
}

// reconcileSCC creates the OpenShift SecurityContextConstraints the CSI workloads
// need (see sccDefinitions). On clusters without the security.openshift.io API
// (plain Kubernetes) this is a no-op.
func (r *TrueNASCSIReconciler) reconcileSCC(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	log := logf.FromContext(ctx)

	for _, def := range sccDefinitions(getNamespace(csi)) {
		scc := &unstructured.Unstructured{}
		scc.SetGroupVersionKind(sccGVK)
		scc.SetName(def.name)

		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, scc, func() error {
			scc.SetLabels(ComponentLabels(def.component))
			for k, v := range def.fields {
				scc.Object[k] = v
			}
			return nil
		})
		if err != nil {
			if meta.IsNoMatchError(err) {
				log.V(1).Info("SecurityContextConstraints API not present; skipping SCC reconciliation (not OpenShift)", "scc", def.name)
				return nil
			}
			return err
		}
	}

	return nil
}