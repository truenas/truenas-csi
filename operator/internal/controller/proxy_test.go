package controller

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

// clusterProxyEnv stands in for the environment OLM gives the operator on a cluster
// with a cluster-wide proxy.
func clusterProxyEnv(name string) string {
	return map[string]string{
		"HTTPS_PROXY": "http://proxy.example:3128",
		"http_proxy":  "http://proxy.example:3128",
		"NO_PROXY":    ".cluster.local,.svc,10.0.0.0/8",
	}[name]
}

func TestProxyConfigMapData(t *testing.T) {
	r := &TrueNASCSIReconciler{Getenv: clusterProxyEnv}

	off := rolloutTestCR()
	for _, v := range proxyVariables {
		if _, ok := r.desiredConfigMapData(off)[v.key]; ok {
			t.Errorf("%s published without useClusterProxy: the driver would start using the cluster proxy on upgrade", v.key)
		}
	}

	on := rolloutTestCR()
	on.Spec.UseClusterProxy = true
	got := r.desiredConfigMapData(on)
	want := map[string]string{
		"httpsProxy": "http://proxy.example:3128",
		// Read in lower case when the upper-case variable is unset.
		"httpProxy": "http://proxy.example:3128",
		"noProxy":   ".cluster.local,.svc,10.0.0.0/8",
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %q, want %q", key, got[key], value)
		}
	}

	if hashWorkloadConfig(r.desiredConfigMapData(off), nil, nil) == hashWorkloadConfig(got, nil, nil) {
		t.Error("turning on useClusterProxy did not change the config hash, so the pods would not pick it up")
	}
}

func TestProxyConfigMapDataLeavesOutUnset(t *testing.T) {
	r := &TrueNASCSIReconciler{Getenv: func(string) string { return "" }}
	on := rolloutTestCR()
	on.Spec.UseClusterProxy = true
	for _, v := range proxyVariables {
		if _, ok := r.desiredConfigMapData(on)[v.key]; ok {
			t.Errorf("%s published although the operator has no %s", v.key, v.env)
		}
	}
}

// The env list is part of the pod template, so its order must not change between
// reconciles, or every reconcile would roll the pods.
func TestProxyEnvVarsKeepTheirOrder(t *testing.T) {
	first := proxyEnvVars()
	for range 20 {
		again := proxyEnvVars()
		for i := range first {
			if again[i].Name != first[i].Name {
				t.Fatalf("proxy env order changed: %v then %v", first, again)
			}
		}
	}
	for _, v := range first {
		ref := v.ValueFrom.ConfigMapKeyRef
		if ref == nil || ref.Optional == nil || !*ref.Optional {
			t.Errorf("%s is not an optional ConfigMap reference, so a missing key would stop the pod", v.Name)
		}
	}
}

var _ = Describe("Cluster proxy", func() {
	ctx := context.Background()

	It("passes the operator's proxy to both driver containers only when asked", func() {
		const (
			name      = "proxy-truenascsi"
			namespace = "truenas-csi-proxy"
		)
		DeferCleanup(deleteTrueNASCSI, ctx, name)
		createTrueNASCSI(ctx, name, namespace)

		r := &TrueNASCSIReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Getenv: clusterProxyEnv}
		reconcileResource := func() {
			GinkgoHelper()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
			Expect(err).NotTo(HaveOccurred())
		}
		configMap := func() map[string]string {
			GinkgoHelper()
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ConfigMapName, Namespace: namespace}, cm)).To(Succeed())
			return cm.Data
		}
		setUseClusterProxy := func(on bool) {
			GinkgoHelper()
			resource := &csiv1alpha1.TrueNASCSI{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
			resource.Spec.UseClusterProxy = on
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())
		}

		By("leaving the proxy out while useClusterProxy is off")
		reconcileResource()
		Expect(configMap()).NotTo(HaveKey("httpsProxy"))

		By("turning useClusterProxy on")
		setUseClusterProxy(true)
		reconcileResource()
		Expect(configMap()).To(HaveKeyWithValue("httpsProxy", "http://proxy.example:3128"))
		Expect(configMap()).To(HaveKeyWithValue("noProxy", ".cluster.local,.svc,10.0.0.0/8"))

		deployment := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ControllerDeploymentName, Namespace: namespace}, deployment)).To(Succeed())
		daemonSet := &appsv1.DaemonSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: NodeDaemonSetName, Namespace: namespace}, daemonSet)).To(Succeed())
		// Both connect to TrueNAS, so both need it.
		for _, template := range []corev1.PodTemplateSpec{deployment.Spec.Template, daemonSet.Spec.Template} {
			var env []string
			for _, c := range template.Spec.Containers {
				if c.Name == ControllerContainerName || c.Name == NodeContainerName {
					for _, e := range c.Env {
						if e.ValueFrom != nil && e.ValueFrom.ConfigMapKeyRef != nil {
							env = append(env, e.Name+"<-"+e.ValueFrom.ConfigMapKeyRef.Key)
						}
					}
				}
			}
			Expect(env).To(ContainElements("HTTP_PROXY<-httpProxy", "HTTPS_PROXY<-httpsProxy", "NO_PROXY<-noProxy"))
		}
		onHash := deployment.Spec.Template.Annotations[ConfigHashAnnotation]

		By("turning it off again")
		setUseClusterProxy(false)
		reconcileResource()
		Expect(configMap()).NotTo(HaveKey("httpsProxy"))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ControllerDeploymentName, Namespace: namespace}, deployment)).To(Succeed())
		Expect(deployment.Spec.Template.Annotations[ConfigHashAnnotation]).NotTo(Equal(onHash))
	})
})
