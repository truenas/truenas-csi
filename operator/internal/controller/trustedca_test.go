package controller

import (
	"context"
	"path"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

var _ = Describe("Trusted CA", func() {
	ctx := context.Background()

	// setTrustedCA points the named resource at a trusted CA ConfigMap.
	setTrustedCA := func(name, configMap string) {
		GinkgoHelper()
		resource := &csiv1alpha1.TrueNASCSI{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
		resource.Spec.TrustedCA = &csiv1alpha1.ConfigMapKeyReference{Name: configMap}
		Expect(k8sClient.Update(ctx, resource)).To(Succeed())
	}

	// driverContainer returns the named container of a pod template.
	driverContainer := func(template corev1.PodTemplateSpec, name string) corev1.Container {
		GinkgoHelper()
		for _, c := range template.Spec.Containers {
			if c.Name == name {
				return c
			}
		}
		Fail("no container " + name)
		return corev1.Container{}
	}

	// expectBundleMounted checks that a pod template hands the driver container
	// the bundle from configMap.
	expectBundleMounted := func(template corev1.PodTemplateSpec, container, configMap string) {
		GinkgoHelper()
		c := driverContainer(template, container)
		Expect(c.Env).To(ContainElement(corev1.EnvVar{Name: EnvCABundle, Value: path.Join(TrustedCAMountDir, TrustedCAFileName)}))
		Expect(c.VolumeMounts).To(ContainElement(corev1.VolumeMount{Name: VolumeTrustedCA, MountPath: TrustedCAMountDir, ReadOnly: true}))

		var volume *corev1.Volume
		for i := range template.Spec.Volumes {
			if template.Spec.Volumes[i].Name == VolumeTrustedCA {
				volume = &template.Spec.Volumes[i]
			}
		}
		Expect(volume).NotTo(BeNil(), "no %s volume", VolumeTrustedCA)
		Expect(volume.ConfigMap).NotTo(BeNil())
		Expect(volume.ConfigMap.Name).To(Equal(configMap))
		Expect(volume.ConfigMap.Items).To(ConsistOf(corev1.KeyToPath{Key: DefaultTrustedCAKey, Path: TrustedCAFileName}))
	}

	It("hands the bundle to both driver containers, and rolls them when it changes", func() {
		const (
			name      = "trusted-ca-truenascsi"
			namespace = "truenas-csi-trusted-ca"
			configMap = "trusted-cabundle"
		)
		DeferCleanup(deleteTrueNASCSI, ctx, name)
		createTrueNASCSI(ctx, name, namespace)

		bundle := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: configMap, Namespace: namespace},
			Data:       map[string]string{DefaultTrustedCAKey: "-----BEGIN CERTIFICATE-----\nfirst"},
		}
		Expect(k8sClient.Create(ctx, bundle)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, bundle))).To(Succeed()) })
		setTrustedCA(name, configMap)

		By("defaulting the key to where OpenShift injects the cluster bundle")
		resource := &csiv1alpha1.TrueNASCSI{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
		Expect(resource.Spec.TrustedCA.Key).To(Equal(DefaultTrustedCAKey))

		_, err := reconcileTrueNASCSI(ctx, name)
		Expect(err).NotTo(HaveOccurred())

		deployment := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ControllerDeploymentName, Namespace: namespace}, deployment)).To(Succeed())
		daemonSet := &appsv1.DaemonSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: NodeDaemonSetName, Namespace: namespace}, daemonSet)).To(Succeed())
		// Both connect to TrueNAS, so both need it.
		expectBundleMounted(deployment.Spec.Template, ControllerContainerName, configMap)
		expectBundleMounted(daemonSet.Spec.Template, NodeContainerName, configMap)
		firstHash := deployment.Spec.Template.Annotations[ConfigHashAnnotation]

		By("replacing the bundle")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bundle), bundle)).To(Succeed())
		bundle.Data[DefaultTrustedCAKey] = "-----BEGIN CERTIFICATE-----\nsecond"
		Expect(k8sClient.Update(ctx, bundle)).To(Succeed())
		_, err = reconcileTrueNASCSI(ctx, name)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(deployment), deployment)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(daemonSet), daemonSet)).To(Succeed())
		Expect(deployment.Spec.Template.Annotations[ConfigHashAnnotation]).NotTo(Equal(firstHash))
		Expect(daemonSet.Spec.Template.Annotations[ConfigHashAnnotation]).To(Equal(deployment.Spec.Template.Annotations[ConfigHashAnnotation]))
	})

	It("reports a trusted CA ConfigMap that does not exist", func() {
		const (
			name      = "trusted-ca-missing"
			namespace = "truenas-csi-trusted-ca-missing"
		)
		DeferCleanup(deleteTrueNASCSI, ctx, name)
		createTrueNASCSI(ctx, name, namespace)
		setTrustedCA(name, "missing-bundle")

		_, err := reconcileTrueNASCSI(ctx, name)
		Expect(err).To(MatchError(ContainSubstring("trusted CA ConfigMap")))

		resource := &csiv1alpha1.TrueNASCSI{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
		degraded := meta.FindStatusCondition(resource.Status.Conditions, csiv1alpha1.ConditionTypeDegraded)
		Expect(degraded).NotTo(BeNil())
		Expect(degraded.Message).To(ContainSubstring("missing-bundle"))
	})
})
