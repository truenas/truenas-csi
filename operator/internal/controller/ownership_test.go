package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

const ownershipSecretName = "truenas-credentials"

// reconcileTrueNASCSI runs one reconcile of the named resource.
func reconcileTrueNASCSI(ctx context.Context, name string) (ctrl.Result, error) {
	r := &TrueNASCSIReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	return r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
}

// createTrueNASCSI creates a TrueNASCSI resource deploying into namespace, along
// with the namespace and the credentials Secret it needs.
func createTrueNASCSI(ctx context.Context, name, namespace string) *csiv1alpha1.TrueNASCSI {
	GinkgoHelper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, ns))).To(Succeed())

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: ownershipSecretName, Namespace: namespace},
		Data:       map[string][]byte{CredentialsSecretKey: []byte("1-original")},
	}
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, secret))).To(Succeed())

	resource := &csiv1alpha1.TrueNASCSI{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: csiv1alpha1.TrueNASCSISpec{
			TrueNASURL:        "wss://truenas.example.com",
			CredentialsSecret: ownershipSecretName,
			DefaultPool:       "tank",
			Namespace:         namespace,
		},
	}
	Expect(k8sClient.Create(ctx, resource)).To(Succeed())
	return resource
}

// deleteTrueNASCSI deletes a TrueNASCSI resource and reconciles the deletion, which
// runs the finalizer. envtest has no garbage collector, so this is what releases
// the fixed-name cluster objects for the next resource to claim.
func deleteTrueNASCSI(ctx context.Context, name string) {
	GinkgoHelper()
	key := types.NamespacedName{Name: name}
	resource := &csiv1alpha1.TrueNASCSI{}
	if err := k8sClient.Get(ctx, key, resource); apierrors.IsNotFound(err) {
		return
	}
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, resource))).To(Succeed())
	_, err := reconcileTrueNASCSI(ctx, name)
	Expect(err).NotTo(HaveOccurred())
	Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, resource))).To(BeTrue(), "%s is still present", name)
}

// ownedObjects returns keys for everything the operator deploys into namespace.
// SCCs are left out: envtest does not serve their API.
func ownedObjects(namespace string) []client.Object {
	return []client.Object{
		&storagev1.CSIDriver{ObjectMeta: metav1.ObjectMeta{Name: DriverName}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: ControllerClusterRoleName}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: NodeClusterRoleName}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: ControllerClusterRoleBindingName}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: NodeClusterRoleBindingName}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: ControllerDeploymentName, Namespace: namespace}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: NodeDaemonSetName, Namespace: namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: namespace}},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: NetworkPolicyName, Namespace: namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: ControllerServiceAccount, Namespace: namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: NodeServiceAccount, Namespace: namespace}},
	}
}

func objectName(obj client.Object) string {
	return client.ObjectKeyFromObject(obj).String()
}

var _ = Describe("Ownership", func() {
	ctx := context.Background()

	It("makes the resource the controller of everything it deploys, including objects an older operator left", func() {
		const (
			name        = "ownership-truenascsi"
			namespace   = "truenas-csi-ownership"
			profile     = "security.openshift.io/csi-ephemeral-volume-profile"
			profileMode = "restricted"
		)
		DeferCleanup(deleteTrueNASCSI, ctx, name)
		resource := createTrueNASCSI(ctx, name, namespace)

		By("leaving objects behind the way an operator without owner references did")
		Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: namespace},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &storagev1.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: DriverName, Labels: map[string]string{profile: profileMode}},
			Spec:       storagev1.CSIDriverSpec{PodInfoOnMount: ptr.To(false)},
		})).To(Succeed())

		_, err := reconcileTrueNASCSI(ctx, name)
		Expect(err).NotTo(HaveOccurred())

		versions := map[string]string{}
		for _, obj := range ownedObjects(namespace) {
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
			owner := metav1.GetControllerOf(obj)
			Expect(owner).NotTo(BeNil(), "%s has no controller", objectName(obj))
			Expect(owner.UID).To(Equal(resource.UID), "%s is controlled by %s", objectName(obj), owner.Name)
			versions[objectName(obj)] = obj.GetResourceVersion()
		}

		By("leaving the namespace unowned, since it also holds the user's Secret")
		ns := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: namespace}, ns)).To(Succeed())
		Expect(ns.OwnerReferences).To(BeEmpty())

		By("keeping the existing CSIDriver's spec and the labels an administrator set")
		csiDriver := &storagev1.CSIDriver{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: DriverName}, csiDriver)).To(Succeed())
		Expect(csiDriver.Labels).To(HaveKeyWithValue(profile, profileMode))
		Expect(csiDriver.Spec.PodInfoOnMount).To(Equal(ptr.To(false)))

		By("writing nothing when reconciling an unchanged resource")
		// Every write would come back through the watches as another reconcile.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
		resourceVersion := resource.ResourceVersion
		_, err = reconcileTrueNASCSI(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		for _, obj := range ownedObjects(namespace) {
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
			Expect(obj.GetResourceVersion()).To(Equal(versions[objectName(obj)]), "%s was written", objectName(obj))
		}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
		Expect(resource.ResourceVersion).To(Equal(resourceVersion), "the resource itself was written")
	})

	It("refuses to take over a driver another resource runs, and leaves it running when deleted", func() {
		const (
			first           = "ownership-first"
			firstNamespace  = "truenas-csi-first"
			second          = "ownership-second"
			secondNamespace = "truenas-csi-second"
		)
		DeferCleanup(deleteTrueNASCSI, ctx, first)
		DeferCleanup(deleteTrueNASCSI, ctx, second)

		owner := createTrueNASCSI(ctx, first, firstNamespace)
		_, err := reconcileTrueNASCSI(ctx, first)
		Expect(err).NotTo(HaveOccurred())

		createTrueNASCSI(ctx, second, secondNamespace)
		_, err = reconcileTrueNASCSI(ctx, second)
		Expect(err).To(MatchError(ContainSubstring("already owned")))

		failed := &csiv1alpha1.TrueNASCSI{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: second}, failed)).To(Succeed())
		Expect(failed.Status.Phase).To(Equal(csiv1alpha1.PhaseFailed))
		Expect(meta.IsStatusConditionTrue(failed.Status.Conditions, csiv1alpha1.ConditionTypeDegraded)).To(BeTrue())

		By("deleting the second resource")
		deleteTrueNASCSI(ctx, second)
		for _, obj := range ownedObjects(firstNamespace) {
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed(), "%s was deleted", objectName(obj))
			Expect(metav1.GetControllerOf(obj).UID).To(Equal(owner.UID))
		}
	})

	It("releases a resource deleted while Unmanaged", func() {
		const (
			name      = "ownership-unmanaged"
			namespace = "truenas-csi-unmanaged"
		)
		DeferCleanup(deleteTrueNASCSI, ctx, name)
		createTrueNASCSI(ctx, name, namespace)
		_, err := reconcileTrueNASCSI(ctx, name)
		Expect(err).NotTo(HaveOccurred())

		resource := &csiv1alpha1.TrueNASCSI{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(resource, FinalizerName)).To(BeTrue())
		resource.Spec.ManagementState = csiv1alpha1.ManagementStateUnmanaged
		Expect(k8sClient.Update(ctx, resource)).To(Succeed())

		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		_, err = reconcileTrueNASCSI(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource))).To(BeTrue(),
			"the resource is stuck terminating")
	})

	It("tears the driver down under Removed and deploys it again under Managed", func() {
		const (
			name      = "ownership-removed"
			namespace = "truenas-csi-removed"
		)
		DeferCleanup(deleteTrueNASCSI, ctx, name)
		createTrueNASCSI(ctx, name, namespace)
		_, err := reconcileTrueNASCSI(ctx, name)
		Expect(err).NotTo(HaveOccurred())

		setManagementState := func(state string) {
			GinkgoHelper()
			resource := &csiv1alpha1.TrueNASCSI{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
			resource.Spec.ManagementState = state
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			_, err := reconcileTrueNASCSI(ctx, name)
			Expect(err).NotTo(HaveOccurred())
		}

		By("switching to Removed")
		setManagementState(csiv1alpha1.ManagementStateRemoved)
		for _, obj := range ownedObjects(namespace) {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "%s is still deployed", objectName(obj))
		}
		ns := &corev1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: namespace}, ns)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ownershipSecretName, Namespace: namespace}, &corev1.Secret{})).To(Succeed())

		removed := &csiv1alpha1.TrueNASCSI{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, removed)).To(Succeed())
		Expect(removed.Status.Phase).To(Equal(csiv1alpha1.PhaseRemoved))
		ready := meta.FindStatusCondition(removed.Status.Conditions, csiv1alpha1.ConditionTypeReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(ReasonRemoved))
		Expect(meta.FindStatusCondition(removed.Status.Conditions, csiv1alpha1.ConditionTypeProgressing)).To(BeNil())

		By("switching back to Managed")
		setManagementState(csiv1alpha1.ManagementStateManaged)
		for _, obj := range ownedObjects(namespace) {
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed(), "%s was not redeployed", objectName(obj))
		}
	})

	It("restores deleted and edited objects through its watches, on a cluster without the SCC API", func() {
		const (
			name      = "ownership-watched"
			namespace = "truenas-csi-watched"
			timeout   = 20 * time.Second
			interval  = 200 * time.Millisecond
		)

		// envtest serves no SCC API, so starting at all shows the SCC watch is
		// conditional.
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 k8sClient.Scheme(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Controller:             config.Controller{SkipNameValidation: ptr.To(true)},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect((&TrueNASCSIReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr)).To(Succeed())

		mgrCtx, stop := context.WithCancel(ctx)
		stopped := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			stopped <- mgr.Start(mgrCtx)
		}()
		DeferCleanup(func() {
			stop()
			Eventually(stopped).WithTimeout(timeout).Should(Receive(BeNil()))
		})
		// Registered after the manager's cleanup so it runs first, while the
		// manager is still there to run the finalizer.
		DeferCleanup(func() {
			resource := &csiv1alpha1.TrueNASCSI{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource); err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource))
			}).WithTimeout(timeout).WithPolling(interval).Should(BeTrue())
		})

		createTrueNASCSI(ctx, name, namespace)

		configMapKey := types.NamespacedName{Name: ConfigMapName, Namespace: namespace}
		configMap := &corev1.ConfigMap{}
		Eventually(func() error { return k8sClient.Get(ctx, configMapKey, configMap) }).
			WithTimeout(timeout).WithPolling(interval).Should(Succeed())
		// Once the status settles, the next periodic reconcile is minutes away, so
		// anything restored within the timeout came through a watch.
		Eventually(func(g Gomega) {
			resource := &csiv1alpha1.TrueNASCSI{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, resource)).To(Succeed())
			g.Expect(resource.Status.Phase).To(Equal(csiv1alpha1.PhasePending))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		By("deleting the ConfigMap")
		originalUID := configMap.UID
		Expect(k8sClient.Delete(ctx, configMap)).To(Succeed())
		Eventually(func(g Gomega) {
			restored := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, configMapKey, restored)).To(Succeed())
			g.Expect(restored.UID).NotTo(Equal(originalUID))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		By("editing the ConfigMap")
		Expect(k8sClient.Get(ctx, configMapKey, configMap)).To(Succeed())
		configMap.Data["defaultPool"] = "edited"
		Expect(k8sClient.Update(ctx, configMap)).To(Succeed())
		Eventually(func(g Gomega) {
			reverted := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(ctx, configMapKey, reverted)).To(Succeed())
			g.Expect(reverted.Data).To(HaveKeyWithValue("defaultPool", "tank"))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())

		By("deleting the node DaemonSet")
		daemonSetKey := types.NamespacedName{Name: NodeDaemonSetName, Namespace: namespace}
		daemonSet := &appsv1.DaemonSet{}
		Expect(k8sClient.Get(ctx, daemonSetKey, daemonSet)).To(Succeed())
		originalUID = daemonSet.UID
		Expect(k8sClient.Delete(ctx, daemonSet)).To(Succeed())
		Eventually(func(g Gomega) {
			restored := &appsv1.DaemonSet{}
			g.Expect(k8sClient.Get(ctx, daemonSetKey, restored)).To(Succeed())
			g.Expect(restored.UID).NotTo(Equal(originalUID))
		}).WithTimeout(timeout).WithPolling(interval).Should(Succeed())
	})
})
