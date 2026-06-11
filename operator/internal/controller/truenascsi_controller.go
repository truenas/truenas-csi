package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

// TrueNASCSIReconciler reconciles a TrueNASCSI object
type TrueNASCSIReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=csi.truenas.io,resources=truenascsis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=csi.truenas.io,resources=truenascsis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=csi.truenas.io,resources=truenascsis/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=csidrivers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=csinodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=volumeattachments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=volumeattachments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotcontents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotcontents/status,verbs=update;patch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=get;list;watch;create;update;patch;delete;use
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

func (r *TrueNASCSIReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	csi := &csiv1alpha1.TrueNASCSI{}
	if err := r.Get(ctx, req.NamespacedName, csi); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("TrueNASCSI resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get TrueNASCSI")
		return ctrl.Result{}, err
	}

	// Handle ManagementState
	if csi.Spec.ManagementState == csiv1alpha1.ManagementStateUnmanaged {
		log.Info("TrueNASCSI is unmanaged, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	// Handle deletion
	if csi.DeletionTimestamp != nil {
		if controllerutil.ContainsFinalizer(csi, FinalizerName) {
			if err := r.cleanupResources(ctx); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(csi, FinalizerName)
			if err := r.Update(ctx, csi); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(csi, FinalizerName) {
		controllerutil.AddFinalizer(csi, FinalizerName)
		if err := r.Update(ctx, csi); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Set initial phase
	if csi.Status.Phase == "" {
		csi.Status.Phase = csiv1alpha1.PhasePending
		if err := r.Status().Update(ctx, csi); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Get the target namespace
	namespace := getNamespace(csi)

	// Validate configuration before proceeding
	validator := NewValidator(r.Client, namespace)
	if err := validator.Validate(ctx, csi); err != nil {
		log.Error(err, "Validation failed")
		result, _ := r.updateStatusFailed(ctx, csi, err)

		// Configuration errors are permanent - don't retry
		if IsConfigurationError(err) {
			return ctrl.Result{}, reconcile.TerminalError(err)
		}
		// Transient errors (e.g., secret not found yet) - retry with backoff
		return result, err
	}

	// Reconcile all resources
	log.V(1).Info("Reconciling namespace")
	if err := r.reconcileNamespace(ctx, csi); err != nil {
		log.Error(err, "Failed to reconcile namespace")
		return r.updateStatusFailed(ctx, csi, err)
	}

	log.V(1).Info("Reconciling network policy")
	if err := r.reconcileNetworkPolicy(ctx, csi); err != nil {
		log.Error(err, "Failed to reconcile network policy")
		return r.updateStatusFailed(ctx, csi, err)
	}

	log.V(1).Info("Reconciling service accounts")
	if err := r.reconcileServiceAccounts(ctx, csi); err != nil {
		log.Error(err, "Failed to reconcile service accounts")
		return r.updateStatusFailed(ctx, csi, err)
	}

	log.V(1).Info("Reconciling RBAC")
	if err := r.reconcileRBAC(ctx, csi); err != nil {
		log.Error(err, "Failed to reconcile RBAC")
		return r.updateStatusFailed(ctx, csi, err)
	}

	log.V(1).Info("Reconciling CSIDriver")
	if err := r.reconcileCSIDriver(ctx); err != nil {
		log.Error(err, "Failed to reconcile CSIDriver")
		return r.updateStatusFailed(ctx, csi, err)
	}

	log.V(1).Info("Reconciling ConfigMap")
	if err := r.reconcileConfigMap(ctx, csi); err != nil {
		log.Error(err, "Failed to reconcile ConfigMap")
		return r.updateStatusFailed(ctx, csi, err)
	}

	log.V(1).Info("Reconciling controller deployment")
	if err := r.reconcileControllerDeployment(ctx, csi); err != nil {
		log.Error(err, "Failed to reconcile controller deployment")
		return r.updateStatusFailed(ctx, csi, err)
	}

	log.V(1).Info("Reconciling node daemonset")
	if err := r.reconcileNodeDaemonSet(ctx, csi); err != nil {
		log.Error(err, "Failed to reconcile node daemonset")
		return r.updateStatusFailed(ctx, csi, err)
	}

	return r.updateStatusRunning(ctx, csi)
}

func (r *TrueNASCSIReconciler) updateStatusFailed(ctx context.Context, csi *csiv1alpha1.TrueNASCSI, reconcileErr error) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	csi.Status.Phase = csiv1alpha1.PhaseFailed
	csi.Status.ObservedGeneration = csi.Generation
	meta.SetStatusCondition(&csi.Status.Conditions, metav1.Condition{
		Type:    csiv1alpha1.ConditionTypeDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  "ReconcileFailed",
		Message: reconcileErr.Error(),
	})
	if err := r.Status().Update(ctx, csi); err != nil {
		log.Error(err, "Failed to update status after reconciliation error")
	}
	return ctrl.Result{RequeueAfter: RequeueAfterError}, reconcileErr
}

func (r *TrueNASCSIReconciler) updateStatusRunning(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	namespace := getNamespace(csi)

	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: ControllerDeploymentName, Namespace: namespace}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("Waiting for controller deployment", "name", ControllerDeploymentName)
			return ctrl.Result{RequeueAfter: RequeueAfterPending}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get deployment %s: %w", ControllerDeploymentName, err)
	}
	csi.Status.ControllerReady = deployment.Status.ReadyReplicas > 0
	csi.Status.ControllerReplicas = deployment.Status.ReadyReplicas

	daemonset := &appsv1.DaemonSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: NodeDaemonSetName, Namespace: namespace}, daemonset); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("Waiting for node daemonset", "name", NodeDaemonSetName)
			return ctrl.Result{RequeueAfter: RequeueAfterPending}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get daemonset %s: %w", NodeDaemonSetName, err)
	}
	csi.Status.NodeDaemonSetReady = daemonset.Status.NumberReady > 0
	csi.Status.NodeReplicas = daemonset.Status.NumberReady

	// Track generation and version
	csi.Status.ObservedGeneration = csi.Generation
	csi.Status.DriverVersion = extractImageTag(getDriverImage(csi))

	if csi.Status.ControllerReady && csi.Status.NodeDaemonSetReady {
		csi.Status.Phase = csiv1alpha1.PhaseRunning
		meta.SetStatusCondition(&csi.Status.Conditions, metav1.Condition{
			Type:    csiv1alpha1.ConditionTypeReady,
			Status:  metav1.ConditionTrue,
			Reason:  "AllComponentsReady",
			Message: "Controller and node components are running",
		})
		meta.RemoveStatusCondition(&csi.Status.Conditions, csiv1alpha1.ConditionTypeDegraded)
	} else {
		csi.Status.Phase = csiv1alpha1.PhasePending
		meta.SetStatusCondition(&csi.Status.Conditions, metav1.Condition{
			Type:    csiv1alpha1.ConditionTypeProgressing,
			Status:  metav1.ConditionTrue,
			Reason:  "WaitingForComponents",
			Message: "Waiting for controller and node components to be ready",
		})
	}

	if err := r.Status().Update(ctx, csi); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterRunning}, nil
}

func (r *TrueNASCSIReconciler) cleanupResources(ctx context.Context) error {
	log := logf.FromContext(ctx)
	log.Info("Cleaning up TrueNASCSI resources")

	csiDriver := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{Name: DriverName},
	}
	if err := r.Delete(ctx, csiDriver); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	for _, name := range []string{ControllerClusterRoleBindingName, NodeClusterRoleBindingName} {
		crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := r.Delete(ctx, crb); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	for _, name := range []string{ControllerClusterRoleName, NodeClusterRoleName} {
		cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := r.Delete(ctx, cr); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

func (r *TrueNASCSIReconciler) reconcileNamespace(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	namespace := getNamespace(csi)
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: ComponentLabels(""),
		},
	}

	existing := &corev1.Namespace{}
	err := r.Get(ctx, types.NamespacedName{Name: namespace}, existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, ns)
	}
	return err
}

func (r *TrueNASCSIReconciler) reconcileNetworkPolicy(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	namespace := getNamespace(csi)

	policy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NetworkPolicyName,
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, policy, func() error {
		policy.Labels = ComponentLabels("")
		policy.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{}, // All pods in namespace
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				Ports: []networkingv1.NetworkPolicyPort{{
					Port: ptr.To(intstr.FromInt(LivenessProbePort)),
				}},
			}},
		}
		return nil
	})
	return err
}

func (r *TrueNASCSIReconciler) reconcileServiceAccounts(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	namespace := getNamespace(csi)
	serviceAccounts := []string{ControllerServiceAccount, NodeServiceAccount}

	for _, saName := range serviceAccounts {
		sa := &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saName,
				Namespace: namespace,
			},
		}

		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
			sa.Labels = ComponentLabels("")
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *TrueNASCSIReconciler) reconcileRBAC(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	namespace := getNamespace(csi)

	// Controller ClusterRole
	controllerRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: ControllerClusterRoleName},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, controllerRole, func() error {
		controllerRole.Labels = ComponentLabels("")
		controllerRole.Rules = []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"persistentvolumes"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"}, Verbs: []string{"get", "list", "watch", "update", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims/status"}, Verbs: []string{"update", "patch"}},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"storageclasses"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"csinodes"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"list", "watch", "create", "update", "patch"}},
			{APIGroups: []string{"snapshot.storage.k8s.io"}, Resources: []string{"volumesnapshots"}, Verbs: []string{"get", "list", "watch", "update", "patch"}},
			{APIGroups: []string{"snapshot.storage.k8s.io"}, Resources: []string{"volumesnapshots/status"}, Verbs: []string{"update", "patch"}},
			{APIGroups: []string{"snapshot.storage.k8s.io"}, Resources: []string{"volumesnapshotcontents"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
			{APIGroups: []string{"snapshot.storage.k8s.io"}, Resources: []string{"volumesnapshotcontents/status"}, Verbs: []string{"update", "patch"}},
			{APIGroups: []string{"snapshot.storage.k8s.io"}, Resources: []string{"volumesnapshotclasses"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"volumeattachments"}, Verbs: []string{"get", "list", "watch", "update", "patch"}},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"volumeattachments/status"}, Verbs: []string{"patch"}},
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "watch", "list", "delete", "update", "create"}},
			{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch"}},
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Node ClusterRole
	nodeRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: NodeClusterRoleName},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, nodeRole, func() error {
		nodeRole.Labels = ComponentLabels("")
		nodeRole.Rules = []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get"}},
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"volumeattachments"}, Verbs: []string{"get", "list", "watch"}},
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Controller ClusterRoleBinding
	controllerBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: ControllerClusterRoleBindingName},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, controllerBinding, func() error {
		controllerBinding.Labels = ComponentLabels("")
		controllerBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     ControllerClusterRoleName,
		}
		controllerBinding.Subjects = []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: ControllerServiceAccount, Namespace: namespace},
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Node ClusterRoleBinding
	nodeBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: NodeClusterRoleBindingName},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, nodeBinding, func() error {
		nodeBinding.Labels = ComponentLabels("")
		nodeBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     NodeClusterRoleName,
		}
		nodeBinding.Subjects = []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: NodeServiceAccount, Namespace: namespace},
		}
		return nil
	})
	return err
}

func (r *TrueNASCSIReconciler) reconcileCSIDriver(ctx context.Context) error {
	attachRequired := true
	podInfoOnMount := true
	fsGroupPolicy := storagev1.FileFSGroupPolicy

	csiDriver := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name:   DriverName,
			Labels: ComponentLabels(""),
		},
		Spec: storagev1.CSIDriverSpec{
			AttachRequired: &attachRequired,
			PodInfoOnMount: &podInfoOnMount,
			FSGroupPolicy:  &fsGroupPolicy,
			VolumeLifecycleModes: []storagev1.VolumeLifecycleMode{
				storagev1.VolumeLifecyclePersistent,
				storagev1.VolumeLifecycleEphemeral,
			},
		},
	}

	existing := &storagev1.CSIDriver{}
	err := r.Get(ctx, types.NamespacedName{Name: DriverName}, existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, csiDriver)
	} else if err != nil {
		return err
	}
	// CSIDriver spec is mostly immutable after creation, only ensure it exists
	return nil
}

func (r *TrueNASCSIReconciler) reconcileConfigMap(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	namespace := getNamespace(csi)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigMapName,
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = ComponentLabels("")
		cm.Data = map[string]string{
			"truenasURL":      csi.Spec.TrueNASURL,
			"defaultPool":     csi.Spec.DefaultPool,
			"nfsServer":       csi.Spec.NFSServer,
			"iscsiPortal":     csi.Spec.ISCSIPortal,
			"nvmeofPortal":    csi.Spec.NVMeOFPortal,
			"iscsiIQNBase":    csi.Spec.ISCSIIQNBase,
			"truenasInsecure": fmt.Sprintf("%t", csi.Spec.InsecureSkipTLS),
		}
		return nil
	})
	return err
}

func (r *TrueNASCSIReconciler) reconcileControllerDeployment(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	namespace := getNamespace(csi)
	replicas := getControllerReplicas(csi)
	driverImage := getDriverImage(csi)
	logLevel := getLogLevel(csi)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerDeploymentName,
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		deployment.Labels = ComponentLabels("controller")
		deployment.Spec = appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "truenas-csi-controller"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ComponentLabels("controller"),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: ControllerServiceAccount,
					Containers: []corev1.Container{
						r.buildControllerContainer(driverImage, logLevel, csi),
						r.buildProvisionerSidecar(),
						r.buildAttacherSidecar(),
						r.buildSnapshotterSidecar(),
						r.buildResizerSidecar(),
						r.buildLivenessProbeContainer(),
					},
					Volumes: buildControllerVolumes(),
				},
			},
		}
		return nil
	})
	return err
}

func (r *TrueNASCSIReconciler) reconcileNodeDaemonSet(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	namespace := getNamespace(csi)
	driverImage := getDriverImage(csi)
	logLevel := getLogLevel(csi)

	daemonset := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NodeDaemonSetName,
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, daemonset, func() error {
		daemonset.Labels = ComponentLabels("node")
		daemonset.Spec = appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "truenas-csi-node"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ComponentLabels("node"),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: NodeServiceAccount,
					HostNetwork:        true,
					HostPID:            true,
					HostIPC:            true,
					PriorityClassName:  "system-node-critical",
					NodeSelector:       csi.Spec.NodeSelector,
					Tolerations: append(csi.Spec.Tolerations, corev1.Toleration{
						Operator: corev1.TolerationOpExists,
					}),
					Containers: []corev1.Container{
						r.buildNodeContainer(driverImage, logLevel, csi),
						r.buildNodeDriverRegistrarSidecar(),
						r.buildLivenessProbeContainer(),
					},
					Volumes: buildNodeVolumes(),
				},
			},
		}
		return nil
	})
	return err
}

func (r *TrueNASCSIReconciler) buildControllerContainer(image string, logLevel int32, csi *csiv1alpha1.TrueNASCSI) corev1.Container {
	return corev1.Container{
		Name:            ControllerContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(NonRootUID),
		},
		Args: []string{
			"--endpoint=$(CSI_ENDPOINT)",
			"--node-id=$(NODE_ID)",
			"--mode=controller",
			fmt.Sprintf("--v=%d", logLevel),
		},
		Env:          buildTrueNASEnvVars(csi),
		VolumeMounts: []corev1.VolumeMount{socketDirVolumeMount()},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.FromInt(LivenessProbePort),
				},
			},
			InitialDelaySeconds: LivenessProbeInitialDelay,
			PeriodSeconds:       LivenessProbePeriod,
			FailureThreshold:    LivenessProbeFailureThreshold,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: mustParseQuantity(ControllerMemoryRequest),
				corev1.ResourceCPU:    mustParseQuantity(ControllerCPURequest),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: mustParseQuantity(ControllerMemoryLimit),
				corev1.ResourceCPU:    mustParseQuantity(ControllerCPULimit),
			},
		},
	}
}

func (r *TrueNASCSIReconciler) buildNodeContainer(image string, logLevel int32, csi *csiv1alpha1.TrueNASCSI) corev1.Container {
	return corev1.Container{
		Name:            NodeContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptr.To(true),
			RunAsUser:  ptr.To(RootUID),
		},
		Args: []string{
			"--endpoint=$(CSI_ENDPOINT)",
			"--node-id=$(NODE_ID)",
			"--mode=node",
			fmt.Sprintf("--v=%d", logLevel),
		},
		Env: buildTrueNASEnvVars(csi),
		// PostStart creates an iscsiadm wrapper that uses the host's iSCSI stack
		// via nsenter (avoiding container/host iscsiadm version mismatches), and
		// loads the NVMe/TCP fabrics kernel modules (NVMe-oF has no host daemon, so
		// nvme-cli runs in-container; it only needs the modules loaded).
		Lifecycle: &corev1.Lifecycle{
			PostStart: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"/bin/sh", "-c",
						fmt.Sprintf("mkdir -p %s && mv /usr/sbin/iscsiadm /usr/sbin/iscsiadm.orig 2>/dev/null; printf '#!/bin/sh\\nnsenter --mount=/host/proc/1/ns/mnt -- /usr/sbin/iscsiadm \"$@\"\\n' > /usr/sbin/iscsiadm && chmod +x /usr/sbin/iscsiadm; modprobe nvme_tcp 2>/dev/null; modprobe nvme_fabrics 2>/dev/null", ISCSILockDir),
					},
				},
			},
		},
		VolumeMounts: buildNodeVolumeMounts(),
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.FromInt(LivenessProbePort),
				},
			},
			InitialDelaySeconds: LivenessProbeInitialDelay,
			PeriodSeconds:       LivenessProbePeriod,
			FailureThreshold:    LivenessProbeFailureThreshold,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: mustParseQuantity(NodeMemoryRequest),
				corev1.ResourceCPU:    mustParseQuantity(NodeCPURequest),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: mustParseQuantity(NodeMemoryLimit),
				corev1.ResourceCPU:    mustParseQuantity(NodeCPULimit),
			},
		},
	}
}

func (r *TrueNASCSIReconciler) buildProvisionerSidecar() corev1.Container {
	return buildSidecarContainer(SidecarConfig{
		Name:        ProvisionerContainerName,
		ImageEnvVar: EnvProvisionerImage,
		Args: []string{
			"--csi-address=/csi/csi.sock",
			fmt.Sprintf("--v=%d", SidecarLogLevel),
			"--feature-gates=Topology=true",
			"--extra-create-metadata",
			"--leader-election=true",
			fmt.Sprintf("--default-fstype=%s", DefaultFSType),
			fmt.Sprintf("--timeout=%s", SidecarTimeout),
		},
		VolumeMounts: []corev1.VolumeMount{socketDirVolumeMount()},
	})
}

func (r *TrueNASCSIReconciler) buildAttacherSidecar() corev1.Container {
	return buildSidecarContainer(SidecarConfig{
		Name:        AttacherContainerName,
		ImageEnvVar: EnvAttacherImage,
		Args: []string{
			"--csi-address=/csi/csi.sock",
			fmt.Sprintf("--v=%d", SidecarLogLevel),
			"--leader-election=true",
			fmt.Sprintf("--timeout=%s", SidecarTimeout),
		},
		VolumeMounts: []corev1.VolumeMount{socketDirVolumeMount()},
	})
}

func (r *TrueNASCSIReconciler) buildSnapshotterSidecar() corev1.Container {
	return buildSidecarContainer(SidecarConfig{
		Name:        SnapshotterContainerName,
		ImageEnvVar: EnvSnapshotterImage,
		Args: []string{
			"--csi-address=/csi/csi.sock",
			fmt.Sprintf("--v=%d", SidecarLogLevel),
			"--leader-election=true",
			fmt.Sprintf("--timeout=%s", SidecarTimeout),
		},
		VolumeMounts: []corev1.VolumeMount{socketDirVolumeMount()},
	})
}

func (r *TrueNASCSIReconciler) buildResizerSidecar() corev1.Container {
	return buildSidecarContainer(SidecarConfig{
		Name:        ResizerContainerName,
		ImageEnvVar: EnvResizerImage,
		Args: []string{
			"--csi-address=/csi/csi.sock",
			fmt.Sprintf("--v=%d", SidecarLogLevel),
			"--leader-election=true",
			fmt.Sprintf("--timeout=%s", SidecarTimeout),
		},
		VolumeMounts: []corev1.VolumeMount{socketDirVolumeMount()},
	})
}

func (r *TrueNASCSIReconciler) buildNodeDriverRegistrarSidecar() corev1.Container {
	return buildSidecarContainer(SidecarConfig{
		Name:        NodeDriverRegistrarName,
		ImageEnvVar: EnvNodeDriverRegistrar,
		Args: []string{
			"--csi-address=/csi/csi.sock",
			"--kubelet-registration-path=" + KubeletRegistrationPath,
			fmt.Sprintf("--v=%d", SidecarLogLevel),
		},
		VolumeMounts: buildNodeDriverRegistrarVolumeMounts(),
	})
}

func (r *TrueNASCSIReconciler) buildLivenessProbeContainer() corev1.Container {
	return buildSidecarContainer(SidecarConfig{
		Name:        LivenessProbeContainerName,
		ImageEnvVar: EnvLivenessProbeImage,
		Args: []string{
			"--csi-address=/csi/csi.sock",
			fmt.Sprintf("--health-port=%d", LivenessProbePort),
		},
		VolumeMounts: []corev1.VolumeMount{socketDirVolumeMount()},
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *TrueNASCSIReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&csiv1alpha1.TrueNASCSI{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.DaemonSet{}).
		Named("truenascsi").
		Complete(r)
}
