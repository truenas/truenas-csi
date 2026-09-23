package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

var _ = Describe("TrueNASCSI Controller", func() {
	const (
		resourceName    = "test-truenascsi"
		secretName      = "truenas-credentials"
		testNamespace   = "default"
		testTrueNASURL  = "wss://truenas.example.com/api/current"
		testDefaultPool = "tank"
	)

	ctx := context.Background()

	Context("When reconciling a valid resource", func() {
		typeNamespacedName := types.NamespacedName{
			Name: resourceName,
		}

		BeforeEach(func() {
			By("Creating the credentials secret")
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: CSINamespace,
				},
				Data: map[string][]byte{
					"api-key": []byte("test-api-key"),
				},
			}
			// Create namespace first if it doesn't exist
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: CSINamespace},
			}
			err := k8sClient.Create(ctx, ns)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			err = k8sClient.Create(ctx, secret)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			By("Creating the TrueNASCSI resource")
			truenascsi := &csiv1alpha1.TrueNASCSI{}
			err = k8sClient.Get(ctx, typeNamespacedName, truenascsi)
			if err != nil && errors.IsNotFound(err) {
				resource := &csiv1alpha1.TrueNASCSI{
					ObjectMeta: metav1.ObjectMeta{
						Name: resourceName,
					},
					Spec: csiv1alpha1.TrueNASCSISpec{
						TrueNASURL:        testTrueNASURL,
						CredentialsSecret: secretName,
						DefaultPool:       testDefaultPool,
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			By("Cleaning up the TrueNASCSI resource")
			resource := &csiv1alpha1.TrueNASCSI{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &TrueNASCSIReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// Should requeue after running to check status
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
		})

		It("should set status after reconciliation", func() {
			By("Getting the resource after reconciliation")
			// The previous test already reconciled, just verify status
			resource := &csiv1alpha1.TrueNASCSI{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				// Phase should be set (either Pending or Running depending on components)
				Expect(resource.Status.Phase).NotTo(BeEmpty())
			}
		})
	})

	Context("When validation fails", func() {
		It("should fail with invalid URL format", func() {
			resource := &csiv1alpha1.TrueNASCSI{
				ObjectMeta: metav1.ObjectMeta{
					Name: "invalid-url-test",
				},
				Spec: csiv1alpha1.TrueNASCSISpec{
					TrueNASURL:        "http://invalid.example.com", // Should be ws:// or wss://
					CredentialsSecret: secretName,
					DefaultPool:       testDefaultPool,
				},
			}
			err := k8sClient.Create(ctx, resource)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("truenasURL"))
		})

		It("should fail with empty required fields", func() {
			resource := &csiv1alpha1.TrueNASCSI{
				ObjectMeta: metav1.ObjectMeta{
					Name: "empty-fields-test",
				},
				Spec: csiv1alpha1.TrueNASCSISpec{
					// Missing required fields
				},
			}
			err := k8sClient.Create(ctx, resource)
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("Workload rollouts", func() {
	const (
		resourceName = "rollout-truenascsi"
		secretName   = "rollout-credentials"
		namespace    = "truenas-csi-rollout"
		restartedAt  = "kubectl.kubernetes.io/restartedAt"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName}
	secretKey := types.NamespacedName{Name: secretName, Namespace: namespace}

	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, ns))).To(Succeed())

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
			Data:       map[string][]byte{CredentialsSecretKey: []byte("1-original")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())

		resource := &csiv1alpha1.TrueNASCSI{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName},
			Spec: csiv1alpha1.TrueNASCSISpec{
				TrueNASURL:        "wss://truenas.example.com",
				CredentialsSecret: secretName,
				DefaultPool:       "tank",
				NFSServer:         "10.0.0.1",
				Namespace:         namespace,
			},
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		DeferCleanup(func() {
			// Nothing runs the finalizer here, so release it before deleting.
			resource := &csiv1alpha1.TrueNASCSI{}
			if err := k8sClient.Get(ctx, key, resource); err == nil {
				controllerutil.RemoveFinalizer(resource, FinalizerName)
				Expect(k8sClient.Update(ctx, resource)).To(Succeed())
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, resource))).To(Succeed())
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed())
		})
	})

	It("rolls the CSI pods when their configuration changes, and only then", func() {
		r := &TrueNASCSIReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		reconcileResource := func() {
			GinkgoHelper()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
		}
		deployment := func() *appsv1.Deployment {
			GinkgoHelper()
			d := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ControllerDeploymentName, Namespace: namespace}, d)).To(Succeed())
			return d
		}
		daemonSet := func() *appsv1.DaemonSet {
			GinkgoHelper()
			ds := &appsv1.DaemonSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: NodeDaemonSetName, Namespace: namespace}, ds)).To(Succeed())
			return ds
		}
		// Both workloads read the same configuration, so they must carry the same hash.
		configHash := func() string {
			GinkgoHelper()
			hash := deployment().Spec.Template.Annotations[ConfigHashAnnotation]
			Expect(hash).NotTo(BeEmpty())
			Expect(daemonSet().Spec.Template.Annotations).To(HaveKeyWithValue(ConfigHashAnnotation, hash))
			return hash
		}

		reconcileResource()
		initialHash := configHash()
		deploymentGeneration, daemonSetGeneration := deployment().Generation, daemonSet().Generation

		By("reconciling an unchanged configuration")
		reconcileResource()
		Expect(configHash()).To(Equal(initialHash))
		Expect(deployment().Generation).To(Equal(deploymentGeneration))
		Expect(daemonSet().Generation).To(Equal(daemonSetGeneration))

		By("changing a setting the pods read from the ConfigMap")
		resource := &csiv1alpha1.TrueNASCSI{}
		Expect(k8sClient.Get(ctx, key, resource)).To(Succeed())
		resource.Spec.NFSServer = "10.0.0.2"
		Expect(k8sClient.Update(ctx, resource)).To(Succeed())
		reconcileResource()
		nfsHash := configHash()
		Expect(nfsHash).NotTo(Equal(initialHash))
		Expect(deployment().Generation).To(BeNumerically(">", deploymentGeneration))
		Expect(daemonSet().Generation).To(BeNumerically(">", daemonSetGeneration))

		By("rotating the API key")
		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, secretKey, secret)).To(Succeed())
		secret.Data[CredentialsSecretKey] = []byte("1-rotated")
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		Expect(r.requestsForCredentialsSecret(ctx, secret)).To(ConsistOf(reconcile.Request{NamespacedName: key}))
		reconcileResource()
		Expect(configHash()).NotTo(Equal(nfsHash))

		By("restarting the controller by hand")
		restarted := deployment()
		restarted.Spec.Template.Annotations[restartedAt] = "2026-09-23T00:00:00Z"
		Expect(k8sClient.Update(ctx, restarted)).To(Succeed())
		restartedGeneration := deployment().Generation
		reconcileResource()
		Expect(deployment().Spec.Template.Annotations).To(HaveKeyWithValue(restartedAt, "2026-09-23T00:00:00Z"))
		Expect(deployment().Generation).To(Equal(restartedGeneration))
	})
})

var _ = Describe("Error Handling", func() {
	Context("IsConfigurationError", func() {
		It("should return true for ErrInvalidURL", func() {
			Expect(IsConfigurationError(ErrInvalidURL)).To(BeTrue())
		})

		It("should return true for ErrSecretMissingKey", func() {
			Expect(IsConfigurationError(ErrSecretMissingKey)).To(BeTrue())
		})

		It("should return false for ErrSecretNotFound", func() {
			Expect(IsConfigurationError(ErrSecretNotFound)).To(BeFalse())
		})
	})
})

var _ = Describe("Helper Functions", func() {
	Context("extractImageTag", func() {
		It("should extract tag from image with tag", func() {
			Expect(extractImageTag("quay.io/truenas/csi:v1.0.0")).To(Equal("v1.0.0"))
		})

		It("should return latest for image without tag", func() {
			Expect(extractImageTag("quay.io/truenas/csi")).To(Equal("latest"))
		})
	})

	Context("getDriverImage", func() {
		It("should return default when not specified", func() {
			csi := &csiv1alpha1.TrueNASCSI{}
			Expect(getDriverImage(csi)).To(Equal(DefaultDriverImage))
		})

		It("should return specified image", func() {
			csi := &csiv1alpha1.TrueNASCSI{
				Spec: csiv1alpha1.TrueNASCSISpec{
					DriverImage: "custom/image:v1.0.0",
				},
			}
			Expect(getDriverImage(csi)).To(Equal("custom/image:v1.0.0"))
		})
	})

	Context("getNamespace", func() {
		It("should return default namespace when not specified", func() {
			csi := &csiv1alpha1.TrueNASCSI{}
			Expect(getNamespace(csi)).To(Equal(CSINamespace))
		})

		It("should return specified namespace", func() {
			csi := &csiv1alpha1.TrueNASCSI{
				Spec: csiv1alpha1.TrueNASCSISpec{
					Namespace: "custom-namespace",
				},
			}
			Expect(getNamespace(csi)).To(Equal("custom-namespace"))
		})
	})

	Context("ComponentLabels", func() {
		It("should return base labels without component", func() {
			labels := ComponentLabels("")
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "truenas-csi"))
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "truenas-csi-operator"))
			Expect(labels).NotTo(HaveKey("app.kubernetes.io/component"))
		})

		It("should include component when specified", func() {
			labels := ComponentLabels("controller")
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/component", "controller"))
			Expect(labels).To(HaveKeyWithValue("app", "truenas-csi-controller"))
		})
	})
})
