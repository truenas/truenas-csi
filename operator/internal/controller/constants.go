package controller

import "time"

// CSI driver identification
const (
	// DriverName is the CSI driver name registered with Kubernetes
	DriverName = "csi.truenas.io"

	// CSINamespace is the namespace where CSI components are deployed
	CSINamespace = "truenas-csi"

	// FinalizerName is the finalizer used to clean up resources
	FinalizerName = "csi.truenas.io/finalizer"
)

// Root Certificate Bundle Volume constants
const (
	SharedRootCAVolumeName = "root-ca-bundle"
	// This is the default path where the Root CA Bundle is mounted in the controller
	UBIRootCertMountPath        = "/etc/pki/tls/certs/"
	DistrolessRootCertMountPath = "/etc/ssl/certs/"
	UBIRootCertFilename				= "ca-bundle.crt"
	DistrolessRootCertFilename		= "ca-certificates.crt"
)

// LeaderElectionID is the name of the Lease resource used for leader election
const LeaderElectionID = "truenas-csi-operator.truenas.io"

// Resource names
const (
	ControllerDeploymentName = "truenas-csi-controller"
	NodeDaemonSetName        = "truenas-csi-node"
	ConfigMapName            = "truenas-csi-config"
	NetworkPolicyName        = "truenas-csi-allow-metrics"
)

// Service accounts
const (
	ControllerServiceAccount = "truenas-csi-controller-sa"
	NodeServiceAccount       = "truenas-csi-node-sa"
)

// RBAC resource names
const (
	ControllerClusterRoleName        = "truenas-csi-controller-role"
	NodeClusterRoleName              = "truenas-csi-node-role"
	ControllerClusterRoleBindingName = "truenas-csi-controller-binding"
	NodeClusterRoleBindingName       = "truenas-csi-node-binding"
)

// OpenShift SecurityContextConstraints names
const (
	NodeSCCName       = "truenas-csi-node-scc"
	ControllerSCCName = "truenas-csi-controller-scc"
)

// Container names
const (
	ControllerContainerName    = "csi-controller"
	NodeContainerName          = "csi-node"
	ProvisionerContainerName   = "csi-provisioner"
	AttacherContainerName      = "csi-attacher"
	SnapshotterContainerName   = "csi-snapshotter"
	ResizerContainerName       = "csi-resizer"
	NodeDriverRegistrarName    = "csi-node-driver-registrar"
	LivenessProbeContainerName = "liveness-probe"
)

// Volume names
const (
	VolumeSocketDir       = "socket-dir"
	VolumeRegistrationDir = "registration-dir"
	VolumePluginDir       = "plugin-dir"
	VolumeKubeletDir      = "kubelet-dir"
	VolumeDeviceDir       = "device-dir"
	VolumeModulesDir      = "modules-dir"
	VolumeISCSIDir        = "iscsi-dir"
	VolumeISCSILib        = "iscsi-lib"
	VolumeConnectorDir    = "connector-dir"
	VolumeHostRoot        = "host-root"
	VolumeHostFstab       = "host-fstab"
)

// Host paths
const (
	HostPathRegistrationDir = "/var/lib/kubelet/plugins_registry/"
	HostPathPluginDir       = "/var/lib/kubelet/plugins/csi.truenas.io/"
	HostPathKubeletDir      = "/var/lib/kubelet"
	HostPathDeviceDir       = "/dev"
	HostPathModulesDir      = "/lib/modules"
	HostPathISCSIDir        = "/etc/iscsi"
	HostPathISCSILib        = "/var/lib/iscsi"
	// HostPathConnectorDir keeps the driver's per-volume connector files on the
	// node. They record which protocol and target each staged volume uses, and
	// NodeUnstageVolume has no publish context to fall back on, so a container
	// restart must not take them with it.
	HostPathConnectorDir = "/var/lib/truenas-csi"
	HostPathRoot         = "/"
	HostPathFstab        = "/etc/fstab"
)

// CSI socket paths
const (
	CSISocketPath           = "unix:///csi/csi.sock"
	KubeletRegistrationPath = "/var/lib/kubelet/plugins/csi.truenas.io/csi.sock"
)

// Security context UIDs
const (
	// NonRootUID is the UID for non-privileged containers (nobody user)
	NonRootUID int64 = 65534

	// RootUID is the UID for privileged node containers
	RootUID int64 = 0
)

// Resource limits and requests
const (
	ControllerMemoryRequest = "128Mi"
	ControllerMemoryLimit   = "256Mi"
	ControllerCPURequest    = "100m"
	ControllerCPULimit      = "200m"

	NodeMemoryRequest = "128Mi"
	NodeMemoryLimit   = "256Mi"
	NodeCPURequest    = "100m"
	NodeCPULimit      = "200m"
)

// Default values
const (
	DefaultDriverImage        = "quay.io/truenas_solutions/truenas-csi:latest"
	DefaultControllerReplicas = int32(1)
	DefaultLogLevel           = int32(4)
)

// Requeue durations
const (
	RequeueAfterError   = 30 * time.Second
	RequeueAfterPending = 10 * time.Second
	RequeueAfterRunning = 5 * time.Minute
)

// Liveness probe settings
const (
	LivenessProbePort             = 9808
	LivenessProbeInitialDelay     = int32(10)
	LivenessProbePeriod           = int32(10)
	LivenessProbeFailureThreshold = int32(5)
	HealthProbePort               = int32(8081)
)

// Sidecar configuration
const (
	SidecarLogLevel = 5
	DefaultFSType   = "ext4"
	SidecarTimeout  = "60s"
)

// iSCSI paths (for node container initialization)
const (
	ISCSILockDir    = "/run/lock/iscsi"
	ISCSIDaemonPath = "/usr/sbin/iscsid"

	// ContainerISCSIAdmPath is where the node container's own iscsiadm lives, and
	// so where the shim that forwards to the host's has to be written.
	ContainerISCSIAdmPath = "/usr/sbin/iscsiadm"

	// ISCSIAdmBinary is how the host's iscsiadm is invoked: by name, so it is found
	// on PATH. Distributions disagree on where it lives (/usr/sbin on Debian and
	// RHEL, /usr/local/sbin on Talos), and naming an absolute path makes execvp
	// skip the PATH search and fail with a bare exit 127.
	ISCSIAdmBinary = "iscsiadm"

	// HostMountNamespace is the host's mount namespace as seen from the node
	// container, which nsenter joins to reach the host's storage tooling.
	HostMountNamespace = "/host/proc/1/ns/mnt"

	// HostBinarySearchPath is the PATH host binaries are looked up on, set
	// explicitly so the lookup does not depend on the container image's default.
	HostBinarySearchPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)

// Sidecar image environment variable names
const (
	EnvProvisionerImage    = "PROVISIONER_IMAGE"
	EnvAttacherImage       = "ATTACHER_IMAGE"
	EnvSnapshotterImage    = "SNAPSHOTTER_IMAGE"
	EnvResizerImage        = "RESIZER_IMAGE"
	EnvNodeDriverRegistrar = "NODE_DRIVER_REGISTRAR_IMAGE"
	EnvLivenessProbeImage  = "LIVENESS_PROBE_IMAGE"
)

// ComponentLabels returns the standard labels for a component
func ComponentLabels(component string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name":       "truenas-csi",
		"app.kubernetes.io/managed-by": "truenas-csi-operator",
	}
	if component != "" {
		labels["app.kubernetes.io/component"] = component
		labels["app"] = "truenas-csi-" + component
	}
	return labels
}
