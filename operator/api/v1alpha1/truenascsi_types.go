package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TrueNASCSISpec defines the desired state of TrueNASCSI.
type TrueNASCSISpec struct {
	// TrueNAS Connection Settings

	// TrueNASURL is the WebSocket URL to the TrueNAS API (e.g., wss://truenas.example.com/api/current)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^wss?://`
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="TrueNAS URL",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	TrueNASURL string `json:"truenasURL"`

	// CredentialsSecret is the name of the Secret containing the TrueNAS API key
	// The secret must have a key named "api-key"
	// +kubebuilder:validation:Required
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Credentials Secret",xDescriptors="urn:alm:descriptor:io.kubernetes:Secret"
	CredentialsSecret string `json:"credentialsSecret"`

	// DefaultPool is the default ZFS pool to use for volume provisioning
	// +kubebuilder:validation:Required
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Default Pool",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	DefaultPool string `json:"defaultPool"`

	// Protocol Settings

	// NFSServer is the IP address or hostname of the TrueNAS NFS server
	// Required if using NFS storage classes
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="NFS Server",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	NFSServer string `json:"nfsServer,omitempty"`

	// ISCSIPortal is the iSCSI portal address in the format "ip:port" (e.g., "192.168.1.100:3260")
	// Required if using iSCSI storage classes
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="iSCSI Portal",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	ISCSIPortal string `json:"iscsiPortal,omitempty"`

	// NVMeOFPortal is the NVMe-oF/TCP portal address in the format "ip:port" (e.g., "192.168.1.100:4420")
	// Optional; auto-derived from the TrueNAS URL host (port 4420) if omitted
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="NVMe-oF Portal",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	NVMeOFPortal string `json:"nvmeofPortal,omitempty"`

	// ISCSIIQNBase is the base IQN for iSCSI targets (e.g., "iqn.2000-01.io.truenas")
	// +optional
	// +kubebuilder:default="iqn.2000-01.io.truenas"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="iSCSI IQN Base",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	ISCSIIQNBase string `json:"iscsiIQNBase,omitempty"`

	// InsecureSkipTLS skips TLS certificate verification when connecting to TrueNAS
	// Use only for testing or when using self-signed certificates
	// +optional
	// +kubebuilder:default=false
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Skip TLS Verification",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	InsecureSkipTLS bool `json:"insecureSkipTLS,omitempty"`

	// Deployment Options

	// DriverImage is the container image for the TrueNAS CSI driver
	// +optional
	// +kubebuilder:default="quay.io/truenas_solutions/truenas-csi:latest"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Driver Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	DriverImage string `json:"driverImage,omitempty"`

	// ControllerReplicas is the number of controller pod replicas
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Controller Replicas",xDescriptors="urn:alm:descriptor:com.tectonic.ui:podCount"
	ControllerReplicas int32 `json:"controllerReplicas,omitempty"`

	// NodeSelector for the CSI node pods
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Node Selector",xDescriptors="urn:alm:descriptor:com.tectonic.ui:selector:Node"
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for the CSI node pods
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Tolerations"
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// LogLevel sets the verbosity of the CSI driver logs (1-5)
	// +optional
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Log Level",xDescriptors="urn:alm:descriptor:com.tectonic.ui:number"
	LogLevel int32 `json:"logLevel,omitempty"`

	// Operational Control

	// ManagementState controls whether the operator manages this resource
	// Valid values: "Managed", "Unmanaged", "Removed"
	// +optional
	// +kubebuilder:default="Managed"
	// +kubebuilder:validation:Enum=Managed;Unmanaged;Removed
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Management State",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:select:Managed","urn:alm:descriptor:com.tectonic.ui:select:Unmanaged","urn:alm:descriptor:com.tectonic.ui:select:Removed"}
	ManagementState string `json:"managementState,omitempty"`

	// Namespace for deploying CSI driver components
	// +optional
	// +kubebuilder:default="truenas-csi"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Namespace",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Namespace string `json:"namespace,omitempty"`
}

// TrueNASCSIStatus defines the observed state of TrueNASCSI.
type TrueNASCSIStatus struct {
	// Phase represents the current phase of the CSI driver deployment
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Phase",xDescriptors="urn:alm:descriptor:io.kubernetes.phase"
	Phase string `json:"phase,omitempty"`

	// ControllerReady indicates if the controller deployment is ready
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Controller Ready",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	ControllerReady bool `json:"controllerReady,omitempty"`

	// NodeDaemonSetReady indicates if the node daemonset is ready
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Node DaemonSet Ready",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	NodeDaemonSetReady bool `json:"nodeDaemonSetReady,omitempty"`

	// ControllerReplicas is the number of ready controller replicas
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Controller Replicas",xDescriptors="urn:alm:descriptor:com.tectonic.ui:podCount"
	ControllerReplicas int32 `json:"controllerReplicas,omitempty"`

	// NodeReplicas is the number of ready node replicas
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Node Replicas",xDescriptors="urn:alm:descriptor:com.tectonic.ui:podCount"
	NodeReplicas int32 `json:"nodeReplicas,omitempty"`

	// DriverVersion is the version of the deployed CSI driver
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Driver Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	DriverVersion string `json:"driverVersion,omitempty"`

	// ObservedGeneration is the generation last processed by the controller
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the TrueNASCSI's state
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Conditions",xDescriptors="urn:alm:descriptor:io.kubernetes.conditions"
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Phase constants for TrueNASCSI
const (
	PhasePending  = "Pending"
	PhaseRunning  = "Running"
	PhaseFailed   = "Failed"
	PhaseUpdating = "Updating"
)

// Condition types for TrueNASCSI
const (
	ConditionTypeReady       = "Ready"
	ConditionTypeProgressing = "Progressing"
	ConditionTypeDegraded    = "Degraded"
)

// ManagementState values
const (
	ManagementStateManaged   = "Managed"
	ManagementStateUnmanaged = "Unmanaged"
	ManagementStateRemoved   = "Removed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=tnc
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current phase"
// +kubebuilder:printcolumn:name="Controller",type="boolean",JSONPath=".status.controllerReady",description="Controller ready"
// +kubebuilder:printcolumn:name="Nodes",type="boolean",JSONPath=".status.nodeDaemonSetReady",description="Nodes ready"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +operator-sdk:csv:customresourcedefinitions:displayName="TrueNAS CSI",resources={{Deployment,v1},{DaemonSet,v1},{ServiceAccount,v1},{ClusterRole,v1},{ClusterRoleBinding,v1},{CSIDriver,v1},{ConfigMap,v1}}

// TrueNASCSI is the Schema for the truenascsis API.
type TrueNASCSI struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TrueNASCSISpec   `json:"spec,omitempty"`
	Status TrueNASCSIStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TrueNASCSIList contains a list of TrueNASCSI.
type TrueNASCSIList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TrueNASCSI `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TrueNASCSI{}, &TrueNASCSIList{})
}
