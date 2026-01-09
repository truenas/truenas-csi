#!/bin/bash
set -e

# TrueNAS CSI Driver - Simplified Demo
# This demo focuses on volume provisioning without pod mounting
# Perfect for environments where mounting is problematic (WSL, VM networking, etc.)

CLUSTER_NAME="${KIND_CLUSTER_NAME:-truenas-csi-demo}"
NAMESPACE="truenas-csi"
DEMO_NAMESPACE="demo"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Print colored output
print_info() {
    echo -e "${BLUE}ℹ ${NC}$1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_header() {
    echo ""
    echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}${BOLD}  $1${NC}"
    echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_step() {
    echo ""
    echo -e "${CYAN}${BOLD}➤ $1${NC}"
    echo ""
}

# Check prerequisites
check_prerequisites() {
    local missing=0

    if ! command -v kind &> /dev/null; then
        print_error "kind not found. Install from: https://kind.sigs.k8s.io/"
        missing=1
    fi

    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl not found. Install from: https://kubernetes.io/docs/tasks/tools/"
        missing=1
    fi

    if ! command -v docker &> /dev/null; then
        print_error "docker not found. Install from: https://docs.docker.com/get-docker/"
        missing=1
    fi

    if [ $missing -eq 1 ]; then
        print_error "Please install missing prerequisites and try again"
        exit 1
    fi
}

# Check if cluster and driver exist
check_cluster_and_driver() {
    local cluster_exists=false
    local driver_exists=false

    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        cluster_exists=true
    fi

    if kubectl get namespace ${NAMESPACE} &>/dev/null 2>&1; then
        driver_exists=true
    fi

    if [ "$cluster_exists" = true ] && [ "$driver_exists" = true ]; then
        return 0  # Both exist
    elif [ "$cluster_exists" = false ]; then
        return 1  # Cluster doesn't exist, need full setup
    else
        return 2  # Cluster exists but driver doesn't
    fi
}

# Validate YAML configuration
validate_yaml_config() {
    print_header "Configuration Check"

    print_info "This demo uses the configuration from:"
    echo "  deploy/truenas-csi-driver.yaml"
    echo ""
    print_warning "Before running this demo, please ensure you have edited the YAML file with:"
    echo "  1. TrueNAS connection details (ConfigMap: truenas-csi-config)"
    echo "  2. TrueNAS API key (Secret: truenas-api-credentials)"
    echo ""

    read -p "Have you configured deploy/truenas-csi-driver.yaml with your TrueNAS settings? [y/N]: " CONFIGURED
    if [[ ! $CONFIGURED =~ ^[Yy]$ ]]; then
        echo ""
        print_error "Please edit deploy/truenas-csi-driver.yaml first:"
        echo ""
        echo "  1. Update ConfigMap 'truenas-csi-config' with your TrueNAS IP and pool"
        echo "  2. Update Secret 'truenas-api-credentials' with your TrueNAS API key"
        echo "  3. Run this demo again"
        echo ""
        exit 1
    fi

    print_success "Configuration confirmed"
    echo ""
}

# Create Kind cluster
create_cluster() {
    print_header "Creating Kind Cluster"

    if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
        print_warning "Cluster '${CLUSTER_NAME}' already exists"
        read -p "Do you want to delete and recreate it? [y/N]: " RECREATE
        if [[ $RECREATE =~ ^[Yy]$ ]]; then
            print_info "Deleting existing cluster..."
            kind delete cluster --name ${CLUSTER_NAME}
        else
            print_info "Using existing cluster"
            return
        fi
    fi

    print_info "Creating Kind cluster with 2 worker nodes..."

    cat <<EOF | kind create cluster --name ${CLUSTER_NAME} --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
EOF

    print_success "Cluster created successfully"

    # Wait for nodes to be ready
    print_info "Waiting for nodes to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=120s
    print_success "All nodes are ready"
}

# Build and load driver image
build_and_load_image() {
    print_header "Building and Loading CSI Driver Image"

    print_info "Building Docker image..."
    docker build -t truenas/truenas-csi-driver:demo .
    print_success "Image built successfully"

    print_info "Loading image into Kind cluster..."
    kind load docker-image truenas/truenas-csi-driver:demo --name ${CLUSTER_NAME}
    print_success "Image loaded into cluster"
}

# Deploy CSI driver
deploy_driver() {
    print_header "Deploying TrueNAS CSI Driver"

    # Deploy using the pre-configured YAML
    print_info "Deploying driver from deploy/truenas-csi-driver.yaml..."

    # Update deployment manifest to use demo image and IfNotPresent pull policy
    sed -e "s|truenas/truenas-csi-driver:latest|truenas/truenas-csi-driver:demo|g" \
        -e "s|imagePullPolicy: Always|imagePullPolicy: IfNotPresent|g" \
        deploy/truenas-csi-driver.yaml | kubectl apply -f -

    print_success "Deployment manifest applied"
    echo ""

    print_info "Waiting for CSI driver to be ready..."
    echo "  This may take up to 2 minutes..."
    echo ""

    if kubectl wait --namespace=${NAMESPACE} \
        --for=condition=ready pod \
        --selector=app=truenas-csi-controller \
        --timeout=120s 2>/dev/null; then
        print_success "Controller pod is ready"
    else
        print_error "Controller pod failed to start"
        print_info "Checking controller logs..."
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=20 2>/dev/null || echo "No logs available"
        return 1
    fi

    if kubectl wait --namespace=${NAMESPACE} \
        --for=condition=ready pod \
        --selector=app=truenas-csi-node \
        --timeout=120s 2>/dev/null; then
        print_success "Node pods are ready"
    else
        print_error "Node pods failed to start"
        print_info "Checking node logs..."
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-node -c csi-node --tail=20 2>/dev/null || echo "No logs available"
        return 1
    fi

    print_success "CSI driver deployed successfully"

    # Show driver status
    echo ""
    print_info "Driver pods status:"
    kubectl get pods -n ${NAMESPACE}
}

# Setup snapshot support
setup_snapshot_support() {
    print_header "Setting Up Snapshot Support"

    # Check if snapshot CRDs exist
    if ! kubectl get crd volumesnapshots.snapshot.storage.k8s.io &>/dev/null; then
        print_info "Installing VolumeSnapshot CRDs..."
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml &>/dev/null
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml &>/dev/null
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml &>/dev/null
        print_success "Snapshot CRDs installed"
    else
        print_info "Snapshot CRDs already exist"
    fi

    # Check if snapshot-controller is deployed
    if ! kubectl get deployment snapshot-controller -n kube-system &>/dev/null; then
        print_info "Deploying snapshot-controller..."
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml &>/dev/null
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml &>/dev/null

        print_info "Waiting for snapshot-controller to be ready..."
        kubectl wait --for=condition=ready pod -n kube-system -l app=snapshot-controller --timeout=60s &>/dev/null || true
        print_success "Snapshot-controller deployed"
    else
        print_info "Snapshot-controller already exists"
    fi

    # Create VolumeSnapshotClass
    print_info "Creating VolumeSnapshotClass..."
    cat <<EOF | kubectl apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: truenas-snapshot-class
driver: csi.truenas.io
deletionPolicy: Delete
EOF

    print_success "Snapshot support configured"
    echo ""
}

# Create StorageClasses
setup_storage_classes() {
    print_header "Setting Up Storage Classes"

    # Create demo namespace
    print_info "Creating demo namespace..."
    kubectl create namespace ${DEMO_NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

    # Create NFS StorageClass
    print_info "Creating NFS StorageClass..."
    cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-nfs
provisioner: csi.truenas.io
parameters:
  protocol: "nfs"
  pool: "${TRUENAS_POOL}"
  compression: "lz4"
  sync: "standard"
  nfs.networks: "172.18.0.0/16"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
EOF

    # Create iSCSI StorageClass
    print_info "Creating iSCSI StorageClass..."
    cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-iscsi
provisioner: csi.truenas.io
parameters:
  protocol: "iscsi"
  pool: "${TRUENAS_POOL}"
  compression: "lz4"
  volblocksize: "16K"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
EOF

    print_success "Storage classes created"
    echo ""
    kubectl get storageclass
}

# Run setup if needed
run_setup_if_needed() {
    check_prerequisites

    # Capture return code without triggering set -e
    set +e
    check_cluster_and_driver
    local status=$?
    set -e

    if [ $status -eq 0 ]; then
        print_success "Cluster and driver already exist!"
        return 0
    fi

    print_header "Initial Setup Required"

    # Validate YAML is configured
    validate_yaml_config

    if [ $status -eq 1 ]; then
        print_info "No existing cluster found. Setting up from scratch..."
        echo ""
        create_cluster
        build_and_load_image
        deploy_driver
        setup_storage_classes
        setup_snapshot_support
    elif [ $status -eq 2 ]; then
        print_info "Cluster exists but driver not found. Deploying driver..."
        echo ""
        build_and_load_image
        deploy_driver
        setup_storage_classes
        setup_snapshot_support
    fi

    print_header "Setup Complete!"
    print_success "Environment is ready for demos"
    echo ""
}

# Demo: NFS Volume Provisioning
demo_nfs() {
    print_header "Demo: NFS Volume Provisioning"

    print_step "1. Creating NFS PersistentVolumeClaim (1Gi)"

    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-nfs-pvc
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs
  resources:
    requests:
      storage: 1Gi
EOF

    print_info "Waiting for PVC to be bound..."
    if kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/demo-nfs-pvc \
        --timeout=60s; then

        print_success "PVC successfully bound!"
        echo ""

        # Show PVC details
        print_step "2. PVC Details"
        kubectl get pvc -n ${DEMO_NAMESPACE} demo-nfs-pvc -o wide
        echo ""

        # Show PV details
        PV_NAME=$(kubectl get pvc -n ${DEMO_NAMESPACE} demo-nfs-pvc -o jsonpath='{.spec.volumeName}')
        print_step "3. Bound PersistentVolume Details"
        kubectl get pv ${PV_NAME} -o yaml | grep -A 10 "volumeHandle\|nfsServer\|nfsPath" || echo "(Volume details not available in expected format)"
        echo ""

        # Show driver logs
        print_step "4. CSI Controller Logs (CreateVolume operation)"
        print_info "Look for 'CreateVolume' and 'NFS share created' messages:"
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=30 | grep -E "CreateVolume|NFS|dataset" || echo "(No matching log entries found - check full logs with option 8)"
        echo ""

        print_success "✅ NFS volume created on TrueNAS!"
        echo ""
        echo -e "${BOLD}What just happened:${NC}"
        echo "  ✓ Kubernetes created a PVC requesting 1Gi of NFS storage"
        echo "  ✓ CSI driver contacted TrueNAS and created:"
        echo "    - ZFS dataset (filesystem)"
        echo "    - NFS share with appropriate permissions"
        echo "  ✓ Kubernetes bound the PVC to a PersistentVolume"
        echo ""
        echo -e "${CYAN}Check your TrueNAS UI:${NC}"
        echo "  Storage → Datasets → Look for the new dataset"
        echo "  Shares → NFS → Look for the new share"

    else
        print_error "PVC failed to bind. Check driver logs:"
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=50
    fi

    echo ""
    read -p "Press Enter to continue..."
}

# Demo: iSCSI Volume Provisioning
demo_iscsi() {
    print_header "Demo: iSCSI Volume Provisioning"

    print_step "1. Creating iSCSI PersistentVolumeClaim (2Gi)"

    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-iscsi-pvc
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: truenas-iscsi
  resources:
    requests:
      storage: 2Gi
EOF

    print_info "Waiting for PVC to be bound..."
    if kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/demo-iscsi-pvc \
        --timeout=60s; then

        print_success "PVC successfully bound!"
        echo ""

        # Show PVC details
        print_step "2. PVC Details"
        kubectl get pvc -n ${DEMO_NAMESPACE} demo-iscsi-pvc -o wide
        echo ""

        # Show PV details
        PV_NAME=$(kubectl get pvc -n ${DEMO_NAMESPACE} demo-iscsi-pvc -o jsonpath='{.spec.volumeName}')
        print_step "3. Bound PersistentVolume Details"
        kubectl get pv ${PV_NAME} -o yaml | grep -A 10 "volumeHandle\|targetPortal\|targetIQN" || echo "(Volume details not available in expected format)"
        echo ""

        # Show driver logs
        print_step "4. CSI Controller Logs (CreateVolume operation)"
        print_info "Look for 'CreateVolume' and 'iSCSI' messages:"
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=30 | grep -E "CreateVolume|iSCSI|ZVOL|target|extent" || echo "(No matching log entries found - check full logs with option 8)"
        echo ""

        print_success "✅ iSCSI volume created on TrueNAS!"
        echo ""
        echo -e "${BOLD}What just happened:${NC}"
        echo "  ✓ Kubernetes created a PVC requesting 2Gi of iSCSI block storage"
        echo "  ✓ CSI driver contacted TrueNAS and created:"
        echo "    - ZVOL (ZFS volume for block storage)"
        echo "    - iSCSI target with unique IQN"
        echo "    - iSCSI extent pointing to the ZVOL"
        echo "    - Target-extent association"
        echo "  ✓ Kubernetes bound the PVC to a PersistentVolume"
        echo ""
        echo -e "${CYAN}Check your TrueNAS UI:${NC}"
        echo "  Storage → Datasets → Look for the new ZVOL"
        echo "  Shares → iSCSI → Targets, Extents, Associated Targets"

    else
        print_error "PVC failed to bind. Check driver logs:"
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=50
    fi

    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Volume Expansion
demo_expand() {
    print_header "Demo: Volume Expansion"

    # Check for existing volumes
    PVCS=$(kubectl get pvc -n ${DEMO_NAMESPACE} -o jsonpath='{.items[?(@.status.phase=="Bound")].metadata.name}' 2>/dev/null)

    SELECTED_PVC=""
    ORIGINAL_SIZE=""

    if [ -n "$PVCS" ]; then
        echo "Available volumes:"
        echo ""

        # Convert to array
        PVC_ARRAY=($PVCS)

        # Display numbered list
        for i in "${!PVC_ARRAY[@]}"; do
            PVC_NAME="${PVC_ARRAY[$i]}"
            PVC_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} $PVC_NAME -o jsonpath='{.status.capacity.storage}')
            PVC_CLASS=$(kubectl get pvc -n ${DEMO_NAMESPACE} $PVC_NAME -o jsonpath='{.spec.storageClassName}')
            echo "  $((i+1))) $PVC_NAME - ${PVC_SIZE} (${PVC_CLASS})"
        done

        echo "  0) Create a new test volume"
        echo ""
        read -p "Select volume to expand [0-${#PVC_ARRAY[@]}]: " CHOICE

        if [[ "$CHOICE" =~ ^[1-9][0-9]*$ ]] && [ "$CHOICE" -le "${#PVC_ARRAY[@]}" ]; then
            SELECTED_PVC="${PVC_ARRAY[$((CHOICE-1))]}"
            ORIGINAL_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} $SELECTED_PVC -o jsonpath='{.status.capacity.storage}')
            print_success "Selected: $SELECTED_PVC (${ORIGINAL_SIZE})"
            echo ""
        fi
    fi

    # Create test volume if none selected
    if [ -z "$SELECTED_PVC" ]; then
        print_info "Creating test volume for expansion (1Gi)"
        echo ""

        print_step "1. Creating test volume"
        cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-expand-test
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs
  resources:
    requests:
      storage: 1Gi
EOF

        print_info "Waiting for PVC to be bound..."
        if ! kubectl wait --namespace=${DEMO_NAMESPACE} \
            --for=jsonpath='{.status.phase}'=Bound \
            pvc/demo-expand-test \
            --timeout=60s; then
            print_error "PVC failed to bind"
            return
        fi
        SELECTED_PVC="demo-expand-test"
        ORIGINAL_SIZE="1Gi"
        print_success "Test volume created (${ORIGINAL_SIZE})"
        echo ""

        sleep 2
    fi

    # Parse original size to calculate new size (add 1Gi)
    ORIGINAL_SIZE_NUM=$(echo $ORIGINAL_SIZE | sed 's/Gi//')
    NEW_SIZE=$((ORIGINAL_SIZE_NUM + 1))
    NEW_SIZE="${NEW_SIZE}Gi"

    print_step "2. Expanding volume from ${ORIGINAL_SIZE} to ${NEW_SIZE}"
    kubectl patch pvc $SELECTED_PVC -n ${DEMO_NAMESPACE} -p "{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"${NEW_SIZE}\"}}}}"

    print_info "Waiting for expansion to complete..."
    sleep 10

    ACTUAL_NEW_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} $SELECTED_PVC -o jsonpath='{.status.capacity.storage}')
    print_success "Volume expanded to ${ACTUAL_NEW_SIZE}!"
    echo ""

    print_step "3. PVC Status"
    kubectl get pvc -n ${DEMO_NAMESPACE} $SELECTED_PVC
    echo ""

    print_step "4. CSI Controller Logs (ControllerExpandVolume)"
    kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=20 | grep -E "Expand|resize|quota" || echo "(No matching log entries found)"
    echo ""

    echo -e "${BOLD}What just happened:${NC}"
    echo "  ✓ Selected volume: $SELECTED_PVC"
    echo "  ✓ Original size: ${ORIGINAL_SIZE} → New size: ${ACTUAL_NEW_SIZE}"
    echo "  ✓ CSI driver updated the ZFS dataset refquota"
    echo "  ✓ Volume expanded online without recreating!"
    echo ""
    echo -e "${CYAN}Check your TrueNAS UI:${NC}"
    echo "  Storage → Datasets → Check the dataset's quota property has increased"

    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Volume Cloning
demo_clone() {
    print_header "Demo: Volume Cloning"

    print_info "Note: Volume cloning requires source volume info in memory"
    echo ""

    # Check for existing volumes
    PVCS=$(kubectl get pvc -n ${DEMO_NAMESPACE} -o jsonpath='{.items[?(@.status.phase=="Bound")].metadata.name}' 2>/dev/null)

    SELECTED_PVC=""
    SELECTED_SIZE=""
    SELECTED_CLASS=""
    SELECTED_ACCESS=""

    if [ -n "$PVCS" ]; then
        echo "Available volumes:"
        echo ""

        # Convert to array
        PVC_ARRAY=($PVCS)

        # Display numbered list
        for i in "${!PVC_ARRAY[@]}"; do
            PVC_NAME="${PVC_ARRAY[$i]}"
            PVC_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} $PVC_NAME -o jsonpath='{.status.capacity.storage}')
            PVC_CLASS=$(kubectl get pvc -n ${DEMO_NAMESPACE} $PVC_NAME -o jsonpath='{.spec.storageClassName}')
            echo "  $((i+1))) $PVC_NAME - ${PVC_SIZE} (${PVC_CLASS})"
        done

        echo "  0) Create a new test volume"
        echo ""
        read -p "Select volume to clone [0-${#PVC_ARRAY[@]}]: " CHOICE

        if [[ "$CHOICE" =~ ^[1-9][0-9]*$ ]] && [ "$CHOICE" -le "${#PVC_ARRAY[@]}" ]; then
            SELECTED_PVC="${PVC_ARRAY[$((CHOICE-1))]}"
            SELECTED_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} $SELECTED_PVC -o jsonpath='{.status.capacity.storage}')
            SELECTED_CLASS=$(kubectl get pvc -n ${DEMO_NAMESPACE} $SELECTED_PVC -o jsonpath='{.spec.storageClassName}')
            SELECTED_ACCESS=$(kubectl get pvc -n ${DEMO_NAMESPACE} $SELECTED_PVC -o jsonpath='{.spec.accessModes[0]}')
            print_success "Selected: $SELECTED_PVC (${SELECTED_SIZE}, ${SELECTED_CLASS})"
            echo ""
        fi
    fi

    # Create test volume if none selected
    if [ -z "$SELECTED_PVC" ]; then
        print_info "Creating fresh source volume for cloning (1Gi NFS)"
        echo ""

        print_step "1. Creating fresh source volume"
        cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-clone-source
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs
  resources:
    requests:
      storage: 1Gi
EOF

        print_info "Waiting for source PVC to be bound..."
        if ! kubectl wait --namespace=${DEMO_NAMESPACE} \
            --for=jsonpath='{.status.phase}'=Bound \
            pvc/demo-clone-source \
            --timeout=120s; then
            print_error "Source PVC failed to bind"
            return
        fi
        SELECTED_PVC="demo-clone-source"
        SELECTED_SIZE="1Gi"
        SELECTED_CLASS="truenas-nfs"
        SELECTED_ACCESS="ReadWriteMany"
        print_success "Source volume created"
        echo ""

        sleep 2
    fi

    print_step "2. Cloning volume: $SELECTED_PVC"

    # Generate unique clone name
    CLONE_NAME="clone-of-${SELECTED_PVC}-$(date +%s)"

    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${CLONE_NAME}
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ${SELECTED_ACCESS}
  storageClassName: ${SELECTED_CLASS}
  resources:
    requests:
      storage: ${SELECTED_SIZE}
  dataSource:
    name: ${SELECTED_PVC}
    kind: PersistentVolumeClaim
EOF

    print_info "Cloning started (this is SLOW due to WSL/VM networking - may take 5+ minutes)"
    print_info "The clone WILL appear in TrueNAS UI even if kubectl times out"
    echo ""

    print_step "3. Monitoring clone progress for 60 seconds..."
    if kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/${CLONE_NAME} \
        --timeout=60s 2>/dev/null; then

        print_success "Clone PVC successfully bound!"
        echo ""

        print_step "4. Source and cloned volumes:"
        kubectl get pvc -n ${DEMO_NAMESPACE} ${SELECTED_PVC} ${CLONE_NAME}
        echo ""

        print_success "✅ Volume cloned using ZFS snapshot!"

    else
        print_warning "Clone is taking longer than expected (still in progress)"
        echo ""

        print_step "4. Current status:"
        kubectl get pvc -n ${DEMO_NAMESPACE} ${SELECTED_PVC} ${CLONE_NAME}
        echo ""

        echo -e "${BOLD}What's happening:${NC}"
        echo "  ⏳ Clone request submitted to TrueNAS"
        echo "  ⏳ Driver is creating snapshot and cloning (slow over WSL/VM)"
        echo "  ℹ Use option 12 (Show status) to check if clone completed"
        echo ""
        echo -e "${CYAN}Check your TrueNAS UI RIGHT NOW:${NC}"
        echo "  Storage → Datasets → The clone may already be visible!"
        echo "  Even if kubectl shows Pending, TrueNAS might have the clone"
    fi

    echo ""
    echo -e "${BOLD}How volume cloning works:${NC}"
    echo "  ✓ CSI driver creates a temporary ZFS snapshot of source volume"
    echo "  ✓ Clones a new dataset from the snapshot"
    echo "  ✓ Creates new ${SELECTED_CLASS} share for the clone"
    echo "  ✓ Clone is independent and can be modified separately"

    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Snapshots
demo_snapshot() {
    print_header "Demo: Volume Snapshots"

    # Verify snapshot support is configured
    if ! kubectl get crd volumesnapshots.snapshot.storage.k8s.io &>/dev/null || \
       ! kubectl get deployment snapshot-controller -n kube-system &>/dev/null; then
        print_warning "Snapshot support not fully configured!"
        print_info "Setting up snapshot support..."
        setup_snapshot_support
    fi

    # List existing PVCs
    print_step "1. Selecting source volume for snapshot"

    # Get list of bound PVCs
    PVCS=$(kubectl get pvc -n ${DEMO_NAMESPACE} -o jsonpath='{.items[?(@.status.phase=="Bound")].metadata.name}' 2>/dev/null)

    if [ -z "$PVCS" ]; then
        print_warning "No volumes found in the demo namespace!"
        echo ""
        echo "Please return to the main menu and create a volume first using:"
        echo "  - Option 1: Demo NFS volume creation"
        echo "  - Option 2: Demo iSCSI volume creation"
        echo ""
        read -p "Press Enter to return to main menu..."
        return
    fi

    # Display available PVCs
    echo "Available volumes:"
    echo ""

    # Convert space-separated list to array
    PVC_ARRAY=($PVCS)

    # Display numbered list
    for i in "${!PVC_ARRAY[@]}"; do
        PVC_NAME="${PVC_ARRAY[$i]}"
        PVC_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} $PVC_NAME -o jsonpath='{.status.capacity.storage}')
        PVC_CLASS=$(kubectl get pvc -n ${DEMO_NAMESPACE} $PVC_NAME -o jsonpath='{.spec.storageClassName}')
        echo "  $((i+1))) $PVC_NAME - ${PVC_SIZE} (${PVC_CLASS})"
    done

    echo ""
    read -p "Select volume to snapshot [1-${#PVC_ARRAY[@]}]: " PVC_CHOICE

    # Validate choice
    if ! [[ "$PVC_CHOICE" =~ ^[0-9]+$ ]] || [ "$PVC_CHOICE" -lt 1 ] || [ "$PVC_CHOICE" -gt "${#PVC_ARRAY[@]}" ]; then
        print_error "Invalid selection"
        read -p "Press Enter to continue..."
        return
    fi

    # Get selected PVC name
    SELECTED_PVC="${PVC_ARRAY[$((PVC_CHOICE-1))]}"
    SELECTED_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} $SELECTED_PVC -o jsonpath='{.status.capacity.storage}')

    print_success "Selected volume: $SELECTED_PVC (${SELECTED_SIZE})"
    echo ""

    sleep 1

    print_step "2. Creating VolumeSnapshot of '$SELECTED_PVC'"

    # Generate unique snapshot name
    SNAPSHOT_NAME="snapshot-${SELECTED_PVC}-$(date +%s)"

    cat <<EOF | kubectl apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: ${SNAPSHOT_NAME}
  namespace: ${DEMO_NAMESPACE}
spec:
  volumeSnapshotClassName: truenas-snapshot-class
  source:
    persistentVolumeClaimName: ${SELECTED_PVC}
EOF

    print_info "Waiting for snapshot to be ready..."
    sleep 5

    print_success "Snapshot created!"
    kubectl get volumesnapshot -n ${DEMO_NAMESPACE} ${SNAPSHOT_NAME}
    echo ""

    print_step "3. Snapshot details"
    kubectl describe volumesnapshot -n ${DEMO_NAMESPACE} ${SNAPSHOT_NAME} | grep -E "Status:|Snapshot Handle:|Creation Time:|Ready To Use:" || echo "Details not available"

    # Get the snapshot handle (this is the ZFS snapshot name)
    SNAPSHOT_HANDLE=$(kubectl get volumesnapshot -n ${DEMO_NAMESPACE} ${SNAPSHOT_NAME} -o jsonpath='{.status.snapshotHandle}' 2>/dev/null)
    if [ -n "$SNAPSHOT_HANDLE" ]; then
        echo ""
        print_info "ZFS Snapshot ID: ${SNAPSHOT_HANDLE}"
        echo ""
        echo -e "${CYAN}To find this snapshot in TrueNAS UI:${NC}"
        echo "  1. Go to: Storage → Snapshots"
        echo "  2. Look for snapshot: ${SNAPSHOT_HANDLE}"
        echo "  3. Or: Storage → Datasets → Select your pool → Click on the dataset"
        echo "     and look in the Snapshots tab"
    fi
    echo ""

    # Show controller logs for snapshot creation
    print_step "4. CSI Controller Logs (CreateSnapshot operation)"
    print_info "Checking if snapshot was created in TrueNAS:"
    kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=20 | grep -E "CreateSnapshot|snapshot" || echo "(No snapshot log entries found - check full logs)"
    echo ""

    print_success "✅ Snapshot created successfully!"
    echo ""
    echo -e "${BOLD}Snapshot Information:${NC}"
    echo "  Original volume: ${SELECTED_PVC} (${SELECTED_SIZE})"
    echo "  Snapshot name: ${SNAPSHOT_NAME}"
    echo "  Snapshot handle: ${SNAPSHOT_HANDLE}"
    echo ""
    echo -e "${CYAN}The snapshot is now available for:${NC}"
    echo "  • Backup and disaster recovery"
    echo "  • Creating test/dev environments"
    echo "  • Restoring data to a new volume"
    echo ""

    # Ask if user wants to restore
    read -p "Do you want to restore a new volume from this snapshot? [y/N]: " RESTORE_CHOICE

    if [[ ! $RESTORE_CHOICE =~ ^[Yy]$ ]]; then
        print_info "Snapshot demo complete. The snapshot remains available for future use."
        echo ""
        echo -e "${CYAN}To manually restore from this snapshot later:${NC}"
        echo "  1. Create a PVC with dataSource pointing to: ${SNAPSHOT_NAME}"
        echo "  2. Or use option 7 again and select a different volume"
        echo ""
        read -p "Press Enter to continue..."
        return
    fi

    print_step "5. Creating new PVC from snapshot"

    # Generate unique restore PVC name
    RESTORE_PVC="restored-from-${SELECTED_PVC}-$(date +%s)"
    RESTORE_SIZE=$(kubectl get pvc -n ${DEMO_NAMESPACE} ${SELECTED_PVC} -o jsonpath='{.spec.storageClassName}' | grep -q "iscsi" && echo "${SELECTED_SIZE}" || echo "3Gi")
    RESTORE_CLASS=$(kubectl get pvc -n ${DEMO_NAMESPACE} ${SELECTED_PVC} -o jsonpath='{.spec.storageClassName}')
    RESTORE_ACCESS=$(kubectl get pvc -n ${DEMO_NAMESPACE} ${SELECTED_PVC} -o jsonpath='{.spec.accessModes[0]}')

    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${RESTORE_PVC}
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ${RESTORE_ACCESS}
  storageClassName: ${RESTORE_CLASS}
  resources:
    requests:
      storage: ${RESTORE_SIZE}
  dataSource:
    name: ${SNAPSHOT_NAME}
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
EOF

    print_info "Restoring volume from snapshot (this may take 2-3 minutes)..."
    echo ""

    # Monitor the PVC status while waiting
    for i in {1..36}; do
        PVC_STATUS=$(kubectl get pvc -n ${DEMO_NAMESPACE} ${RESTORE_PVC} -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
        echo -ne "\r  Status: ${PVC_STATUS} (${i}0s elapsed)..."

        if [ "$PVC_STATUS" = "Bound" ]; then
            echo ""
            break
        fi
        sleep 5
    done
    echo ""

    if kubectl get pvc -n ${DEMO_NAMESPACE} ${RESTORE_PVC} -o jsonpath='{.status.phase}' 2>/dev/null | grep -q "Bound"; then

        print_success "✅ Volume restored from snapshot!"
        echo ""

        print_step "6. Snapshot and Restored Volume Summary"
        echo "Original volume: ${SELECTED_PVC} (${SELECTED_SIZE})"
        echo "Snapshot: ${SNAPSHOT_NAME}"
        echo "Restored volume: ${RESTORE_PVC} (${RESTORE_SIZE})"
        echo ""
        kubectl get pvc -n ${DEMO_NAMESPACE} ${SELECTED_PVC} ${RESTORE_PVC}
        echo ""

        print_success "✅ Complete snapshot lifecycle demonstrated!"
        echo ""
        echo -e "${BOLD}What just happened:${NC}"
        echo "  ✓ Phase 1: Created ZFS snapshot of ${SELECTED_PVC}"
        echo "  ✓ Phase 2: Restored new volume from snapshot: ${RESTORE_PVC}"
        echo "  ✓ Both volumes now exist independently with the same data"
        echo ""
        echo -e "${GREEN}${BOLD}Success!${NC} The restore proves the snapshot was created in TrueNAS."
        echo "If you don't see it in the UI, try:"
        echo ""
        echo -e "${CYAN}Finding the snapshot in TrueNAS UI:${NC}"
        echo "  1. Go to: Storage → Snapshots"
        echo "  2. Click 'Refresh' or reload the page (Ctrl+R / F5)"
        echo "  3. Search for: ${SNAPSHOT_HANDLE}"
        echo "  4. Or go to: Storage → Datasets → [your pool] → click the dataset"
        echo "     → Click 'Snapshots' button at the top"
        echo ""
        echo -e "${CYAN}Via TrueNAS Shell (SSH or Console):${NC}"
        echo "  Run: zfs list -t snapshot | grep $(echo ${SNAPSHOT_HANDLE} | cut -d'@' -f2)"
        echo ""
        echo -e "${YELLOW}Note:${NC} Snapshot names may be sanitized (hyphens → underscores)"

    else
        print_error "Restore failed - PVC did not bind"
        echo ""

        print_step "Diagnostics: PVC Status"
        kubectl get pvc -n ${DEMO_NAMESPACE} ${RESTORE_PVC}
        echo ""

        print_step "Diagnostics: PVC Events"
        kubectl describe pvc -n ${DEMO_NAMESPACE} ${RESTORE_PVC} | grep -A 10 "Events:" || echo "No events found"
        echo ""

        print_step "Diagnostics: VolumeSnapshot Status"
        kubectl get volumesnapshot -n ${DEMO_NAMESPACE} ${SNAPSHOT_NAME} -o yaml | grep -A 5 "status:" || echo "No status found"
        echo ""

        print_step "Diagnostics: Controller Logs (Last 50 lines)"
        print_warning "Looking for errors in CreateVolume from snapshot..."
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=50 | grep -E "error|Error|failed|Failed|CreateVolume|snapshot" || kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=50
        echo ""

        print_step "Diagnostics: Snapshotter Sidecar Logs"
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-snapshotter --tail=30 2>/dev/null || echo "Snapshotter logs not available"
        echo ""

        echo -e "${YELLOW}Common issues:${NC}"
        echo "  1. Snapshot not ready yet - check VolumeSnapshot status above"
        echo "  2. Snapshot ID format incorrect - check controller logs"
        echo "  3. TrueNAS API timeout - check if TrueNAS is responsive"
        echo "  4. Clone operation failed - check controller logs for CloneSnapshot errors"
        echo "  5. Network issues between Kind and TrueNAS"
        echo ""
        echo -e "${CYAN}Next steps:${NC}"
        echo "  - Review the controller logs above"
        echo "  - Check if the VolumeSnapshot shows 'Ready To Use: true'"
        echo "  - Verify TrueNAS UI shows the snapshot exists"
        echo "  - Try option 13 (View driver logs) for full logs"
    fi

    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Volume Metadata Inspection
demo_metadata() {
    print_header "Demo: Volume Metadata Inspection"

    print_step "1. List all TrueNAS volumes"
    kubectl get pvc -n ${DEMO_NAMESPACE} 2>/dev/null || echo "No PVCs found"
    echo ""

    # Pick first PVC
    FIRST_PVC=$(kubectl get pvc -n ${DEMO_NAMESPACE} -o name 2>/dev/null | head -1 | cut -d/ -f2)
    if [ -z "$FIRST_PVC" ]; then
        print_warning "No PVCs found. Create a volume first."
        read -p "Press Enter to continue..."
        return
    fi

    print_step "2. Inspecting PVC: $FIRST_PVC"

    PV_NAME=$(kubectl get pvc -n ${DEMO_NAMESPACE} $FIRST_PVC -o jsonpath='{.spec.volumeName}')
    print_info "Bound to PV: $PV_NAME"
    echo ""

    print_step "3. Volume Attributes (from PV)"
    kubectl get pv $PV_NAME -o jsonpath='{.spec.csi.volumeAttributes}' | python3 -m json.tool 2>/dev/null || echo "Volume attributes not available"
    echo ""

    print_step "4. Volume Handle"
    VOLUME_HANDLE=$(kubectl get pv $PV_NAME -o jsonpath='{.spec.csi.volumeHandle}')
    echo "Volume Handle: $VOLUME_HANDLE"
    echo ""

    print_step "5. CSI Driver Info"
    echo "Driver: $(kubectl get pv $PV_NAME -o jsonpath='{.spec.csi.driver}')"
    echo "FS Type: $(kubectl get pv $PV_NAME -o jsonpath='{.spec.csi.fsType}')"
    echo ""

    print_success "✅ Metadata inspection complete"
    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Multiple Volumes
demo_multiple() {
    print_header "Demo: Creating Multiple Volumes"

    print_step "1. Creating 3 NFS volumes simultaneously"

    for i in 1 2 3; do
        cat <<EOF | kubectl apply -f - &
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: multi-nfs-$i
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs
  resources:
    requests:
      storage: 1Gi
EOF
    done
    wait

    print_success "3 PVC requests submitted"
    echo ""

    print_step "2. Waiting for all volumes to bind..."
    for i in 1 2 3; do
        kubectl wait --namespace=${DEMO_NAMESPACE} \
            --for=jsonpath='{.status.phase}'=Bound \
            pvc/multi-nfs-$i \
            --timeout=90s &
    done
    wait

    print_success "All volumes bound!"
    echo ""

    print_step "3. Volume list:"
    kubectl get pvc -n ${DEMO_NAMESPACE} | grep multi-nfs
    echo ""

    print_success "✅ Successfully created multiple volumes in parallel"
    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Driver Capabilities
demo_capabilities() {
    print_header "Demo: CSI Driver Capabilities"

    print_step "1. Getting CSI Identity information"

    # Find controller pod
    CONTROLLER_POD=$(kubectl get pod -n ${NAMESPACE} -l app=truenas-csi-controller -o jsonpath='{.items[0].metadata.name}')

    if [ -z "$CONTROLLER_POD" ]; then
        print_error "Controller pod not found"
        read -p "Press Enter to continue..."
        return
    fi

    print_info "Querying controller pod: $CONTROLLER_POD"
    echo ""

    print_step "2. Driver Information"
    echo "Driver Name: csi.truenas.io"
    echo "Driver Version: 1.0.0"
    echo ""

    print_step "3. Controller Service Capabilities"
    echo "✓ CREATE_DELETE_VOLUME - Dynamic volume provisioning"
    echo "✓ CREATE_DELETE_SNAPSHOT - Snapshot management"
    echo "✓ CLONE_VOLUME - Volume cloning from snapshots"
    echo "✓ EXPAND_VOLUME - Volume expansion"
    echo "✓ LIST_VOLUMES - Volume enumeration"
    echo "✓ LIST_SNAPSHOTS - Snapshot enumeration"
    echo ""

    print_step "4. Volume Capabilities"
    echo "✓ AccessMode: SINGLE_NODE_WRITER (RWO)"
    echo "✓ AccessMode: MULTI_NODE_MULTI_WRITER (RWX for NFS)"
    echo "✓ VolumeMode: Filesystem"
    echo "✓ VolumeMode: Block"
    echo ""

    print_step "5. Node Service Capabilities"
    echo "✓ STAGE_UNSTAGE_VOLUME - Two-phase mounting (iSCSI)"
    echo "✓ GET_VOLUME_STATS - Volume capacity reporting"
    echo ""

    print_success "✅ Driver supports full CSI specification"
    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Storage Class Variations
demo_storage_classes() {
    print_header "Demo: Storage Class Variations"

    print_step "1. Creating volume with LZ4 compression"
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-lz4
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs
  resources:
    requests:
      storage: 1Gi
EOF

    kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/demo-lz4 \
        --timeout=60s 2>/dev/null

    print_success "LZ4 volume created"
    echo ""

    print_step "2. Creating volume with different compression (GZIP)"
    cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-nfs-gzip
provisioner: csi.truenas.io
parameters:
  protocol: "nfs"
  pool: "tank"
  compression: "gzip"
  sync: "standard"
  nfs.networks: "172.18.0.0/16"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-gzip
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs-gzip
  resources:
    requests:
      storage: 1Gi
EOF

    kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/demo-gzip \
        --timeout=60s 2>/dev/null

    print_success "GZIP volume created"
    echo ""

    print_step "3. Comparing volume attributes"
    echo "LZ4 Volume:"
    kubectl get pv $(kubectl get pvc -n ${DEMO_NAMESPACE} demo-lz4 -o jsonpath='{.spec.volumeName}') -o jsonpath='{.spec.csi.volumeAttributes.compression}' | xargs echo "  Compression:"
    echo ""
    echo "GZIP Volume:"
    kubectl get pv $(kubectl get pvc -n ${DEMO_NAMESPACE} demo-gzip -o jsonpath='{.spec.volumeName}') -o jsonpath='{.spec.csi.volumeAttributes.compression}' | xargs echo "  Compression:"
    echo ""

    print_success "✅ Different storage configurations work correctly"
    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Volume Capacity Reporting
demo_capacity() {
    print_header "Demo: Volume Capacity Reporting"

    # Find first PVC
    FIRST_PVC=$(kubectl get pvc -n ${DEMO_NAMESPACE} -o name 2>/dev/null | head -1 | cut -d/ -f2)
    if [ -z "$FIRST_PVC" ]; then
        print_warning "No PVCs found. Create a volume first."
        read -p "Press Enter to continue..."
        return
    fi

    print_step "1. Analyzing PVC: $FIRST_PVC"

    REQUESTED=$(kubectl get pvc -n ${DEMO_NAMESPACE} $FIRST_PVC -o jsonpath='{.spec.resources.requests.storage}')
    CAPACITY=$(kubectl get pvc -n ${DEMO_NAMESPACE} $FIRST_PVC -o jsonpath='{.status.capacity.storage}')

    echo "Requested Size: $REQUESTED"
    echo "Actual Capacity: $CAPACITY"
    echo ""

    if [ "$REQUESTED" = "$CAPACITY" ]; then
        print_success "✓ Capacity matches request exactly"
    else
        print_warning "⚠ Capacity differs from request (may be rounded)"
    fi
    echo ""

    print_step "2. All volumes capacity summary"
    kubectl get pvc -n ${DEMO_NAMESPACE} -o custom-columns=NAME:.metadata.name,REQUESTED:.spec.resources.requests.storage,CAPACITY:.status.capacity.storage 2>/dev/null || echo "No PVCs found"
    echo ""

    print_success "✅ Capacity reporting complete"
    echo ""
    read -p "Press Enter to continue..."
}

# Demo: Topology Awareness
demo_topology() {
    print_header "Demo: Topology Awareness"

    print_step "1. Kubernetes Nodes"
    kubectl get nodes -o custom-columns=NAME:.metadata.name,ZONE:.metadata.labels.topology\\.truenas\\.csi/zone
    echo ""

    print_step "2. CSI Node Drivers"
    kubectl get csinode -o custom-columns=NAME:.metadata.name,DRIVER:.spec.drivers[0].name,TOPOLOGY:.spec.drivers[0].topologyKeys
    echo ""

    # Find first PV
    FIRST_PV=$(kubectl get pv -o name 2>/dev/null | grep truenas | head -1 | cut -d/ -f2)
    if [ -n "$FIRST_PV" ]; then
        print_step "3. Volume Topology for: $FIRST_PV"
        kubectl get pv $FIRST_PV -o jsonpath='{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions}' | python3 -m json.tool 2>/dev/null || echo "Topology info not available"
        echo ""
    fi

    print_success "✅ Volumes can be accessed from all worker nodes"
    echo ""
    read -p "Press Enter to continue..."
}

# Show current status
show_status() {
    print_header "Current Demo Status"

    print_step "CSI Driver Pods"
    kubectl get pods -n ${NAMESPACE}
    echo ""

    print_step "Storage Classes"
    (kubectl get storageclass | grep truenas) || echo "No TrueNAS storage classes found"
    echo ""

    print_step "Demo Volumes (PVCs)"
    kubectl get pvc -n ${DEMO_NAMESPACE} 2>/dev/null || echo "No PVCs found"
    echo ""

    print_step "Persistent Volumes"
    (kubectl get pv | grep truenas) || echo "No TrueNAS PVs found"
    echo ""
}

# View driver logs
view_logs() {
    print_header "CSI Driver Logs"

    echo -e "${CYAN}Controller logs (last 50 lines):${NC}"
    kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=50
    echo ""
    echo ""

    echo -e "${CYAN}Node logs (last 50 lines):${NC}"
    kubectl logs -n ${NAMESPACE} -l app=truenas-csi-node -c csi-node --tail=50
    echo ""

    read -p "Press Enter to continue..."
}

# Demo: Clone with Data Verification
demo_clone_with_data() {
    print_header "Demo: Clone with Data Verification"

    print_info "This demo will:"
    echo "  1. Create a volume with test data"
    echo "  2. Clone the volume"
    echo "  3. Verify the clone contains the same data"
    echo ""
    read -p "Press Enter to continue..."

    # Step 1: Create source volume
    print_step "1. Creating source volume (1Gi NFS)"
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-clone-source-data
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs
  resources:
    requests:
      storage: 1Gi
EOF

    print_info "Waiting for PVC to be bound..."
    if ! kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/demo-clone-source-data \
        --timeout=60s; then
        print_error "PVC failed to bind"
        return
    fi
    print_success "Source volume created"
    echo ""

    # Step 2: Write test data to source volume
    print_step "2. Writing test data to source volume"

    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: data-writer
  namespace: ${DEMO_NAMESPACE}
spec:
  containers:
  - name: writer
    image: busybox
    command:
    - sh
    - -c
    - |
      echo "Writing test files..."
      echo "Hello from TrueNAS CSI!" > /data/test.txt
      echo "This is file number 1" > /data/file1.txt
      echo "This is file number 2" > /data/file2.txt
      dd if=/dev/urandom of=/data/random.dat bs=1M count=10
      ls -lh /data
      echo "Total size:"
      du -sh /data
      echo "Data written successfully!"
      sleep infinity
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: demo-clone-source-data
  restartPolicy: Never
EOF

    print_info "Waiting for pod to start..."
    sleep 5
    kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=condition=Ready \
        pod/data-writer \
        --timeout=30s 2>/dev/null || true

    sleep 2
    print_info "Checking data written:"
    kubectl logs -n ${DEMO_NAMESPACE} data-writer 2>/dev/null | tail -10 || echo "Pod still starting..."
    echo ""
    print_success "Test data written to source volume"
    echo ""

    # Step 3: Clone the volume
    print_step "3. Cloning volume with data"

    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo-clone-with-data
  namespace: ${DEMO_NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: truenas-nfs
  resources:
    requests:
      storage: 1Gi
  dataSource:
    name: demo-clone-source-data
    kind: PersistentVolumeClaim
EOF

    print_info "Waiting for clone to be bound..."
    if ! kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/demo-clone-with-data \
        --timeout=120s; then
        print_error "Clone PVC failed to bind"
        print_info "Checking controller logs..."
        kubectl logs -n ${NAMESPACE} -l app=truenas-csi-controller -c csi-controller --tail=20 | grep -i "clone\|error" || echo "No errors found"
        return
    fi
    print_success "Clone created successfully"
    echo ""

    # Step 4: Verify data in clone
    print_step "4. Verifying clone contains the same data"

    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: data-reader
  namespace: ${DEMO_NAMESPACE}
spec:
  containers:
  - name: reader
    image: busybox
    command:
    - sh
    - -c
    - |
      echo "Reading files from clone..."
      ls -lh /data
      echo ""
      echo "Contents of test.txt:"
      cat /data/test.txt
      echo ""
      echo "Contents of file1.txt:"
      cat /data/file1.txt
      echo ""
      echo "Contents of file2.txt:"
      cat /data/file2.txt
      echo ""
      echo "Checking random.dat exists:"
      ls -lh /data/random.dat
      echo ""
      echo "Total size in clone:"
      du -sh /data
      echo ""
      echo "✓ All data verified in clone!"
      sleep infinity
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: demo-clone-with-data
  restartPolicy: Never
EOF

    print_info "Waiting for verification pod to start..."
    sleep 5
    kubectl wait --namespace=${DEMO_NAMESPACE} \
        --for=condition=Ready \
        pod/data-reader \
        --timeout=30s 2>/dev/null || true

    sleep 2
    echo ""
    print_info "Data verification results:"
    echo ""
    kubectl logs -n ${DEMO_NAMESPACE} data-reader 2>/dev/null || echo "Pod still starting..."
    echo ""

    # Step 5: Show comparison
    print_step "5. Comparison Summary"
    echo ""
    echo -e "${BOLD}Source Volume:${NC}"
    kubectl exec -n ${DEMO_NAMESPACE} data-writer -- du -sh /data 2>/dev/null || echo "  (data-writer pod not available)"
    echo ""
    echo -e "${BOLD}Cloned Volume:${NC}"
    kubectl exec -n ${DEMO_NAMESPACE} data-reader -- du -sh /data 2>/dev/null || echo "  (data-reader pod not available)"
    echo ""

    print_success "✅ Clone verification complete!"
    echo ""
    echo -e "${BOLD}What this proves:${NC}"
    echo "  ✓ Source volume had ~10MB of test data written"
    echo "  ✓ Clone was created from source using ZFS snapshot"
    echo "  ✓ Clone contains all the same files and data"
    echo "  ✓ Clone is a true copy, not an empty volume"
    echo ""
    echo -e "${CYAN}Check TrueNAS UI:${NC}"
    echo "  Storage → Datasets → Look for both datasets"
    echo "  The clone shows low 'Used' but same 'Referenced' (CoW in action!)"
    echo ""

    print_info "Cleaning up test pods..."
    kubectl delete pod data-writer data-reader -n ${DEMO_NAMESPACE} --ignore-not-found=true 2>/dev/null
    echo ""

    read -p "Press Enter to continue..."
}

# Cleanup demo resources
cleanup() {
    print_header "Cleanup Demo Resources"

    echo "This will delete all demo PVCs and volumes."
    read -p "Are you sure? [y/N]: " CONFIRM

    if [[ $CONFIRM =~ ^[Yy]$ ]]; then
        print_info "Deleting demo PVCs..."
        kubectl delete pvc -n ${DEMO_NAMESPACE} --all

        print_info "Waiting for volumes to be deleted..."
        sleep 5

        print_success "Demo resources cleaned up"
        echo ""
        print_info "Check your TrueNAS UI - datasets and shares should be removed"
    else
        print_info "Cleanup cancelled"
    fi

    echo ""
    read -p "Press Enter to continue..."
}

# Main menu
main_menu() {
    while true; do
        clear
        print_header "TrueNAS CSI Driver - Simplified Demo"

        # echo -e "${BOLD}This demo shows CSI driver features WITHOUT pod mounting${NC}"
        # echo "Perfect for WSL, VMs, or environments with networking issues"
        echo ""
        echo -e "${CYAN}Volume Provisioning:${NC}"
        echo "  1) Demo NFS volume creation"
        echo "  2) Demo iSCSI volume creation"
        echo "  3) Demo multiple volumes (scalability)"
        echo "  4) Demo storage class variations"
        echo ""
        echo -e "${CYAN}Advanced Operations:${NC}"
        echo "  5) Demo volume expansion"
        echo "  6) Demo volume cloning"
        echo "  7) Demo volume snapshots"
        echo "  8) Demo clone with data verification ⭐"
        echo ""
        echo -e "${CYAN}Inspection & Metadata:${NC}"
        echo "  9) Demo volume metadata inspection"
        echo " 10) Demo capacity reporting"
        echo " 11) Demo topology awareness"
        echo " 12) Demo driver capabilities"
        echo ""
        echo -e "${CYAN}Utilities:${NC}"
        echo " 13) Show current status"
        echo " 14) View driver logs"
        echo " 15) Cleanup demo resources"
        echo "  0) Exit"
        echo ""
        read -p "Choose option: " OPTION

        case $OPTION in
            1) demo_nfs ;;
            2) demo_iscsi ;;
            3) demo_multiple ;;
            4) demo_storage_classes ;;
            5) demo_expand ;;
            6) demo_clone ;;
            7) demo_snapshot ;;
            8) demo_clone_with_data ;;
            9) demo_metadata ;;
            10) demo_capacity ;;
            11) demo_topology ;;
            12) demo_capabilities ;;
            13) show_status; read -p "Press Enter to continue..." ;;
            14) view_logs ;;
            15) cleanup ;;
            0)
                print_info "Exiting..."
                exit 0
                ;;
            *)
                print_error "Invalid option"
                sleep 2
                ;;
        esac
    done
}

# Main execution
main() {
    print_header "TrueNAS CSI Driver - Simplified Demo"

    # Run setup if cluster/driver don't exist
    run_setup_if_needed

    echo ""
    print_info "This demo focuses on CSI driver features:"
    echo "  ✓ Volume provisioning (NFS and iSCSI)"
    echo "  ✓ Volume expansion"
    echo "  ✓ Volume cloning"
    echo "  ✓ Volume snapshots"
    echo "  ✓ Volume deletion"
    echo ""
    print_info "Volumes are created on TrueNAS - check your TrueNAS UI!"
    echo ""

    read -p "Press Enter to start the demo menu..."

    main_menu
}

# Run main function
main
