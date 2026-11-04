#!/usr/bin/env bash
# Render the chart and check expected output.
# Usage: bash helm/test-chart.sh
set -uo pipefail

CHART="$(cd "$(dirname "$0")/chart" && pwd)"
ERRORS=0

check() {
  local desc="$1" pattern="$2"
  if echo "$output" | grep -q "$pattern"; then
    echo "  PASS  $desc"
  else
    echo "  FAIL  $desc (pattern not found: $pattern)"
    ERRORS=$((ERRORS + 1))
  fi
}

absent() {
  local desc="$1" pattern="$2"
  if echo "$output" | grep -q "$pattern"; then
    echo "  FAIL  $desc (pattern should be absent: $pattern)"
    ERRORS=$((ERRORS + 1))
  else
    echo "  PASS  $desc"
  fi
}

# ── 1. required validation ────────────────────────────────────────────────────
echo "1. required validation (missing apiSecret.name)"
output=$(helm template test "$CHART" 2>&1 || true)
check "apiSecret.name required error" "apiSecret.name must be set"

# ── 2. Standard render ────────────────────────────────────────────────────────
echo ""
echo "2. Standard render (openshift.enabled=false)"
output=$(helm template test "$CHART" \
  --namespace truenas-csi \
  --set apiSecret.name=truenas-secret \
  --set truenas.url="wss://192.168.1.1" \
  --set truenas.defaultPool=tank \
  --set truenas.nfsServer=192.168.1.1 \
  --set truenas.iscsiPortal="192.168.1.1:3260" 2>&1)

absent "no Namespace object rendered"   "kind: Namespace"
check "controller ServiceAccount"       "truenas-csi-controller-sa"
check "node ServiceAccount"             "truenas-csi-node-sa"
check "controller ClusterRole"          "truenas-csi-controller-role"
check "node ClusterRole"                "truenas-csi-node-role"
check "CSIDriver"                       "kind: CSIDriver"
check "driver name"                     "csi.truenas.io"
check "truenasURL in ConfigMap"         "wss://192.168.1.1"
check "defaultPool in ConfigMap"        "defaultPool.*tank"
check "CSI_DRIVER_NAME env"             "CSI_DRIVER_NAME"
check "NVMe-oF portal env"              "TRUENAS_NVMEOF_PORTAL"
check "node loads nvme modules"         "modprobe nvme_tcp"
check "node mounts host /lib/modules"   "path: /lib/modules"
absent "nvmeofPortal absent when unset" "nvmeofPortal:"
check "image tag defaults to appVersion" "truenas-csi:v1.1.1"
check "standard app name label"         "app.kubernetes.io/name: truenas-csi"
check "standard instance label"         "app.kubernetes.io/instance: test"
check "standard version label"          "app.kubernetes.io/version:"
check "controller component label"      "app.kubernetes.io/component: controller"
check "node component label"            "app.kubernetes.io/component: node"
absent "SCC absent (openshift.enabled=false)" "kind: SecurityContextConstraints"

# ── 3. OpenShift render ───────────────────────────────────────────────────────
echo ""
echo "3. OpenShift render (openshift.enabled=true)"
output=$(helm template test "$CHART" \
  --namespace truenas-csi \
  --set apiSecret.name=truenas-secret \
  --set truenas.url="wss://192.168.1.1" \
  --set truenas.defaultPool=tank \
  --set truenas.nfsServer=192.168.1.1 \
  --set truenas.iscsiPortal="192.168.1.1:3260" \
  --set openshift.enabled=true 2>&1)

check "node SCC"                              "truenas-csi-node-scc"
check "controller SCC"                        "truenas-csi-controller-scc"
check "capabilities ConfigMap"                "truenas-csi-capabilities"
check "node SCC user namespaced"              "serviceaccount:truenas-csi:truenas-csi-node-sa"
check "controller SCC user namespaced"        "serviceaccount:truenas-csi:truenas-csi-controller-sa"
check "capabilities driver-name from values"  "driver-name.*csi.truenas.io"
check "capabilities version from appVersion"  "driver-version.*v1.1.1"

# ── 4. Custom driver name ─────────────────────────────────────────────────────
echo ""
echo "4. Custom driver name (csiDriver.name=csi.custom.io)"
output=$(helm template test "$CHART" \
  --namespace truenas-csi \
  --set apiSecret.name=truenas-secret \
  --set truenas.url="wss://192.168.1.1" \
  --set truenas.defaultPool=tank \
  --set truenas.nfsServer=192.168.1.1 \
  --set truenas.iscsiPortal="192.168.1.1:3260" \
  --set csiDriver.name=csi.custom.io 2>&1)

check "CSIDriver object name"    "name: csi.custom.io"
check "CSI_DRIVER_NAME env value" "value: \"csi.custom.io\""
check "node hostPath"            "plugins/csi.custom.io/"

# ── 5. Custom image tag ───────────────────────────────────────────────────────
echo ""
echo "5. Custom image tag (images.csiDriver.tag=v9.9.9)"
output=$(helm template test "$CHART" \
  --namespace truenas-csi \
  --set apiSecret.name=truenas-secret \
  --set truenas.url="wss://192.168.1.1" \
  --set truenas.defaultPool=tank \
  --set truenas.nfsServer=192.168.1.1 \
  --set truenas.iscsiPortal="192.168.1.1:3260" \
  --set images.csiDriver.tag=v9.9.9 2>&1)

check "custom image tag honoured" "truenas-csi:v9.9.9"

# ── 6. NVMe-oF portal set ────────────────────────────────────────────────────
echo ""
echo "6. NVMe-oF portal set (truenas.nvmeofPortal)"
output=$(helm template test "$CHART" \
  --namespace truenas-csi \
  --set apiSecret.name=truenas-secret \
  --set truenas.url="wss://192.168.1.1" \
  --set truenas.defaultPool=tank \
  --set truenas.nfsServer=192.168.1.1 \
  --set truenas.iscsiPortal="192.168.1.1:3260" \
  --set truenas.nvmeofPortal="192.168.1.1:4420" 2>&1)

check "nvmeofPortal in ConfigMap"       "nvmeofPortal:.*192.168.1.1:4420"

# ── Result ────────────────────────────────────────────────────────────────────
echo ""
if [ "$ERRORS" -eq 0 ]; then
  echo "All checks passed."
else
  echo "$ERRORS check(s) failed."
  exit 1
fi
