# TrueNAS CSI Helm repository

This directory is a Helm chart repository. The chart source lives in
[`truenas-csi/`](truenas-csi), and the packaged versions plus `index.yaml` are
committed alongside it, so the directory can be added as a repository directly.

## Install

```bash
helm repo add truenas-csi https://raw.githubusercontent.com/truenas/truenas-csi/master/charts
helm repo update

helm install truenas-csi truenas-csi/truenas-csi \
  --namespace truenas-csi --create-namespace \
  --set truenas.url=wss://10.0.0.100 \
  --set truenas.apiKey=YOUR-API-KEY \
  --set truenas.defaultPool=tank \
  --set truenas.insecureSkipTLS=true
```

`insecureSkipTLS=true` is needed while TrueNAS still uses the self-signed
certificate it ships with. Leaving it false against a self-signed certificate
leaves the pods un-Ready without an obvious error.

To install a specific version, add `--version 1.3.0`. To install straight from a
checkout without the repository, point Helm at the directory:

```bash
helm install truenas-csi ./charts/truenas-csi --namespace truenas-csi --create-namespace -f my-values.yaml
```

See [`truenas-csi/README.md`](truenas-csi/README.md) for the full list of values,
including how to use a Secret you manage yourself, how to set the kubelet
directory for K3s and MicroK8s, and how to create StorageClasses with the chart.

## Relationship to deploy/truenas-csi-driver.yaml

The chart and [`deploy/truenas-csi-driver.yaml`](../deploy/truenas-csi-driver.yaml)
install the same thing; pick whichever fits how you manage the cluster. Both are
maintained, and `make verify-chart` compares the rendered chart against the flat
manifest so the two cannot drift apart: it checks images, container arguments,
environment variables, volumes and host paths, mounts, lifecycle hooks, RBAC
rules and the CSIDriver spec.

The chart deliberately keeps the same object names and the same `app` pod
selector labels as the flat manifest. A Deployment's selector cannot be changed
after it is created, so this is what lets an existing `kubectl apply` install be
adopted by Helm rather than having to be torn down first.

The OpenShift install path is separate: it uses the operator and OLM, not this
chart. See [`docs/openshift`](../docs/openshift).

## Publishing a new version

```bash
make chart-package     # lints, verifies against the manifest, packages, reindexes
```

Then commit `charts/index.yaml` and the new `.tgz`. `make bump-version` keeps the
chart version and appVersion in step with the driver release.
