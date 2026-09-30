# Snapshots and Clones

A VolumeSnapshot is a ZFS snapshot of the volume's dataset, `<dataset>@<name>`,
where the name comes from the Kubernetes snapshotter: `snapshot-<uid>` unless its
`--snapshot-name-prefix` is changed. A volume restored from a
VolumeSnapshot, and a volume cloned from another PVC, is a ZFS clone: it shares the
blocks it has in common with its source, so it costs only the space its changes
take.

## Lifecycle

A ZFS clone depends on the snapshot it was made from, so that snapshot has to stay
as long as the clone does. The driver deletes snapshots with the destroy deferred,
so ZFS removes each one at the right moment:

- **Deleting a VolumeSnapshot** removes it from Kubernetes at once. If a volume
  restored from it still exists, TrueNAS keeps the ZFS snapshot, marked for
  destruction, until that volume is deleted too. The driver no longer lists it.
- **Cloning a PVC** takes a snapshot of the source named
  `csi-clone-<volume>-<time>`, clones it, and deletes it straight away. ZFS keeps
  it until the clone is deleted, then destroys it.

While a snapshot is kept for a clone, the blocks it holds are counted against the
source volume's dataset on TrueNAS, and they are freed when the clone goes.

## Leftovers from earlier versions

Before v1.3.1 the driver deleted snapshots without deferring the destroy. ZFS
refused whenever a clone depended on the snapshot, and the driver carried on, so
two kinds of snapshot were left on the source volumes, holding space for as long as
those volumes existed.

**`csi-clone-*` snapshots**, one for every PVC ever cloned, are removed
automatically: the controller deletes them each time it starts, and logs how many.
Any a clone still uses is destroyed when that clone is deleted.

**`snapshot-*` snapshots** whose VolumeSnapshot was deleted while a restored volume
existed cannot be told apart from live ones by the driver, so check for them by
hand. List the snapshots Kubernetes knows about, on every cluster that uses the
appliance:

```bash
kubectl get volumesnapshotcontent -o jsonpath='{range .items[*]}{.status.snapshotHandle}{"\n"}{end}'
```

and compare them with the ones on TrueNAS, from its shell:

```bash
zfs list -H -t snapshot -o name | grep '@snapshot-'
```

A snapshot in the second list but not in the first is an orphan, unless you kept
it on purpose by deleting its VolumeSnapshotContent under `deletionPolicy: Retain`.
Delete it with `zfs destroy -d <name>`, which also defers the destroy if a volume
still depends on it.
