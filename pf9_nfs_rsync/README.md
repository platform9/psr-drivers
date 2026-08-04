# PF9 NFS + rsync Replication Driver

A Cinder driver that gives an **NFS backend the same group-replication + manage API
surface a real replication array exposes**, using rsync between two NFS exports to
play the role of array-to-array replication. It lets PSR run its full DR flow
(enable → replicate → failover → adopt → boot) end-to-end with **no storage array**.

Prototype / lab / CI only — not a supported production driver.

---
## What's included

| File | Purpose |
|------|---------|
| `cinder/volume/drivers/pf9_nfs_rsync/pf9_nfs_rsync.py` | The driver (group replication + manage + in-driver replication thread) |
| `cinder/volume/drivers/pf9_nfs_rsync/__init__.py` | Package marker |
| `pf9_nfs_rsync/cinder.conf.sample` | Backend stanza to copy into cinder.conf |

Class path (note the package): `cinder.volume.drivers.pf9_nfs_rsync.pf9_nfs_rsync.PF9NFSRsyncDriver`

---
## What it implements

Subclasses the upstream `NfsDriver` and adds the standard Cinder replication +
manage contract — the same methods a real array driver provides, and the ones
Hitachi's driver leaves as gaps in replication-active mode:

| Method | Role |
|--------|------|
| `enable_replication` | create a pair per volume — distinct pvol (1001+) / svol (2001+) LUN ids, state `PAIR`, writes `cg-<id>/.copygroup.json` (the "copy-group status" PSR discovers) |
| `disable_replication` | tear pairs down → `SMPL` |
| `failover_replication` | split + promote → `SSWS`, `reversed`; `--secondary-backend-id default` = **failback** → `PAIR` |
| `manage_existing` | adopt a promoted secondary volume **by its svol id** (`source-name`); resolves svol → file → registers as a local volume; also drops a `promoted` marker |
| `manage_existing_get_size`, `unmanage`, `create_group*`, `update_group`, `delete_group`, `list_replication_targets` | rest of the contract |

**In-driver replication (no external script).** A daemon thread started in
`do_setup()` does what the old `pf9_rsync_loop.sh` did — each interval, if a CG is
`PAIR`/`COPY`, it ships the export to the secondary as a fresh generation, atomically
flips `incoming/current` to it, prunes old generations, and stamps per-CG lag
(`.last_sync.json`, which PSR reads for RPO). State-gated: `SSWS`/`SMPL`/reversed
stop it; `PAIR` resumes it.

**Failover from the secondary.** When B adopts a volume (`manage_existing`), the
driver writes a `promoted` marker on B. The primary's replication thread checks the
peer for that marker each cycle and pauses shipping — so a failover triggered on B
halts A even while A is alive (no split-brain overwrite). Failback clears it.

---
## Setup (per site)

Each site runs its own cinder-volume with this driver + a local NFS export.

1. **NFS export + backing store** (example uses a dedicated disk):
   ```bash
   mkfs.ext4 /dev/vdb && mkdir -p /export/psr
   echo '/dev/vdb /export/psr ext4 defaults 0 2' >> /etc/fstab && mount /export/psr
   chmod 1777 /export/psr
   apt-get install -y nfs-kernel-server
   echo '/export/psr 10.0.0.0/8(rw,sync,no_root_squash,no_subtree_check)' >> /etc/exports
   exportfs -ra && systemctl restart nfs-kernel-server
   mkdir -p /etc/cinder && echo '<this-host-ip>:/export/psr' > /etc/cinder/pf9_nfs_shares
   ```
2. **Driver + config:** deploy the driver (see repo `DEPLOYMENT.md` / `deploy.sh`),
   copy `cinder.conf.sample` into the `[psr-dr]` stanza, restart cinder-volume.
   Confirm `Driver initialization completed successfully` in
   `/var/log/pf9/cindervolume-base.log`.
3. **Cross-site SSH (primary → secondary):** the replication thread rsyncs over
   ssh as the cinder host user. Add the primary host's key to the secondary's
   `authorized_keys` and set `pf9_replication_peer = <user>@<secondary-ip>`.
4. **Types (on the primary DU):**
   ```bash
   openstack volume type create psr-dr-repl \
     --property volume_backend_name=psr-dr --property replication_enabled='<is> True'
   cinder --os-volume-api-version 3.38 group-type-create psr-dr-cg
   cinder --os-volume-api-version 3.38 group-type-key psr-dr-cg set \
     consistent_group_replication_enabled='<is> True'
   ```

---
## DR flow (what PSR / an operator drives)

```
# --- primary ---
openstack volume create --type psr-dr-repl --image <cirros> --size 1 vol-A
cinder group-create --name cg-A psr-dr-cg psr-dr-repl
cinder group-update --add-volumes <vol-A-id> <cg>
cinder group-enable-replication <cg>            # -> PAIR; thread replicates to secondary

cinder group-failover-replication <cg>          # planned failover -> SSWS; thread stops

# --- secondary (recover; also works standalone if primary is dead) ---
POOL=$(cinder get-pools | awk '/\| name/{print $4}')   # note: pool name is per-DU
cinder manage $POOL <svolId> --bootable --name vol-A-recovered   # adopt by svol id
openstack server create --flavor m1.tiny --volume <recovered> --network <net> vm-A

# --- primary (failback) ---
cinder group-failover-replication <cg> --secondary-backend-id default   # -> PAIR; resumes
```

---
## Gotchas (learned the hard way — already handled in the driver, listed for operators)

- **Volume must report `replication_status`** before `enable_replication` — the driver
  seeds `disabled` for replication-capable types on create.
- **`failed-over` is a Cinder dead-end** — exit only via failback
  (`--secondary-backend-id default`).
- **Pool name is per-DU** — the manage host is `<host>#<pool>`; read the exact pool
  from `cinder get-pools` (it is not necessarily `#psr-dr`).
- **`incoming/` must exist on the secondary** — rsync will not create the parent dir.
- **manage uses NfsDriver layout** — the adopted file lands as `volume-<id>` at the
  export root with `provider_location = <share>`, so Nova attach works.
- **Snapshots need `nfs_snapshot_support = true`** for the test-without-failover flow
  (`snapshot create` → `volume create --snapshot`).
