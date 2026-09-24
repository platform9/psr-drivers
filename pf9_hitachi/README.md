# Hitachi VSP Extended Replication Driver for Platform9

Platform9 fork of the upstream OpenStack Cinder Hitachi VSP driver, extended to
close 8 gaps that block a full disaster-recovery workflow on Universal
Replicator (UR) consistency groups.

The modules are a drop-in replacement for upstream
`cinder/volume/drivers/hitachi/`. They import `cinder.volume.drivers.hitachi.*`,
so they only take effect when installed there; a copy anywhere else runs the
stock upstream code.

Driver version: **2.9.0** (`hbsd_utils.VERSION`) — upstream 2.8.4 plus
`2.9.0 - Add volume group replication support`.

---

## 📦 What's Included

### Driver modules — `cinder/volume/drivers/pf9_hitachi/` (installed as `cinder/volume/drivers/hitachi/`)

| File | Purpose |
|------|---------|
| `hbsd_replication.py` | `HBSDREPLICATION` — remote replication (GAD/UR) **and all 8 gap implementations** [See implemented gaps](#-implemented-gaps-h1h8) |
| `hbsd_common.py` | `HBSDCommon` — shared logic for FC and iSCSI; also holds the non-replicating stubs that reject group-replication calls |
| `hbsd_rest.py` | `HBSDREST` — REST-backed implementation of the common interface |
| `hbsd_rest_api.py` | `RestApiClient` — low-level REST client, remote-copy-group and journal calls |
| `hbsd_fc.py` | Fibre Channel driver (`HBSDFCDriver`) |
| `hbsd_iscsi.py` | iSCSI driver (`HBSDISCSIDriver`) |
| `hbsd_rest_fc.py` | FC-specific REST operations |
| `hbsd_rest_iscsi.py` | iSCSI-specific REST operations |
| `hbsd_utils.py` | `HBSDMsg` message catalogue, `Config`, connector caching, shared constants |

### Unit tests — `cinder/tests/unit/volume/drivers/pf9_hitachi/`

| File | Covers |
|------|--------|
| `test_hitachi_hbsd_replication_fc.py` | `HBSDREPLICATION`, group replication helpers, REST group-replication calls, entry-point driver classes |
| `test_hitachi_hbsd_rest_fc.py` | FC REST path, `update_group`, group snapshots, group-replication rejection path |
| `test_hitachi_hbsd_rest_iscsi.py` | iSCSI REST path |
| `test_hitachi_hbsd_mirror_fc.py` | GAD mirror path |
| `test_hitachi_hbsd_utils.py` | `HBSDMsg` catalogue and message-id uniqueness |

---

## 📋 Implemented Gaps (H1–H8)

All eight are implemented on `hbsd_replication.HBSDREPLICATION` and surfaced
through `hbsd_fc.HBSDFCDriver` / `hbsd_iscsi.HBSDISCSIDriver`.

| Gap | Method | Purpose |
|-----|--------|---------|
| H1 | `manage_existing()` | Import promoted S-VOLs after failover |
| H2 | `unmanage()` | Remove Cinder claim without affecting the pair |
| H3 | `update_group()` | Add/remove volumes from a live CG |
| H4 | `create_group_snapshot()` | Snapshot S-VOLs for test recovery |
| H5 | `enable_replication()` | Activate UR replication on a CG |
| H6 | `disable_replication()` | Delete UR pairs and their journals |
| H7 | `failover_replication()` | Split/promote S-VOLs, and fail back |
| H8 | `list_replication_targets()` | Return the secondary `backend_id` |

When the backend is **not** configured for replication, `HBSDCommon` answers
H5–H7 by raising `VolumeDriverException` via
`HBSDMsg.GROUP_REPLICATION_NOT_CONFIGURED`, and H8 by returning
`{'replication_targets': []}` — so a misconfigured backend fails loudly at the
API rather than silently doing nothing.

---

## 🔍 Additional Behaviour

Beyond the eight entry points:

| Behaviour | Where |
|-----------|-------|
| `manage_existing_get_size()` — size of an existing LDEV for import validation | `hbsd_replication.py` |
| Copy-group binding recorded in volume metadata (`replication_copy_group`), so a volume added to a CG later still resolves to the right copy group | `_resolve_copy_group_name()` |
| Group-name binding by prefix `hbsd-cg:<name>` for adopting an existing array copy group | `_resolve_copy_group_name()` |
| Journal lifecycle — created on first pair, deleted on `disable_replication()` | `_group_repl_journal_ids()` / `_group_repl_delete_journals()` |
| S side found per copy group — adoption, failover, pair status and target listing find which array holds each copy group's S side; a volume with only an S-VOL is found on the array whose LDEV carries its label | `_copy_group_svol_side()` / `_resolve_sldev_owner()` |
| Per-copy-group pair state in `update_volume_stats()` capabilities — **off by default**, see `hitachi_replication_report_pair_status` | `_pair_status_capabilities()` |
| Graceful vs emergency failover (`split` vs `takeover ... forceSplit`) | `_failover_mode()` |
| Attach and detach of an adopted S-VOL on the array holding it; attach needs its copy group taken over (SSWS) or its pair gone | `_local_adopted_svol()` / `_check_adopted_svol_writable()` |

---

## 🏗️ Architecture

`volume_driver` points at the upstream `hbsd_fc.HBSDFCDriver` or
`hbsd_iscsi.HBSDISCSIDriver`, which keep upstream's `CI_WIKI_NAME`
(`utils.CI_WIKI_NAME`, `Hitachi_CI`).

The replication logic is reached by **composition, not inheritance**. Both
`HBSDFCDriver.__init__` and `HBSDISCSIDriver.__init__` choose their `common`
object at construction time:

```
if hitachi_mirror_storage_id or replication_device:
        self.common = hbsd_replication.HBSDREPLICATION(...)   # H1–H8 live here
else:
        self.common = <rest_fc.HBSDRESTFC | rest_iscsi.HBSDRESTISCSI>(...)
```

```
hbsd_common.HBSDCommon
└── hbsd_rest.HBSDREST
        ├── hbsd_rest_fc.HBSDRESTFC          (no replication)
        ├── hbsd_rest_iscsi.HBSDRESTISCSI    (no replication)
        └── hbsd_replication.HBSDREPLICATION (GAD + UR + group replication)
```

---

## ⚙️ Configuration

### cinder.conf — FC, source site

```ini
[hitachi_vsp_fc]
volume_driver = cinder.volume.drivers.hitachi.hbsd_fc.HBSDFCDriver
volume_backend_name = hitachi_vsp_fc
san_ip = <primary-cm-ip>
san_login = <user>
san_password = <password>
hitachi_storage_id = <primary-serial>
hitachi_pools = <pool>
hitachi_replication_journal_size = 100
replication_device = backend_id:<label>,san_ip:<secondary-cm-ip>,san_login:<user>,san_password:<password>,storage_id:<secondary-serial>,pool:<pool>
```

For iSCSI, use `cinder.volume.drivers.hitachi.hbsd_iscsi.HBSDISCSIDriver`; `use_chap_auth`,
`chap_username` and `chap_password` are accepted both in the backend section
and inside `replication_device`.

### cinder.conf — DR site

A DR backend is configured the same way: its own array in the backend section,
the source array in `replication_device`. There is no role to set.

For each copy group, the driver asks the arrays which one holds the S side,
starting with its own array, which answers without the peer. A volume that has
only an S-VOL (`{"sldev": N}`) is found on the array whose LDEV N carries the
volume's label. One backend can therefore hold copy groups in both directions.

### Replication options

| Option | Default | Notes |
|--------|---------|-------|
| `hitachi_replication_report_pair_status` | `False` | Report each copy group's pair state as the `group_replication_pairs` pool capability. Costs one REST call per copy group on every stats poll, so it is off by default — turn it on only where a consumer reads that capability |
| `hitachi_replication_report_pair_status_ttl` | `300` | Seconds to cache that report. Only read when the option above is `True` |
| `hitachi_replication_mun` | `1` | Mirror unit ID (0–3) |
| `hitachi_replication_journal_size` | *(unset)* | GB, 10–1024. **Required** for UR — the driver errors out without it |
| `hitachi_replication_journal_overflow_tolerance` | `60` | Seconds before a pair splits on journal-full |
| `hitachi_replication_journal_use_cache` | `True` | Cache restore journal data |
| `hitachi_replication_journal_transfer_speed` | `256` | Mbps; one of `3`, `10`, `100`, `256` |
| `hitachi_replication_journal_creation_speed` | `L` | Initial-copy speed; `L`, `M`, `H` |
| `hitachi_replication_journal_path_failure_tolerance` | `5` | Minutes before a pair splits on path failure |

### Group type specs

Create the group type with either spec set — the driver accepts both. Set them
with whichever client your deployment uses (`cinder group-type-key` or
`openstack volume group type set`); check `--help` for the exact syntax of
your client version.

| Spec | Values | Effect |
|------|--------|--------|
| `consistent_group_replication_enabled` | `<is> True` | Marks the group as group-replicated |
| `group_replication_enabled` | `<is> True` | Equivalent alternative |
| `hbsd:group_replication_failover_mode` | `graceful` | Default failover mode for this group type |

### Volume type specs

| Spec | Values | Effect |
|------|--------|--------|
| `replication_enabled` | `<is> True` | Standard Cinder: the volume is replicated |
| `group_replication_enabled` | `<is> True` | This volume's pair is created by `enable_replication` when it joins a copy group — **not** at create time |

The key is unscoped, so Cinder's `CapabilitiesFilter` matches it against the
`group_replication_enabled` pool capability. The driver reports that capability
only on a backend configured for replication. A volume of this type can
therefore only be scheduled to a backend that can do group replication; on any
other backend the create fails with `No valid backend`.

`group_replication_enabled` exists because the group type cannot answer
in time. A volume created *before* its Cinder group exists — the normal order,
since a protection group is formed from volumes that already exist — has no
`group_id`, so the driver cannot see that a copy group will claim it. It pairs
immediately, and `enable_replication` then fails with
`GROUP_REPLICATION_ALREADY_PAIRED`: the driver pins one mirror unit
(`hitachi_replication_mun`), and the array rejects a second pair at it.

With this spec set, one backend serves both models. A plain
`replication_enabled` type still pairs at create, as upstream does; a type
carrying this spec waits for the copy group.

#### Allowed values

The spec must be exactly `<is> True`, or absent. The scheduler and the driver
both read the value, and they do not parse it the same way. The scheduler goes
through `extra_specs_ops.match`, where `<is>` compares with
`strutils.bool_from_string`. The driver accepts only the literal `<is> True`,
after trimming surrounding whitespace, which is the rule
`volume_utils.is_group_a_type` applies to group types.

| Value | Scheduler | Driver | Result |
|---|---|---|---|
| `<is> True` | matches | group-replicated | Works |
| absent | not evaluated | not group-replicated | Pairs at create (intended) |
| `True`, `true` | no match (compared as a plain string) | not group-replicated | Unschedulable: `No valid backend` |
| `<is> False`, `<is> false` | requires a backend reporting `False`; none does | not group-replicated | Unschedulable everywhere |
| `<is> true` | matches | **not** group-replicated | Scheduled, but pairs at create and can never join a copy group |

#### One name, three meanings

`group_replication_enabled` names three different things in this driver. They
do not collide, because each lives on a different object:

| # | Object | Set by | Read by | Means |
|---|---|---|---|---|
| 1 | Pool capability | The driver, in `update_volume_stats()` | Cinder's `CapabilitiesFilter` | This backend *can* do group replication |
| 2 | Group type `group_specs` | Operator | `Group.is_replicated` (Cinder) | This Cinder **group** is replicated |
| 3 | Volume type `extra_specs` | Operator | `CapabilitiesFilter`, and `_typed_for_group_replication()` | This **volume** is paired by `enable_replication`, not at create |

(1) and (3) share a name on purpose. That pairing is what `CapabilitiesFilter`
enforces. (2) is Cinder's own standard group spec, the sibling of
`consistent_group_replication_enabled`, and is hardcoded in
`Group.is_replicated`. The driver does not rename it.

### What PSR expects

Platform9 Site Recovery drives **group-level replication only** — it never uses
the per-volume replication path. A PSR deployment therefore configures:

| | Source site | DR site |
|---|---|---|
| Volume type | `replication_enabled=<is> True` **and** `group_replication_enabled=<is> True` | same |
| Group type | `consistent_group_replication_enabled=<is> True` | same |
| `hitachi_replication_report_pair_status` | `True` if psr-dr should read pair state through Cinder rather than calling the array directly; otherwise leave off | same |

The volume-type spec is the only way to stop a volume pairing at create. Without
it a volume pairs at create and cannot later join a copy group.

Neither site sets a replication role. A DR backend learns that it holds a copy
group's S side by reading the group from its own array, so it still works when
the source array is down.

### Failover semantics

`failover_replication` accepts an optional mode suffix on the
`secondary_backend_id`:

| `secondary_backend_id` | Behaviour |
|------------------------|-----------|
| `<backend_id>` | Mode from `hbsd:group_replication_failover_mode`, else **emergency** |
| `<backend_id>:graceful` | `split_remote_copy_grp` — primary reachable, no data loss |
| `<backend_id>:emergency` | `takeover ... {"mode": "forceSplit"}` — primary unreachable |
| `default` (Cinder's failback sentinel) | `resync_remote_copy_grp(swap=True, is_secondary=True)` |

A mode suffix on failback is rejected with `InvalidReplicationTarget` — there
is no "graceful failback" variant.

**The default is `emergency`, not `graceful`.** A failover with no mode suffix
and no group-type spec will force-split the pair.

---

## 🧪 Running the unit tests

The tests are written against the Cinder test harness, so they need a Cinder
checkout to run in. The driver modules overwrite upstream's
`cinder/volume/drivers/hitachi/`, which is what the tests import:

```bash
# from a cinder checkout
rsync -a --exclude __pycache__ \
  psr-drivers/cinder/volume/drivers/pf9_hitachi/ \
  cinder/cinder/volume/drivers/hitachi/
rsync -a --exclude __pycache__ \
  psr-drivers/cinder/tests/unit/volume/drivers/pf9_hitachi/ \
  cinder/cinder/tests/unit/volume/drivers/pf9_hitachi/

cd cinder
tox -e py3 -- cinder.tests.unit.volume.drivers.pf9_hitachi
tox -e pep8
```

> **macOS:** `hbsd_rest_api.KeepAliveAdapter` reads `socket.TCP_KEEPIDLE`,
> which is Linux-only, so most of the suite errors out on macOS. This is
> inherited from upstream (`cinder/volume/drivers/hitachi/hbsd_rest_api.py`
> has the same unguarded reference) — run the tests on Linux.

---

## How to track OpenStack Cinder Hitachi driver

**Process for syncing upstream Hitachi driver improvements:**

1. **Clone the OpenStack Cinder repository locally**
   ```bash
   git clone git@github.com/openstack/cinder.git
   ```

2. **Compare branches for Hitachi driver changes**
   - Check the released branch (e.g., `stable/2026.1`) for current production version
   - Check the main branch for latest development changes
   - Compare Hitachi driver files between branches:
     ```bash
     git diff stable/2026.1..main -- cinder/volume/drivers/hitachi/
     ```

3. **Identify new commits in main branch**
   - List commits affecting Hitachi driver:
     ```bash
     git log stable/2026.1..main --oneline -- cinder/volume/drivers/hitachi/
     ```
   - Review commit messages to determine importance and relevance

4. **Copy files from main branch if needed**
   - If important changes are found in main but missing in released:
     ```bash
     git show main:cinder/volume/drivers/hitachi/<filename> > <destination>
     ```
   - Test integration and verify compatibility with Platform9 extensions

5. **Update this driver**
   - Merge upstream improvements into corresponding `pf9_hitachi/` files
   - Ensure replication gaps (H1-H8) remain unaffected
   - Re-test all 8 gap implementations after upstream sync

---

## 📚 Next Steps

1. **Read:** [DEPLOYMENT.md](/psr-drivers/DEPLOYMENT.md) (installation details)
2. **Deploy:** copy the modules over `cinder/volume/drivers/hitachi/` on the
   host. `./deploy.sh <user@host> pf9_hitachi` does not do this yet: it
   installs to `drivers/pf9_hitachi/`, where the modules are never loaded.
3. **Verify:** Check logs for errors
4. **Test:** Create volumes with replication enabled
