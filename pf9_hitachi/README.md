# Hitachi VSP Extended Replication Driver for Platform9

Platform9 fork of the upstream OpenStack Cinder Hitachi VSP driver, extended to
close 8 gaps that block a full disaster-recovery workflow on Universal
Replicator (UR) consistency groups.

Driver version: **2.9.0** (`hbsd_utils.VERSION`) — upstream 2.8.4 plus
`2.9.0 - Add volume group replication support`.

---

## 📦 What's Included

### Driver modules — `cinder/volume/drivers/pf9_hitachi/`

| File | Purpose |
|------|---------|
| `pf9_hitachi_replication.py` | Entry-point classes registered as `volume_driver` ([see below](#-architecture)) |
| `hbsd_replication.py` | `HBSDREPLICATION` — remote replication (GAD/UR) **and all 8 gap implementations** [See implemented gaps](#-implemented-gaps-h1h8) |
| `hbsd_common.py` | `HBSDCommon` — shared logic for FC and iSCSI; also holds the non-replicating stubs that reject group-replication calls |
| `hbsd_rest.py` | `HBSDREST` — REST-backed implementation of the common interface |
| `hbsd_rest_api.py` | `RestApiClient` — low-level REST client, remote-copy-group and journal calls |
| `hbsd_fc.py` | Fibre Channel driver (`HBSDFCDriver`) |
| `hbsd_iscsi.py` | iSCSI driver (`HBSDISCSIDriver`) |
| `hbsd_rest_fc.py` | FC-specific REST operations |
| `hbsd_rest_iscsi.py` | iSCSI-specific REST operations |
| `hbsd_utils.py` | `HBSDMsg` message catalogue, `Config`, connector caching, shared constants |

> `pf9_allocator.py` (secondary LDEV allocator) was **removed** in
> `64c4834`. Secondary LDEVs are now allocated by the array when the
> remote-copy pair is created, so the driver no longer picks IDs itself.

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
| Target-role adoption — a DR backend adopts already-promoted S-VOLs instead of creating pairs | `hitachi_replication_role = target` |
| Per-pair state reported in `update_volume_stats()` capabilities | `_pair_status_capabilities()` |
| Graceful vs emergency failover (`split` vs `takeover ... forceSplit`) | `_failover_mode()` |

---

## 🏗️ Architecture

There is **no mixin**. `pf9_hitachi_replication.py` contains two thin
subclasses whose only job is to carry distinct `CI_WIKI_NAME` values for
third-party CI:

```
hbsd_fc.HBSDFCDriver
└── pf9_hitachi_replication.HBSDGroupReplicationFCDriver
        CI_WIKI_NAME = Hitachi_VSP_Extended_Replication_FC

hbsd_iscsi.HBSDISCSIDriver
└── pf9_hitachi_replication.HBSDGroupReplicationISCSIDriver
        CI_WIKI_NAME = Hitachi_VSP_Extended_Replication_ISCSI
```

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

**Consequence:** pointing `volume_driver` at `hbsd_fc.HBSDFCDriver` gives you
the same group-replication behaviour. Use the
`pf9_hitachi_replication.*` classes when you want the deployment to be
self-documenting and separately CI-tracked.

---

## ⚙️ Configuration

### cinder.conf — FC, source site

```ini
[hitachi_vsp_fc]
volume_driver = cinder.volume.drivers.pf9_hitachi.pf9_hitachi_replication.HBSDGroupReplicationFCDriver
volume_backend_name = hitachi_vsp_fc
san_ip = <primary-cm-ip>
san_login = <user>
san_password = <password>
hitachi_storage_id = <primary-serial>
hitachi_pools = <pool>
hitachi_replication_role = source
hitachi_replication_journal_size = 100
replication_device = backend_id:<label>,san_ip:<secondary-cm-ip>,san_login:<user>,san_password:<password>,storage_id:<secondary-serial>,pool:<pool>
```

For iSCSI, swap in `HBSDGroupReplicationISCSIDriver`; `use_chap_auth`,
`chap_username` and `chap_password` are accepted both in the backend section
and inside `replication_device`.

### cinder.conf — DR (target) site

The disaster-recovery backend adopts promoted S-VOLs rather than creating
pairs. This cannot be derived automatically:

```ini
hitachi_replication_role = target
```

### Replication options

| Option | Default | Notes |
|--------|---------|-------|
| `hitachi_replication_role` | `source` | `source` creates volumes and the copy group; `target` adopts promoted S-VOLs |
| `hitachi_replication_group_only` | `False` | `True` = replicate only once a volume joins a replication group, not at create time |
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
checkout to run in:

```bash
# from a cinder checkout
rsync -a --exclude __pycache__ \
  psr-drivers/cinder/volume/drivers/pf9_hitachi/ \
  cinder/cinder/volume/drivers/pf9_hitachi/
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
2. **Run:** `./deploy.sh <user@host> pf9_hitachi`
3. **Verify:** Check logs for errors
4. **Test:** Create volumes with replication enabled
