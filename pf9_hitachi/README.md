# Hitachi VSP Extended Replication Driver for Platform9

Hitachi custom driver implementing 8 missing Cinder gaps for disaster recovery replication.

> **Status:** H1-H8 are implemented and unit tested (71 tests,
> `cinder/tests/unit/volume/drivers/pf9_hitachi/test_hitachi_hbsd_replication.py`).
> The other test modules in that directory are upstream-derived and cover the
> base driver.

---

## 📦 What's Included

| File | Purpose |
|------|---------|
| `cinder/volume/drivers/pf9_hitachi/hbsd_replication.py` | Replication logic, including the group-replication contract (all 8 gaps) [See implemented gaps](#-implemented-gaps-h1h8) |
| `cinder/volume/drivers/pf9_hitachi/hbsd_common.py` | Shared logic for FC and iSCSI drivers |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest.py` | REST API interface abstraction |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest_api.py` | Low-level REST API client |
| `cinder/volume/drivers/pf9_hitachi/hbsd_fc.py` | Fibre Channel driver entry point |
| `cinder/volume/drivers/pf9_hitachi/hbsd_iscsi.py` | iSCSI driver entry point |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest_fc.py` | FC-specific REST operations |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest_iscsi.py` | iSCSI-specific REST operations |
| `cinder/volume/drivers/pf9_hitachi/hbsd_utils.py` | Utilities and constants |
| `cinder/volume/drivers/pf9_hitachi/pf9_hitachi_replication.py` | Named `volume_driver` entry points (thin transport subclasses) |

---

## 📋 Implemented Gaps (H1–H8)

| Gap | Method | Purpose |
|-----|--------|---------|
| H1 | `manage_existing()` | Import promoted S-VOLs after failover |
| H2 | `unmanage()` | Remove Cinder claim without affecting pair |
| H3 | `update_group()` | Add/remove volumes from live CG |
| H4 | `create_group_snapshot()` | Snapshot S-VOLs for test recovery |
| H5 | `enable_replication()` | Activate UR replication on CG |
| H6 | `disable_replication()` | Delete UR pairs |
| H7 | `failover_replication()` | Split and promote S-VOLs |
| H8 | `list_replication_targets()` | Implemented, but **never called** — the volume manager answers this action out of `cinder.conf` and does not dispatch to the driver. Kept only so the contract is complete. |

---

## 🔍 Additional Features

Beyond the 8 core gaps:

| Feature | Purpose |
|---------|---------|
| **Pair state and RPO in the pool capabilities** | With `hitachi_replication_report_pair_status` (default `true`), each pool carries `pf9_group_replication_pairs` — a JSON map of copy group → `{pair_status, consistency_time, journal_usage_rate, pair_count}` — plus `..._updated_at`, `..._peer_initialized` and `..._enumerated`. Read it with `GET /v3/scheduler-stats/get_pools?detail=True`. Cinder exposes replication lag and vendor pair state through no other API. Costs one Configuration Manager request per copy group per statistics cycle (60s by default), capped at 64 groups. |
| **LDEV ids in volume metadata** | `psr_pvol_id`, `psr_svol_id` and `psr_copy_group` are stamped on each member, because `provider_location` appears in no Cinder API view and this driver sets no `provider_id`. |
| **Selectable failover mode** | `secondary_backend_id` accepts a `:graceful` or `:emergency` suffix; a group type may set `hbsd:group_replication_failover_mode`. See [Failover mode](#failover-mode). |
| **Recovery-site adoption** | A backend set to `hitachi_replication_role = target` adopts promoted S-VOLs instead of creating them: `manage_existing` accepts an LDEV whose copy pair still exists, `enable_replication` records the existing pairs without touching the array, and the takeover is issued to the local storage system rather than the peer. See [Recovery-site backends](#recovery-site-backends). |
| **Explicit copy-group binding** | A Cinder group can be bound to a named array copy group through its members' `psr_copy_group` metadata, instead of deriving the name from the Cinder group id. Required at a recovery site, where the group has a different id. |
| **Restart instead of rebuild** | `enable_replication` on a copy group that still exists resyncs the suspended pairs (non-swap) rather than allocating fresh S-VOLs. |
| `manage_existing_get_size()` | Size of an existing LDEV, for import validation. |

There is **no** `get_replication_lag()`. Cinder defines no such driver
contract, so a driver method would be unreachable; the pool capabilities
above are the working substitute.

---

## Recovery-site backends

A backend at the disaster recovery site is configured the same way as any
other — its own storage system in `[backend]`, the peer in
`replication_device` — plus one key:

```ini
hitachi_replication_role = target
```

`source` is the default and changes nothing: volumes are created there, the
copy group is made there, and the S-VOLs live on the `replication_device`.
`target` says the opposite — this backend adopts S-VOLs that already exist on
its own storage system, and the peer may be unreachable. The role cannot be
worked out at run time: Cinder's group replication actions reach whichever
backend owns the group object, and a recovery site owns nothing until it has
adopted something, so it has to be told.

**Why the binding is needed.** The array copy-group name is normally derived
from the Cinder group id, keeping 25 of its 32 hex characters. A group created
at the recovery site has a different id — and the derivation is not reversible
— so it would name a copy group the storage system has never heard of. Bind
the group explicitly instead, in precedence order:

1. **Members' `psr_copy_group` metadata** — the intended channel.
   `POST /manageable_volumes` accepts a `metadata` dict, and Cinder puts it on
   the volume before the driver sees it, so the binding arrives on the same
   call that adopts the S-VOL.
2. **A marker on the group's name** — `hbsd-cg:<copy group>`. An operator
   escape hatch. Weaker, because a group can be renamed with no validation.
3. **Derivation from the group id** — unchanged behaviour at the source site.

Members that disagree about their copy group are refused rather than
arbitrated.

**Order of operations at the recovery site.** Adopt before mapping: the
manageability check still requires the LDEV to have no LUN paths, because
Cinder owns the export from the adopt onward.

1. `POST /manageable_volumes` per S-VOL, with `metadata:
   {"psr_copy_group": "<copy group>"}` and a replication-enabled volume type.
2. `POST /groups` with a group-replication group type.
3. `PUT /groups/{id}` to add the members.
4. `POST /groups/{id}/action {"enable_replication": {}}` — records the
   existing pairs, no array change, group becomes `ENABLED`.
5. `POST /groups/{id}/action {"failover_replication": {...}}` — takeover on
   the local storage system.

Step 4 exists because Cinder will not accept `failover_replication` until the
group reports `ENABLED`, and `enable_replication` is the only transition it
offers.

**Prerequisite, and it is not a driver setting:** the recovery site's Cinder
must already have a group type carrying
`consistent_group_replication_enabled` (or `group_replication_enabled`) and
volume types carrying `replication_enabled='<is> True'`. Cinder rejects every
group replication action otherwise, before the driver is consulted.

---

## Failover mode

Cinder hands `failover_replication` two parameters and consumes one of them
itself — `allow_attached_volume` is a manager-side precondition and never
reaches a driver — so `secondary_backend_id` is the only per-request channel
a client has.

| `secondary_backend_id` | Array operation | Consistency | Needs the primary? |
|---|---|---|---|
| `<backend_id>` (default) | copy-group takeover, `mode: forceSplit` | crash consistent | no |
| `<backend_id>:emergency` | same as above, stated explicitly | crash consistent | no |
| `<backend_id>:graceful` | copy-group pairsplit from the primary | every acknowledged write | **yes** |
| `default` | swap resync (failback) | — | yes |

A group type may set `hbsd:group_replication_failover_mode = graceful` to
change the default for every request against that group; a per-request
suffix overrides it. A mode suffix on `default` is rejected, because the
volume manager compares the value it was given against its own failback
sentinel and would otherwise record the group as failed over while the
driver resynced it.

---

## 🏗️ Architecture

**Folded into the stock drivers.** Group replication is a capability of the
existing `HBSDFCDriver` and `HBSDISCSIDriver`, not a separate implementation
per transport. There is no mixin.

```
HBSDFCDriver / HBSDISCSIDriver   (unchanged entry points)
└── self.common = HBSDREPLICATION   (built when replication_device is set)
    ├── H5-H8  group-replication contract (enable/disable/failover/list)
    └── H1-H4  lifecycle methods, branching on the group type
```

`pf9_hitachi_replication.py` adds `HBSDGroupReplicationFCDriver` and
`HBSDGroupReplicationISCSIDriver`, which are thin subclasses that fix the
transport and nothing else. **Either they or the stock classes may be named
in `volume_driver`; the behaviour is identical.** What actually turns the
feature on is `replication_device` plus a group type carrying
`consistent_group_replication_enabled` or `group_replication_enabled`
(Cinder's `Group.is_replicated` accepts either, so this driver does too).
Volumes and groups without that type take the unmodified upstream code
path.

**Benefits:**
- No new driver classes, so no new third-party CI is required upstream
- Non-replicated volumes keep upstream's validated behaviour
- All PF9 additions are bracketed by `# PF9 Start` / `# PF9 End` markers

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
2. **Run:** `./deploy.sh <user@host-ip> <driver_name>`
3. **Verify:** Check logs for errors
4. **Test:** Create volumes with replication enabled
