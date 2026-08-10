# Hitachi VSP Extended Replication Driver for Platform9

Hitachi custom driver implementing 8 missing Cinder gaps for disaster recovery replication.

---

## 📦 What's Included

| File | Purpose |
|------|---------|
| `cinder/volume/drivers/pf9_hitachi/pf9_hitachi_replication.py` | Platform9 Extended Replication Driver (all 8 gaps) [See implemented gaps](#-implemented-gaps-h1h8) |
| `cinder/volume/drivers/pf9_hitachi/pf9_allocator.py` | Secondary LDEV allocator for replication pairs |
| `cinder/volume/drivers/pf9_hitachi/hbsd_common.py` | Shared logic for FC and iSCSI drivers |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest.py` | REST API interface abstraction |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest_api.py` | Low-level REST API client |
| `cinder/volume/drivers/pf9_hitachi/hbsd_fc.py` | Fibre Channel driver entry point |
| `cinder/volume/drivers/pf9_hitachi/hbsd_iscsi.py` | iSCSI driver entry point |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest_fc.py` | FC-specific REST operations |
| `cinder/volume/drivers/pf9_hitachi/hbsd_rest_iscsi.py` | iSCSI-specific REST operations |
| `cinder/volume/drivers/pf9_hitachi/hbsd_replication.py` | Upstream replication mixin |
| `cinder/volume/drivers/pf9_hitachi/hbsd_utils.py` | Utilities and constants |

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
| H8 | `list_replication_targets()` | Return secondary backend_id |

---

## 🏗️ Architecture

**Mixin Pattern:** Reusable replication logic across FC and iSCSI transports.

```
HBSDGroupReplicationMixin (replication methods)
├── HBSDGroupReplicationFCDriver (+ FC transport)
└── HBSDGroupReplicationISCSIDriver (+ iSCSI transport)
```

**Benefits:**
- Zero code duplication
- Supports both FC and iSCSI
- Upstream-compatible pattern
- Easier to maintain and test

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
