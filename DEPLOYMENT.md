# Custom Driver Deployment Guide

Guide for deploying custom drivers to Cinder enabled VMs.

---
## Prerequisites

- SSH/SCP access to target Cinder-enabled VM
- User must have sudo privileges on target VM
- pf9-cinder must be installed at `/opt/pf9/pf9-cindervolume-base/` on target VM
- Storage backend REST API access (if applicable)

---
## Step 1: Prepare the Repository

```bash
git clone git@github.com:platform9/psr-drivers.git
cd psr-drivers
```

**Directory structure should be:**
```
psr-drivers/
├── deploy.sh
├── DEPLOYMENT.md
├── README.md
├── .gitignore
├── <driver_name>/
   └── README.md
└── cinder/
    └── volume/
        └── drivers/
            └── <driver_name>/
                ├── custom_driver.py
                └── ... other files
```

---
## Step 2: Run Deployment Script

**Deployment:**
```bash
./deploy.sh <user@host> <driver_name>
```

**Example:**
```bash
./deploy.sh root@192.168.1.50 pf9_hitachi
```

**The script will:**
1. Validate SSH access
2. Display deployment plan with all steps
3. Request explicit confirmation before proceeding
4. Copy all driver files via SCP
5. Restart Cinder service
6. Verify installation

**Example interactive output:**
```
╔══════════════════════════════════════════════════════════════════════════════╗
║                    pf9_hitachi Driver — Deployment Plan                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

Target VM: root@192.168.1.50
pf9-cinder location: /opt/pf9/pf9-cindervolume-base
Driver name: pf9_hitachi

DISCLAIMER:
  This script will restart the pf9-cindervolume-base service.
  During restart, Cinder volume operations will be temporarily unavailable.
  Ensure you have scheduled a maintenance window before proceeding.

This deployment will perform the following steps:

  STEP 1: Copy driver files via SCP
          → Copy file1.py
          → Copy file2.py
  STEP 2: Restart pf9-cindervolume-base service
          → systemctl restart pf9-cindervolume-base
  STEP 3: Verify installation
          → Check if driver can be imported successfully

WARNING: Service restart will briefly interrupt Cinder volume operations.

─────────────────────────────────────────────────────────────────────────────

Do you want to proceed? (yes/no)
```

Type `yes|y` to proceed.

---
## Step 3: Apply Configuration in Platform9

1. Add storage backend in Cluster Blueprint UI:
   - Backend Name: `<backend_name>`
   - Volume Backend Name: `<backend_name>`
   - Driver: Custom driver (already deployed)

2. Create volume type and link to backend:
   ```bash
   cinder type-create <type_name>
   cinder type-key <type_name> set volume_backend_name=<backend_name>
   ```

3. Apply role to Cinder VM to activate configuration.

4. Verify the VM is in "Applied" state.

---
## Troubleshooting

### Script fails to find driver directory

**Error:**
```
[ERROR] Driver directory not found: cinder/volume/drivers/<driver_name>
```

**Solution:**
- Ensure you're in the correct repository directory
- Verify driver files exist at `cinder/volume/drivers/<driver_name>/`
- List available drivers: `ls -la cinder/volume/drivers/`

---
### SSH connection fails

**Error:**
```
[ERROR] Cannot connect to <user@host> via SSH
```

**Solution:**
```bash
# Test connectivity
ping <host>

# Verify SSH service running on target
ssh <user@host> sudo systemctl status ssh

# Add SSH key if using key-based auth
ssh-add ~/.ssh/id_rsa
ssh-keyscan -H <host> >> ~/.ssh/known_hosts
```

---
### Files not copied to VM

**Symptom:** Driver files not present on remote VM after deployment

**Check directory exists:**
```bash
ssh <user@host> "ls -la /opt/pf9/pf9-cindervolume-base/lib/python3.12/site-packages/cinder/volume/drivers/<driver_name>/"
```

**Check file permissions:**
```bash
ssh <user@host> "stat /opt/pf9/pf9-cindervolume-base/lib/python3.12/site-packages/cinder/volume/drivers/<driver_name>/*.py"
```

---
### Driver import fails

**Symptom:** Service won't start or logs show import errors

**Check Python syntax:**
```bash
ssh <user@host> "/opt/pf9/pf9-cindervolume-base/bin/python -m py_compile /opt/pf9/pf9-cindervolume-base/lib/python3.12/site-packages/cinder/volume/drivers/<driver_name>/*.py"
```

**Check for missing dependencies:**
```bash
ssh <user@host> "/opt/pf9/pf9-cindervolume-base/bin/python -c 'from cinder.volume.drivers.<driver_name> import <DriverClass>'"
```

**Check service logs:**
```bash
ssh <user@host> "sudo journalctl -u pf9-cinder-volume-base -n 100 | tail -50"

ssh <user@host> "sudo tail -100 /var/log/pf9/cindervolume-base.log | grep -A 10 'Error\|Traceback'"
```

---
### Service fails to start

**Error:**
```
[ERROR] Failed to restart service
```

**Check cinder.conf syntax:**
```bash
ssh <user@host> sudo cinder-manage service list

ssh <user@host> "grep -A 15 '^\[<backend_name>\]' /opt/pf9/etc/pf9-cindervolume-base/conf.d/cinder.conf"
```

**Validate configuration:**
```bash
ssh <user@host> "sudo python -m py_compile /opt/pf9/etc/pf9-cindervolume-base/conf.d/cinder.conf"
```

---
### Storage backend connectivity issues

**Test REST API connection:**
```bash
ssh <user@host>

# Example for pf9_hitachi
curl -u <username>:<password> https://<storage_ip>:443/ConfigurationManager/v1/objects/sessions -X POST -k

# Check Cinder can reach backend
/opt/pf9/pf9-cindervolume-base/bin/python -c "
from cinder.volume.drivers.<driver_name> import <DriverClass>
driver = <DriverClass>()
# Verify connection parameters
"
```

---
## Script Usage Reference

```bash
./deploy.sh <user@host> <driver_name> [service_name]
```

**Arguments:**
- `user@host`: SSH connection string (required)
- `driver_name`: Directory name in `cinder/volume/drivers/` (required)
- `service_name`: Cinder service name to restart (optional, defaults to `pf9-cinder-volume-base`)

**Environment:**
- Script must run from repository root directory
- User running script needs local file read permissions
- Remote user needs sudo privileges for service restart

Should return a JSON response with `sessionId`.

---
## Next Steps

1. **Create volume type:**
   ```bash
   cinder type-create pf9-hitachi-replicated
   cinder type-key pf9-hitachi-replicated set volume_backend_name=pf9_hitachi_vsp_fc
   cinder type-key pf9-hitachi-replicated set replication_enabled='<is> True'
   ```

2. **Create consistency group:**
   ```bash
   cinder group-create pf9-hitachi-replicated my-protection-group
   ```

3. **Enable replication:**
   ```bash
   cinder group-enable-replication <group-id>
   ```
