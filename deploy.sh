#!/bin/bash
#
# Custom Driver Deployment Script to Cinder enabled VM
#
# Usage:
#   ./deploy.sh <user@host> <driver_name> <ssh_key_path>
#
# Examples:
#   ./deploy.sh root@192.168.1.50 pf9_hitachi ~/.ssh/id_rsa
#
# PREREQUISITES:
#   - SSH access to the target VM must be configured
#   - SSH key-based authentication or password authentication required
#   - User must have sudo privileges on the target VM
#   - pf9-cinder must be installed at /opt/pf9/pf9-cindervolume-base/ on target VM
#

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

REMOTE_HOST="$1"
DRIVER_NAME="$2"
SSH_KEY="$3"
SERVICE_NAME="pf9-cindervolume-base"
REMOTE_CINDER_ROOT="/opt/pf9/pf9-cindervolume-base"
LOCAL_DRIVER_DIR="cinder/volume/drivers"

if [ -z "$REMOTE_HOST" ] || [ -z "$DRIVER_NAME" ] || [ -z "$SSH_KEY" ]; then
    cat << 'EOF'

╔══════════════════════════════════════════════════════════════════════════════╗
║                    Custom Driver Deployment — Generic                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage: ./deploy.sh <user@host> <driver_name> <ssh_key_path>

Examples:
  ./deploy.sh root@192.168.1.50 pf9_hitachi ~/.ssh/id_rsa

Arguments:
  user@host       Remote VM SSH connection string (required)
  driver_name     Driver directory name in cinder/volume/drivers/ (required)
  ssh_key_path    SSH private key used for every ssh/scp call (required)

PREREQUISITES:
  - SSH access to the target VM must be configured
  - SSH key-based authentication or password authentication required
  - User must have sudo privileges on the target VM
  - pf9-cinder must be installed at /opt/pf9/pf9-cindervolume-base/ on target VM

DISCLAIMER:
  This script will copy driver files to the remote VM and restart the
  Cinder service. Ensure you have proper backups and maintenance windows
  scheduled before running this deployment.

EOF
    exit 1
fi

# Validate remote host format
if [[ ! "$REMOTE_HOST" =~ @ ]]; then
    echo -e "${RED}[ERROR]${NC} Invalid format: $REMOTE_HOST"
    echo -e "${BLUE}Please use:${NC} <user>@<host>"
    echo ""
    echo "Examples:"
    echo "  $0 root@192.168.1.50 pf9_hitachi ~/.ssh/id_rsa"
    exit 1
fi

# A quoted "~/..." reaches the script without shell tilde expansion.
case "$SSH_KEY" in
    "~") SSH_KEY="$HOME" ;;
    "~/"*) SSH_KEY="$HOME/${SSH_KEY#"~/"}" ;;
esac
if [ ! -f "$SSH_KEY" ] || [ ! -r "$SSH_KEY" ]; then
    echo -e "${RED}[ERROR]${NC} SSH key not found: $SSH_KEY"
    exit 1
fi

# Verify SSH access
echo -e "${BLUE}[CHECK]${NC} Verifying SSH access to $REMOTE_HOST..."
if ! ssh -o ConnectTimeout=5 -i "$SSH_KEY" "$REMOTE_HOST" "echo OK" &>/dev/null; then
    echo -e "${RED}[ERROR]${NC} Cannot connect to $REMOTE_HOST via SSH"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo "  1. Host is reachable: ping ${REMOTE_HOST##*@}"
    echo "  2. SSH is enabled on the target VM"
    echo "  3. SSH credentials are correct (password or key-based auth)"
    echo "  4. Firewall allows SSH (port 22)"
    echo ""
    echo -e "${YELLOW}For key-based auth setup:${NC}"
    echo "  ssh-add ~/.ssh/id_rsa"
    echo "  ssh-keyscan -H ${REMOTE_HOST##*@} >> ~/.ssh/known_hosts"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} SSH access verified"
echo ""

# scp does not expand wildcards in its destination, so the remote shell resolves the Python version once.
SITE_PACKAGES_GLOB="$REMOTE_CINDER_ROOT/lib/python*/site-packages"
echo -e "${BLUE}[CHECK]${NC} Locating $SITE_PACKAGES_GLOB on $REMOTE_HOST..."
SITE_PACKAGES_DIRS=($(ssh -i "$SSH_KEY" "$REMOTE_HOST" "ls -d $SITE_PACKAGES_GLOB 2>/dev/null"))
if [ ${#SITE_PACKAGES_DIRS[@]} -eq 0 ]; then
    echo -e "${RED}[ERROR]${NC} No match for $SITE_PACKAGES_GLOB on $REMOTE_HOST"
    exit 1
fi
if [ ${#SITE_PACKAGES_DIRS[@]} -gt 1 ]; then
    echo -e "${RED}[ERROR]${NC} Multiple matches for $SITE_PACKAGES_GLOB on $REMOTE_HOST:"
    printf '  %s\n' "${SITE_PACKAGES_DIRS[@]}"
    exit 1
fi
REMOTE_DRIVERS_DIR="${SITE_PACKAGES_DIRS[0]}/cinder/volume/drivers"
if ! ssh -i "$SSH_KEY" "$REMOTE_HOST" "test -d '$REMOTE_DRIVERS_DIR'"; then
    echo -e "${RED}[ERROR]${NC} $REMOTE_DRIVERS_DIR not found on $REMOTE_HOST - is pf9-cinder installed?"
    exit 1
fi
REMOTE_DRIVER_PATH="$REMOTE_DRIVERS_DIR/$DRIVER_NAME"
echo -e "${GREEN}[OK]${NC} Driver path: $REMOTE_DRIVER_PATH"
echo ""

# Get list of driver files
DRIVER_FILES=($(find "$LOCAL_DRIVER_DIR/$DRIVER_NAME" -type f | sort))
if [ ${#DRIVER_FILES[@]} -eq 0 ]; then
    echo -e "${RED}[ERROR]${NC} No driver files found in $LOCAL_DRIVER_DIR/$DRIVER_NAME"
    exit 1
fi

# Display deployment plan
clear
DRIVER_DISPLAY=$(echo "$DRIVER_NAME" | tr '[:lower:]' '[:upper:]')
cat << EOF

╔══════════════════════════════════════════════════════════════════════════════╗
║                ${DRIVER_DISPLAY} Driver — Deployment Plan                    ║
╚══════════════════════════════════════════════════════════════════════════════╝

EOF

echo -e "${BLUE}Target VM:${NC} $REMOTE_HOST"
echo -e "${BLUE}pf9-cinder location:${NC} $REMOTE_CINDER_ROOT"
echo -e "${BLUE}Driver name:${NC} $DRIVER_NAME"
echo -e "${BLUE}Driver path:${NC} $REMOTE_DRIVER_PATH"
echo ""
echo -e "${YELLOW}DISCLAIMER:${NC}"
echo "  This script will restart the $SERVICE_NAME service."
echo "  During restart, Cinder volume operations will be temporarily unavailable."
echo "  Ensure you have scheduled a maintenance window before proceeding."
echo ""
echo "This deployment will perform the following steps:"
echo ""
echo "  STEP 1: Copy driver files via SCP"
echo "          → Create $REMOTE_DRIVER_PATH if missing"
for file in "${DRIVER_FILES[@]}"; do
    basename_file=$(basename "$file")
    echo "          → Copy $basename_file"
done
echo "          → Set owner pf9:pf9group"
echo ""
echo "  STEP 2: Restart $SERVICE_NAME service"
echo "          → systemctl restart $SERVICE_NAME"
echo ""
echo "  STEP 3: Verify installation"
echo "          → Check if driver can be imported successfully"
echo ""
echo -e "${RED}WARNING:${NC} Service restart will briefly interrupt Cinder volume operations."
echo ""
echo "─────────────────────────────────────────────────────────────────────────────"
echo ""
read -p "Do you want to proceed? (yY/nN) " -r
echo ""

echo $REPLY
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Deployment cancelled.${NC}"
    exit 0
fi

# Copy files
echo -e "${BLUE}[STEP 3]${NC} Copying driver files..."
if ! ssh -i "$SSH_KEY" "$REMOTE_HOST" "test -d '$REMOTE_DRIVER_PATH'"; then
    ssh -i "$SSH_KEY" "$REMOTE_HOST" "sudo mkdir '$REMOTE_DRIVER_PATH'" || { echo -e "${RED}[ERROR]${NC} Failed to create $REMOTE_DRIVER_PATH"; exit 1; }
    echo -e "${GREEN}[OK]${NC} Created $REMOTE_DRIVER_PATH"
fi
# The driver folder belongs to pf9, so a non-root SSH user cannot scp into it; files go through a staging folder.
REMOTE_STAGING_DIR=$(ssh -i "$SSH_KEY" "$REMOTE_HOST" "mktemp -d") || { echo -e "${RED}[ERROR]${NC} Failed to create a staging folder on $REMOTE_HOST"; exit 1; }
for file in "${DRIVER_FILES[@]}"; do
    filename=$(basename "$file")
    scp -i "$SSH_KEY" -q "$file" "$REMOTE_HOST:$REMOTE_STAGING_DIR/" || { echo -e "${RED}[ERROR]${NC} Failed to copy $filename"; ssh -i "$SSH_KEY" "$REMOTE_HOST" "rm -rf '$REMOTE_STAGING_DIR'"; exit 1; }
    echo -e "${GREEN}[OK]${NC} $filename copied"
done
ssh -i "$SSH_KEY" "$REMOTE_HOST" "sudo cp '$REMOTE_STAGING_DIR'/* '$REMOTE_DRIVER_PATH'/; rc=\$?; rm -rf '$REMOTE_STAGING_DIR'; exit \$rc" || { echo -e "${RED}[ERROR]${NC} Failed to move driver files into $REMOTE_DRIVER_PATH"; exit 1; }

# Change ownership to pf9:pf9group
ssh -i "$SSH_KEY" "$REMOTE_HOST" "sudo chown -R pf9:pf9group '$REMOTE_DRIVER_PATH'" || { echo -e "${RED}[ERROR]${NC} Failed to change ownership of $REMOTE_DRIVER_PATH"; exit 1; }
echo -e "${GREEN}[OK]${NC} Owner set to pf9:pf9group"
echo ""

# Restart service
echo -e "${BLUE}[STEP 4]${NC} Restarting $SERVICE_NAME service..."
ssh -i "$SSH_KEY" "$REMOTE_HOST" "sudo systemctl restart $SERVICE_NAME" || { echo -e "${RED}[ERROR]${NC} Failed to restart service"; exit 1; }
sleep 10
echo -e "${GREEN}[OK]${NC} Service restarted"
echo ""

# Verify
echo -e "${BLUE}[STEP 5]${NC} Verifying installation..."
if ssh -i "$SSH_KEY" "$REMOTE_HOST" "$REMOTE_CINDER_ROOT/bin/python -m py_compile '$REMOTE_DRIVER_PATH'/*.py" 2>/dev/null; then
    echo -e "${GREEN}[OK]${NC} Driver files are syntactically correct"
else
    echo -e "${YELLOW}[WARN]${NC} Driver import verification failed - check logs"
fi
echo ""

# Summary
DRIVER_DISPLAY=$(echo "$DRIVER_NAME" | tr '[:lower:]' '[:upper:]')
cat << EOF
╔══════════════════════════════════════════════════════════════════════════════╗
║                     Deployment Complete ✓                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
EOF
echo -e "${BLUE}Driver name:${NC} $DRIVER_NAME"
echo -e "${BLUE}Driver files installed at:${NC}"
echo "  $REMOTE_DRIVER_PATH/"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Configure $DRIVER_NAME volume backend from UI"
echo "  2. Use custom driver option and provide class path in the configuration"
echo "  3. Apply the configuration on the Cinder VM"
echo ""
