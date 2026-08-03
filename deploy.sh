#!/bin/bash
#
# Custom Driver Deployment Script to Cinder enabled VM
#
# Usage:
#   ./deploy.sh <user@host> <driver_name>
#
# Examples:
#   ./deploy.sh root@192.168.1.50 pf9_hitachi
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
SERVICE_NAME="pf9-cindervolume-base"
REMOTE_CINDER_ROOT="/opt/pf9/pf9-cindervolume-base"
LOCAL_DRIVER_DIR="cinder/volume/drivers"

if [ -z "$REMOTE_HOST" ] || [ -z "$DRIVER_NAME" ]; then
    cat << 'EOF'

╔══════════════════════════════════════════════════════════════════════════════╗
║                    Custom Driver Deployment — Generic                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage: ./deploy.sh <user@host> <driver_name>

Examples:
  ./deploy.sh root@192.168.1.50 pf9_hitachi

Arguments:
  user@host       Remote VM SSH connection string (required)
  driver_name     Driver directory name in cinder/volume/drivers/ (required)

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
    echo "  $0 root@192.168.1.50 pf9_hitachi"
    exit 1
fi

# Verify SSH access
echo -e "${BLUE}[CHECK]${NC} Verifying SSH access to $REMOTE_HOST..."
if ! ssh -o ConnectTimeout=5 "$REMOTE_HOST" "echo OK" &>/dev/null; then
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
echo ""
echo -e "${YELLOW}DISCLAIMER:${NC}"
echo "  This script will restart the $SERVICE_NAME service."
echo "  During restart, Cinder volume operations will be temporarily unavailable."
echo "  Ensure you have scheduled a maintenance window before proceeding."
echo ""
echo "This deployment will perform the following steps:"
echo ""
echo "  STEP 1: Copy driver files via SCP"
for file in "${DRIVER_FILES[@]}"; do
    basename_file=$(basename "$file")
    echo "          → Copy $basename_file"
done
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
read -p "Do you want to proceed? (yes/no) " -r
echo ""

echo $REPLY
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Deployment cancelled.${NC}"
    exit 0
fi

REMOTE_DRIVER_PATH="$REMOTE_CINDER_ROOT/lib/python3.12/site-packages/cinder/volume/drivers/$DRIVER_NAME"
echo -e "${GREEN}[OK]${NC} Driver path: $REMOTE_DRIVER_PATH"
echo ""

# Copy files
echo -e "${BLUE}[STEP 3]${NC} Copying driver files..."
for file in "${DRIVER_FILES[@]}"; do
    filename=$(basename "$file")
    scp -q "$file" "$REMOTE_HOST:$REMOTE_DRIVER_PATH/" || { echo -e "${RED}[ERROR]${NC} Failed to copy $filename"; exit 1; }
    echo -e "${GREEN}[OK]${NC} $filename copied"
done
echo ""

# Restart service
echo -e "${BLUE}[STEP 4]${NC} Restarting $SERVICE_NAME service..."
ssh "$REMOTE_HOST" "sudo systemctl restart $SERVICE_NAME" || { echo -e "${RED}[ERROR]${NC} Failed to restart service"; exit 1; }
sleep 10
echo -e "${GREEN}[OK]${NC} Service restarted"
echo ""

# Verify
echo -e "${BLUE}[STEP 5]${NC} Verifying installation..."
if ssh "$REMOTE_HOST" "$REMOTE_CINDER_ROOT/bin/python -m py_compile '$REMOTE_DRIVER_PATH'/*.py" 2>/dev/null; then
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
