#!/bin/bash
# Deploy Microsoft Graph MCP Server to Hermes VPS
#
# Prerequisites:
# - SSH access to hermes configured in ~/.ssh/config
# - .env file configured at deployment/hermes/.env
#
# Usage:
#   ./scripts/deploy-to-hermes.sh [--restart]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
REMOTE_HOST="hermes"
REMOTE_PATH="/opt/services/m365-mcp"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Microsoft Graph MCP Server Deployment ===${NC}"
echo "Target: $REMOTE_HOST:$REMOTE_PATH"
echo ""

# Check SSH connectivity
echo -e "${YELLOW}Checking SSH connectivity...${NC}"
if ! ssh -q "$REMOTE_HOST" exit; then
    echo -e "${RED}ERROR: Cannot connect to $REMOTE_HOST${NC}"
    exit 1
fi
echo -e "${GREEN}SSH connection OK${NC}"

# Check .env exists locally (for reference, actual .env is on server)
if [ ! -f "$REPO_ROOT/deployment/hermes/.env" ] && [ ! -f "$REPO_ROOT/.env.example" ]; then
    echo -e "${YELLOW}WARNING: No .env file found. Ensure .env is configured on server.${NC}"
fi

# Sync source code
echo -e "${YELLOW}Syncing source code...${NC}"
rsync -av --delete \
    "$REPO_ROOT/src/" \
    "$REMOTE_HOST:$REMOTE_PATH/code/" \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude '.env'

# Sync docker-compose.yml
echo -e "${YELLOW}Syncing docker-compose.yml...${NC}"
rsync -av \
    "$REPO_ROOT/deployment/hermes/docker-compose.yml" \
    "$REMOTE_HOST:$REMOTE_PATH/"

# Handle restart flag
if [ "$1" == "--restart" ]; then
    echo -e "${YELLOW}Restarting container...${NC}"
    ssh "$REMOTE_HOST" "cd $REMOTE_PATH && docker compose restart"
else
    echo -e "${YELLOW}Recreating container...${NC}"
    ssh "$REMOTE_HOST" "cd $REMOTE_PATH && docker compose up -d"
fi

# Show container status
echo -e "${YELLOW}Container status:${NC}"
ssh "$REMOTE_HOST" "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E '(NAMES|m365-mcp)'"

echo ""
echo -e "${GREEN}=== Deployment Complete ===${NC}"
echo ""
echo "To view logs:  ssh $REMOTE_HOST 'docker logs m365-mcp --tail 50'"
echo "To restart:    ssh $REMOTE_HOST 'cd $REMOTE_PATH && docker compose restart'"
