#!/bin/bash
# Deploy discord-tracker to DO VPS.
# Usage: bash deploy.sh
# Or remotely: ssh root@138.68.57.166 "cd /opt/discord-tracker && bash deploy.sh"
set -euo pipefail

DEPLOY_DIR="/opt/discord-tracker"
ECOSYSTEM="$DEPLOY_DIR/ecosystem.config.cjs"

echo "[DEPLOY] Pulling latest code..."
git -C "$DEPLOY_DIR" pull origin main

echo "[DEPLOY] Installing/updating Python dependencies..."
"$DEPLOY_DIR/.venv/bin/pip" install -r "$DEPLOY_DIR/requirements.txt" -q

echo "[DEPLOY] Updating Claude Code CLI..."
claude update 2>&1 | tail -3 || echo "[DEPLOY] claude update skipped (continuing)"

echo "[DEPLOY] Applying ecosystem config..."
pm2 startOrReload "$ECOSYSTEM" --update-env
pm2 save

echo "[DEPLOY] Done."
pm2 show discord-tracker | grep -E "status|memory|restart"
