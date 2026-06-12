#!/bin/bash
# Deploy on the VPS: pull latest, install deps, and restart EVERY pm2 process so
# nothing is left running stale code (the "forgot to restart X" trap that leaves
# the exit loop dead while the listener keeps buying).
set -uo pipefail

REPO=/root/telegram-bot
cd "$REPO/bot/python_ai" || { echo "❌ repo path not found"; exit 1; }

echo "→ git pull"
git -C "$REPO" pull || { echo "❌ git pull failed"; exit 1; }

echo "→ pip install"
source .venv/bin/activate
pip install -r requirements.txt -q

# restart ALL pm2 processes — automatically covers new instances (shadow-monitor,
# holder-collector, …) without having to remember to add them here.
echo "→ restarting all pm2 processes"
pm2 restart all --update-env

pm2 save
echo ""
echo "✅ Deployed — all processes restarted. Status:"
pm2 status
