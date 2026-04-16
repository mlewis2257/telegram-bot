#!/bin/bash
# Run this on the VPS after git pull
cd /root/telegram-bot/bot/python_ai
source .venv/bin/activate
pip install -r requirements.txt -q
pm2 restart sol-listener
pm2 restart sol-backfill
pm2 restart sol-monitor
if pm2 describe sol-ws-monitor >/dev/null 2>&1; then
  pm2 restart sol-ws-monitor
fi
pm2 save
echo "Deployed successfully"
