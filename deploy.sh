#!/bin/bash
# Run this on the VPS after git pull
cd /root/telegram-bot/bot/python_ai
source .venv/bin/activate
pip install -r requirements.txt -q
pm2 restart sol-listener
pm2 restart sol-backfill
pm2 restart sol-monitor
echo "Deployed successfully"
