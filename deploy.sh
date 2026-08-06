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

# ── Live readiness sanity-check ───────────────────────────────────────────────
# Surfaces the config the freshly-restarted processes ACTUALLY loaded, so you never
# have to grep by hand to confirm live is armed for today. Never fails the deploy.
echo ""
echo "──────── LIVE READINESS ────────"
sleep 5   # let processes boot + flush their startup banner to the logs

ENVF="$REPO/bot/python_ai/.env"
if [[ -f "$ENVF" ]]; then
  echo "→ .env flags:"
  grep -E '^(LIVE_TRADING_ENABLED|LIVE_LANE_STRATEGY|EXIT_STRATEGY|LIVE_EXIT_USE_QUOTE|QSIM_ENABLED|LIVE_POSITION_SIZE_SOL)=' "$ENVF" | sed 's/^/    /' || echo "    (expected flags not found)"
  if grep -qE '^LIVE_TRADING_ENABLED=true$' "$ENVF"; then
    echo "    ✅ live trading ENABLED"
  else
    echo "    ⚠️  LIVE_TRADING_ENABLED is not 'true' — live will NOT place trades"
  fi
else
  echo "    ⚠️  .env not found at $ENVF"
fi

echo "→ startup banner (what sol-listener actually loaded):"
pm2 logs sol-listener --nostream --lines 100 2>/dev/null \
  | grep -E "\[live\] (exit strategy|allowed hours|lane-policy gate|STARTUP: circuit)" \
  | tail -4 | sed 's/^/    /' || echo "    (banner not in recent logs — run: pm2 logs sol-listener)"

if pm2 logs sol-listener --nostream --lines 200 2>/dev/null | grep -q "not 'true'"; then
  echo "    ⚠️  saw \"LIVE_TRADING_ENABLED is not 'true'\" recently — enable didn't take; fix .env + redeploy"
fi

echo "→ today is $(date -u +%A) UTC — confirm this matches one of your LIVE_LANES days"
echo "→ every process above should read 'online'"
echo "────────────────────────────────"
