# Deployment Guide

## Prerequisites

- Ubuntu 22.04 VPS (or equivalent)
- A Telegram account with API credentials from https://my.telegram.org
- A Telegram bot token from @BotFather
- PostgreSQL 14+ access (local or remote)

---

## 1. SSH into VPS

```bash
ssh user@your-vps-ip
```

---

## 2. Install System Dependencies

### Python 3.11+

```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3-pip
python3.11 --version
```

### PostgreSQL 14+

```bash
sudo apt install -y postgresql postgresql-contrib
sudo systemctl enable postgresql
sudo systemctl start postgresql
```

Create the database and user:

```bash
sudo -u postgres psql <<EOF
CREATE USER botuser WITH PASSWORD 'yourpassword';
CREATE DATABASE solana_tracker OWNER botuser;
EOF
```

### Node.js and PM2 (process manager)

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm install -g pm2
```

---

## 3. Clone the Repository

```bash
git clone https://github.com/youruser/telegram-bot.git
cd telegram-bot
```

---

## 4. Install Python Dependencies

```bash
cd bot/python_ai
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 5. Set Up Environment Files

Create `bot/python_ai/.env` with the following variables:

```env
# Telegram listener (from https://my.telegram.org)
API_ID=12345678
API_HASH=your_api_hash_here

# PostgreSQL connection
DB_HOST=localhost
DB_PORT=5432
DB_NAME=solana_tracker
DB_USER=botuser
DB_PASSWORD=yourpassword

# Alert bot (from @BotFather)
TELEGRAM_BOT_TOKEN=123456789:AAFxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TELEGRAM_ALERT_CHAT_ID=987654321
```

> Never commit `.env` to version control. It is listed in `.gitignore`.

---

## 6. Run Database Migrations

Run all migration files in order against your PostgreSQL database.
Replace connection details as needed.

```bash
PGPASSWORD=yourpassword psql -h localhost -U botuser -d solana_tracker \
  -f database/schema.sql

PGPASSWORD=yourpassword psql -h localhost -U botuser -d solana_tracker \
  -f database/migrate_v2.sql

PGPASSWORD=yourpassword psql -h localhost -U botuser -d solana_tracker \
  -f database/migrate_v3.sql

PGPASSWORD=yourpassword psql -h localhost -U botuser -d solana_tracker \
  -f database/migrate_v4.sql

PGPASSWORD=yourpassword psql -h localhost -U botuser -d solana_tracker \
  -f database/migrate_v5.sql

PGPASSWORD=yourpassword psql -h localhost -U botuser -d solana_tracker \
  -f database/migrate_v6.sql
```

Verify the schema applied cleanly:

```bash
PGPASSWORD=yourpassword psql -h localhost -U botuser -d solana_tracker \
  -c "\dt"
```

---

## 7. Authenticate the Telegram Listener (First Run)

The listener requires an interactive first-run to create the Telethon session file.
Run it once manually and follow the prompts:

```bash
cd bot/python_ai
source .venv/bin/activate
python3 telegram_client.py
# Enter your phone number and the OTP code when prompted.
# Once "Live monitoring" appears, press Ctrl+C.
```

This creates `bot/python_ai/Solana_meme_tracker_session.session`.
Subsequent runs use this file and do not prompt again.

> If you ever see `AuthKeyDuplicatedError`, delete the `.session` file and
> re-run this step. This happens when the same session is connected from two
> IP addresses simultaneously.

---

## 8. Start All Processes with PM2

Run from the repo root. All commands use the venv interpreter.

```bash
VENV=bot/python_ai/.venv/bin/python3

# Telegram listener (live message ingestion)
pm2 start bot/python_ai/telegram_client.py \
  --name sol-listener \
  --interpreter $VENV

# Backfill job (price snapshots at 1h / 4h / 24h)
pm2 start bot/python_ai/backfill.py \
  --name sol-backfill \
  --interpreter $VENV \
  -- --loop

# Real-time price monitor (peak tracking, milestone & drawdown alerts)
pm2 start bot/python_ai/monitor.py \
  --name sol-monitor \
  --interpreter $VENV

pm2 save
```

Check that all processes are running:

```bash
pm2 status
pm2 logs sol-listener --lines 50
pm2 logs sol-backfill --lines 20
pm2 logs sol-monitor --lines 20
```

---

## 9. Auto-Restart on VPS Reboot

```bash
pm2 startup
# PM2 will print a command to run as root — copy and run it, e.g.:
sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u youruser --hp /home/youruser

pm2 save
```

After rebooting, verify:

```bash
sudo reboot
# reconnect, then:
pm2 status
```

---

## Useful PM2 Commands

```bash
pm2 status                        # overview of all processes
pm2 logs <name>                   # tail logs
pm2 logs <name> --lines 200       # last 200 lines
pm2 restart <name>                # restart a process
pm2 stop <name>                   # stop without removing
pm2 delete <name>                 # remove from PM2
pm2 monit                         # live dashboard
```

---

## One-Shot Scripts (run manually as needed)

```bash
cd bot/python_ai && source .venv/bin/activate

# Score all existing unscored calls
python3 score_backfill.py

# Backfill missing on-chain metadata for realtime tokens
python3 fix_missing_metadata.py

# Fix misattributed outcome data (run once after initial deploy)
python3 fix_misattributed_outcomes.py --dry-run   # preview
python3 fix_misattributed_outcomes.py             # apply
```
