# Telegram listener process guard

`bot/python_ai/telegram_client.py` creates `bot/python_ai/.telegram_client.pid`
on startup so two Telethon listener instances cannot run against the same
session at the same time.

If the listener refuses to start with:

```text
[error] Already running as PID <pid>. Kill it first or delete .telegram_client.pid
```

check the running process before restarting:

```bash
ps aux | grep telegram_client
```

If the PID is stale, delete `bot/python_ai/.telegram_client.pid` and start the
listener again.
