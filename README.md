ps aux | grep telegram_client

```

If you see more than one process, kill them all before restarting.

Also send this to Claude Code to make it self-protecting:
```

Add a PID lock file to telegram_client.py to prevent
running two instances simultaneously.

On startup, before connecting to Telegram:

1. Check if .telegram_client.pid exists
2. If it does, read the PID and check if that
   process is still running:
   - If running: print error and exit immediately
     "[error] Already running as PID {pid}.
     Kill it first or delete .telegram_client.pid"
   - If not running: stale lock, delete it and continue
3. Write current PID to .telegram_client.pid
4. On clean shutdown (finally block), delete the
   .telegram_client.pid file

This prevents the dual-session Telegram error entirely.
