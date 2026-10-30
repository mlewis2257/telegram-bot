"""
news_recorder.py — record news (RSS) and every new pump.fun coin, to match stories to launches.

WHY (2026-10-31). Launch data said the first coin on a topic takes the money (Mars Coin
4.2 SOL vs its copies 0.18 / 0; "Official Terminal" 9.1 vs "Terminal Dog" 0.13) and that
on-chain trend-following is too late. The open question for a launcher is how many MINUTES
pass between a story breaking and the first coin about it, and whether that first coin
does better. This records the two sides; launcher/news_report.py matches them.

Two loops in one process:
  RSS        every RSS_POLL_S seconds, each feed in NEWS_RSS_FEEDS (or the defaults below);
             new articles -> news_posts (source 'rss'). Telegram news channels are recorded
             by the listener (NEWS_CHANNELS in the .env), into the same table.
  NEW COINS  PumpPortal's FREE data websocket, subscribeNewToken: every pump.fun creation
             (mint, name, symbol, dev, dev buy) -> new_tokens. No API key, no RPC credits.
             One connection; reconnects wait RECONNECT_S (repeated reconnects can be banned).

Research only. Run:  pm2 start news_recorder.py --name news-recorder --interpreter <venv python>
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import db  # noqa: E402

DEFAULT_FEEDS = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss",
    "cointelegraph": "https://cointelegraph.com/rss",
    "decrypt": "https://decrypt.co/feed",
    "theblock": "https://www.theblock.co/rss.xml",
    "cryptoslate": "https://cryptoslate.com/feed/",
    "gnews-hacks": ("https://news.google.com/rss/search?q=crypto%20hack%20OR%20exploit%20OR%20drained"
                    "&hl=en-US&gl=US&ceid=US:en"),
}
RSS_POLL_S = float(os.getenv("NEWS_RSS_POLL_S", "60"))
NEW_TOKEN_WS = "wss://pumpportal.fun/api/data"
RECONNECT_S = 60
UA = {"User-Agent": "Mozilla/5.0 (news-recorder)"}


def feeds() -> dict[str, str]:
    """NEWS_RSS_FEEDS='name=url,name=url' replaces the defaults."""
    raw = os.getenv("NEWS_RSS_FEEDS", "").strip()
    if not raw:
        return dict(DEFAULT_FEEDS)
    out = {}
    for part in raw.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _text(el, tag: str) -> str | None:
    x = el.find(tag)
    return x.text.strip() if x is not None and x.text else None


def parse_rss(xml: bytes) -> list[dict]:
    """Items of an RSS 2.0 or Atom feed: {id, title, url, published, summary}."""
    root = ET.fromstring(xml)
    items = []
    for it in root.iter("item"):
        link = _text(it, "link") or _text(it, "guid")
        pub = _text(it, "pubDate")
        try:
            when = parsedate_to_datetime(pub) if pub else None
        except (TypeError, ValueError):
            when = None
        desc = _text(it, "description") or ""
        items.append({"id": _text(it, "guid") or link, "title": _text(it, "title"), "url": link,
                      "published": when, "summary": re.sub(r"<[^>]+>", " ", desc)[:2000]})
    ns = "{http://www.w3.org/2005/Atom}"
    for en in root.iter(ns + "entry"):
        link_el = en.find(ns + "link")
        link = link_el.get("href") if link_el is not None else None
        upd = _text(en, ns + "updated") or _text(en, ns + "published")
        try:
            when = datetime.fromisoformat(upd.replace("Z", "+00:00")) if upd else None
        except ValueError:
            when = None
        items.append({"id": _text(en, ns + "id") or link, "title": _text(en, ns + "title"), "url": link,
                      "published": when, "summary": (_text(en, ns + "summary") or "")[:2000]})
    return [i for i in items if i["id"]]


def poll_feeds_once(stats: dict) -> None:
    for name, url in feeds().items():
        try:
            r = requests.get(url, headers=UA, timeout=15)
            if r.status_code != 200:
                stats[f"{name}_http{r.status_code}"] = stats.get(f"{name}_http{r.status_code}", 0) + 1
                continue
            for it in parse_rss(r.content):
                if db.insert_news_post("rss", name, it["id"], it["title"], it["summary"], it["url"],
                                       it["published"]):
                    stats["rss_new"] = stats.get("rss_new", 0) + 1
        except Exception as e:
            db.safe_rollback()
            stats[f"{name}_error"] = stats.get(f"{name}_error", 0) + 1
            if stats.get(f"{name}_error") == 1:
                print(f"[news] {name}: {type(e).__name__}: {e}", flush=True)


async def rss_loop(stats: dict) -> None:
    while True:
        started = time.monotonic()
        await asyncio.to_thread(poll_feeds_once, stats)
        await asyncio.sleep(max(5.0, RSS_POLL_S - (time.monotonic() - started)))


def store_new_token(msg: dict) -> bool:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO new_tokens (mint, name, symbol, uri, dev, dev_buy_sol, mcap_sol)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (mint) DO NOTHING
            """, (msg.get("mint"), (msg.get("name") or "")[:200], (msg.get("symbol") or "")[:50],
                  msg.get("uri"), msg.get("traderPublicKey"), msg.get("solAmount"), msg.get("marketCapSol")))
        new = cur.rowcount == 1
        conn.commit()
    return new


async def new_token_loop(stats: dict) -> None:
    import websockets
    while True:
        started = time.monotonic()
        try:
            async with websockets.connect(NEW_TOKEN_WS, ping_interval=30, ping_timeout=30) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    if msg.get("txType") != "create" or not msg.get("mint"):
                        continue
                    try:
                        if store_new_token(msg):
                            stats["new_tokens"] = stats.get("new_tokens", 0) + 1
                    except Exception as e:
                        db.safe_rollback()
                        stats["token_store_error"] = stats.get("token_store_error", 0) + 1
                        if stats["token_store_error"] == 1:
                            print(f"[news] new token store failed: {e}", flush=True)
        except Exception as e:
            print(f"[news] new-token feed ended: {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(max(RECONNECT_S, RECONNECT_S * 3 - (time.monotonic() - started)))


async def tally_loop(stats: dict) -> None:
    while True:
        await asyncio.sleep(1800)
        print("[news] 30m: " + " ".join(f"{k}={v}" for k, v in sorted(stats.items())), flush=True)
        stats.clear()


async def main() -> None:
    db.ensure_news_tables()
    stats: dict = {}
    print(f"[news] recording {len(feeds())} RSS feeds every {RSS_POLL_S:g}s and every new pump.fun coin "
          f"(PumpPortal free feed). Telegram news channels are recorded by the listener (NEWS_CHANNELS).",
          flush=True)
    await asyncio.gather(rss_loop(stats), new_token_loop(stats), tally_loop(stats))


if __name__ == "__main__":
    asyncio.run(main())
