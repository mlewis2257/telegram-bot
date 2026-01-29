# Solana Meme Coin Intelligence System

A modular system to scrape Telegram meme coin calls, track token performance, wallet behavior, social virality, and generate AI-powered reports.

## Modules

- `scraper.py`: Extracts Telegram messages.
- `filters.py`: Solana-related message filters.
- `call_tracker.py`: Detects first-time token mentions.
- `price_monitor.py`: Logs price movements post-call.
- `wallet_sniffer.py`: Tracks smart wallet entries.
- `twitter_monitor.py`: Tracks tweet volume and sentiment.
- `meme_origin.py`: Finds meme or narrative origin.
- `report_builder.py`: Compiles data into Markdown/PDF.
- `ai_analyzer.py`: GPT/AI-based analysis on pump success.

## Setup

1. Create a `.env` file with Telegram and API credentials.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run individual modules or orchestrate with a controller script.
