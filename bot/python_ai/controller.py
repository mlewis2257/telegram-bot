from scraper import get_messages
from call_tracker import extract_token_mentions
from axiom_trader import fetch_price, trigger_trade
from ai_analyzer import analyze_token
from report_builder import generate_report
import datetime


def run_pipeline():
    messages = get_messages()
    tokens = extract_token_mentions(messages)

    for token in tokens:
        token_address = resolve_address_from_token(token)  # optional helper
        price_data = fetch_price(token_address)

        # Compose dataset
        coin_data = {
            "token": token,
            "token_address": token_address,
            "call_time": datetime.datetime.utcnow().isoformat(),
            "price_at_call": price_data["price"],
            "price_1h": None,  # fill later
            "wallets": ["..."],  # from wallet_sniffer
            "tweets": [],  # from twitter_monitor
            "meme_origin": "...",  # from meme_origin
            "gpt_summary": "..."  # from ai_analyzer
        }

        summary = analyze_token(coin_data)
        coin_data["gpt_summary"] = summary

        generate_report(coin_data)

        if "strong pump" in summary.lower():  # example filter
            trigger_trade(token_address, amount=0.4)
