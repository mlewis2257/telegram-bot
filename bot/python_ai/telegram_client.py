from telethon.sync import TelegramClient
from telethon.tl.types import Message
from dotenv import load_dotenv
import os
import re
import csv
from datetime import datetime


load_dotenv()

api_id = int(os.environ.get("API_ID"))
api_hash = os.environ.get("API_HASH")
channel_username = os.getenv("CHANNEL_USERNAME")  # e.g. @Solhouse_signals


# Message filtering function
def is_sol_meme_call(text):
    sol_keywords = ["solana", "$", "degods",
                    "bonk", "pump", "call", "launch", "raydium"]
    token_pattern = r'\$?[A-Za-z]{2,5}'

    if any(kw in text.lower() for kw in sol_keywords) and re.search(token_pattern, text):
        return True
    return False

# Save to CSV


def save_to_csv(data, filename="calls.csv"):
    with open(filename, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "Sender ID", "Message"])
        for row in data:
            writer.writerow(row)


with TelegramClient("Solana_meme_tracker_session", api_id, api_hash) as client:
    messages_to_save = []
    print(f"Scanning")

    for message in client.iter_messages(channel_username, limit=200):
        if message.text and is_sol_meme_call(message.text):
            messages_to_save.append([
                message.date.strftime("%Y-%m-%d %H:%M:%S"),
                message.sender_id,
                message.text.strip().replace('\n', ' '),
            ])
            print(f"[MATCH] {message.date} | {message.text[:100]}")

    if messages_to_save:
        save_to_csv(messages_to_save)
        print(f"✅ Saved {len(messages_to_save)} filtered messages to call.csv")
    else:
        print("No matches found.")
