import openai
import os

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = openai.Client(OPENAI_API_KEY)


def classify_message_with_gpt(msg):
    prompt = f""" Classify this message from a Telegram crypto call channel:
    
    Message: "{msg}"

    Categories"
    -- High-Potenial Call
    
    """
