import openai
import os

openai.api_key = os.getenv("OPENAI_API_KEY")


def classify_message_with_gpt(msg):
    prompt = f""" Classify this message from a Telegram crypto call channel:
    
    Message: "{msg}"

    Categories"
    -- High-Potenial Call
    
    """
