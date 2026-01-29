import asyncio
from axiomtradeapi import AxiomTradeClient
from dotenv import load_dotenv
import os

load_dotenv()
AUTH_TOKEN = os.getenv("AXIOM_AUTH_TOKEN")
REFRESH_TOKEN = os.getenv("AXIOM_REFRESH_TOKEN")
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS")
USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")


async def handle_new_tokens(tokens):
    """Process incoming tokens"""

    for token in tokens:
        print(token)
        # print(f"🚨 NEW TOKEN ALERT!")
        # print(f"Name: {token['tokenName']} ({token["tokenTicker"]})")
        # print(f"Token Address: {token["tokenAddress"]}")
        # print(f"   Market Cap: {token['marketCapSol']} SOL")
        # print(f"   Volume: {token['volumeSol']} SOL")
        # print(f"   Protocol: {token['protocol']}")
        # print("-" * 50)


async def handle_authentication(username, password, auth_token, refresh_token):
    # initialize authenticated client test
    try:

        client = AxiomTradeClient()
        client.set_tokens(
            access_token=AUTH_TOKEN,
            refresh_token=REFRESH_TOKEN)

        # Subscribe to new token pairs
        # await client.get_trending_tokens(time_period="1h")

        # print("🔄 Listening for new tokens... (Press Ctrl+C to stop)")
        client.get_sol_balance(WALLET_ADDRESS)
    except Exception as e:
        print(f"Authentication failed: {e}")
        return False


async def main():
    await handle_authentication(USER_NAME, PASSWORD, AUTH_TOKEN, REFRESH_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
