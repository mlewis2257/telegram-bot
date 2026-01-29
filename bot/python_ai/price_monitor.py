import asyncio
import websockets
import json


async def monitor_price(token_mint):
    uri = "wss://quote-api.jup.ag/v4/ws"

    # Subscribe to price stream for a token
    async with websockets.connect(uri) as websocket:
        subcribe_payload = {
            "type": "subscribe",
            "topic": "price",
            "params": {
                "mint": token_mint,  # e.g. BONK = "DezX1fDW"
                "interval": "1s"     # get price every second
            }
        }

        await websocket.send(json.dumps(subcribe_payload))

        print(f"Subscribed to {token_mint} price updates...")
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                price = data.get("price")
                if price:
                    print(f"[{token_mint}] Price: {price}")
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed, reconnecting...")
                return await monitor_price(token_mint)

asyncio.run(monitor_price("BNn9DESty5NCCLMDWhc1dQCdGRp2tLuTjVqPSgyETRND"))
