# test_bybit_account.py
import asyncio
import aiohttp
import hmac
import hashlib
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()


async def test_bybit_account():
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    base_url = "https://api-testnet.bybit.com"

    session = aiohttp.ClientSession()

    # Test 1: Check account info
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"

    def sign(params_str):
        param_str = f"{timestamp}{api_key}{recv_window}{params_str}"
        return hmac.new(api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()

    headers = {
        'X-BAPI-API-KEY': api_key,
        'X-BAPI-TIMESTAMP': timestamp,
        'X-BAPI-RECV-WINDOW': recv_window,
        'X-BAPI-SIGN': sign("")
    }

    # Get account info
    url = f"{base_url}/v5/account/info"
    async with session.get(url, headers=headers) as resp:
        data = await resp.json()
        print("Account Info:", json.dumps(data, indent=2))

    # Test different position endpoints
    for params in [
        {"category": "linear"},
        {"category": "linear", "settleCoin": "USDT"},
        {"category": "linear", "baseCoin": "USDT"}
    ]:
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        headers['X-BAPI-SIGN'] = sign(query_string)

        url = f"{base_url}/v5/position/list?{query_string}"
        async with session.get(url, headers=headers) as resp:
            data = await resp.json()
            if data.get('retCode') == 0:
                positions = data.get('result', {}).get('list', [])
                print(f"\nPositions with {params}:")
                for pos in positions:
                    if float(pos.get('size', 0)) > 0:
                        print(f"  {pos['symbol']}: {pos['size']} @ {pos.get('avgPrice')}")

    await session.close()


asyncio.run(test_bybit_account())