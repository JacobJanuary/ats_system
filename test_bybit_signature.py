#!/usr/bin/env python3
"""Test Bybit V5 API signature generation"""

import hmac
import hashlib
import time
import asyncio
import aiohttp
import json

API_KEY = "WrG37DUFHbejVGkqU2"
API_SECRET = "ACUTQBTtKGfGKtH0D19IS9xYUCwiRuofLJf6"
TESTNET = True

async def test_positions():
    """Test getting positions with correct signature"""
    base_url = "https://api-testnet.bybit.com" if TESTNET else "https://api.bybit.com"
    url = f"{base_url}/v5/position/list"
    
    timestamp = int(time.time() * 1000)
    recv_window = "5000"
    
    # Parameters for the request
    params = {
        "category": "linear",
        "settleCoin": "USDT",
        "limit": "200"
    }
    
    # Create query string - must be sorted alphabetically
    param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    
    # Bybit V5 signature: timestamp + api_key + recv_window + query_string
    sign_str = f"{timestamp}{API_KEY}{recv_window}{param_str}"
    print(f"Sign string: {sign_str}")
    
    signature = hmac.new(
        API_SECRET.encode(),
        sign_str.encode(),
        hashlib.sha256
    ).hexdigest()
    
    print(f"Signature: {signature}")
    
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": str(timestamp),
        "X-BAPI-RECV-WINDOW": recv_window
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            print(f"Status: {response.status}")
            text = await response.text()
            
            try:
                data = json.loads(text)
                if data.get('retCode') == 0:
                    positions = data.get('result', {}).get('list', [])
                    print(f"Success! Found {len(positions)} positions")
                    for pos in positions[:3]:  # Show first 3
                        print(f"  - {pos.get('symbol')}: {pos.get('side')} {pos.get('size')}")
                else:
                    print(f"API Error: {data.get('retMsg')}")
            except:
                print(f"Response: {text}")

if __name__ == "__main__":
    asyncio.run(test_positions())