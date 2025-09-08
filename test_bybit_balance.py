#!/usr/bin/env python3
"""Test Bybit balance retrieval"""

import asyncio
import aiohttp
import hmac
import hashlib
import time
import json

API_KEY = "WrG37DUFHbejVGkqU2"
API_SECRET = "ACUTQBTtKGfGKtH0D19IS9xYUCwiRuofLJf6"
TESTNET = True

async def test_balance():
    base_url = "https://api-testnet.bybit.com" if TESTNET else "https://api.bybit.com"
    url = f"{base_url}/v5/account/wallet-balance"
    
    timestamp = int(time.time() * 1000)
    
    # Try UNIFIED first
    param_str = "accountType=UNIFIED"
    sign_str = f"{timestamp}{API_KEY}5000{param_str}"
    signature = hmac.new(
        API_SECRET.encode(),
        sign_str.encode(),
        hashlib.sha256
    ).hexdigest()
    
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": str(timestamp),
        "X-BAPI-RECV-WINDOW": "5000"
    }
    
    full_url = f"{url}?{param_str}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(full_url, headers=headers) as response:
            print(f"Status: {response.status}")
            data = await response.json()
            
            if data.get('retCode') != 0:
                print(f"Error: {data.get('retMsg')}")
                
                # Try CONTRACT
                timestamp = int(time.time() * 1000)
                param_str = "accountType=CONTRACT"
                sign_str = f"{timestamp}{API_KEY}5000{param_str}"
                signature = hmac.new(
                    API_SECRET.encode(),
                    sign_str.encode(),
                    hashlib.sha256
                ).hexdigest()
                
                headers["X-BAPI-TIMESTAMP"] = str(timestamp)
                headers["X-BAPI-SIGN"] = signature
                
                full_url = f"{url}?{param_str}"
                
                async with session.get(full_url, headers=headers) as response2:
                    print(f"\nCONTRACT Status: {response2.status}")
                    data = await response2.json()
            
            if data.get('retCode') == 0:
                print(f"Success!")
                for account in data.get('result', {}).get('list', []):
                    print(f"\nAccount Type: {account.get('accountType')}")
                    for coin in account.get('coin', []):
                        print(f"  {coin.get('coin')}:")
                        print(f"    walletBalance: {coin.get('walletBalance')}")
                        print(f"    availableToWithdraw: {coin.get('availableToWithdraw')}")
                        print(f"    locked: {coin.get('locked')}")
                        print(f"    usdValue: {coin.get('usdValue')}")
                        print(f"    Raw coin data: {json.dumps(coin, indent=4)}")
            else:
                print(f"Failed: {data}")

if __name__ == "__main__":
    asyncio.run(test_balance())