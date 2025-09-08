#!/usr/bin/env python3
"""
Set protection using the exact same signature method as position fetching
"""

import asyncio
import aiohttp
import hmac
import hashlib
import time
import os
import json
from dotenv import load_dotenv

load_dotenv()

async def set_protection_simple(symbol: str, side: str, avg_price: float):
    """Set simple stop loss using GET parameter style signature"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
    
    base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
    
    # Configuration
    stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', 6.5))
    
    # Calculate stop loss price
    if side == "Buy":
        stop_loss_price = avg_price * (1 - stop_loss_percent / 100)
    else:
        stop_loss_price = avg_price * (1 + stop_loss_percent / 100)
    
    # Format price
    stop_loss_price = round(stop_loss_price, 2)
    
    # Build params as query string style for signature
    param_str = f"category=linear&positionIdx=0&stopLoss={stop_loss_price}&symbol={symbol}"
    
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    
    # Sign with query string style
    sign_str = timestamp + api_key + recv_window + param_str
    
    signature = hmac.new(
        api_secret.encode('utf-8'),
        sign_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json"
    }
    
    # But send as JSON body
    params = {
        "category": "linear",
        "symbol": symbol,
        "stopLoss": str(stop_loss_price),
        "positionIdx": 0
    }
    
    url = f"{base_url}/v5/position/trading-stop"
    
    print(f"Setting stop loss for {symbol} at {stop_loss_price}...")
    print(f"Sign string: {sign_str}")
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=params) as response:
            result = await response.json()
            
            if result.get('retCode') == 0:
                return True, f"SL set at {stop_loss_price}"
            else:
                return False, result.get('retMsg', 'Unknown error')

async def main():
    """Test with one position first"""
    
    print("Testing protection on single position...")
    
    # Test with BTCUSDT
    success, message = await set_protection_simple("BTCUSDT", "Buy", 68645.8)
    
    if success:
        print(f"✅ SUCCESS: {message}")
    else:
        print(f"❌ FAILED: {message}")

if __name__ == "__main__":
    asyncio.run(main())