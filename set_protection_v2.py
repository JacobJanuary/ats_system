#!/usr/bin/env python3
"""
Set protection using correct API signature
"""

import asyncio
import aiohttp
import hmac
import hashlib
import time
import os
from dotenv import load_dotenv

load_dotenv()

async def set_protection_simple(symbol: str, side: str, avg_price: float):
    """Set simple stop loss only (more reliable)"""
    
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
    
    # Format price based on value
    if stop_loss_price >= 1000:
        stop_loss_price = round(stop_loss_price, 1)
    elif stop_loss_price >= 1:
        stop_loss_price = round(stop_loss_price, 4)
    else:
        stop_loss_price = round(stop_loss_price, 6)
    
    # Parameters in correct order for signature
    params = {
        "category": "linear",
        "symbol": symbol,
        "stopLoss": str(stop_loss_price),
        "positionIdx": 0
    }
    
    # Build param string for signature (alphabetical order)
    param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    
    timestamp = str(int(time.time() * 1000))
    sign_str = timestamp + api_key + "5000" + param_str
    
    signature = hmac.new(
        api_secret.encode('utf-8'),
        sign_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": "5000",
        "Content-Type": "application/json"
    }
    
    url = f"{base_url}/v5/position/trading-stop"
    
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