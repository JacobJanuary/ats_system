#!/usr/bin/env python3
"""
EMERGENCY SCRIPT: Add protection to all unprotected positions
"""

import asyncio
import aiohttp
import hmac
import hashlib
import time
import os
from dotenv import load_dotenv
from decimal import Decimal
import json

load_dotenv()

async def set_position_protection(symbol: str, side: str, avg_price: float):
    """Set trailing stop for a specific position"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
    
    base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
    
    # Configuration from .env
    stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', 6.5))
    trailing_callback_rate = float(os.getenv('TRAILING_CALLBACK_RATE', 0.5))
    activation_percent = float(os.getenv('TRAILING_ACTIVATION_PERCENT', 3.5))
    
    # Calculate stop loss price (fixed stop first as safety)
    if side == "Buy":
        stop_loss_price = avg_price * (1 - stop_loss_percent / 100)
        activation_price = avg_price * (1 + activation_percent / 100)
    else:  # Sell
        stop_loss_price = avg_price * (1 + stop_loss_percent / 100)
        activation_price = avg_price * (1 - activation_percent / 100)
    
    # Round prices
    stop_loss_price = round(stop_loss_price, 4)
    activation_price = round(activation_price, 4)
    trailing_distance = round(avg_price * trailing_callback_rate / 100, 4)
    
    # Set Trading Stop (includes both SL and Trailing)
    params = {
        "category": "linear",
        "symbol": symbol,
        "positionIdx": 0,
        "stopLoss": str(stop_loss_price),
        "slTriggerBy": "MarkPrice",
        "trailingStop": str(trailing_distance),
        "activePrice": str(activation_price)
    }
    
    timestamp = str(int(time.time() * 1000))
    param_str = json.dumps(params, separators=(',', ':'))
    
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
                return True, "Protection set successfully"
            else:
                return False, result.get('retMsg', 'Unknown error')

async def protect_all_positions():
    """Main function to protect all positions"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
    
    base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
    
    print("\n" + "=" * 80)
    print("EMERGENCY POSITION PROTECTION")
    print("=" * 80)
    print(f"Mode: {'TESTNET' if testnet else 'MAINNET'}")
    print(f"Stop Loss: {os.getenv('STOP_LOSS_PERCENT')}%")
    print(f"Trailing Stop: {os.getenv('TRAILING_CALLBACK_RATE')}%")
    print(f"Activation: {os.getenv('TRAILING_ACTIVATION_PERCENT')}%")
    print("=" * 80 + "\n")
    
    # Get all positions with pagination
    all_positions = []
    cursor = ""
    
    while True:
        timestamp = str(int(time.time() * 1000))
        
        if cursor:
            # Build params manually to ensure correct order
            params = {"category": "linear", "settleCoin": "USDT", "limit": "200"}
            param_str = f"category=linear&limit=200&settleCoin=USDT&cursor={cursor}"
        else:
            params = {"category": "linear", "settleCoin": "USDT", "limit": "200"}
            param_str = "category=linear&limit=200&settleCoin=USDT"
    
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
        "X-BAPI-RECV-WINDOW": "5000"
    }
    
        url = f"{base_url}/v5/position/list?{param_str}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                
                if data.get('retCode') != 0:
                    print(f"Error getting positions: {data.get('retMsg')}")
                    break
                
                positions = data.get('result', {}).get('list', [])
                all_positions.extend(positions)
                
                # Check for next page
                next_cursor = data.get('result', {}).get('nextPageCursor', '')
                if not next_cursor:
                    break
                cursor = next_cursor
    
    # Now process all collected positions
    unprotected = []
    
    # Find unprotected positions
    for pos in all_positions:
        if float(pos.get('size', 0)) > 0:
            stop_loss = pos.get('stopLoss', '')
            trailing_stop = pos.get('trailingStop', '')
            
            has_sl = stop_loss and float(stop_loss) > 0
            has_ts = trailing_stop and float(trailing_stop) > 0
            
            if not has_sl and not has_ts:
                unprotected.append(pos)
    
    if not unprotected:
        print("✅ All positions are already protected!")
        return
    
    print(f"Found {len(unprotected)} unprotected positions\n")
    
    # Confirm action
    print("Positions to protect:")
    for pos in unprotected[:10]:  # Show first 10
        print(f"  - {pos['symbol']} ({pos['side']}): {pos['size']} @ {pos['avgPrice']}")
    if len(unprotected) > 10:
        print(f"  ... and {len(unprotected) - 10} more positions")
    
    response = input(f"\nProtect all {len(unprotected)} positions? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled")
        return
    
    print("\nAdding protection...\n")
    
    # Add protection to each position
    success_count = 0
    failed_count = 0
    
    for pos in unprotected:
        symbol = pos['symbol']
        side = pos['side']
        avg_price = float(pos['avgPrice'])
        
        print(f"Protecting {symbol}...", end=" ")
        
        success, message = await set_position_protection(symbol, side, avg_price)
        
        if success:
            print("✅ SUCCESS")
            success_count += 1
        else:
            print(f"❌ FAILED: {message}")
            failed_count += 1
        
        # Small delay to avoid rate limiting
        await asyncio.sleep(0.2)
    
    print("\n" + "=" * 80)
    print("PROTECTION RESULTS:")
    print(f"  Success: {success_count} ✅")
    print(f"  Failed: {failed_count} ❌")
    
    if success_count > 0:
        print(f"\n✅ Successfully protected {success_count} positions!")
    
    if failed_count > 0:
        print(f"\n⚠️  Failed to protect {failed_count} positions. Please check manually!")

if __name__ == "__main__":
    asyncio.run(protect_all_positions())