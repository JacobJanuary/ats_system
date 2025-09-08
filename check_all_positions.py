#!/usr/bin/env python3
"""
Check protection status on BOTH testnet and mainnet
"""

import asyncio
import aiohttp
import hmac
import hashlib
import time
import os
from dotenv import load_dotenv

load_dotenv()

async def check_positions(testnet: bool):
    """Check positions on specific environment"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    
    base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
    env_name = "TESTNET" if testnet else "MAINNET"
    
    # Get all positions with pagination
    all_positions = []
    cursor = ""
    
    while True:
        timestamp = str(int(time.time() * 1000))
        
        if cursor:
            param_str = f"category=linear&settleCoin=USDT&limit=200&cursor={cursor}"
        else:
            param_str = "category=linear&settleCoin=USDT&limit=200"
        
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
                    print(f"Error on {env_name}: {data.get('retMsg')}")
                    break
                
                positions = data.get('result', {}).get('list', [])
                all_positions.extend(positions)
                
                # Check for next page
                next_cursor = data.get('result', {}).get('nextPageCursor', '')
                if not next_cursor:
                    break
                cursor = next_cursor
    
    # Filter active positions
    active_positions = [p for p in all_positions if float(p.get('size', 0)) > 0]
    
    print(f"\n{'='*80}")
    print(f"BYBIT {env_name} POSITIONS")
    print(f"{'='*80}")
    print(f"Total active positions: {len(active_positions)}")
    
    protected = 0
    unprotected = 0
    unprotected_list = []
    
    for pos in active_positions:
        symbol = pos.get('symbol')
        side = pos.get('side')
        size = pos.get('size')
        avg_price = pos.get('avgPrice')
        
        # Check protection
        stop_loss = pos.get('stopLoss', '')
        take_profit = pos.get('takeProfit', '')
        trailing_stop = pos.get('trailingStop', '')
        
        has_sl = stop_loss and float(stop_loss) > 0
        has_tp = take_profit and float(take_profit) > 0
        has_ts = trailing_stop and float(trailing_stop) > 0
        
        if has_sl or has_tp or has_ts:
            protected += 1
            status = "✅"
        else:
            unprotected += 1
            unprotected_list.append(f"{symbol} ({side})")
            status = "❌"
        
        # Show first 5 positions as sample
        if unprotected <= 5 and status == "❌":
            print(f"  {status} {symbol} ({side}): {size} @ {avg_price}")
    
    if unprotected > 5:
        print(f"  ... and {unprotected - 5} more unprotected positions")
    
    print(f"\nSUMMARY:")
    print(f"  Protected: {protected} ✅")
    print(f"  Unprotected: {unprotected} ❌")
    
    if unprotected > 0:
        print(f"\n⚠️  WARNING: {unprotected} positions are UNPROTECTED on {env_name}!")
        print(f"Unprotected symbols: {', '.join(unprotected_list[:10])}")
        if len(unprotected_list) > 10:
            print(f"... and {len(unprotected_list) - 10} more")
    
    return unprotected

async def main():
    """Check both environments"""
    
    print("\n" + "="*80)
    print("CHECKING ALL BYBIT POSITIONS")
    print("="*80)
    
    # Check testnet
    testnet_unprotected = await check_positions(testnet=True)
    
    # Check mainnet
    mainnet_unprotected = await check_positions(testnet=False)
    
    print("\n" + "="*80)
    print("TOTAL RISK EXPOSURE:")
    print(f"  Testnet: {testnet_unprotected} unprotected positions")
    print(f"  Mainnet: {mainnet_unprotected} unprotected positions")
    print(f"  TOTAL: {testnet_unprotected + mainnet_unprotected} positions at risk!")
    
    if testnet_unprotected + mainnet_unprotected > 0:
        print("\n🚨 CRITICAL: Positions need immediate protection!")
        print("Run: python protect_all_positions.py")

if __name__ == "__main__":
    asyncio.run(main())