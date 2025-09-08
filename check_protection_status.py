#!/usr/bin/env python3
"""
Quick script to check protection status of all open Bybit positions
"""

import asyncio
import aiohttp
import hmac
import hashlib
import time
import os
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()

async def check_positions_protection():
    """Check all open positions and their protection status"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
    
    base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
    
    # Get positions
    timestamp = str(int(time.time() * 1000))
    params = {"category": "linear", "settleCoin": "USDT"}
    param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    
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
            
            if data.get('retCode') == 0:
                positions = data.get('result', {}).get('list', [])
                
                print("\n" + "=" * 80)
                print("BYBIT POSITIONS PROTECTION STATUS")
                print("=" * 80)
                print(f"Mode: {'TESTNET' if testnet else 'MAINNET'}")
                print(f"Total positions: {len(positions)}\n")
                
                protected_count = 0
                unprotected_count = 0
                
                for pos in positions:
                    if float(pos.get('size', 0)) > 0:
                        symbol = pos.get('symbol')
                        side = pos.get('side')
                        size = pos.get('size')
                        entry_price = pos.get('avgPrice')
                        
                        # Check protection
                        stop_loss = pos.get('stopLoss', '')
                        take_profit = pos.get('takeProfit', '')
                        trailing_stop = pos.get('trailingStop', '')
                        
                        has_sl = stop_loss and float(stop_loss) > 0
                        has_tp = take_profit and float(take_profit) > 0
                        has_ts = trailing_stop and float(trailing_stop) > 0
                        
                        has_protection = has_sl or has_tp or has_ts
                        
                        if has_protection:
                            protected_count += 1
                            status = "✅ PROTECTED"
                        else:
                            unprotected_count += 1
                            status = "❌ UNPROTECTED"
                        
                        print(f"{symbol} ({side}): {status}")
                        print(f"  Size: {size}")
                        print(f"  Entry: {entry_price}")
                        
                        if has_sl:
                            print(f"  Stop Loss: {stop_loss}")
                        if has_tp:
                            print(f"  Take Profit: {take_profit}")
                        if has_ts:
                            print(f"  Trailing Stop: {trailing_stop}")
                        
                        if not has_protection:
                            print(f"  ⚠️  NO PROTECTION SET!")
                        
                        print()
                
                print("=" * 80)
                print(f"SUMMARY:")
                print(f"  Protected positions: {protected_count} ✅")
                print(f"  Unprotected positions: {unprotected_count} ❌")
                
                if unprotected_count > 0:
                    print(f"\n⚠️  WARNING: {unprotected_count} positions are UNPROTECTED!")
                    print("  These positions are at risk!")
                
                # Check stop loss configuration
                print("\n" + "=" * 80)
                print("CURRENT CONFIGURATION:")
                print(f"  USE_STOP_LOSS: {os.getenv('USE_STOP_LOSS', 'false')}")
                print(f"  STOP_LOSS_TYPE: {os.getenv('STOP_LOSS_TYPE', 'fixed')}")
                print(f"  STOP_LOSS_PERCENT: {os.getenv('STOP_LOSS_PERCENT', 'N/A')}%")
                
                if os.getenv('USE_TRAILING_STOP', 'false').lower() == 'true':
                    print(f"  TRAILING_CALLBACK_RATE: {os.getenv('TRAILING_CALLBACK_RATE', 'N/A')}%")
                    print(f"  TRAILING_ACTIVATION_PERCENT: {os.getenv('TRAILING_ACTIVATION_PERCENT', 'N/A')}%")
                
            else:
                print(f"Error: {data.get('retMsg')}")

if __name__ == "__main__":
    asyncio.run(check_positions_protection())