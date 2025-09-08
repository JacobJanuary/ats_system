#!/usr/bin/env python3
"""
Check protection status using SDK
"""

import os
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

load_dotenv()

def check_protection_status():
    """Check protection status of all positions"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
    
    # Initialize client
    client = HTTP(
        testnet=testnet,
        api_key=api_key,
        api_secret=api_secret
    )
    
    env_name = "TESTNET" if testnet else "MAINNET"
    
    print("\n" + "=" * 80)
    print(f"BYBIT {env_name} PROTECTION STATUS")
    print("=" * 80)
    
    # Get all positions
    all_positions = []
    cursor = ""
    
    while True:
        params = {
            "category": "linear",
            "settleCoin": "USDT",
            "limit": 200
        }
        
        if cursor:
            params["cursor"] = cursor
        
        response = client.get_positions(**params)
        
        if response['retCode'] != 0:
            print(f"Error getting positions: {response.get('retMsg')}")
            break
        
        positions = response.get('result', {}).get('list', [])
        all_positions.extend(positions)
        
        cursor = response.get('result', {}).get('nextPageCursor', '')
        if not cursor:
            break
    
    # Analyze protection
    total_active = 0
    protected = 0
    unprotected = 0
    unprotected_list = []
    
    for pos in all_positions:
        if float(pos.get('size', 0)) > 0:
            total_active += 1
            
            symbol = pos['symbol']
            side = pos['side']
            size = pos['size']
            avg_price = pos['avgPrice']
            
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
                protection_type = []
                if has_sl:
                    protection_type.append(f"SL:{stop_loss}")
                if has_ts:
                    protection_type.append(f"TS:{trailing_stop}")
                if has_tp:
                    protection_type.append(f"TP:{take_profit}")
                protection = " | ".join(protection_type)
            else:
                unprotected += 1
                unprotected_list.append(f"{symbol} ({side})")
                status = "❌"
                protection = "NO PROTECTION"
            
            # Show first 5 positions as examples
            if total_active <= 5:
                print(f"{status} {symbol} ({side}): {size} @ {avg_price}")
                print(f"   Protection: {protection}")
    
    if total_active > 5:
        print(f"... and {total_active - 5} more positions")
    
    print(f"\nSUMMARY:")
    print(f"  Total Active: {total_active}")
    print(f"  Protected: {protected} ✅")
    print(f"  Unprotected: {unprotected} ❌")
    
    if unprotected > 0:
        print(f"\n⚠️  WARNING: {unprotected} positions are UNPROTECTED!")
        print(f"Unprotected symbols: {', '.join(unprotected_list[:10])}")
        if len(unprotected_list) > 10:
            print(f"... and {len(unprotected_list) - 10} more")
    else:
        print(f"\n✅ EXCELLENT: All {total_active} positions are protected!")
    
    return protected, unprotected

if __name__ == "__main__":
    check_protection_status()