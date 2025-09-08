#!/usr/bin/env python3
"""
Set protection using pybit SDK (same as main system)
"""

import asyncio
import os
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

load_dotenv()

def set_protection_for_position(symbol: str, side: str, avg_price: float):
    """Set protection using pybit SDK"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
    
    # Initialize client
    client = HTTP(
        testnet=testnet,
        api_key=api_key,
        api_secret=api_secret
    )
    
    # Configuration
    stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', 6.5))
    trailing_callback_rate = float(os.getenv('TRAILING_CALLBACK_RATE', 0.5))
    activation_percent = float(os.getenv('TRAILING_ACTIVATION_PERCENT', 3.5))
    
    # Calculate stop loss price
    if side == "Buy":
        stop_loss_price = avg_price * (1 - stop_loss_percent / 100)
        activation_price = avg_price * (1 + activation_percent / 100)
    else:
        stop_loss_price = avg_price * (1 + stop_loss_percent / 100)
        activation_price = avg_price * (1 - activation_percent / 100)
    
    # Round prices
    stop_loss_price = round(stop_loss_price, 2)
    activation_price = round(activation_price, 2)
    trailing_distance = round(avg_price * trailing_callback_rate / 100, 2)
    
    print(f"Setting protection for {symbol}:")
    print(f"  Stop Loss: {stop_loss_price}")
    print(f"  Trailing Distance: {trailing_distance}")
    print(f"  Activation Price: {activation_price}")
    
    try:
        # Set trading stop
        response = client.set_trading_stop(
            category="linear",
            symbol=symbol,
            stopLoss=str(stop_loss_price),
            slTriggerBy="MarkPrice",
            trailingStop=str(trailing_distance),
            activePrice=str(activation_price),
            positionIdx=0
        )
        
        if response['retCode'] == 0:
            return True, "Protection set successfully"
        else:
            return False, response.get('retMsg', 'Unknown error')
            
    except Exception as e:
        return False, str(e)

async def protect_all_positions():
    """Protect all unprotected positions"""
    
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')
    testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
    
    # Initialize client
    client = HTTP(
        testnet=testnet,
        api_key=api_key,
        api_secret=api_secret
    )
    
    print("\n" + "=" * 80)
    print("EMERGENCY POSITION PROTECTION (Using SDK)")
    print("=" * 80)
    print(f"Mode: {'TESTNET' if testnet else 'MAINNET'}")
    print(f"Stop Loss: {os.getenv('STOP_LOSS_PERCENT')}%")
    print(f"Trailing Stop: {os.getenv('TRAILING_CALLBACK_RATE')}%")
    print(f"Activation: {os.getenv('TRAILING_ACTIVATION_PERCENT')}%")
    print("=" * 80 + "\n")
    
    # Get all positions
    print("Fetching positions...")
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
    
    # Find unprotected positions
    unprotected = []
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
    
    print(f"\nFound {len(unprotected)} unprotected positions\n")
    
    # Show positions to protect
    print("Positions to protect:")
    for i, pos in enumerate(unprotected[:10], 1):
        print(f"  {i}. {pos['symbol']} ({pos['side']}): {pos['size']} @ {pos['avgPrice']}")
    if len(unprotected) > 10:
        print(f"  ... and {len(unprotected) - 10} more positions")
    
    # Auto-confirm for emergency protection
    print(f"\nAuto-protecting all {len(unprotected)} positions...")
    
    print("\nAdding protection...\n")
    
    # Add protection to each position
    success_count = 0
    failed_count = 0
    failed_symbols = []
    
    for i, pos in enumerate(unprotected, 1):
        symbol = pos['symbol']
        side = pos['side']
        avg_price = float(pos['avgPrice'])
        
        print(f"[{i}/{len(unprotected)}] Protecting {symbol}...", end=" ")
        
        success, message = set_protection_for_position(symbol, side, avg_price)
        
        if success:
            print("✅ SUCCESS")
            success_count += 1
        else:
            print(f"❌ FAILED: {message}")
            failed_count += 1
            failed_symbols.append(symbol)
        
        # Small delay to avoid rate limiting
        await asyncio.sleep(0.1)
    
    print("\n" + "=" * 80)
    print("PROTECTION RESULTS:")
    print(f"  Success: {success_count} ✅")
    print(f"  Failed: {failed_count} ❌")
    
    if success_count > 0:
        print(f"\n✅ Successfully protected {success_count} positions!")
    
    if failed_count > 0:
        print(f"\n⚠️  Failed to protect {failed_count} positions:")
        for sym in failed_symbols[:10]:
            print(f"  - {sym}")
        if len(failed_symbols) > 10:
            print(f"  ... and {len(failed_symbols) - 10} more")
        print("Please check these positions manually!")

if __name__ == "__main__":
    asyncio.run(protect_all_positions())