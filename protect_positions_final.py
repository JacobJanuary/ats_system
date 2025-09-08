#!/usr/bin/env python3
"""
FINAL VERSION: Protect all positions with proper price handling
"""

import asyncio
import os
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from decimal import Decimal, ROUND_DOWN

load_dotenv()

def calculate_price_precision(price: float) -> int:
    """Calculate appropriate decimal places based on price value"""
    if price >= 1000:
        return 1
    elif price >= 100:
        return 2
    elif price >= 10:
        return 3
    elif price >= 1:
        return 4
    elif price >= 0.1:
        return 5
    elif price >= 0.01:
        return 6
    elif price >= 0.001:
        return 7
    else:
        return 8

def set_protection_for_position(symbol: str, side: str, avg_price: float, mark_price: float = None):
    """Set protection using proper price formatting"""
    
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
    
    # Use mark price if available (more accurate for current position)
    base_price = mark_price if mark_price else avg_price
    
    # Calculate stop loss price
    if side == "Buy":
        stop_loss_price = base_price * (1 - stop_loss_percent / 100)
        activation_price = base_price * (1 + activation_percent / 100)
    else:
        stop_loss_price = base_price * (1 + stop_loss_percent / 100)
        activation_price = base_price * (1 - activation_percent / 100)
    
    # Get precision based on price
    precision = calculate_price_precision(base_price)
    
    # Round prices with proper precision
    stop_loss_price = round(stop_loss_price, precision)
    activation_price = round(activation_price, precision)
    trailing_distance = round(base_price * trailing_callback_rate / 100, precision)
    
    # Ensure trailing distance is not 0
    if trailing_distance == 0:
        min_value = 10 ** (-precision)
        trailing_distance = min_value
    
    print(f"  Price: {base_price:.{precision}f}")
    print(f"  Stop Loss: {stop_loss_price:.{precision}f}")
    print(f"  Trailing: {trailing_distance:.{precision}f}")
    print(f"  Activation: {activation_price:.{precision}f}")
    
    try:
        # Try with trailing stop first
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
            return True, "Full protection set"
        elif "not modified" in str(response.get('retMsg', '')):
            return True, "Already protected"
        else:
            # Try simple stop loss only
            response = client.set_trading_stop(
                category="linear",
                symbol=symbol,
                stopLoss=str(stop_loss_price),
                slTriggerBy="MarkPrice",
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                return True, "Stop loss set"
            else:
                return False, response.get('retMsg', 'Unknown error')
            
    except Exception as e:
        # Last resort - try even simpler stop loss
        try:
            response = client.set_trading_stop(
                category="linear",
                symbol=symbol,
                stopLoss=str(stop_loss_price),
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                return True, "Basic stop loss set"
            else:
                return False, str(e)
        except Exception as e2:
            return False, str(e2)

async def protect_all_positions():
    """Protect all unprotected positions with improved logic"""
    
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
    print("FINAL POSITION PROTECTION SCRIPT")
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
    
    print(f"\nFound {len(unprotected)} unprotected positions")
    print("\nStarting protection process...\n")
    
    # Add protection to each position
    success_count = 0
    failed_count = 0
    failed_symbols = []
    
    for i, pos in enumerate(unprotected, 1):
        symbol = pos['symbol']
        side = pos['side']
        avg_price = float(pos['avgPrice'])
        mark_price = float(pos.get('markPrice', avg_price))
        
        print(f"[{i}/{len(unprotected)}] Protecting {symbol} ({side})...")
        
        success, message = set_protection_for_position(symbol, side, avg_price, mark_price)
        
        if success:
            print(f"  ✅ {message}\n")
            success_count += 1
        else:
            print(f"  ❌ FAILED: {message}\n")
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
        for sym in failed_symbols:
            print(f"  - {sym}")
        print("\nThese positions may need manual review.")
    
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(protect_all_positions())