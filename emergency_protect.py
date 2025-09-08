#!/usr/bin/env python3
"""
Emergency protection for positions without any stop loss
"""

import os
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

load_dotenv()

def protect_unprotected_positions():
    """Add stop loss to positions without any protection"""
    
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
    
    print("\n" + "=" * 80)
    print("EMERGENCY PROTECTION - ADDING STOP LOSS")
    print("=" * 80)
    print(f"Stop Loss: {stop_loss_percent}%")
    
    # List of unprotected positions from diagnostic
    unprotected_symbols = [
        ('AINUSDT', 'Buy', 0.10049),
        ('BRUSDT', 'Buy', 0.06912844),
        ('DASHUSDT', 'Buy', 28.15215686),
        ('LDOUSDT', 'Buy', 1.222),
        ('CELRUSDT', 'Buy', 0.00657304),
        ('LSKUSDT', 'Buy', 0.3559),
        ('JUPUSDT', 'Buy', 0.50824797)
    ]
    
    success_count = 0
    failed_count = 0
    
    for symbol, side, entry_price in unprotected_symbols:
        print(f"\nProtecting {symbol}...")
        
        # Calculate stop loss price
        if side == 'Buy':
            stop_loss_price = entry_price * (1 - stop_loss_percent / 100)
        else:
            stop_loss_price = entry_price * (1 + stop_loss_percent / 100)
        
        # Determine precision based on price
        if stop_loss_price >= 100:
            precision = 2
        elif stop_loss_price >= 10:
            precision = 3
        elif stop_loss_price >= 1:
            precision = 4
        elif stop_loss_price >= 0.1:
            precision = 5
        elif stop_loss_price >= 0.01:
            precision = 6
        elif stop_loss_price >= 0.001:
            precision = 7
        else:
            precision = 8
        
        stop_loss_price = round(stop_loss_price, precision)
        
        print(f"  Entry: {entry_price}")
        print(f"  Stop Loss: {stop_loss_price}")
        
        try:
            # Set stop loss
            response = client.set_trading_stop(
                category="linear",
                symbol=symbol,
                stopLoss=str(stop_loss_price),
                slTriggerBy="MarkPrice",
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                print(f"  ✅ Stop Loss set successfully")
                success_count += 1
                
                # Try to add trailing stop too
                trailing_distance = entry_price * 0.005  # 0.5%
                activation_price = entry_price * 1.005  # 0.5% above entry
                
                trailing_distance = round(trailing_distance, precision)
                activation_price = round(activation_price, precision)
                
                try:
                    ts_response = client.set_trading_stop(
                        category="linear",
                        symbol=symbol,
                        trailingStop=str(trailing_distance),
                        activePrice=str(activation_price),
                        positionIdx=0
                    )
                    
                    if ts_response['retCode'] == 0:
                        print(f"  ✅ Trailing Stop also added!")
                    else:
                        print(f"  ⚠️  Trailing Stop failed: {ts_response.get('retMsg')}")
                except:
                    pass  # Trailing stop is optional
                    
            else:
                print(f"  ❌ Failed: {response.get('retMsg')}")
                failed_count += 1
                
        except Exception as e:
            print(f"  ❌ Exception: {str(e)}")
            failed_count += 1
    
    print("\n" + "=" * 80)
    print("RESULTS:")
    print(f"  Success: {success_count} ✅")
    print(f"  Failed: {failed_count} ❌")
    print("=" * 80)

if __name__ == "__main__":
    protect_unprotected_positions()