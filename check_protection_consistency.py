#!/usr/bin/env python3
"""
Check Protection Consistency
Script to verify protection status consistency across different monitoring systems
"""
import asyncio
import os
import sys
import logging
from dotenv import load_dotenv
from decimal import Decimal
from typing import Dict, List

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


async def check_bybit_protection():
    """Check Bybit positions protection status"""
    from exchanges.bybit import BybitExchange
    
    print("\n" + "="*60)
    print("CHECKING BYBIT POSITIONS PROTECTION")
    print("="*60)
    
    try:
        # Initialize Bybit exchange
        config = {
            'api_key': os.getenv('BYBIT_API_KEY'),
            'api_secret': os.getenv('BYBIT_API_SECRET'),
            'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
        }
        
        exchange = BybitExchange(config)
        await exchange.initialize()
        
        # Get all positions
        positions = await exchange.get_positions()
        
        if not positions:
            print("No open positions found on Bybit")
            return
        
        print(f"\nFound {len(positions)} open positions on Bybit\n")
        
        # Track protection status
        fully_protected = 0
        has_sl_only = 0
        has_ts_only = 0
        unprotected = 0
        protection_details = []
        
        for pos in positions:
            if pos.quantity == 0:
                continue
            
            symbol = pos.symbol
            
            # Get open orders for this position
            orders = await exchange.get_open_orders(symbol)
            
            # Check for protection orders
            has_stop_loss = False
            has_trailing_stop = False
            stop_price = None
            trailing_rate = None
            
            for order in orders:
                order_type = order.get('orderType', '').upper()
                order_status = order.get('orderStatus', '').upper()
                
                # Only count active orders
                if order_status not in ['NEW', 'UNTRIGGERED', 'PARTIALLY_FILLED']:
                    continue
                
                # Check for stop loss orders
                if 'STOP' in order_type:
                    if order.get('tpslMode') == 'Full':
                        # This is a position-level stop loss
                        has_stop_loss = True
                        stop_price = order.get('triggerPrice')
                        
                        # Check if it's trailing
                        if order.get('triggerBy') == 'TrailingStop':
                            has_trailing_stop = True
                            trailing_rate = order.get('trailingStop')
            
            # Alternative: Check position's built-in SL/TP
            # Note: pybit client is synchronous, not async
            position_response = exchange.client.get_positions(
                category="linear",
                symbol=symbol
            )
            
            if position_response['retCode'] == 0 and position_response['result']['list']:
                pos_data = position_response['result']['list'][0]
                
                # Check stopLoss field
                if pos_data.get('stopLoss') and float(pos_data.get('stopLoss', 0)) > 0:
                    has_stop_loss = True
                    stop_price = pos_data.get('stopLoss')
                
                # Check for trailing stop
                if pos_data.get('trailingStop') and float(pos_data.get('trailingStop', 0)) > 0:
                    has_trailing_stop = True
                    trailing_rate = pos_data.get('trailingStop')
            
            # Determine protection status
            if has_stop_loss and has_trailing_stop:
                status = "✅ FULLY PROTECTED (SL + TS)"
                fully_protected += 1
                status_color = "\033[92m"  # Green
            elif has_stop_loss:
                status = "⚠️ PARTIAL (SL only)"
                has_sl_only += 1
                status_color = "\033[93m"  # Yellow
            elif has_trailing_stop:
                status = "⚠️ PARTIAL (TS only)"
                has_ts_only += 1
                status_color = "\033[93m"  # Yellow
            else:
                status = "❌ UNPROTECTED"
                unprotected += 1
                status_color = "\033[91m"  # Red
            
            # Print position details
            print(f"{status_color}{pos.symbol:15} {pos.side:5} Size: {float(pos.quantity):10.4f} {status}\033[0m")
            
            if stop_price:
                print(f"  └─ Stop Loss: {stop_price}")
            if trailing_rate:
                print(f"  └─ Trailing: {trailing_rate}%")
            
            protection_details.append({
                'symbol': symbol,
                'side': pos.side,
                'has_sl': has_stop_loss,
                'has_ts': has_trailing_stop,
                'stop_price': stop_price,
                'trailing_rate': trailing_rate
            })
        
        # Print summary
        print("\n" + "="*60)
        print("PROTECTION SUMMARY")
        print("="*60)
        print(f"Total positions:     {len(positions)}")
        print(f"Fully protected:     {fully_protected} ({fully_protected*100/len(positions):.1f}%)")
        print(f"SL only:            {has_sl_only}")
        print(f"TS only:            {has_ts_only}")
        print(f"Unprotected:        {unprotected} ({unprotected*100/len(positions):.1f}%)")
        
        # Check consistency with database
        print("\n" + "="*60)
        print("CHECKING DATABASE CONSISTENCY")
        print("="*60)
        
        from database.connection import DatabaseManager
        from core.config import SystemConfig
        
        config = SystemConfig()
        db = DatabaseManager(config)
        await db.initialize()
        
        # Get positions from database
        db_positions = await db.positions.get_open_positions()
        
        print(f"Database shows: {len(db_positions)} open positions")
        
        # Compare with exchange data
        db_symbols = {p.symbol for p in db_positions}
        exchange_symbols = {p.symbol for p in positions}
        
        only_in_db = db_symbols - exchange_symbols
        only_on_exchange = exchange_symbols - db_symbols
        
        if only_in_db:
            print(f"\n⚠️ Positions only in database (not on exchange):")
            for symbol in only_in_db:
                print(f"  - {symbol}")
        
        if only_on_exchange:
            print(f"\n⚠️ Positions only on exchange (not in database):")
            for symbol in only_on_exchange:
                print(f"  - {symbol}")
        
        if not only_in_db and not only_on_exchange:
            print("✅ Database and exchange positions are synchronized")
        
        await db.close()
        await exchange.close()
        
        return protection_details
        
    except Exception as e:
        logger.error(f"Error checking Bybit protection: {e}")
        import traceback
        traceback.print_exc()
        return []


async def check_binance_protection():
    """Check Binance positions protection status"""
    from exchanges.binance import BinanceExchange
    
    print("\n" + "="*60)
    print("CHECKING BINANCE POSITIONS PROTECTION")
    print("="*60)
    
    try:
        # Initialize Binance exchange
        config = {
            'api_key': os.getenv('BINANCE_API_KEY'),
            'api_secret': os.getenv('BINANCE_API_SECRET'),
            'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
        }
        
        exchange = BinanceExchange(config)
        await exchange.initialize()
        
        # Get all positions
        positions = await exchange.get_positions()
        
        if not positions:
            print("No open positions found on Binance")
            return []
        
        print(f"\nFound {len(positions)} open positions on Binance\n")
        
        # Track protection status
        protection_details = []
        
        for pos in positions:
            # Get open orders for this position
            orders = await exchange.get_open_orders(pos.symbol)
            
            # Check for protection orders
            has_stop_loss = False
            has_trailing_stop = False
            
            for order in orders:
                if order.get('type') == 'STOP_MARKET':
                    has_stop_loss = True
                elif order.get('type') == 'TRAILING_STOP_MARKET':
                    has_trailing_stop = True
            
            # Determine protection status
            if has_stop_loss and has_trailing_stop:
                status = "✅ FULLY PROTECTED"
            elif has_stop_loss:
                status = "⚠️ SL only"
            elif has_trailing_stop:
                status = "⚠️ TS only"
            else:
                status = "❌ UNPROTECTED"
            
            print(f"{pos.symbol:15} {pos.side:5} {status}")
            
            protection_details.append({
                'symbol': pos.symbol,
                'side': pos.side,
                'has_sl': has_stop_loss,
                'has_ts': has_trailing_stop
            })
        
        await exchange.close()
        return protection_details
        
    except Exception as e:
        logger.error(f"Error checking Binance protection: {e}")
        return []


async def main():
    """Main function"""
    print("\n" + "="*60)
    print("PROTECTION CONSISTENCY CHECK")
    print("="*60)
    
    # Check Bybit
    bybit_details = await check_bybit_protection()
    
    # Check Binance
    binance_details = await check_binance_protection()
    
    # Final summary
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    
    total_positions = len(bybit_details) + len(binance_details)
    total_unprotected = sum(1 for d in bybit_details + binance_details 
                           if not d['has_sl'] and not d['has_ts'])
    
    print(f"Total positions across all exchanges: {total_positions}")
    print(f"Total unprotected positions:          {total_unprotected}")
    
    if total_unprotected > 0:
        print("\n⚠️ ACTION REQUIRED: Found unprotected positions!")
        print("Run protection scripts to add stop loss and trailing stop")
    else:
        print("\n✅ All positions are protected!")


if __name__ == "__main__":
    asyncio.run(main())