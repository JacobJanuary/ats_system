#!/usr/bin/env python3
"""
Test script for Binance order creation with protection
Tests the enhanced Binance manager with automatic SL and TS
"""

import asyncio
import logging
import os
from decimal import Decimal
from dotenv import load_dotenv
from datetime import datetime

# Add project root to path
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from exchanges.binance_enhanced import BinanceEnhancedManager
from database.models import TradingSignal

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


async def test_binance_order_with_protection():
    """Test creating a Binance order with automatic protection"""
    
    logger.info("=" * 80)
    logger.info("BINANCE ORDER TEST WITH PROTECTION")
    logger.info("=" * 80)
    
    # Initialize enhanced Binance manager
    config = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
    }
    
    manager = BinanceEnhancedManager(config)
    
    try:
        # Initialize connection
        await manager.initialize()
        logger.info("✅ Connected to Binance")
        
        # Test signal
        test_symbol = "BTCUSDT"
        test_side = "BUY"
        test_quantity = Decimal("0.001")  # Small test position
        
        logger.info(f"\n📊 Test Parameters:")
        logger.info(f"  Symbol: {test_symbol}")
        logger.info(f"  Side: {test_side}")
        logger.info(f"  Quantity: {test_quantity}")
        logger.info(f"  Leverage: 10x")
        
        # Get current price for reference
        ticker = await manager.get_ticker(test_symbol)
        if ticker:
            current_price = ticker.get('last')
            logger.info(f"  Current Price: ${current_price}")
            notional = float(test_quantity) * float(current_price)
            logger.info(f"  Notional Value: ${notional:.2f}")
        
        # Create order with protection
        logger.info("\n🚀 Creating order with protection...")
        
        result = await manager.create_order_with_protection(
            symbol=test_symbol,
            side=test_side,
            quantity=test_quantity,
            order_type="MARKET",
            leverage=10
        )
        
        # Display results
        logger.info("\n" + "=" * 80)
        logger.info("📋 RESULTS:")
        logger.info("=" * 80)
        
        if result.get('success'):
            logger.info("✅ ORDER CREATED SUCCESSFULLY!")
            
            # Order details
            order = result.get('order', {})
            logger.info(f"\n📦 Order Details:")
            logger.info(f"  Order ID: {order.get('orderId')}")
            logger.info(f"  Status: {order.get('status')}")
            logger.info(f"  Executed Qty: {order.get('executedQty')}")
            logger.info(f"  Avg Price: {order.get('avgPrice')}")
            
            # Protection details
            protection = result.get('protection', {})
            logger.info(f"\n🛡️ Protection Details:")
            
            if protection:
                # Stop Loss
                sl = protection.get('stop_loss', {})
                if sl and sl.get('success'):
                    logger.info(f"  ✅ Stop Loss: Set at ${sl.get('stop_price')}")
                else:
                    logger.info(f"  ❌ Stop Loss: {sl.get('error', 'Not set')}")
                
                # Trailing Stop
                ts = protection.get('trailing_stop', {})
                if ts and ts.get('success'):
                    logger.info(f"  ✅ Trailing Stop: Activation=${ts.get('activation_price')}, Callback={ts.get('callback_rate')}%")
                else:
                    logger.info(f"  ❌ Trailing Stop: {ts.get('error', 'Not set')}")
                
                # Take Profit
                tp = protection.get('take_profit', {})
                if tp and tp.get('success'):
                    logger.info(f"  ✅ Take Profit: Set at ${tp.get('stop_price')}")
                elif tp:
                    logger.info(f"  ❌ Take Profit: {tp.get('error', 'Not set')}")
            else:
                logger.warning("  ⚠️ No protection details available")
            
            # Check position
            logger.info(f"\n📍 Checking position...")
            position = await manager.get_position(test_symbol)
            if position:
                logger.info(f"  Position Amount: {position.position_amount}")
                logger.info(f"  Entry Price: ${position.entry_price}")
                logger.info(f"  Mark Price: ${position.mark_price}")
                logger.info(f"  Unrealized PnL: ${position.unrealized_pnl}")
            
            # Check open orders (protection orders)
            logger.info(f"\n📋 Checking protection orders...")
            open_orders = await manager.get_open_orders(test_symbol)
            if open_orders:
                logger.info(f"  Found {len(open_orders)} protection orders:")
                for order in open_orders:
                    order_type = order.get('type')
                    order_side = order.get('side')
                    stop_price = order.get('stopPrice', 'N/A')
                    logger.info(f"    - {order_type} {order_side} @ ${stop_price}")
            else:
                logger.warning("  No protection orders found")
            
        else:
            logger.error("❌ ORDER FAILED!")
            logger.error(f"Error: {result.get('error')}")
            if result.get('order'):
                logger.error(f"Order details: {result.get('order')}")
        
    except Exception as e:
        logger.error(f"❌ Test failed with exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    finally:
        # Close connection
        await manager.close()
        logger.info("\n👋 Connection closed")
    
    logger.info("\n" + "=" * 80)
    logger.info("TEST COMPLETED")
    logger.info("=" * 80)


async def test_signal_processing():
    """Test processing a signal through the enhanced manager"""
    
    logger.info("\n" + "=" * 80)
    logger.info("SIGNAL PROCESSING TEST")
    logger.info("=" * 80)
    
    # Initialize manager
    config = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
    }
    
    manager = BinanceEnhancedManager(config)
    
    try:
        await manager.initialize()
        
        # Create test signal
        test_signal = {
            'exchange_id': 1,  # Binance
            'exchange_name': 'binance',
            'pair_symbol': 'ETHUSDT',
            'action': 'BUY',
            'leverage': 5,
            'prediction': True,
            'confidence_level': 'HIGH'
        }
        
        logger.info(f"Test signal: {test_signal}")
        
        # Process signal
        result = await manager.process_signal_for_binance(test_signal)
        
        if result.get('success'):
            logger.info("✅ Signal processed successfully!")
            logger.info(f"Protection: {result.get('protection')}")
        else:
            logger.error(f"❌ Signal processing failed: {result.get('error')}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
    
    finally:
        await manager.close()


async def cleanup_test_position():
    """Close any test positions if needed"""
    
    logger.info("\n" + "=" * 80)
    logger.info("CLEANUP TEST POSITIONS")
    logger.info("=" * 80)
    
    config = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
    }
    
    manager = BinanceEnhancedManager(config)
    
    try:
        await manager.initialize()
        
        # Check for open positions
        test_symbols = ['BTCUSDT', 'ETHUSDT']
        
        for symbol in test_symbols:
            position = await manager.get_position(symbol)
            if position and position.position_amount != 0:
                logger.info(f"Found open position for {symbol}: {position.position_amount}")
                
                # Cancel all orders first
                await manager.cancel_all_orders(symbol)
                logger.info(f"Cancelled all orders for {symbol}")
                
                # Close position
                side = "SELL" if position.position_amount > 0 else "BUY"
                quantity = abs(position.position_amount)
                
                result = await manager.place_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    reduce_only=True
                )
                
                if result.get('status') != 'REJECTED':
                    logger.info(f"✅ Closed position for {symbol}")
                else:
                    logger.error(f"Failed to close position: {result}")
        
        logger.info("Cleanup completed")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
    
    finally:
        await manager.close()


async def main():
    """Main test function"""
    
    print("\n" + "=" * 80)
    print("BINANCE ENHANCED MANAGER TEST SUITE")
    print("=" * 80)
    print("\nSelect test to run:")
    print("1. Test order with protection")
    print("2. Test signal processing")
    print("3. Cleanup test positions")
    print("4. Run all tests")
    print("0. Exit")
    
    choice = input("\nEnter choice (0-4): ").strip()
    
    if choice == "1":
        await test_binance_order_with_protection()
    elif choice == "2":
        await test_signal_processing()
    elif choice == "3":
        await cleanup_test_position()
    elif choice == "4":
        await test_binance_order_with_protection()
        await asyncio.sleep(2)
        await test_signal_processing()
    elif choice == "0":
        print("Exiting...")
    else:
        print("Invalid choice")


if __name__ == "__main__":
    asyncio.run(main())