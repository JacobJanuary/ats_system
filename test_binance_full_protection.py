#!/usr/bin/env python3
"""
Test script for Binance Futures order creation with full protection
Tests the complete flow: order creation, stop loss, and trailing stop
"""

import asyncio
import logging
import sys
from decimal import Decimal
from datetime import datetime
import os
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from exchanges.binance_enhanced import BinanceEnhancedManager
from exchanges.binance import BinanceExchange
from database.models import Position

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


async def test_binance_order_with_protection():
    """Test creating a Binance order with automatic protection"""
    
    try:
        logger.info("=" * 60)
        logger.info("BINANCE FUTURES - ORDER WITH PROTECTION TEST")
        logger.info("=" * 60)
        
        # Configuration
        config = {
            'api_key': os.getenv('BINANCE_API_KEY'),
            'api_secret': os.getenv('BINANCE_API_SECRET'),
            'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
        }
        
        # Initialize the enhanced manager
        logger.info("Initializing Binance Enhanced Manager...")
        manager = BinanceEnhancedManager(config)
        await manager.initialize()
        
        # Test parameters
        symbol = "BTCUSDT"
        side = "BUY"  # Open LONG position
        leverage = 10
        size_usd = 100  # $100 position
        
        logger.info(f"\nTest Parameters:")
        logger.info(f"  Symbol: {symbol}")
        logger.info(f"  Side: {side} (LONG)")
        logger.info(f"  Leverage: {leverage}x")
        logger.info(f"  Size: ${size_usd}")
        logger.info(f"  Testnet: {config['testnet']}")
        
        # Get current market price
        market_data = await manager.get_market_data(symbol)
        if not market_data:
            logger.error("Failed to get market data")
            return
        
        current_price = market_data.last_price
        logger.info(f"\nCurrent {symbol} price: ${current_price:,.2f}")
        
        # Calculate quantity
        quantity = Decimal(str(size_usd)) / current_price
        symbol_info = await manager.get_symbol_info(symbol)
        if symbol_info:
            quantity = symbol_info.round_quantity(quantity)
        
        logger.info(f"Calculated quantity: {quantity} BTC")
        
        # Create order with protection
        logger.info("\n" + "=" * 40)
        logger.info("CREATING ORDER WITH PROTECTION...")
        logger.info("=" * 40)
        
        result = await manager.create_order_with_protection(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type="MARKET",
            leverage=leverage
        )
        
        # Display results
        if result.get('success'):
            logger.info("\n✅ ORDER CREATION SUCCESSFUL!")
            
            # Order details
            order = result.get('order', {})
            logger.info(f"\nOrder Details:")
            logger.info(f"  Order ID: {order.get('orderId')}")
            logger.info(f"  Status: {order.get('status')}")
            logger.info(f"  Executed Qty: {order.get('executedQty')}")
            logger.info(f"  Average Price: {order.get('avgPrice', 'N/A')}")
            
            # Protection details
            protection = result.get('protection', {})
            if protection:
                logger.info(f"\n🛡️ Protection Orders:")
                
                if protection.get('stop_loss'):
                    sl = protection['stop_loss']
                    logger.info(f"  Stop Loss:")
                    logger.info(f"    - Order ID: {sl.get('order_id')}")
                    logger.info(f"    - Stop Price: ${sl.get('stop_price', 'N/A')}")
                    logger.info(f"    - Type: {sl.get('type', 'STOP_MARKET')}")
                
                if protection.get('trailing_stop'):
                    ts = protection['trailing_stop']
                    logger.info(f"  Trailing Stop:")
                    logger.info(f"    - Order ID: {ts.get('order_id')}")
                    logger.info(f"    - Activation: ${ts.get('activation_price', 'N/A')}")
                    logger.info(f"    - Callback: {ts.get('callback_rate', 'N/A')}%")
                
                if protection.get('take_profit'):
                    tp = protection['take_profit']
                    logger.info(f"  Take Profit:")
                    logger.info(f"    - Order ID: {tp.get('order_id')}")
                    logger.info(f"    - Target Price: ${tp.get('stop_price', 'N/A')}")
            else:
                logger.warning("⚠️ No protection orders were set")
            
            # Verify position
            logger.info("\n" + "=" * 40)
            logger.info("VERIFYING POSITION...")
            logger.info("=" * 40)
            
            await asyncio.sleep(2)  # Wait for position to be established
            
            position = await manager.get_position(symbol)
            if position:
                logger.info(f"\n📊 Current Position:")
                logger.info(f"  Symbol: {position.symbol}")
                logger.info(f"  Side: {'LONG' if position.position_amount > 0 else 'SHORT'}")
                logger.info(f"  Amount: {abs(position.position_amount)}")
                logger.info(f"  Entry Price: ${position.entry_price:,.2f}")
                logger.info(f"  Mark Price: ${position.mark_price:,.2f}")
                logger.info(f"  Unrealized PnL: ${position.unrealized_pnl:,.2f}")
            
            # Check open orders
            open_orders = await manager.get_open_orders(symbol)
            logger.info(f"\n📋 Open Orders: {len(open_orders)} orders")
            for order in open_orders:
                logger.info(f"  - {order.get('type')}: {order.get('orderId')} "
                          f"(Price: {order.get('stopPrice', order.get('price', 'N/A'))})")
            
        else:
            logger.error(f"\n❌ ORDER CREATION FAILED!")
            logger.error(f"Error: {result.get('error')}")
            
            if result.get('order'):
                logger.error(f"Order details: {result.get('order')}")
        
        # Cleanup
        logger.info("\n" + "=" * 40)
        logger.info("TEST COMPLETED")
        logger.info("=" * 40)
        
        await manager.close()
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()


async def test_signal_processing():
    """Test signal processing for Binance"""
    
    try:
        logger.info("\n" * 2)
        logger.info("=" * 60)
        logger.info("BINANCE SIGNAL PROCESSING TEST")
        logger.info("=" * 60)
        
        # Configuration
        config = {
            'api_key': os.getenv('BINANCE_API_KEY'),
            'api_secret': os.getenv('BINANCE_API_SECRET'),
            'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
        }
        
        # Initialize manager
        manager = BinanceEnhancedManager(config)
        await manager.initialize()
        
        # Simulate a trading signal
        test_signal = {
            'exchange': 'binance',
            'symbol': 'ETHUSDT',
            'side': 'SELL',  # Open SHORT position
            'quantity': 0.05,  # 0.05 ETH
            'leverage': 5,
            'signal_id': 'TEST_SIGNAL_001',
            'confidence': 0.85
        }
        
        logger.info(f"\nTest Signal:")
        logger.info(f"  Exchange: {test_signal['exchange']}")
        logger.info(f"  Symbol: {test_signal['symbol']}")
        logger.info(f"  Side: {test_signal['side']} (SHORT)")
        logger.info(f"  Quantity: {test_signal['quantity']}")
        logger.info(f"  Leverage: {test_signal['leverage']}x")
        
        # Process the signal (similar to what signal_processor would do)
        logger.info("\nProcessing signal...")
        
        result = await manager.create_order_with_protection(
            symbol=test_signal['symbol'],
            side='SELL',  # For SHORT position
            quantity=Decimal(str(test_signal['quantity'])),
            leverage=test_signal['leverage']
        )
        
        if result.get('success'):
            logger.info("✅ Signal processed successfully!")
            logger.info(f"Protection established: {result.get('protection')}")
        else:
            logger.error(f"❌ Signal processing failed: {result.get('error')}")
        
        await manager.close()
        
    except Exception as e:
        logger.error(f"Signal test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    logger.info("Starting Binance Futures Protection Test...")
    
    # Run tests
    asyncio.run(test_binance_order_with_protection())
    
    # Optional: Test signal processing
    # asyncio.run(test_signal_processing())