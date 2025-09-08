#!/usr/bin/env python3
"""
Test script to verify automatic position protection (Stop Loss & Trailing Stop)
"""

import asyncio
import logging
from decimal import Decimal
from pathlib import Path
import sys
import os

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from exchanges.bybit import BybitExchange
from trading.models import Position
from core.config import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def check_current_positions():
    """Check current open positions and their protection status"""
    try:
        # Initialize exchange
        exchange_config = {
            'api_key': config.exchange.api_key,
            'api_secret': config.exchange.api_secret,
            'testnet': config.exchange.testnet
        }
        
        exchange = BybitExchange(exchange_config)
        await exchange.initialize()
        
        logger.info("=" * 60)
        logger.info("CHECKING CURRENT POSITIONS PROTECTION")
        logger.info("=" * 60)
        
        # Get all open positions
        positions = await exchange.get_positions()
        
        if not positions:
            logger.info("No open positions found")
            return None
        
        protected_count = 0
        unprotected_positions = []
        
        for pos in positions:
            symbol = pos.symbol
            has_protection = False
            
            # Check for stop loss orders
            open_orders = await exchange.get_open_orders(symbol)
            
            sl_orders = [o for o in open_orders if 'Stop' in o.get('type', '')]
            tp_orders = [o for o in open_orders if 'TakeProfit' in o.get('type', '')]
            
            if sl_orders or tp_orders:
                has_protection = True
                protected_count += 1
                
            status = "✅ PROTECTED" if has_protection else "❌ UNPROTECTED"
            
            logger.info(f"\n{symbol}: {status}")
            logger.info(f"  Side: {pos.side}")
            logger.info(f"  Entry: {pos.entry_price}")
            logger.info(f"  Quantity: {pos.quantity}")
            logger.info(f"  Stop Loss Orders: {len(sl_orders)}")
            logger.info(f"  Take Profit Orders: {len(tp_orders)}")
            
            if not has_protection:
                unprotected_positions.append(pos)
        
        logger.info(f"\n📊 Summary:")
        logger.info(f"  Total positions: {len(positions)}")
        logger.info(f"  Protected: {protected_count}")
        logger.info(f"  Unprotected: {len(unprotected_positions)}")
        
        return unprotected_positions
        
    except Exception as e:
        logger.error(f"Error checking positions: {e}")
        return None

async def add_protection_to_position(position: Position):
    """Add protection to an existing unprotected position"""
    try:
        # Initialize exchange
        exchange_config = {
            'api_key': config.exchange.api_key,
            'api_secret': config.exchange.api_secret,
            'testnet': config.exchange.testnet
        }
        
        exchange = BybitExchange(exchange_config)
        await exchange.initialize()
        
        logger.info(f"\nAdding protection to {position.symbol}...")
        
        # Get current config
        use_trailing = config.risk.stop_loss_type == 'trailing'
        stop_loss_percent = config.risk.stop_loss_percent
        
        if use_trailing:
            callback_rate = config.risk.trailing_callback_rate
            logger.info(f"Setting TRAILING STOP with {callback_rate}% callback")
            
            success, order_id, error = await exchange.set_stop_loss(
                position=position,
                stop_loss_percent=stop_loss_percent,
                is_trailing=True,
                callback_rate=callback_rate
            )
        else:
            logger.info(f"Setting FIXED STOP LOSS at {stop_loss_percent}%")
            
            success, order_id, error = await exchange.set_stop_loss(
                position=position,
                stop_loss_percent=stop_loss_percent,
                is_trailing=False
            )
        
        if success:
            logger.info(f"✅ Protection added successfully! Order ID: {order_id}")
        else:
            logger.error(f"❌ Failed to add protection: {error}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error adding protection: {e}")
        return False

async def test_new_position_with_protection():
    """Test opening a new position with automatic protection"""
    try:
        # Initialize exchange
        exchange_config = {
            'api_key': config.exchange.api_key,
            'api_secret': config.exchange.api_secret,
            'testnet': config.exchange.testnet,
            'risk': {
                'use_stop_loss': config.risk.stop_loss_enabled,
                'stop_loss_type': config.risk.stop_loss_type,
                'stop_loss_percent': config.risk.stop_loss_percent,
                'trailing_callback_rate': config.risk.trailing_callback_rate,
                'trailing_activation_percent': config.risk.trailing_activation_percent
            }
        }
        
        exchange = BybitExchange(exchange_config)
        await exchange.initialize()
        
        # Test symbol (use a liquid one)
        symbol = "BTCUSDT"
        
        logger.info("\n" + "=" * 60)
        logger.info("TESTING NEW POSITION WITH PROTECTION")
        logger.info("=" * 60)
        logger.info(f"Symbol: {symbol}")
        logger.info(f"Protection enabled: {config.risk.stop_loss_enabled}")
        logger.info(f"Type: {config.risk.stop_loss_type}")
        logger.info(f"Stop Loss: {config.risk.stop_loss_percent}%")
        
        if config.risk.stop_loss_type == 'trailing':
            logger.info(f"Trailing Callback: {config.risk.trailing_callback_rate}%")
            logger.info(f"Activation: {config.risk.trailing_activation_percent}%")
        
        # Get market price
        market_data = await exchange.get_market_data(symbol)
        if not market_data:
            logger.error("Could not get market data")
            return
        
        # Create a minimal test position
        position = Position(
            symbol=symbol,
            side="LONG",
            leverage=config.position.leverage,
            size_usd=10  # Minimum size for test
        )
        
        logger.info(f"\nOpening test position...")
        logger.info(f"  Side: {position.side}")
        logger.info(f"  Size: ${position.size_usd}")
        logger.info(f"  Leverage: {position.leverage}x")
        
        # Open position (this should automatically set protection)
        success, order_result, error = await exchange.open_position(
            symbol=symbol,
            side=position.side,
            position=position,
            entry_price=market_data.mark_price,
            size_usd=position.size_usd
        )
        
        if success:
            logger.info(f"✅ Position opened successfully!")
            
            # Wait a bit for orders to settle
            await asyncio.sleep(2)
            
            # Check if protection was set
            open_orders = await exchange.get_open_orders(symbol)
            sl_orders = [o for o in open_orders if 'Stop' in o.get('type', '')]
            
            if sl_orders:
                logger.info(f"✅ Protection automatically set! Found {len(sl_orders)} stop order(s)")
            else:
                logger.error("❌ No protection orders found!")
        else:
            logger.error(f"❌ Failed to open position: {error}")
        
    except Exception as e:
        logger.error(f"Error in test: {e}")

async def main():
    """Main test function"""
    logger.info("Starting Position Protection Test")
    logger.info(f"Mode: {'TESTNET' if config.exchange.testnet else 'MAINNET'}")
    logger.info(f"Exchange: {config.exchange.name}")
    
    # Step 1: Check current positions
    unprotected = await check_current_positions()
    
    if unprotected:
        logger.info("\n" + "=" * 60)
        logger.info("ADDING PROTECTION TO UNPROTECTED POSITIONS")
        logger.info("=" * 60)
        
        for pos in unprotected[:1]:  # Only process first position as test
            await add_protection_to_position(pos)
    
    # Step 2: Test new position (only on testnet)
    if config.exchange.testnet:
        response = input("\nDo you want to test opening a new position with protection? (y/n): ")
        if response.lower() == 'y':
            await test_new_position_with_protection()
    
    logger.info("\n✅ Test complete!")

if __name__ == "__main__":
    asyncio.run(main())