#!/usr/bin/env python3
"""
Binance Position Monitor
Monitors and ensures all Binance positions have protection
"""

import asyncio
import logging
import os
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

from exchanges.binance_enhanced import BinanceEnhancedManager

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('binance_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BinancePositionMonitor:
    """
    Monitors Binance positions and ensures protection
    """
    
    def __init__(self):
        config = {
            'api_key': os.getenv('BINANCE_API_KEY'),
            'api_secret': os.getenv('BINANCE_API_SECRET'),
            'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
        }
        
        self.manager = BinanceEnhancedManager(config)
        self.check_interval = int(os.getenv('PROTECTION_CHECK_INTERVAL', 30))
        self.running = False
        
        self.stats = {
            'checks': 0,
            'positions_found': 0,
            'protection_added': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
    
    async def start(self):
        """Start monitoring positions"""
        self.running = True
        
        logger.info("=" * 60)
        logger.info("🚀 BINANCE POSITION MONITOR STARTED")
        logger.info("=" * 60)
        logger.info(f"Mode: {'TESTNET' if self.manager.testnet else 'MAINNET'}")
        logger.info(f"Check interval: {self.check_interval} seconds")
        logger.info(f"Stop Loss: {self.manager.stop_loss_percent}%")
        logger.info(f"Trailing Stop: {self.manager.trailing_callback_rate}%")
        logger.info("=" * 60)
        
        try:
            # Initialize exchange connection
            await self.manager.initialize()
            logger.info("✅ Connected to Binance")
            
            while self.running:
                await self.check_and_protect_positions()
                await asyncio.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Stopping monitor...")
            self.running = False
        except Exception as e:
            logger.error(f"Monitor error: {e}")
        finally:
            await self.manager.close()
            self.print_stats()
    
    async def check_and_protect_positions(self):
        """Check all positions and add protection if needed"""
        self.stats['checks'] += 1
        
        try:
            # Get all positions
            positions = await self.manager.get_positions()
            
            if not positions:
                return
            
            active_positions = []
            for pos in positions:
                if pos.position_amount != 0:
                    active_positions.append(pos)
            
            if not active_positions:
                logger.info(f"Check #{self.stats['checks']}: No open positions")
                return
            
            self.stats['positions_found'] += len(active_positions)
            
            logger.info(f"\n📊 Check #{self.stats['checks']} at {datetime.now().strftime('%H:%M:%S')}")
            logger.info(f"  Found {len(active_positions)} open positions")
            
            # Check each position for protection
            for position in active_positions:
                symbol = position.symbol
                
                # Get open orders for this symbol
                open_orders = await self.manager.get_open_orders(symbol)
                
                # Check for protection orders
                has_stop_loss = False
                has_trailing_stop = False
                has_take_profit = False
                
                for order in open_orders:
                    order_type = order.get('type', '')
                    if order_type == 'STOP_MARKET':
                        has_stop_loss = True
                    elif order_type == 'TRAILING_STOP_MARKET':
                        has_trailing_stop = True
                    elif order_type == 'TAKE_PROFIT_MARKET':
                        has_take_profit = True
                
                # Log position status
                protection_status = []
                if has_stop_loss:
                    protection_status.append("SL✅")
                else:
                    protection_status.append("SL❌")
                    
                if has_trailing_stop:
                    protection_status.append("TS✅")
                else:
                    protection_status.append("TS❌")
                
                status_str = " | ".join(protection_status)
                logger.info(f"  {symbol}: {position.position_amount} @ ${position.entry_price} [{status_str}]")
                
                # Add missing protection
                if not has_stop_loss or not has_trailing_stop:
                    await self.add_protection(position, has_stop_loss, has_trailing_stop)
            
        except Exception as e:
            logger.error(f"Error checking positions: {e}")
            self.stats['errors'] += 1
    
    async def add_protection(self, position, has_stop_loss: bool, has_trailing_stop: bool):
        """Add missing protection to a position"""
        symbol = position.symbol
        side = "BUY" if position.position_amount > 0 else "SELL"
        position_amount = abs(position.position_amount)
        entry_price = position.entry_price
        
        logger.info(f"  🛡️ Adding protection to {symbol}...")
        
        try:
            # Add Stop Loss if missing
            if not has_stop_loss and self.manager.use_stop_loss:
                sl_result = await self.manager._set_stop_loss(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    position_amount=position_amount
                )
                
                if sl_result.get('success'):
                    logger.info(f"    ✅ Stop Loss added at ${sl_result.get('stop_price')}")
                    self.stats['protection_added'] += 1
                else:
                    logger.error(f"    ❌ Failed to add SL: {sl_result.get('error')}")
                    self.stats['errors'] += 1
            
            # Add Trailing Stop if missing
            if not has_trailing_stop:
                await asyncio.sleep(0.5)  # Small delay between orders
                
                ts_result = await self.manager._set_trailing_stop(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    position_amount=position_amount
                )
                
                if ts_result.get('success'):
                    logger.info(f"    ✅ Trailing Stop added: activation=${ts_result.get('activation_price')}")
                    self.stats['protection_added'] += 1
                else:
                    logger.error(f"    ❌ Failed to add TS: {ts_result.get('error')}")
                    self.stats['errors'] += 1
                    
        except Exception as e:
            logger.error(f"    ❌ Exception adding protection: {e}")
            self.stats['errors'] += 1
    
    def print_stats(self):
        """Print monitor statistics"""
        runtime = (datetime.now() - self.stats['start_time']).total_seconds() / 60
        
        logger.info("\n" + "=" * 60)
        logger.info("📈 MONITOR STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Runtime: {runtime:.1f} minutes")
        logger.info(f"Total checks: {self.stats['checks']}")
        logger.info(f"Positions found: {self.stats['positions_found']}")
        logger.info(f"Protection added: {self.stats['protection_added']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("=" * 60)


async def check_binance_positions():
    """One-time check of all Binance positions"""
    
    logger.info("\n" + "=" * 60)
    logger.info("BINANCE POSITION CHECK")
    logger.info("=" * 60)
    
    config = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
    }
    
    manager = BinanceEnhancedManager(config)
    
    try:
        await manager.initialize()
        
        # Get all positions
        positions = await manager.get_positions()
        
        total_positions = 0
        protected_positions = 0
        unprotected_positions = []
        
        for pos in positions:
            if pos.position_amount != 0:
                total_positions += 1
                symbol = pos.symbol
                
                # Check protection
                open_orders = await manager.get_open_orders(symbol)
                
                has_protection = False
                protection_types = []
                
                for order in open_orders:
                    order_type = order.get('type', '')
                    if order_type in ['STOP_MARKET', 'TRAILING_STOP_MARKET', 'TAKE_PROFIT_MARKET']:
                        has_protection = True
                        protection_types.append(order_type)
                
                if has_protection:
                    protected_positions += 1
                    logger.info(f"✅ {symbol}: Protected ({', '.join(protection_types)})")
                else:
                    unprotected_positions.append(symbol)
                    logger.warning(f"❌ {symbol}: NO PROTECTION!")
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total positions: {total_positions}")
        logger.info(f"Protected: {protected_positions} ✅")
        logger.info(f"Unprotected: {len(unprotected_positions)} ❌")
        
        if unprotected_positions:
            logger.warning(f"\n⚠️ Unprotected positions: {', '.join(unprotected_positions)}")
            logger.info("Run the monitor to add protection automatically")
        
    except Exception as e:
        logger.error(f"Check failed: {e}")
    
    finally:
        await manager.close()


async def main():
    """Main function"""
    
    print("\n" + "=" * 60)
    print("BINANCE POSITION PROTECTION")
    print("=" * 60)
    print("\n1. Run continuous monitor")
    print("2. One-time position check")
    print("0. Exit")
    
    choice = input("\nEnter choice: ").strip()
    
    if choice == "1":
        monitor = BinancePositionMonitor()
        await monitor.start()
    elif choice == "2":
        await check_binance_positions()
    else:
        print("Exiting...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")