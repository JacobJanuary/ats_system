#!/usr/bin/env python3
"""
ATS 2.0 - Position Monitor Test
Test and validate position monitoring functionality
"""
import asyncio
import logging
import sys
from datetime import datetime
from decimal import Decimal
import os

sys.path.insert(0, '/Users/evgeniyyanvarskiy/PycharmProjects/ats_system')

from core.config import SystemConfig
from database.connection import DatabaseManager
from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PositionMonitorTest:
    """Test position monitoring functionality"""
    
    def __init__(self):
        self.config = SystemConfig()
        self.db = None
        self.exchange = None
        
    async def initialize(self):
        """Initialize test environment"""
        logger.info("Initializing Position Monitor Test...")
        
        # Initialize database
        self.db = DatabaseManager(self.config)
        await self.db.initialize()
        
        # Initialize exchange
        exchange_config = {
            'api_key': self.config.exchange.api_key,
            'api_secret': self.config.exchange.api_secret,
            'testnet': self.config.exchange.testnet
        }
        
        if self.config.exchange.name == 'binance':
            self.exchange = BinanceExchange(exchange_config)
        else:
            self.exchange = BybitExchange(exchange_config)
        
        await self.exchange.initialize()
        logger.info(f"Connected to {self.config.exchange.name} (testnet={self.config.exchange.testnet})")
    
    async def test_position_retrieval(self):
        """Test retrieving positions from database and exchange"""
        logger.info("\n" + "="*60)
        logger.info("TEST 1: Position Retrieval")
        logger.info("="*60)
        
        # Get positions from database
        logger.info("\n📊 Database Positions:")
        db_positions = await self.db.positions.get_open_positions()
        
        if db_positions:
            for pos in db_positions:
                logger.info(
                    f"  - {pos.symbol} {pos.side}: "
                    f"Qty={pos.quantity}, Entry=${pos.entry_price}, "
                    f"Current=${pos.current_price}, ID={pos.id}"
                )
        else:
            logger.info("  No open positions in database")
        
        # Get positions from exchange
        logger.info("\n🔄 Exchange Positions:")
        exchange_positions = await self.exchange.get_all_positions()
        
        if exchange_positions:
            for pos in exchange_positions:
                logger.info(
                    f"  - {pos.symbol} {pos.side}: "
                    f"Qty={pos.quantity}, Entry=${pos.entry_price}, "
                    f"Mark=${pos.mark_price}"
                )
        else:
            logger.info("  No open positions on exchange")
        
        # Compare counts
        db_count = len(db_positions)
        exchange_count = len(exchange_positions)
        
        if db_count == exchange_count:
            logger.info(f"\n✅ Position counts match: {db_count}")
        else:
            logger.warning(f"\n⚠️ Position count mismatch: DB={db_count}, Exchange={exchange_count}")
        
        return db_count == exchange_count
    
    async def test_price_updates(self):
        """Test price update functionality"""
        logger.info("\n" + "="*60)
        logger.info("TEST 2: Price Updates")
        logger.info("="*60)
        
        positions = await self.db.positions.get_open_positions()
        
        if not positions:
            logger.info("No positions to test price updates")
            return True
        
        test_position = positions[0]
        logger.info(f"\nTesting with position: {test_position.symbol} {test_position.side}")
        
        # Get current market data
        market = await self.exchange.get_market_data(test_position.symbol)
        
        if market:
            old_price = test_position.current_price
            new_price = market.last_price
            
            logger.info(f"  Old price: ${old_price}")
            logger.info(f"  Market price: ${new_price}")
            
            # Update price in database
            await self.db.positions.update_position_price(
                test_position.id,
                new_price
            )
            
            # Verify update
            updated_position = await self.db.positions.get_position(test_position.id)
            
            if updated_position and updated_position.current_price == new_price:
                logger.info(f"✅ Price updated successfully to ${new_price}")
                return True
            else:
                logger.error(f"❌ Price update failed")
                return False
        else:
            logger.error(f"❌ Could not get market data for {test_position.symbol}")
            return False
    
    async def test_pnl_calculation(self):
        """Test PNL calculation"""
        logger.info("\n" + "="*60)
        logger.info("TEST 3: PNL Calculation")
        logger.info("="*60)
        
        positions = await self.db.positions.get_open_positions()
        
        if not positions:
            logger.info("No positions to test PNL calculation")
            return True
        
        for position in positions:
            if position.current_price and position.entry_price:
                # Calculate expected PNL
                if position.side == "LONG":
                    expected_pnl_percent = ((position.current_price - position.entry_price) / position.entry_price) * 100
                else:
                    expected_pnl_percent = ((position.entry_price - position.current_price) / position.entry_price) * 100
                
                expected_pnl = (expected_pnl_percent / 100) * position.quantity * position.entry_price
                
                # Get actual PNL from position
                actual_pnl = position.unrealized_pnl
                actual_pnl_percent = position.unrealized_pnl_percent
                
                logger.info(f"\n{position.symbol} {position.side}:")
                logger.info(f"  Entry: ${position.entry_price}, Current: ${position.current_price}")
                logger.info(f"  Expected PNL: {expected_pnl_percent:.2f}% (${expected_pnl:.2f})")
                logger.info(f"  Actual PNL: {actual_pnl_percent:.2f}% (${actual_pnl:.2f})")
                
                # Allow small rounding differences
                if abs(float(expected_pnl_percent) - float(actual_pnl_percent)) < 0.01:
                    logger.info("  ✅ PNL calculation correct")
                else:
                    logger.warning("  ⚠️ PNL calculation mismatch")
        
        return True
    
    async def test_stop_orders(self):
        """Test stop loss and take profit orders"""
        logger.info("\n" + "="*60)
        logger.info("TEST 4: Stop Orders")
        logger.info("="*60)
        
        # Get positions with stop orders
        query = """
            SELECT * FROM ats.positions 
            WHERE status = 'OPEN' 
            AND (stop_loss_order_id IS NOT NULL OR take_profit_order_id IS NOT NULL)
        """
        
        rows = await self.db.pool.fetch(query)
        
        if not rows:
            logger.info("No positions with stop orders to test")
            return True
        
        logger.info(f"Found {len(rows)} positions with stop orders")
        
        for row in rows:
            symbol = row['symbol']
            
            # Get open orders from exchange
            open_orders = await self.exchange.get_open_orders(symbol)
            order_ids = [str(o.get('orderId', o.get('order_id', ''))) for o in open_orders]
            
            logger.info(f"\n{symbol}:")
            
            # Check stop loss
            if row['stop_loss_order_id']:
                if str(row['stop_loss_order_id']) in order_ids:
                    logger.info(f"  ✅ Stop loss order {row['stop_loss_order_id']} found")
                else:
                    logger.warning(f"  ⚠️ Stop loss order {row['stop_loss_order_id']} NOT FOUND")
            
            # Check take profit
            if row['take_profit_order_id']:
                if str(row['take_profit_order_id']) in order_ids:
                    logger.info(f"  ✅ Take profit order {row['take_profit_order_id']} found")
                else:
                    logger.warning(f"  ⚠️ Take profit order {row['take_profit_order_id']} NOT FOUND")
        
        return True
    
    async def test_real_time_monitoring(self):
        """Test real-time position monitoring"""
        logger.info("\n" + "="*60)
        logger.info("TEST 5: Real-time Monitoring (30 seconds)")
        logger.info("="*60)
        
        positions = await self.db.positions.get_open_positions()
        
        if not positions:
            logger.info("No positions to monitor")
            return True
        
        logger.info(f"Monitoring {len(positions)} positions for 30 seconds...")
        
        start_time = datetime.utcnow()
        last_prices = {}
        
        while (datetime.utcnow() - start_time).total_seconds() < 30:
            for position in positions:
                try:
                    # Get current market data
                    market = await self.exchange.get_market_data(position.symbol)
                    
                    if market:
                        current_price = market.last_price
                        
                        # Check for price change
                        if position.symbol not in last_prices:
                            last_prices[position.symbol] = current_price
                            logger.info(f"  {position.symbol}: Initial price ${current_price:.2f}")
                        elif current_price != last_prices[position.symbol]:
                            change = current_price - last_prices[position.symbol]
                            change_pct = (change / last_prices[position.symbol]) * 100
                            
                            arrow = "↑" if change > 0 else "↓"
                            logger.info(
                                f"  {position.symbol}: ${last_prices[position.symbol]:.2f} → "
                                f"${current_price:.2f} {arrow} ({change_pct:+.3f}%)"
                            )
                            
                            last_prices[position.symbol] = current_price
                
                except Exception as e:
                    logger.error(f"Error monitoring {position.symbol}: {e}")
            
            await asyncio.sleep(5)  # Check every 5 seconds
        
        logger.info("Real-time monitoring test completed")
        return True
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.db:
            await self.db.close()
        if self.exchange:
            await self.exchange.close()
    
    async def run(self):
        """Run all tests"""
        try:
            await self.initialize()
            
            tests = [
                ("Position Retrieval", self.test_position_retrieval),
                ("Price Updates", self.test_price_updates),
                ("PNL Calculation", self.test_pnl_calculation),
                ("Stop Orders", self.test_stop_orders),
                ("Real-time Monitoring", self.test_real_time_monitoring)
            ]
            
            results = {}
            
            for test_name, test_func in tests:
                try:
                    result = await test_func()
                    results[test_name] = "PASSED" if result else "FAILED"
                except Exception as e:
                    logger.error(f"Test {test_name} crashed: {e}")
                    results[test_name] = "ERROR"
            
            # Summary
            logger.info("\n" + "="*60)
            logger.info("TEST SUMMARY")
            logger.info("="*60)
            
            for test_name, result in results.items():
                emoji = "✅" if result == "PASSED" else "❌"
                logger.info(f"{emoji} {test_name}: {result}")
            
            passed = sum(1 for r in results.values() if r == "PASSED")
            total = len(results)
            
            logger.info(f"\nTotal: {passed}/{total} tests passed")
            
            if passed == total:
                logger.info("🎉 All tests passed! Position Monitor is working correctly.")
            else:
                logger.warning("⚠️ Some tests failed. Please review the issues above.")
            
        finally:
            await self.cleanup()


async def main():
    """Main entry point"""
    test = PositionMonitorTest()
    await test.run()


if __name__ == "__main__":
    asyncio.run(main())