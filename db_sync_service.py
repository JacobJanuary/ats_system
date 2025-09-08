#!/usr/bin/env python3
"""
Database Synchronization Service
Automatically syncs positions between exchanges and database
"""
import asyncio
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from database.connection import DatabaseManager
from core.config import SystemConfig

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseSyncService:
    """Service for automatic database synchronization with exchanges"""
    
    def __init__(self):
        self.config = SystemConfig()
        self.db = None
        self.sync_interval = int(os.getenv('DB_SYNC_INTERVAL', '300'))  # 5 minutes default
        self.running = False
        self.stats = {
            'syncs_completed': 0,
            'last_sync': None,
            'bybit_added': 0,
            'bybit_removed': 0,
            'bybit_updated': 0,
            'binance_added': 0,
            'binance_removed': 0,
            'binance_updated': 0
        }
        
    async def start(self):
        """Start the synchronization service"""
        self.running = True
        
        # Initialize database
        self.db = DatabaseManager(self.config)
        await self.db.initialize()
        
        logger.info(f"📊 Database Sync Service started (interval: {self.sync_interval}s)")
        
        while self.running:
            try:
                start_time = datetime.now(timezone.utc)
                await self.sync_all_exchanges()
                
                self.stats['syncs_completed'] += 1
                self.stats['last_sync'] = start_time
                
                # Log statistics
                logger.info(f"📈 Sync #{self.stats['syncs_completed']} completed - "
                          f"Bybit: +{self.stats['bybit_added']} -{self.stats['bybit_removed']} "
                          f"~{self.stats['bybit_updated']}")
                
                # Reset counters for next sync
                self.stats['bybit_added'] = 0
                self.stats['bybit_removed'] = 0
                self.stats['bybit_updated'] = 0
                self.stats['binance_added'] = 0
                self.stats['binance_removed'] = 0
                self.stats['binance_updated'] = 0
                
                await asyncio.sleep(self.sync_interval)
                
            except Exception as e:
                logger.error(f"Sync error: {e}", exc_info=True)
                await asyncio.sleep(30)  # Short delay on error
    
    async def sync_all_exchanges(self):
        """Synchronize all exchanges with database"""
        logger.info(f"🔄 Starting database sync at {datetime.now(timezone.utc)}")
        
        # Sync Bybit
        await self.sync_bybit_positions()
        
        # Sync Binance (if enabled)
        if os.getenv('BINANCE_ENABLED', 'false').lower() == 'true':
            await self.sync_binance_positions()
        
        # Clean up old closed positions
        await self.cleanup_old_positions()
    
    async def sync_bybit_positions(self):
        """Synchronize Bybit positions"""
        try:
            from exchanges.bybit import BybitExchange
            
            # Initialize exchange
            config = {
                'api_key': os.getenv('BYBIT_API_KEY'),
                'api_secret': os.getenv('BYBIT_API_SECRET'),
                'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
            }
            
            exchange = BybitExchange(config)
            await exchange.initialize()
            
            # Get positions from exchange
            exchange_positions = await exchange.get_positions()
            
            # Get positions from database
            db_positions = await self.db.positions.get_open_positions()
            db_bybit_positions = [p for p in db_positions if p.exchange == 'bybit']
            
            # Create lookup sets
            exchange_symbols = {p.symbol: p for p in exchange_positions if p.quantity > 0}
            db_symbols = {p.symbol: p for p in db_bybit_positions}
            
            # Find differences
            to_add = set(exchange_symbols.keys()) - set(db_symbols.keys())
            to_remove = set(db_symbols.keys()) - set(exchange_symbols.keys())
            to_check = set(exchange_symbols.keys()) & set(db_symbols.keys())
            
            # Add new positions
            for symbol in to_add:
                pos = exchange_symbols[symbol]
                # Create Position object
                from database.models import Position, PositionStatus
                from datetime import datetime, timezone
                
                new_position = Position(
                    exchange='bybit',
                    symbol=symbol,
                    side=pos.side,
                    quantity=pos.quantity,
                    entry_price=pos.entry_price,
                    opened_at=datetime.now(timezone.utc),
                    status=PositionStatus.OPEN
                )
                
                # Create position in database
                position_id = await self.db.positions.create_position(new_position)
                logger.info(f"  ➕ Added Bybit position: {symbol}")
                self.stats['bybit_added'] += 1
            
            # Remove closed positions
            for symbol in to_remove:
                pos = db_symbols[symbol]
                from database.models import CloseReason
                from decimal import Decimal
                
                # Calculate PNL
                exit_price = pos.current_price or pos.entry_price
                pnl = Decimal(0)
                if pos.side == "LONG":
                    pnl = (exit_price - pos.entry_price) * pos.quantity
                else:
                    pnl = (pos.entry_price - exit_price) * pos.quantity
                
                await self.db.positions.close_position(
                    position_id=pos.id,
                    exit_price=exit_price,
                    realized_pnl=pnl,
                    close_reason=CloseReason.MANUAL
                )
                logger.info(f"  ➖ Removed Bybit position: {symbol}")
                self.stats['bybit_removed'] += 1
            
            # Update existing positions
            for symbol in to_check:
                exchange_pos = exchange_symbols[symbol]
                db_pos = db_symbols[symbol]
                
                # Update quantity if changed
                if abs(float(exchange_pos.quantity) - float(db_pos.quantity)) > 0.0001:
                    db_pos.quantity = exchange_pos.quantity
                    await self.db.positions.update_position(db_pos)
                    logger.debug(f"  🔄 Updated quantity for {symbol}")
                    self.stats['bybit_updated'] += 1
                
                # Update current price
                if exchange_pos.mark_price:
                    await self.db.positions.update_position_price(
                        position_id=db_pos.id,
                        price=exchange_pos.mark_price
                    )
            
            await exchange.close()
            
        except Exception as e:
            logger.error(f"Bybit sync error: {e}", exc_info=True)
    
    async def sync_binance_positions(self):
        """Synchronize Binance positions"""
        try:
            from exchanges.binance import BinanceExchange
            
            # Initialize exchange
            config = {
                'api_key': os.getenv('BINANCE_API_KEY'),
                'api_secret': os.getenv('BINANCE_API_SECRET'),
                'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
            }
            
            exchange = BinanceExchange(config)
            await exchange.initialize()
            
            # Get positions
            try:
                exchange_positions = await exchange.get_positions()
            except Exception as e:
                logger.warning(f"Binance get_positions error: {e}")
                await exchange.close()
                return
            
            # Similar sync logic as Bybit...
            # (Implementation similar to sync_bybit_positions)
            
            await exchange.close()
            
        except Exception as e:
            logger.error(f"Binance sync error: {e}", exc_info=True)
    
    async def cleanup_old_positions(self):
        """Clean up old closed positions from database"""
        try:
            # Delete positions closed more than 7 days ago
            query = """
                DELETE FROM monitoring.positions 
                WHERE status = 'CLOSED' 
                AND closed_at < NOW() - INTERVAL '7 days'
                RETURNING id
            """
            
            async with self.db.pool.acquire() as conn:
                deleted = await conn.fetch(query)
                
            if deleted:
                logger.info(f"  🗑️ Cleaned up {len(deleted)} old closed positions")
                
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    
    async def stop(self):
        """Stop the synchronization service"""
        logger.info("Stopping Database Sync Service...")
        self.running = False
        
        if self.db:
            await self.db.close()
        
        logger.info("Database Sync Service stopped")
    
    def get_stats(self):
        """Get synchronization statistics"""
        return self.stats


async def main():
    """Main function"""
    service = DatabaseSyncService()
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())