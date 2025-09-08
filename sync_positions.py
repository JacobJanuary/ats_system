#!/usr/bin/env python3
"""
ATS 2.0 - Position Synchronization
Automatically sync positions between database and exchanges
"""
import asyncio
import logging
import sys
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional
import os

sys.path.insert(0, '/Users/evgeniyyanvarskiy/PycharmProjects/ats_system')

from core.config import SystemConfig
from database.connection import DatabaseManager
from database.models import Position, PositionStatus, CloseReason
from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PositionSynchronizer:
    """Synchronize positions between database and exchanges"""
    
    def __init__(self, dry_run: bool = True):
        self.config = SystemConfig()
        self.db: Optional[DatabaseManager] = None
        self.exchanges: Dict[str, Any] = {}
        self.dry_run = dry_run
        self.changes_made = []
        
    async def initialize(self):
        """Initialize connections"""
        logger.info(f"Initializing Position Synchronizer (dry_run={self.dry_run})...")
        
        # Initialize database
        self.db = DatabaseManager(self.config)
        await self.db.initialize()
        
        # Initialize exchanges
        exchange_config = {
            'api_key': self.config.exchange.api_key,
            'api_secret': self.config.exchange.api_secret,
            'testnet': self.config.exchange.testnet
        }
        
        # Binance
        try:
            self.exchanges['binance'] = BinanceExchange(exchange_config)
            await self.exchanges['binance'].initialize()
            logger.info("Binance connected")
        except Exception as e:
            logger.error(f"Failed to connect Binance: {e}")
        
        # Bybit
        try:
            if os.getenv('BYBIT_API_KEY'):
                bybit_config = {
                    'api_key': os.getenv('BYBIT_API_KEY'),
                    'api_secret': os.getenv('BYBIT_API_SECRET'),
                    'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
                }
                self.exchanges['bybit'] = BybitExchange(bybit_config)
                await self.exchanges['bybit'].initialize()
                logger.info("Bybit connected")
        except Exception as e:
            logger.warning(f"Bybit not available: {e}")
    
    async def sync_exchange_to_db(self, exchange_name: str):
        """Sync positions from exchange to database"""
        logger.info(f"\n{'='*60}")
        logger.info(f"Syncing {exchange_name.upper()} positions to database...")
        
        if exchange_name not in self.exchanges:
            logger.error(f"Exchange {exchange_name} not initialized")
            return
        
        exchange = self.exchanges[exchange_name]
        
        # Get positions from exchange
        exchange_positions = await exchange.get_all_positions()
        logger.info(f"Found {len(exchange_positions)} positions on {exchange_name}")
        
        # Get open positions from database
        db_positions = await self.db.positions.get_open_positions()
        db_positions_by_symbol = {}
        
        for pos in db_positions:
            if pos.exchange == exchange_name:
                key = f"{pos.symbol}_{pos.side}"
                db_positions_by_symbol[key] = pos
        
        # Process each exchange position
        for exch_pos in exchange_positions:
            key = f"{exch_pos.symbol}_{exch_pos.side}"
            
            if key in db_positions_by_symbol:
                # Update existing position
                db_pos = db_positions_by_symbol[key]
                
                # Check for changes
                changes = []
                
                if abs(float(db_pos.quantity) - float(exch_pos.quantity)) > 0.0001:
                    changes.append(f"quantity: {db_pos.quantity} -> {exch_pos.quantity}")
                    db_pos.quantity = exch_pos.quantity
                
                if db_pos.current_price != exch_pos.mark_price:
                    changes.append(f"price: {db_pos.current_price} -> {exch_pos.mark_price}")
                    db_pos.current_price = exch_pos.mark_price
                
                if changes:
                    logger.info(f"Updating {exch_pos.symbol} {exch_pos.side}: {', '.join(changes)}")
                    
                    if not self.dry_run:
                        # Update in database
                        await self.db.positions.update_position_price(
                            db_pos.id,
                            exch_pos.mark_price
                        )
                        
                        # Update quantity if changed
                        if 'quantity' in str(changes):
                            query = """
                                UPDATE ats.positions 
                                SET quantity = $2, last_updated = NOW()
                                WHERE id = $1
                            """
                            await self.db.pool.execute(query, db_pos.id, exch_pos.quantity)
                    
                    self.changes_made.append({
                        'action': 'UPDATE',
                        'position': f"{exch_pos.symbol} {exch_pos.side}",
                        'changes': changes
                    })
                
                # Remove from dict to track what's left
                del db_positions_by_symbol[key]
                
            else:
                # New position on exchange not in database
                logger.warning(f"NEW position on {exchange_name}: {exch_pos.symbol} {exch_pos.side} - {exch_pos.quantity} units")
                
                if not self.dry_run:
                    # Create new position in database
                    new_position = Position(
                        exchange=exchange_name,
                        symbol=exch_pos.symbol,
                        side=exch_pos.side,
                        entry_price=exch_pos.entry_price,
                        quantity=exch_pos.quantity,
                        leverage=exch_pos.leverage or 1,
                        status=PositionStatus.OPEN,
                        current_price=exch_pos.mark_price,
                        opened_at=datetime.utcnow()
                    )
                    
                    position_id = await self.db.positions.create_position(new_position)
                    logger.info(f"Created position {position_id} in database")
                
                self.changes_made.append({
                    'action': 'CREATE',
                    'position': f"{exch_pos.symbol} {exch_pos.side}",
                    'quantity': float(exch_pos.quantity)
                })
        
        # Check for orphan database positions
        for key, db_pos in db_positions_by_symbol.items():
            logger.warning(f"ORPHAN position in DB: {db_pos.symbol} {db_pos.side} (ID: {db_pos.id})")
            
            if not self.dry_run:
                # Mark as closed
                await self.db.positions.close_position(
                    db_pos.id,
                    db_pos.current_price or db_pos.entry_price,
                    Decimal('0'),
                    CloseReason.MANUAL
                )
                logger.info(f"Closed orphan position {db_pos.id}")
            
            self.changes_made.append({
                'action': 'CLOSE',
                'position': f"{db_pos.symbol} {db_pos.side}",
                'reason': 'Not found on exchange'
            })
    
    async def verify_stop_orders(self):
        """Verify stop loss and take profit orders"""
        logger.info(f"\n{'='*60}")
        logger.info("Verifying stop orders...")
        
        # Get all open positions with stop orders
        query = """
            SELECT 
                p.*,
                o1.status as sl_status,
                o2.status as tp_status
            FROM ats.positions p
            LEFT JOIN ats.orders o1 ON o1.id::text = p.stop_loss_order_id
            LEFT JOIN ats.orders o2 ON o2.id::text = p.take_profit_order_id
            WHERE p.status = 'OPEN'
            AND (p.stop_loss_order_id IS NOT NULL OR p.take_profit_order_id IS NOT NULL)
        """
        
        rows = await self.db.pool.fetch(query)
        
        for row in rows:
            exchange_name = row['exchange']
            symbol = row['symbol']
            
            if exchange_name not in self.exchanges:
                continue
            
            exchange = self.exchanges[exchange_name]
            
            # Get open orders from exchange
            open_orders = await exchange.get_open_orders(symbol)
            order_ids = [str(o.get('orderId', o.get('order_id', ''))) for o in open_orders]
            
            # Check stop loss
            if row['stop_loss_order_id']:
                if str(row['stop_loss_order_id']) not in order_ids:
                    logger.warning(f"Position {row['id']} ({symbol}): Stop loss order {row['stop_loss_order_id']} NOT FOUND on exchange")
                    
                    if not self.dry_run:
                        # Clear invalid stop loss reference
                        query = """
                            UPDATE ats.positions 
                            SET stop_loss_order_id = NULL, last_updated = NOW()
                            WHERE id = $1
                        """
                        await self.db.pool.execute(query, row['id'])
                    
                    self.changes_made.append({
                        'action': 'CLEAR_SL',
                        'position_id': row['id'],
                        'symbol': symbol
                    })
                else:
                    logger.info(f"✅ Position {row['id']} ({symbol}): Stop loss order verified")
            
            # Check take profit
            if row['take_profit_order_id']:
                if str(row['take_profit_order_id']) not in order_ids:
                    logger.warning(f"Position {row['id']} ({symbol}): Take profit order {row['take_profit_order_id']} NOT FOUND on exchange")
                    
                    if not self.dry_run:
                        # Clear invalid take profit reference
                        query = """
                            UPDATE ats.positions 
                            SET take_profit_order_id = NULL, last_updated = NOW()
                            WHERE id = $1
                        """
                        await self.db.pool.execute(query, row['id'])
                    
                    self.changes_made.append({
                        'action': 'CLEAR_TP',
                        'position_id': row['id'],
                        'symbol': symbol
                    })
                else:
                    logger.info(f"✅ Position {row['id']} ({symbol}): Take profit order verified")
    
    async def update_prices(self):
        """Update current prices for all open positions"""
        logger.info(f"\n{'='*60}")
        logger.info("Updating current prices...")
        
        open_positions = await self.db.positions.get_open_positions()
        
        for pos in open_positions:
            if pos.exchange not in self.exchanges:
                continue
            
            exchange = self.exchanges[pos.exchange]
            
            try:
                # Get current market data
                market = await exchange.get_market_data(pos.symbol)
                
                if market and market.last_price:
                    old_price = pos.current_price
                    new_price = market.last_price
                    
                    if old_price != new_price:
                        logger.info(f"Updating {pos.symbol}: ${old_price} -> ${new_price}")
                        
                        if not self.dry_run:
                            await self.db.positions.update_position_price(
                                pos.id,
                                new_price
                            )
                        
                        self.changes_made.append({
                            'action': 'PRICE_UPDATE',
                            'symbol': pos.symbol,
                            'old_price': float(old_price) if old_price else None,
                            'new_price': float(new_price)
                        })
            
            except Exception as e:
                logger.error(f"Error updating price for {pos.symbol}: {e}")
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.db:
            await self.db.close()
        
        for exchange in self.exchanges.values():
            await exchange.close()
    
    async def run(self):
        """Run synchronization"""
        try:
            await self.initialize()
            
            # Sync each exchange
            for exchange_name in self.exchanges.keys():
                await self.sync_exchange_to_db(exchange_name)
            
            # Verify stop orders
            await self.verify_stop_orders()
            
            # Update prices
            await self.update_prices()
            
            # Summary
            logger.info(f"\n{'='*60}")
            logger.info("SYNCHRONIZATION COMPLETE")
            logger.info(f"{'='*60}")
            
            if self.changes_made:
                logger.info(f"Total changes: {len(self.changes_made)}")
                
                # Group changes by action
                by_action = {}
                for change in self.changes_made:
                    action = change['action']
                    if action not in by_action:
                        by_action[action] = []
                    by_action[action].append(change)
                
                for action, changes in by_action.items():
                    logger.info(f"  {action}: {len(changes)}")
                
                if self.dry_run:
                    logger.warning("\n⚠️ DRY RUN MODE - No changes were actually made")
                    logger.info("Run with --execute to apply changes")
            else:
                logger.info("✅ Everything is already in sync!")
            
        finally:
            await self.cleanup()


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Synchronize positions between database and exchanges')
    parser.add_argument('--execute', action='store_true', help='Actually make changes (default is dry run)')
    args = parser.parse_args()
    
    synchronizer = PositionSynchronizer(dry_run=not args.execute)
    await synchronizer.run()


if __name__ == "__main__":
    asyncio.run(main())