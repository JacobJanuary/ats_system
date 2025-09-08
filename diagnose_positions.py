#!/usr/bin/env python3
"""
ATS 2.0 - Position Diagnostics
Comprehensive position monitoring and reconciliation tool
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any
import json
from tabulate import tabulate

# Setup path
sys.path.insert(0, '/Users/evgeniyyanvarskiy/PycharmProjects/ats_system')

from core.config import SystemConfig
from database.connection import DatabaseManager
from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PositionDiagnostics:
    """Diagnose and reconcile position discrepancies"""
    
    def __init__(self):
        self.config = SystemConfig()
        self.db: Optional[DatabaseManager] = None
        self.exchanges: Dict[str, Any] = {}
        self.discrepancies = []
        
    async def initialize(self):
        """Initialize connections"""
        logger.info("Initializing Position Diagnostics...")
        
        # Initialize database
        self.db = DatabaseManager(self.config)
        await self.db.initialize()
        
        # Initialize exchanges
        if self.config.exchange.name == 'binance' or True:  # Check both
            exchange_config = {
                'api_key': self.config.exchange.api_key,
                'api_secret': self.config.exchange.api_secret,
                'testnet': self.config.exchange.testnet
            }
            
            # Initialize Binance
            try:
                self.exchanges['binance'] = BinanceExchange(exchange_config)
                await self.exchanges['binance'].initialize()
                logger.info(f"Binance connected (testnet={exchange_config['testnet']})")
            except Exception as e:
                logger.error(f"Failed to connect Binance: {e}")
            
            # Initialize Bybit if credentials available
            try:
                import os
                if os.getenv('BYBIT_API_KEY'):
                    bybit_config = {
                        'api_key': os.getenv('BYBIT_API_KEY'),
                        'api_secret': os.getenv('BYBIT_API_SECRET'),
                        'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
                    }
                    self.exchanges['bybit'] = BybitExchange(bybit_config)
                    await self.exchanges['bybit'].initialize()
                    logger.info(f"Bybit connected (testnet={bybit_config['testnet']})")
            except Exception as e:
                logger.warning(f"Bybit not available: {e}")
    
    async def get_database_positions(self) -> List[Dict]:
        """Get all positions from database"""
        query = """
            SELECT 
                p.id,
                p.exchange,
                p.symbol,
                p.side,
                p.entry_price,
                p.quantity,
                p.status,
                p.current_price,
                p.stop_loss_price,
                p.stop_loss_order_id,
                p.take_profit_price,
                p.take_profit_order_id,
                p.realized_pnl,
                p.opened_at,
                p.closed_at,
                p.last_updated
            FROM ats.positions p
            ORDER BY p.opened_at DESC
        """
        
        rows = await self.db.pool.fetch(query)
        positions = []
        
        for row in rows:
            positions.append({
                'id': row['id'],
                'exchange': row['exchange'],
                'symbol': row['symbol'],
                'side': row['side'],
                'entry_price': float(row['entry_price']) if row['entry_price'] else 0,
                'quantity': float(row['quantity']) if row['quantity'] else 0,
                'status': row['status'],
                'current_price': float(row['current_price']) if row['current_price'] else None,
                'stop_loss': float(row['stop_loss_price']) if row['stop_loss_price'] else None,
                'stop_loss_order': row['stop_loss_order_id'],
                'take_profit': float(row['take_profit_price']) if row['take_profit_price'] else None,
                'realized_pnl': float(row['realized_pnl']) if row['realized_pnl'] else None,
                'opened_at': row['opened_at'],
                'closed_at': row['closed_at'],
                'last_updated': row['last_updated']
            })
        
        return positions
    
    async def get_exchange_positions(self, exchange_name: str) -> List[Dict]:
        """Get all positions from exchange"""
        if exchange_name not in self.exchanges:
            return []
        
        exchange = self.exchanges[exchange_name]
        positions = []
        
        try:
            # Get all positions from exchange
            exchange_positions = await exchange.get_all_positions()
            
            for pos in exchange_positions:
                positions.append({
                    'exchange': exchange_name,
                    'symbol': pos.symbol,
                    'side': pos.side,
                    'quantity': float(pos.quantity),
                    'entry_price': float(pos.entry_price),
                    'mark_price': float(pos.mark_price) if pos.mark_price else None,
                    'unrealized_pnl': float(pos.unrealized_pnl) if pos.unrealized_pnl else None,
                    'leverage': pos.leverage,
                    'margin_type': pos.margin_type
                })
        except Exception as e:
            logger.error(f"Error getting positions from {exchange_name}: {e}")
        
        return positions
    
    async def get_open_orders(self, exchange_name: str, symbol: Optional[str] = None) -> List[Dict]:
        """Get open orders from exchange"""
        if exchange_name not in self.exchanges:
            return []
        
        exchange = self.exchanges[exchange_name]
        orders = []
        
        try:
            open_orders = await exchange.get_open_orders(symbol)
            
            for order in open_orders:
                orders.append({
                    'exchange': exchange_name,
                    'symbol': order.get('symbol'),
                    'order_id': order.get('orderId', order.get('order_id')),
                    'side': order.get('side'),
                    'type': order.get('type', order.get('orderType')),
                    'price': float(order.get('price', 0)),
                    'quantity': float(order.get('origQty', order.get('qty', 0))),
                    'status': order.get('status', order.get('orderStatus')),
                    'time': order.get('time', order.get('createdTime'))
                })
        except Exception as e:
            logger.error(f"Error getting orders from {exchange_name}: {e}")
        
        return orders
    
    async def compare_positions(self):
        """Compare database positions with exchange positions"""
        logger.info("\n" + "="*80)
        logger.info("POSITION COMPARISON")
        logger.info("="*80)
        
        # Get database positions
        db_positions = await self.get_database_positions()
        db_open = [p for p in db_positions if p['status'] == 'OPEN']
        db_closed = [p for p in db_positions if p['status'] in ['CLOSED', 'CLOSING']]
        
        logger.info(f"\n📊 DATABASE POSITIONS:")
        logger.info(f"  - Open: {len(db_open)}")
        logger.info(f"  - Closed: {len(db_closed)}")
        logger.info(f"  - Total: {len(db_positions)}")
        
        # Display database positions
        if db_open:
            logger.info("\n📋 Open Positions in Database:")
            table_data = []
            for pos in db_open:
                table_data.append([
                    pos['id'],
                    pos['exchange'],
                    pos['symbol'],
                    pos['side'],
                    f"{pos['quantity']:.4f}",
                    f"${pos['entry_price']:.2f}",
                    f"${pos['current_price']:.2f}" if pos['current_price'] else "N/A",
                    f"${pos['realized_pnl']:.2f}" if pos['realized_pnl'] else "N/A",
                    "✅" if pos['stop_loss_order'] else "❌",
                    pos['last_updated'].strftime('%Y-%m-%d %H:%M:%S') if pos['last_updated'] else "N/A"
                ])
            
            headers = ["ID", "Exchange", "Symbol", "Side", "Qty", "Entry", "Current", "PNL", "SL", "Updated"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        # Get exchange positions for each configured exchange
        for exchange_name in self.exchanges.keys():
            logger.info(f"\n🔄 {exchange_name.upper()} EXCHANGE POSITIONS:")
            
            exchange_positions = await self.get_exchange_positions(exchange_name)
            
            if exchange_positions:
                logger.info(f"  Found {len(exchange_positions)} positions on {exchange_name}")
                
                table_data = []
                for pos in exchange_positions:
                    table_data.append([
                        pos['symbol'],
                        pos['side'],
                        f"{pos['quantity']:.4f}",
                        f"${pos['entry_price']:.2f}",
                        f"${pos['mark_price']:.2f}" if pos['mark_price'] else "N/A",
                        f"${pos['unrealized_pnl']:.2f}" if pos['unrealized_pnl'] else "N/A",
                        f"{pos['leverage']}x"
                    ])
                
                headers = ["Symbol", "Side", "Qty", "Entry", "Mark", "PNL", "Leverage"]
                print(tabulate(table_data, headers=headers, tablefmt="grid"))
            else:
                logger.info(f"  No positions found on {exchange_name}")
            
            # Check for discrepancies
            await self.find_discrepancies(db_open, exchange_positions, exchange_name)
    
    async def find_discrepancies(self, db_positions: List[Dict], exchange_positions: List[Dict], exchange_name: str):
        """Find discrepancies between database and exchange"""
        logger.info(f"\n🔍 DISCREPANCY ANALYSIS FOR {exchange_name.upper()}:")
        
        # Create lookup dictionaries
        db_by_symbol = {}
        for pos in db_positions:
            if pos['exchange'] == exchange_name:
                key = f"{pos['symbol']}_{pos['side']}"
                db_by_symbol[key] = pos
        
        exchange_by_symbol = {}
        for pos in exchange_positions:
            key = f"{pos['symbol']}_{pos['side']}"
            exchange_by_symbol[key] = pos
        
        discrepancies = []
        
        # Check positions in DB but not on exchange
        for key, db_pos in db_by_symbol.items():
            if key not in exchange_by_symbol:
                discrepancies.append({
                    'type': 'ORPHAN_DB',
                    'symbol': db_pos['symbol'],
                    'side': db_pos['side'],
                    'db_id': db_pos['id'],
                    'description': f"Position exists in DB but not on {exchange_name}"
                })
        
        # Check positions on exchange but not in DB
        for key, exch_pos in exchange_by_symbol.items():
            if key not in db_by_symbol:
                discrepancies.append({
                    'type': 'ORPHAN_EXCHANGE',
                    'symbol': exch_pos['symbol'],
                    'side': exch_pos['side'],
                    'quantity': exch_pos['quantity'],
                    'description': f"Position exists on {exchange_name} but not in DB"
                })
        
        # Check mismatched quantities
        for key in set(db_by_symbol.keys()) & set(exchange_by_symbol.keys()):
            db_pos = db_by_symbol[key]
            exch_pos = exchange_by_symbol[key]
            
            db_qty = db_pos['quantity']
            exch_qty = exch_pos['quantity']
            
            if abs(db_qty - exch_qty) > 0.0001:  # Allow small rounding differences
                discrepancies.append({
                    'type': 'QUANTITY_MISMATCH',
                    'symbol': db_pos['symbol'],
                    'side': db_pos['side'],
                    'db_quantity': db_qty,
                    'exchange_quantity': exch_qty,
                    'difference': exch_qty - db_qty,
                    'description': f"Quantity mismatch: DB={db_qty:.4f}, Exchange={exch_qty:.4f}"
                })
        
        if discrepancies:
            logger.warning(f"  ⚠️ Found {len(discrepancies)} discrepancies:")
            for disc in discrepancies:
                logger.warning(f"    - {disc['type']}: {disc['description']}")
            self.discrepancies.extend(discrepancies)
        else:
            logger.info(f"  ✅ No discrepancies found")
    
    async def check_stop_orders(self):
        """Check stop loss and take profit orders"""
        logger.info("\n" + "="*80)
        logger.info("STOP ORDER ANALYSIS")
        logger.info("="*80)
        
        # Get open positions with stop orders
        query = """
            SELECT 
                p.id,
                p.exchange,
                p.symbol,
                p.side,
                p.quantity,
                p.stop_loss_order_id,
                p.stop_loss_price,
                p.stop_loss_type,
                p.take_profit_order_id,
                p.take_profit_price
            FROM ats.positions p
            WHERE p.status = 'OPEN'
            AND (p.stop_loss_order_id IS NOT NULL OR p.take_profit_order_id IS NOT NULL)
        """
        
        rows = await self.db.pool.fetch(query)
        
        logger.info(f"Found {len(rows)} positions with stop orders")
        
        for row in rows:
            exchange_name = row['exchange']
            symbol = row['symbol']
            
            if exchange_name not in self.exchanges:
                continue
            
            # Get open orders for this symbol
            orders = await self.get_open_orders(exchange_name, symbol)
            order_ids = [o['order_id'] for o in orders]
            
            issues = []
            
            # Check stop loss order
            if row['stop_loss_order_id'] and row['stop_loss_order_id'] not in order_ids:
                issues.append(f"Stop loss order {row['stop_loss_order_id']} not found on exchange")
            
            # Check take profit order
            if row['take_profit_order_id'] and row['take_profit_order_id'] not in order_ids:
                issues.append(f"Take profit order {row['take_profit_order_id']} not found on exchange")
            
            if issues:
                logger.warning(f"\n⚠️ Issues with position {row['id']} ({symbol}):")
                for issue in issues:
                    logger.warning(f"  - {issue}")
            else:
                logger.info(f"✅ Position {row['id']} ({symbol}) - stop orders OK")
    
    async def suggest_fixes(self):
        """Suggest fixes for found discrepancies"""
        if not self.discrepancies:
            logger.info("\n✅ No discrepancies found - system is in sync!")
            return
        
        logger.info("\n" + "="*80)
        logger.info("SUGGESTED FIXES")
        logger.info("="*80)
        
        fixes = {
            'ORPHAN_DB': [],
            'ORPHAN_EXCHANGE': [],
            'QUANTITY_MISMATCH': []
        }
        
        for disc in self.discrepancies:
            fixes[disc['type']].append(disc)
        
        # Suggest fixes for orphan DB positions
        if fixes['ORPHAN_DB']:
            logger.info("\n📝 Orphan Database Positions (exist in DB but not on exchange):")
            for disc in fixes['ORPHAN_DB']:
                logger.info(f"  - Position ID {disc['db_id']}: {disc['symbol']} {disc['side']}")
                logger.info(f"    FIX: Mark as CLOSED in database or investigate why exchange closed it")
        
        # Suggest fixes for orphan exchange positions
        if fixes['ORPHAN_EXCHANGE']:
            logger.info("\n📝 Orphan Exchange Positions (exist on exchange but not in DB):")
            for disc in fixes['ORPHAN_EXCHANGE']:
                logger.info(f"  - {disc['symbol']} {disc['side']}: {disc['quantity']} units")
                logger.info(f"    FIX: Create position record in database or close on exchange")
        
        # Suggest fixes for quantity mismatches
        if fixes['QUANTITY_MISMATCH']:
            logger.info("\n📝 Quantity Mismatches:")
            for disc in fixes['QUANTITY_MISMATCH']:
                logger.info(f"  - {disc['symbol']} {disc['side']}:")
                logger.info(f"    DB: {disc['db_quantity']:.4f}, Exchange: {disc['exchange_quantity']:.4f}")
                logger.info(f"    FIX: Update database quantity to match exchange")
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.db:
            await self.db.close()
        
        for exchange in self.exchanges.values():
            await exchange.close()
    
    async def run(self):
        """Run full diagnostics"""
        try:
            await self.initialize()
            
            # Run diagnostics
            await self.compare_positions()
            await self.check_stop_orders()
            await self.suggest_fixes()
            
            # Summary
            logger.info("\n" + "="*80)
            logger.info("DIAGNOSTICS COMPLETE")
            logger.info("="*80)
            
            if self.discrepancies:
                logger.warning(f"⚠️ Total issues found: {len(self.discrepancies)}")
            else:
                logger.info("✅ System is in sync!")
            
        finally:
            await self.cleanup()


async def main():
    """Main entry point"""
    diagnostics = PositionDiagnostics()
    await diagnostics.run()


if __name__ == "__main__":
    asyncio.run(main())