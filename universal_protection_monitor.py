#!/usr/bin/env python3
"""
Universal Protection Monitor for ATS 2.0
Monitors all positions across all exchanges and ensures protection
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from dotenv import load_dotenv
import aiohttp
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('universal_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class PositionInfo:
    """Position information from exchange"""
    exchange: str
    symbol: str
    side: str
    quantity: float
    entry_price: float
    current_price: float
    pnl: float
    pnl_percent: float
    has_stop_loss: bool
    has_trailing_stop: bool
    stop_loss_price: Optional[float] = None
    trailing_stop_rate: Optional[float] = None


class UniversalProtectionMonitor:
    """Monitors and protects all positions across exchanges"""
    
    def __init__(self):
        # Configuration
        self.mode = os.getenv('MODE', 'testnet').lower()
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', '6.5'))
        self.trailing_stop_percent = float(os.getenv('TRAILING_STOP_PERCENT', '0.5'))
        self.auto_protect = os.getenv('AUTO_PROTECT', 'false').lower() == 'true'
        
        # Binance configuration
        self.binance_api_key = os.getenv('BINANCE_API_KEY')
        self.binance_api_secret = os.getenv('BINANCE_API_SECRET')
        self.binance_testnet = os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
        
        # Bybit configuration
        self.bybit_api_key = os.getenv('BYBIT_API_KEY')
        self.bybit_api_secret = os.getenv('BYBIT_API_SECRET')
        self.bybit_testnet = os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
        
        # Statistics
        self.check_count = 0
        self.positions_tracked: Dict[str, PositionInfo] = {}
        self.unprotected_alerts: Set[str] = set()
        
    async def initialize(self):
        """Initialize the monitor"""
        logger.info("=" * 60)
        logger.info("🚀 Universal Protection Monitor Starting")
        logger.info("=" * 60)
        logger.info(f"Mode: {self.mode.upper()}")
        logger.info(f"Check interval: {self.check_interval} seconds")
        logger.info(f"Stop Loss: {self.stop_loss_percent}%")
        logger.info(f"Trailing Stop: {self.trailing_stop_percent}%")
        logger.info(f"Auto Protection: {'ENABLED' if self.auto_protect else 'DISABLED'}")
        logger.info("=" * 60)
        
        # Initial check
        await self.check_all_positions()
        
    async def get_binance_positions(self) -> List[PositionInfo]:
        """Get positions from Binance"""
        positions = []
        
        if not self.binance_api_key:
            return positions
            
        try:
            # Import Binance exchange
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from exchanges.binance import BinanceExchange
            
            # Initialize exchange
            config = {
                'api_key': self.binance_api_key,
                'api_secret': self.binance_api_secret,
                'testnet': self.binance_testnet
            }
            
            exchange = BinanceExchange(config)
            await exchange.initialize()
            
            # Get positions
            exchange_positions = await exchange.get_positions()
            
            for pos in exchange_positions:
                if pos.quantity > 0:
                    # Check for protection orders
                    has_sl = False
                    has_ts = False
                    sl_price = None
                    ts_rate = None
                    
                    # Get open orders for this symbol
                    orders = await exchange.get_open_orders(pos.symbol)
                    for order in orders:
                        if order.get('type') == 'STOP_MARKET':
                            has_sl = True
                            sl_price = float(order.get('stopPrice', 0))
                        elif order.get('type') == 'TRAILING_STOP_MARKET':
                            has_ts = True
                            ts_rate = float(order.get('callbackRate', 0))
                    
                    positions.append(PositionInfo(
                        exchange='binance',
                        symbol=pos.symbol,
                        side=pos.side,
                        quantity=float(pos.quantity),
                        entry_price=float(pos.entry_price),
                        current_price=float(pos.mark_price),
                        pnl=float(pos.unrealized_pnl),
                        pnl_percent=float(pos.unrealized_pnl) / (float(pos.quantity) * float(pos.entry_price)) * 100,
                        has_stop_loss=has_sl,
                        has_trailing_stop=has_ts,
                        stop_loss_price=sl_price,
                        trailing_stop_rate=ts_rate
                    ))
            
            await exchange.close()
            
        except Exception as e:
            logger.error(f"Error getting Binance positions: {e}")
            
        return positions
    
    async def get_bybit_positions(self) -> List[PositionInfo]:
        """Get positions from Bybit"""
        positions = []
        
        if not self.bybit_api_key:
            return positions
            
        try:
            # Import Bybit exchange
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from exchanges.bybit import BybitExchange
            
            # Initialize exchange
            config = {
                'api_key': self.bybit_api_key,
                'api_secret': self.bybit_api_secret,
                'testnet': self.bybit_testnet
            }
            
            exchange = BybitExchange(config)
            await exchange.initialize()
            
            # Get positions
            exchange_positions = await exchange.get_positions()
            
            for pos in exchange_positions:
                if pos.quantity > 0:
                    # Check for protection orders
                    has_sl = False
                    has_ts = False
                    sl_price = None
                    ts_rate = None
                    
                    # Get open orders for this symbol
                    orders = await exchange.get_open_orders(pos.symbol)
                    for order in orders:
                        order_type = order.get('type', '').upper()
                        if 'STOP' in order_type and not 'TRAILING' in order_type:
                            has_sl = True
                            sl_price = float(order.get('price', 0))
                        elif 'TRAILING' in order_type:
                            has_ts = True
                            # Bybit uses basis points
                            ts_rate = float(order.get('callbackRate', 0)) / 100
                    
                    positions.append(PositionInfo(
                        exchange='bybit',
                        symbol=pos.symbol,
                        side=pos.side,
                        quantity=float(pos.quantity),
                        entry_price=float(pos.entry_price),
                        current_price=float(pos.mark_price),
                        pnl=float(pos.unrealized_pnl),
                        pnl_percent=float(pos.unrealized_pnl) / (float(pos.quantity) * float(pos.entry_price)) * 100,
                        has_stop_loss=has_sl,
                        has_trailing_stop=has_ts,
                        stop_loss_price=sl_price,
                        trailing_stop_rate=ts_rate
                    ))
            
            await exchange.close()
            
        except Exception as e:
            logger.error(f"Error getting Bybit positions: {e}")
            
        return positions
    
    async def protect_position(self, position: PositionInfo) -> bool:
        """Add protection to an unprotected position"""
        if not self.auto_protect:
            return False
            
        try:
            logger.info(f"🛡️ Adding protection to {position.exchange} {position.symbol}")
            
            if position.exchange == 'binance':
                from exchanges.binance import BinanceExchange
                from database.models import Position
                
                config = {
                    'api_key': self.binance_api_key,
                    'api_secret': self.binance_api_secret,
                    'testnet': self.binance_testnet
                }
                
                exchange = BinanceExchange(config)
                await exchange.initialize()
                
                # Create position object
                pos = Position(
                    exchange='binance',
                    symbol=position.symbol,
                    side=position.side,
                    quantity=Decimal(str(position.quantity)),
                    entry_price=Decimal(str(position.entry_price))
                )
                
                # Set stop loss with trailing
                success, order_id, error = await exchange.set_stop_loss(
                    pos,
                    self.stop_loss_percent,
                    is_trailing=True,
                    callback_rate=self.trailing_stop_percent
                )
                
                await exchange.close()
                
                if success:
                    logger.info(f"✅ Protection added: {order_id}")
                    return True
                else:
                    logger.error(f"❌ Failed to add protection: {error}")
                    return False
                    
            elif position.exchange == 'bybit':
                from exchanges.bybit import BybitExchange
                from database.models import Position
                
                config = {
                    'api_key': self.bybit_api_key,
                    'api_secret': self.bybit_api_secret,
                    'testnet': self.bybit_testnet
                }
                
                exchange = BybitExchange(config)
                await exchange.initialize()
                
                # Create position object
                pos = Position(
                    exchange='bybit',
                    symbol=position.symbol,
                    side=position.side,
                    quantity=Decimal(str(position.quantity)),
                    entry_price=Decimal(str(position.entry_price))
                )
                
                # Set stop loss with trailing
                success, order_id, error = await exchange.set_stop_loss(
                    pos,
                    self.stop_loss_percent,
                    is_trailing=True,
                    callback_rate=self.trailing_stop_percent
                )
                
                await exchange.close()
                
                if success:
                    logger.info(f"✅ Protection added: {order_id}")
                    return True
                else:
                    logger.error(f"❌ Failed to add protection: {error}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error protecting position: {e}")
            return False
    
    async def check_all_positions(self):
        """Check all positions across all exchanges"""
        self.check_count += 1
        
        # Get positions from all exchanges
        all_positions = []
        
        # Get Binance positions
        binance_positions = await self.get_binance_positions()
        all_positions.extend(binance_positions)
        
        # Get Bybit positions
        bybit_positions = await self.get_bybit_positions()
        all_positions.extend(bybit_positions)
        
        # Statistics
        total_positions = len(all_positions)
        unprotected = []
        missing_trailing = []
        
        # Check each position
        for pos in all_positions:
            pos_key = f"{pos.exchange}_{pos.symbol}_{pos.side}"
            
            # Track position changes
            if pos_key in self.positions_tracked:
                old_pos = self.positions_tracked[pos_key]
                
                # Check for significant PNL changes
                if abs(pos.pnl_percent - old_pos.pnl_percent) > 1:
                    pnl_emoji = "🟢" if pos.pnl_percent > 0 else "🔴"
                    logger.info(
                        f"{pnl_emoji} {pos.exchange.upper()} {pos.symbol} {pos.side}: "
                        f"PNL {pos.pnl_percent:+.2f}% (${pos.pnl:+.2f})"
                    )
                
                # Check for price changes
                price_change = ((pos.current_price - old_pos.current_price) / old_pos.current_price) * 100
                if abs(price_change) > 0.5:
                    logger.info(
                        f"💰 {pos.exchange.upper()} {pos.symbol}: "
                        f"${old_pos.current_price:.2f} → ${pos.current_price:.2f} ({price_change:+.2f}%)"
                    )
            
            # Update tracking
            self.positions_tracked[pos_key] = pos
            
            # Check protection status
            if not pos.has_stop_loss and not pos.has_trailing_stop:
                unprotected.append(pos)
                
                # Alert for unprotected positions
                if pos_key not in self.unprotected_alerts:
                    logger.warning(
                        f"⚠️ UNPROTECTED: {pos.exchange.upper()} {pos.symbol} {pos.side} "
                        f"(${pos.quantity * pos.current_price:.2f})"
                    )
                    self.unprotected_alerts.add(pos_key)
                    
                    # Auto-protect if enabled
                    if self.auto_protect:
                        await self.protect_position(pos)
                        
            elif pos.has_stop_loss and not pos.has_trailing_stop:
                missing_trailing.append(pos)
                logger.debug(
                    f"📌 Fixed SL only: {pos.exchange.upper()} {pos.symbol} @ ${pos.stop_loss_price:.2f}"
                )
            
            # Remove from alerts if now protected
            if pos_key in self.unprotected_alerts and (pos.has_stop_loss or pos.has_trailing_stop):
                logger.info(f"✅ Now protected: {pos.exchange.upper()} {pos.symbol}")
                self.unprotected_alerts.remove(pos_key)
        
        # Summary
        logger.info(
            f"📊 Check #{self.check_count}: "
            f"{total_positions} positions, "
            f"{len(unprotected)} unprotected, "
            f"{len(missing_trailing)} missing TS"
        )
        
        # Detailed summary if issues found
        if unprotected:
            logger.warning("⚠️ Unprotected positions:")
            for pos in unprotected:
                logger.warning(
                    f"  - {pos.exchange.upper()} {pos.symbol} {pos.side}: "
                    f"${pos.quantity * pos.current_price:.2f}"
                )
        
        if missing_trailing:
            logger.info("📌 Positions with fixed SL only:")
            for pos in missing_trailing:
                logger.info(
                    f"  - {pos.exchange.upper()} {pos.symbol}: SL @ ${pos.stop_loss_price:.2f}"
                )
    
    async def run(self):
        """Main monitoring loop"""
        await self.initialize()
        
        while True:
            try:
                await asyncio.sleep(self.check_interval)
                await self.check_all_positions()
                
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(10)


async def main():
    """Main entry point"""
    monitor = UniversalProtectionMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)