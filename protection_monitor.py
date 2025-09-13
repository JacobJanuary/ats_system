#!/usr/bin/env python3
"""
Protection Monitor - PRODUCTION READY v4.0 (FINAL)
Complete refactoring with monitoring schema integration:
- Reads positions from exchange APIs (source of truth)
- Syncs with monitoring.positions for context
- Sets Trailing Stop if missing
- Verifies Stop Loss presence
- Closes timeout positions at breakeven
- Full logging to monitoring.protection_events
- Independent operation from main_trader
"""

import asyncio
import asyncpg
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Union, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
from decimal import Decimal, ROUND_DOWN
from dotenv import load_dotenv
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange
from utils.rate_limiter import RateLimiter

load_dotenv()

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO if os.getenv('DEBUG', 'false').lower() != 'true' else logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] %(message)s',
    handlers=[
        logging.FileHandler('logs/protection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProtectionType(Enum):
    STOP_LOSS = "stop_loss"
    TRAILING_STOP = "trailing_stop"
    TAKE_PROFIT = "take_profit"
    BREAKEVEN = "breakeven"


class PositionStatus(Enum):
    UNPROTECTED = "unprotected"
    PARTIALLY_PROTECTED = "partially_protected"
    FULLY_PROTECTED = "fully_protected"
    TRAILING_ACTIVE = "trailing_active"
    PENDING_CLOSE = "pending_close"


@dataclass
class PositionInfo:
    """Enhanced position information tracking"""
    symbol: str
    exchange: str
    side: str
    quantity: float
    entry_price: float
    current_price: float = 0.0
    pnl: float = 0.0
    pnl_percent: float = 0.0
    age_hours: float = 0.0
    has_sl: bool = False
    has_tp: bool = False
    has_trailing: bool = False
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    trailing_activation_price: Optional[float] = None
    status: PositionStatus = PositionStatus.UNPROTECTED
    update_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    protection_attempts: int = 0
    close_attempts: int = 0
    db_position_id: Optional[int] = None
    db_trade_id: Optional[int] = None


class ProtectionMonitor:
    def __init__(self):
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

        # Protection configuration
        self.sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.trailing_activation = float(os.getenv('TRAILING_ACTIVATION_PERCENT', '1'))
        self.trailing_callback = float(os.getenv('TRAILING_CALLBACK_RATE', '0.5'))

        # Timing configuration
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.max_position_duration_hours = int(os.getenv('MAX_POSITION_DURATION_HOURS', '24'))
        self.breakeven_timeout_hours = float(os.getenv('BREAKEVEN_TIMEOUT_HOURS', '6'))
        self.min_profit_for_breakeven = float(os.getenv('MIN_PROFIT_FOR_BREAKEVEN', '0.3'))

        # Environment and fees
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.taker_fee_percent = float(os.getenv('TAKER_FEE_PERCENT', '0.06'))
        self.maker_fee_percent = float(os.getenv('MAKER_FEE_PERCENT', '0.02'))

        # Rate limiting
        if self.testnet:
            self.request_delay = 0.5
            self.between_positions_delay = 1.0
        else:
            self.request_delay = 0.1  # 100ms for mainnet
            self.between_positions_delay = 0.2

        # Exchange instances
        self.binance: Optional[BinanceExchange] = None
        self.bybit: Optional[BybitExchange] = None
        self.db_pool: Optional[asyncpg.Pool] = None

        # Position tracking
        self.tracked_positions: Dict[str, PositionInfo] = {}
        self.positions_to_close: List[str] = []

        # Performance statistics
        self.stats = {
            'checks': 0,
            'positions_found': 0,
            'positions_protected': 0,
            'sl_added': 0,
            'trailing_added': 0,
            'positions_closed': 0,
            'breakeven_closes': 0,
            'timeout_closes': 0,
            'protection_failures': 0,
            'errors': 0,
            'start_time': datetime.now(timezone.utc)
        }

        # System components
        self.rate_limiter = RateLimiter()
        self.last_health_check = datetime.now(timezone.utc)
        self.health_check_interval = 60

        self._log_configuration()

    def _log_configuration(self):
        """Log system configuration"""
        logger.info("=" * 80)
        logger.info("PROTECTION MONITOR CONFIGURATION v4.0")
        logger.info("=" * 80)
        logger.info(f"Environment: {'TESTNET' if self.testnet else 'MAINNET'}")
        logger.info(f"Stop Loss: {self.sl_percent}%")
        logger.info(f"Trailing Activation: {self.trailing_activation}%")
        logger.info(f"Trailing Callback: {self.trailing_callback}%")
        logger.info(f"Max Position Duration: {self.max_position_duration_hours}h")
        logger.info(f"Breakeven Timeout: {self.breakeven_timeout_hours}h")
        logger.info(f"Check Interval: {self.check_interval}s")
        logger.info("=" * 80)

    async def _init_db(self):
        """Initialize database connection pool"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.db_pool = await asyncpg.create_pool(
                    **self.db_config,
                    min_size=2,
                    max_size=20,
                    command_timeout=10,
                    max_queries=50000,
                    max_inactive_connection_lifetime=300
                )

                # Test connection
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")

                logger.info("✅ Database connected successfully")
                return

            except Exception as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    self.db_pool = None
                    logger.warning("Running without database connection")

    async def initialize(self):
        """Initialize exchange connections and database"""
        logger.info("🚀 Initializing Protection Monitor...")

        # Initialize database
        await self._init_db()

        # Initialize Binance
        if os.getenv('BINANCE_API_KEY'):
            try:
                self.binance = BinanceExchange({
                    'api_key': os.getenv('BINANCE_API_KEY'),
                    'api_secret': os.getenv('BINANCE_API_SECRET'),
                    'testnet': self.testnet
                })
                await self.binance.initialize()
                balance = await self.binance.get_balance()
                logger.info(f"✅ Binance initialized. Balance: ${balance:.2f}")
            except Exception as e:
                logger.error(f"Failed to initialize Binance: {e}")
                self.binance = None

        # Initialize Bybit
        if os.getenv('BYBIT_API_KEY'):
            try:
                self.bybit = BybitExchange({
                    'api_key': os.getenv('BYBIT_API_KEY'),
                    'api_secret': os.getenv('BYBIT_API_SECRET'),
                    'testnet': self.testnet
                })
                await self.bybit.initialize()
                balance = await self.bybit.get_balance()
                logger.info(f"✅ Bybit initialized. Balance: ${balance:.2f}")
            except Exception as e:
                logger.error(f"Failed to initialize Bybit: {e}")
                self.bybit = None

        if not self.binance and not self.bybit:
            raise Exception("No exchanges available!")

        # Log initial system health
        await self._log_system_health("protection_monitor", "RUNNING")

        logger.info("✅ Protection Monitor initialized")

    async def _log_system_health(self, service_name: str, status: str, error: Optional[str] = None):
        """Log system health to monitoring.system_health"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO monitoring.system_health (
                        service_name, status, binance_connected, bybit_connected,
                        database_connected, positions_protected_count, error_count,
                        last_error, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                                   service_name,
                                   status,
                                   self.binance is not None,
                                   self.bybit is not None,
                                   True,
                                   self.stats['positions_protected'],
                                   self.stats.get('errors', 0),
                                   error,
                                   json.dumps({
                                       'positions_found': self.stats['positions_found'],
                                       'sl_added': self.stats['sl_added'],
                                       'trailing_added': self.stats['trailing_added'],
                                       'positions_closed': self.stats['positions_closed'],
                                       'breakeven_closes': self.stats['breakeven_closes'],
                                       'timeout_closes': self.stats['timeout_closes']
                                   })
                                   )
        except Exception as e:
            logger.error(f"Failed to log system health: {e}")

    async def get_db_positions(self, exchange_name: str) -> Dict[str, Any]:
        """Get positions from database for context"""
        if not self.db_pool:
            return {}

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT p.*, t.signal_id
                    FROM monitoring.positions p
                    LEFT JOIN monitoring.trades t ON p.trade_id = t.id
                    WHERE p.exchange = $1 
                    AND p.status = 'OPEN'
                """, exchange_name)

                positions = {}
                for row in rows:
                    key = f"{row['exchange']}_{row['symbol']}"
                    positions[key] = dict(row)

                return positions

        except Exception as e:
            logger.error(f"Error fetching DB positions: {e}")
            return {}

    async def log_protection_event(self, position_info: PositionInfo,
                                   event_type: str, protection_type: str,
                                   price_level: Optional[float] = None,
                                   activation_price: Optional[float] = None,
                                   callback_rate: Optional[float] = None,
                                   success: bool = True,
                                   error_message: Optional[str] = None):
        """Log protection event to monitoring.protection_events"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO monitoring.protection_events (
                        position_id, symbol, exchange, event_type, protection_type,
                        price_level, activation_price, callback_rate, success, error_message
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                                   position_info.db_position_id,
                                   position_info.symbol,
                                   position_info.exchange,
                                   event_type,
                                   protection_type,
                                   price_level,
                                   activation_price,
                                   callback_rate,
                                   success,
                                   error_message
                                   )
        except Exception as e:
            logger.error(f"Error logging protection event: {e}")

    async def update_position_protection(self, position_info: PositionInfo):
        """Update position protection status in database"""
        if not self.db_pool or not position_info.db_position_id:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE monitoring.positions
                    SET has_stop_loss = $1,
                        stop_loss_price = $2,
                        has_trailing_stop = $3,
                        trailing_activation = $4,
                        trailing_callback = $5,
                        current_price = $6,
                        pnl = $7,
                        pnl_percent = $8,
                        updated_at = now()
                    WHERE id = $9
                """,
                                   position_info.has_sl,
                                   position_info.sl_price,
                                   position_info.has_trailing,
                                   position_info.trailing_activation_price,
                                   self.trailing_callback if position_info.has_trailing else None,
                                   position_info.current_price,
                                   position_info.pnl,
                                   position_info.pnl_percent,
                                   position_info.db_position_id
                                   )
        except Exception as e:
            logger.error(f"Error updating position protection: {e}")

    async def close_position_in_db(self, position_info: PositionInfo, close_reason: str):
        """Mark position as closed in database"""
        if not self.db_pool or not position_info.db_position_id:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE monitoring.positions
                    SET status = 'CLOSED',
                        closed_at = now(),
                        updated_at = now()
                    WHERE id = $1
                """, position_info.db_position_id)

                # Log the close event
                await self.log_protection_event(
                    position_info,
                    "POSITION_CLOSED",
                    close_reason,
                    price_level=position_info.current_price,
                    success=True
                )
        except Exception as e:
            logger.error(f"Error closing position in DB: {e}")

    def _calculate_position_age(self, position: Dict, exchange_name: str) -> float:
        """Calculate position age in hours"""
        try:
            # Get update timestamp based on exchange
            if exchange_name == "Binance":
                timestamp_ms = position.get("updateTime", 0)
            else:  # Bybit
                timestamp_ms = position.get("updatedTime", 0)

            if not timestamp_ms:
                return 0.0

            # Convert to seconds and calculate age
            timestamp_sec = timestamp_ms / 1000
            age_seconds = datetime.now(timezone.utc).timestamp() - timestamp_sec
            return age_seconds / 3600

        except Exception as e:
            logger.warning(f"Error calculating position age: {e}")
            return 0.0

    def _calculate_pnl_percent(self, entry_price: float, current_price: float, side: str) -> float:
        """Calculate PnL percentage"""
        if entry_price <= 0:
            return 0.0

        if side.upper() in ['LONG', 'BUY']:
            return ((current_price - entry_price) / entry_price) * 100
        else:
            return ((entry_price - current_price) / entry_price) * 100

    async def _check_protection_status(self, exchange_name: str, position: Dict,
                                       open_orders: List[Dict], db_position: Optional[Dict] = None) -> PositionInfo:
        """Comprehensive protection status check"""
        symbol = position.get('symbol')

        pos_info = PositionInfo(
            symbol=symbol,
            exchange=exchange_name,
            side=position.get('side', '').upper(),
            quantity=float(position.get('quantity', 0)),
            entry_price=float(position.get('entry_price', 0)),
            current_price=float(position.get('mark_price', position.get('entry_price', 0))),
            pnl=float(position.get('pnl', 0)),
            age_hours=self._calculate_position_age(position, exchange_name)
        )

        # Link to database position if available
        if db_position:
            pos_info.db_position_id = db_position.get('id')
            pos_info.db_trade_id = db_position.get('trade_id')

        # Calculate PnL percentage
        pos_info.pnl_percent = self._calculate_pnl_percent(
            pos_info.entry_price, pos_info.current_price, pos_info.side
        )

        # Check protection based on exchange
        if exchange_name == 'Bybit':
            # Bybit stores protection in position data
            pos_info.has_sl = bool(position.get('stopLoss') and float(position.get('stopLoss', 0)) > 0)
            pos_info.has_tp = bool(position.get('takeProfit') and float(position.get('takeProfit', 0)) > 0)
            pos_info.has_trailing = bool(position.get('trailingStop') and float(position.get('trailingStop', 0)) > 0)

            if pos_info.has_sl:
                pos_info.sl_price = float(position.get('stopLoss', 0))
            if pos_info.has_tp:
                pos_info.tp_price = float(position.get('takeProfit', 0))

        else:  # Binance
            # Check orders for protection
            symbol_orders = [o for o in open_orders if o.get('symbol') == symbol]

            for order in symbol_orders:
                order_type = order.get('type', '').upper()
                if order_type in ['STOP_MARKET', 'STOP', 'STOP_LOSS']:
                    pos_info.has_sl = True
                    pos_info.sl_price = float(order.get('stopPrice', 0))
                elif order_type in ['TAKE_PROFIT_MARKET', 'TAKE_PROFIT']:
                    pos_info.has_tp = True
                    pos_info.tp_price = float(order.get('stopPrice', 0))
                elif order_type == 'TRAILING_STOP_MARKET':
                    pos_info.has_trailing = True
                    pos_info.trailing_activation_price = float(order.get('activationPrice', 0))

        # Determine protection status
        if pos_info.has_sl and pos_info.has_trailing:
            pos_info.status = PositionStatus.FULLY_PROTECTED
        elif pos_info.has_sl or pos_info.has_trailing:
            pos_info.status = PositionStatus.PARTIALLY_PROTECTED
        else:
            pos_info.status = PositionStatus.UNPROTECTED

        # Check if trailing is active
        if pos_info.has_trailing and pos_info.pnl_percent >= self.trailing_activation:
            pos_info.status = PositionStatus.TRAILING_ACTIVE

        return pos_info

    async def _apply_protection(self, exchange: Union[BinanceExchange, BybitExchange],
                                pos_info: PositionInfo) -> bool:
        """Apply protection to position with validation"""
        symbol = pos_info.symbol
        logger.info(f"🛡️ Applying protection to {symbol} on {pos_info.exchange}")
        protection_applied = False

        try:
            # Get current market price for validation
            ticker = await exchange.get_ticker(symbol)
            if not ticker or not ticker.get('price'):
                logger.error(f"Cannot get market price for {symbol}")
                return False

            market_price = float(ticker['price'])

            # 1. Check and add Stop Loss if missing
            if not pos_info.has_sl:
                if pos_info.side in ['LONG', 'BUY']:
                    sl_price = pos_info.entry_price * (1 - self.sl_percent / 100)
                    sl_valid = sl_price < market_price
                else:
                    sl_price = pos_info.entry_price * (1 + self.sl_percent / 100)
                    sl_valid = sl_price > market_price

                if sl_valid:
                    await asyncio.sleep(self.request_delay)
                    if await exchange.set_stop_loss(symbol, sl_price):
                        logger.info(f"✅ Stop Loss added at ${sl_price:.4f}")
                        pos_info.has_sl = True
                        pos_info.sl_price = sl_price
                        self.stats['sl_added'] += 1
                        protection_applied = True

                        await self.log_protection_event(
                            pos_info, "STOP_LOSS_ADDED", "STOP_LOSS",
                            price_level=sl_price, success=True
                        )
                else:
                    logger.warning(f"SL price ${sl_price:.4f} invalid vs market ${market_price:.4f}")

            # 2. Add Trailing Stop if not present
            if not pos_info.has_trailing:
                # Calculate activation price (slightly above current for profit)
                if pos_info.side in ['LONG', 'BUY']:
                    activation_price = pos_info.entry_price * (1 + self.trailing_activation / 100)
                else:
                    activation_price = pos_info.entry_price * (1 - self.trailing_activation / 100)

                await asyncio.sleep(self.request_delay)
                if await exchange.set_trailing_stop(symbol, activation_price, self.trailing_callback):
                    logger.info(
                        f"✅ Trailing Stop added: activation=${activation_price:.4f}, callback={self.trailing_callback}%")
                    pos_info.has_trailing = True
                    pos_info.trailing_activation_price = activation_price
                    self.stats['trailing_added'] += 1
                    protection_applied = True

                    await self.log_protection_event(
                        pos_info, "TRAILING_STOP_ADDED", "TRAILING_STOP",
                        activation_price=activation_price,
                        callback_rate=self.trailing_callback,
                        success=True
                    )

            if protection_applied:
                self.stats['positions_protected'] += 1
                pos_info.protection_attempts = 0

                # Update database
                await self.update_position_protection(pos_info)
            else:
                pos_info.protection_attempts += 1

            return protection_applied

        except Exception as e:
            logger.error(f"Error applying protection to {symbol}: {e}", exc_info=True)
            self.stats['protection_failures'] += 1
            pos_info.protection_attempts += 1

            await self.log_protection_event(
                pos_info, "PROTECTION_FAILED", "ERROR",
                error_message=str(e), success=False
            )

            return False

    async def _close_position_breakeven(self, exchange: Union[BinanceExchange, BybitExchange],
                                        pos_info: PositionInfo) -> bool:
        """Close position at breakeven or small profit"""
        symbol = pos_info.symbol

        try:
            # Get current ticker
            ticker = await exchange.get_ticker(symbol)
            if not ticker:
                logger.error(f"Cannot get ticker for {symbol}")
                return False

            current_price = float(ticker['price'])
            pos_info.current_price = current_price

            # Calculate breakeven price including fees
            total_fee_percent = self.taker_fee_percent * 2  # Entry + Exit

            if pos_info.side in ['LONG', 'BUY']:
                breakeven_price = pos_info.entry_price * (
                        1 + total_fee_percent / 100 + self.min_profit_for_breakeven / 100)
                can_close = current_price >= breakeven_price
            else:
                breakeven_price = pos_info.entry_price * (
                        1 - total_fee_percent / 100 - self.min_profit_for_breakeven / 100)
                can_close = current_price <= breakeven_price

            if can_close:
                logger.info(
                    f"📈 Closing {symbol} at breakeven/profit. Current: ${current_price:.4f}, Breakeven: ${breakeven_price:.4f}")

                # Cancel existing orders first
                await exchange.cancel_all_open_orders(symbol)
                await asyncio.sleep(self.request_delay)

                # Close position
                if await exchange.close_position(symbol):
                    logger.info(f"✅ Position {symbol} closed at breakeven")
                    self.stats['breakeven_closes'] += 1
                    self.stats['positions_closed'] += 1

                    # Update database
                    await self.close_position_in_db(pos_info, "BREAKEVEN")

                    return True
            else:
                # Set limit order at breakeven if not in profit yet
                logger.info(f"📊 Setting breakeven limit order for {symbol} at ${breakeven_price:.4f}")

                side = 'SELL' if pos_info.side in ['LONG', 'BUY'] else 'BUY'
                await exchange.create_limit_order(
                    symbol=symbol,
                    side=side,
                    quantity=pos_info.quantity,
                    price=breakeven_price,
                    reduce_only=True
                )

                await self.log_protection_event(
                    pos_info, "BREAKEVEN_ORDER_SET", "BREAKEVEN",
                    price_level=breakeven_price, success=True
                )

                return False

        except Exception as e:
            logger.error(f"Error closing position at breakeven: {e}")
            return False

    async def _handle_timeout_position(self, exchange: Union[BinanceExchange, BybitExchange],
                                       pos_info: PositionInfo) -> bool:
        """Handle positions that exceeded timeout"""
        symbol = pos_info.symbol

        logger.warning(f"⏰ Position {symbol} exceeded max duration ({pos_info.age_hours:.1f}h)")

        try:
            # First try breakeven close if position is old but not max timeout
            if pos_info.age_hours < self.max_position_duration_hours:
                if pos_info.pnl_percent >= -self.taker_fee_percent * 2:
                    if await self._close_position_breakeven(exchange, pos_info):
                        return True

            # Force close if max timeout exceeded
            logger.warning(f"⚠️ Force closing {symbol} due to timeout")

            # Cancel all orders
            await exchange.cancel_all_open_orders(symbol)
            await asyncio.sleep(self.request_delay)

            # Close position
            if await exchange.close_position(symbol):
                logger.info(f"✅ Position {symbol} closed due to timeout")
                self.stats['timeout_closes'] += 1
                self.stats['positions_closed'] += 1

                # Update database
                await self.close_position_in_db(pos_info, "TIMEOUT")

                return True
            else:
                pos_info.close_attempts += 1
                return False

        except Exception as e:
            logger.error(f"Error handling timeout position: {e}")
            pos_info.close_attempts += 1
            return False

    async def process_exchange_positions(self, exchange_name: str):
        """Process all positions for an exchange"""
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        if not exchange:
            return

        try:
            # Get positions from exchange API (source of truth)
            positions = await exchange.get_open_positions()
            if not positions:
                logger.debug(f"No open positions on {exchange_name}")
                return

            logger.info(f"Found {len(positions)} open positions on {exchange_name}")
            self.stats['positions_found'] += len(positions)

            # Get database positions for context
            db_positions = await self.get_db_positions(exchange_name)

            # Get all open orders once
            all_orders = await exchange.get_open_orders()
            if all_orders is None:
                all_orders = []

            # Process each position
            for position in positions:
                symbol = position.get('symbol')
                if not symbol:
                    continue

                # Add delay between positions
                await asyncio.sleep(self.between_positions_delay)

                # Find matching DB position
                pos_key = f"{exchange_name}_{symbol}"
                db_position = db_positions.get(pos_key)

                # Check protection status
                pos_info = await self._check_protection_status(
                    exchange_name, position, all_orders, db_position
                )

                # Store position info
                self.tracked_positions[pos_key] = pos_info

                # Log position status
                logger.info(
                    f"📊 {symbol}: PnL={pos_info.pnl_percent:.2f}%, Age={pos_info.age_hours:.1f}h, Status={pos_info.status.value}")

                # Handle based on position age and status

                # 1. Check for timeout
                if self.max_position_duration_hours > 0 and pos_info.age_hours > self.max_position_duration_hours:
                    await self._handle_timeout_position(exchange, pos_info)
                    continue

                # 2. Check for breakeven close opportunity
                if (self.breakeven_timeout_hours > 0 and
                        pos_info.age_hours > self.breakeven_timeout_hours):

                    # Only close if trailing not active or if in good profit
                    if pos_info.status != PositionStatus.TRAILING_ACTIVE:
                        if pos_info.pnl_percent > self.min_profit_for_breakeven:
                            logger.info(f"Position {symbol} eligible for breakeven close")
                            await self._close_position_breakeven(exchange, pos_info)
                            continue
                    elif pos_info.pnl_percent > 2:  # If trailing active and good profit, let it run
                        logger.info(
                            f"Position {symbol} has trailing active with {pos_info.pnl_percent:.2f}% profit, keeping open")

                # 3. Apply protection if needed
                if pos_info.status in [PositionStatus.UNPROTECTED, PositionStatus.PARTIALLY_PROTECTED]:
                    if pos_info.protection_attempts < 3:  # Max 3 attempts
                        await self._apply_protection(exchange, pos_info)
                    else:
                        logger.error(f"Failed to protect {symbol} after 3 attempts")

                        await self.log_protection_event(
                            pos_info, "PROTECTION_ABANDONED", "ERROR",
                            error_message="Max attempts reached", success=False
                        )

                # Update position info in database
                if pos_info.db_position_id:
                    await self.update_position_protection(pos_info)

        except Exception as e:
            logger.error(f"Error processing {exchange_name} positions: {e}", exc_info=True)
            self.stats['errors'] += 1
            await self._log_system_health("protection_monitor", "ERROR", str(e))

    async def update_performance_metrics(self):
        """Update hourly performance metrics"""
        if not self.db_pool:
            return

        try:
            current_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

            async with self.db_pool.acquire() as conn:
                # Update metrics for each exchange
                for exchange_name in ['Binance', 'Bybit']:
                    exchange = self.binance if exchange_name == 'Binance' else self.bybit
                    if not exchange:
                        continue

                    # Get current hour's stats
                    positions_protected = 0
                    positions_closed = 0

                    # Count from our stats (rough estimate for this hour)
                    # In production, you'd want more precise tracking

                    await conn.execute("""
                        INSERT INTO monitoring.performance_metrics (
                            metric_date, metric_hour, exchange,
                            positions_protected, positions_closed
                        ) VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (metric_date, metric_hour, exchange)
                        DO UPDATE SET
                            positions_protected = monitoring.performance_metrics.positions_protected + EXCLUDED.positions_protected,
                            positions_closed = monitoring.performance_metrics.positions_closed + EXCLUDED.positions_closed
                    """,
                                       current_hour.date(),
                                       current_hour.hour,
                                       exchange_name,
                                       positions_protected,
                                       positions_closed
                                       )
        except Exception as e:
            logger.error(f"Error updating performance metrics: {e}")

    async def health_check(self):
        """Perform health check on exchanges"""
        now = datetime.now(timezone.utc)
        if (now - self.last_health_check).seconds < self.health_check_interval:
            return

        self.last_health_check = now

        # Check Binance
        if self.binance:
            try:
                await self.binance.get_balance()
            except Exception as e:
                logger.warning(f"Binance health check failed: {e}")
                # Try to reinitialize
                try:
                    await self.binance.initialize()
                except:
                    self.binance = None

        # Check Bybit
        if self.bybit:
            try:
                await self.bybit.get_balance()
            except Exception as e:
                logger.warning(f"Bybit health check failed: {e}")
                # Try to reinitialize
                try:
                    await self.bybit.initialize()
                except:
                    self.bybit = None

        # Check database
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
            except Exception as e:
                logger.warning(f"Database health check failed: {e}")
                await self._init_db()

    def log_statistics(self):
        """Log performance statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        hours = runtime.total_seconds() / 3600

        logger.info("=" * 60)
        logger.info("PROTECTION MONITOR STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Runtime: {hours:.2f} hours")
        logger.info(f"Total Checks: {self.stats['checks']}")
        logger.info(f"Positions Found: {self.stats['positions_found']}")
        logger.info(f"Positions Protected: {self.stats['positions_protected']}")
        logger.info(f"  - Stop Loss Added: {self.stats['sl_added']}")
        logger.info(f"  - Trailing Added: {self.stats['trailing_added']}")
        logger.info(f"Positions Closed: {self.stats['positions_closed']}")
        logger.info(f"  - Breakeven: {self.stats['breakeven_closes']}")
        logger.info(f"  - Timeout: {self.stats['timeout_closes']}")
        logger.info(f"Protection Failures: {self.stats['protection_failures']}")
        logger.info(f"Errors: {self.stats['errors']}")

        # Current positions summary
        if self.tracked_positions:
            logger.info(f"\nActive Positions: {len(self.tracked_positions)}")
            total_pnl = sum(p.pnl for p in self.tracked_positions.values())
            avg_age = sum(p.age_hours for p in self.tracked_positions.values()) / len(self.tracked_positions)
            logger.info(f"Total PnL: ${total_pnl:.2f}")
            logger.info(f"Average Age: {avg_age:.1f} hours")

        logger.info("=" * 60)

    async def run(self):
        """Main monitoring loop"""
        logger.info("🚀 Starting Protection Monitor v4.0 - PRODUCTION READY")
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'MAINNET'}")

        try:
            await self.initialize()
        except Exception as e:
            logger.critical(f"Failed to initialize: {e}")
            return

        # Periodic statistics logging
        async def log_stats_periodically():
            while True:
                await asyncio.sleep(300)  # Every 5 minutes
                self.log_statistics()
                await self.update_performance_metrics()

        stats_task = asyncio.create_task(log_stats_periodically())

        try:
            while True:
                try:
                    self.stats['checks'] += 1

                    # Health check
                    await self.health_check()

                    # Process positions
                    logger.info(f"\n{'=' * 40}")
                    logger.info(f"Protection Check #{self.stats['checks']}")
                    logger.info(f"{'=' * 40}")

                    tasks = []
                    if self.binance:
                        tasks.append(self.process_exchange_positions('Binance'))
                    if self.bybit:
                        tasks.append(self.process_exchange_positions('Bybit'))

                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)

                    # Log brief summary
                    logger.info(f"Check complete. Protected: {self.stats['positions_protected']}, "
                                f"Closed: {self.stats['positions_closed']}")

                    # Wait for next check
                    await asyncio.sleep(self.check_interval)

                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    self.stats['errors'] += 1
                    await self._log_system_health("protection_monitor", "ERROR", str(e))
                    await asyncio.sleep(10)

        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        finally:
            stats_task.cancel()
            await self._log_system_health("protection_monitor", "STOPPED")
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up...")

        # Final statistics
        self.log_statistics()

        # Close database pool
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Database pool closed")

        # Close exchange connections
        if self.binance:
            await self.binance.close()
        if self.bybit:
            await self.bybit.close()

        logger.info("✅ Cleanup complete")


async def main():
    """Entry point"""
    monitor = ProtectionMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)