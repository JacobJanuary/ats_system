#!/usr/bin/env python3
"""
Main Trading Script - PRODUCTION READY v7.1 REFINED
Complete refactoring with monitoring schema integration:
- Reads from fas.scoring_history with is_active=true
- Full logging to monitoring.trades and monitoring.positions
- Sets only Stop Loss after position opening
- Independent operation with API verification
- Production-ready error handling and recovery
- Unified 'executed_qty' handling for all exchanges
"""

import asyncio
import asyncpg
import logging
import os
import sys
import signal
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple, Union, Any
from dataclasses import dataclass, field
from enum import Enum
import traceback
from decimal import Decimal
from dotenv import load_dotenv
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange
from utils.rate_limiter import RateLimiter

load_dotenv()

from logging.handlers import RotatingFileHandler

# --- Enhanced Logging Configuration ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO)

# Create logs directory if not exists
os.makedirs('logs', exist_ok=True)

file_handler = RotatingFileHandler(
    'logs/trader.log', maxBytes=50 * 1024 * 1024, backupCount=10
)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] %(message)s')
)

console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


class OrderStatus(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TradingMode(Enum):
    TESTNET = "TESTNET"
    MAINNET = "MAINNET"


@dataclass
class Signal:
    """Signal from fas.scoring_history"""
    id: int
    trading_pair_id: int
    pair_symbol: str
    exchange_id: int
    exchange_name: str
    score_week: float
    score_month: float
    recommended_action: str  # BUY/SELL
    timestamp: datetime
    patterns_details: Optional[Dict] = None
    combinations_details: Optional[Dict] = None


@dataclass
class TradeRecord:
    """Record for monitoring.trades table"""
    signal_id: int
    trading_pair_id: int
    symbol: str
    exchange: str
    side: str
    quantity: float
    executed_qty: float
    price: float
    status: str
    order_id: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class PositionRecord:
    """Record for monitoring.positions table"""
    trade_id: int
    symbol: str
    exchange: str
    side: str
    quantity: float
    entry_price: float
    has_stop_loss: bool = False
    stop_loss_price: Optional[float] = None
    status: str = "OPEN"


class MainTrader:
    def __init__(self):
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

        # Trading parameters
        self.min_score_week = float(os.getenv('MIN_SCORE_WEEK', '70'))
        self.min_score_month = float(os.getenv('MIN_SCORE_MONTH', '80'))
        self.position_size_usd = float(os.getenv('POSITION_SIZE_USD', '10'))
        self.min_position_size_usd = float(os.getenv('MIN_POSITION_SIZE_USD', '10.0'))

        # Ensure minimum position size
        if self.position_size_usd < self.min_position_size_usd:
            logger.warning(
                f"Position size ${self.position_size_usd:.2f} below minimum ${self.min_position_size_usd:.2f}. Adjusting."
            )
            self.position_size_usd = self.min_position_size_usd

        self.leverage = int(os.getenv('LEVERAGE', '10'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.signal_time_window = int(os.getenv('SIGNAL_TIME_WINDOW', '5'))

        # Retry configuration
        self.order_retry_max = int(os.getenv('ORDER_RETRY_MAX', '3'))
        self.order_retry_delay = float(os.getenv('ORDER_RETRY_DELAY', '1.0'))

        # Risk management
        self.initial_sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2.0'))
        self.max_spread_percent = float(os.getenv('MAX_SPREAD_PERCENT', '0.5'))

        # Environment detection
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.trading_mode = TradingMode.TESTNET if self.testnet else TradingMode.MAINNET

        # Dynamic rate limiting based on environment
        if self.trading_mode == TradingMode.MAINNET:
            self.delay_between_trades = 0.1  # 100ms for mainnet
            self.delay_between_requests = 0.05  # 50ms between API calls
            self.spread_limit = 0.5  # 0.5% max spread
        else:
            self.delay_between_trades = 2.0  # 2s for testnet
            self.delay_between_requests = 0.5  # 500ms between API calls
            self.spread_limit = 100.0  # 100% for testnet (effectively disabled)

        # Exchange instances
        self.binance: Optional[BinanceExchange] = None
        self.bybit: Optional[BybitExchange] = None
        self.db_pool: Optional[asyncpg.Pool] = None

        # State management
        self.processing_signals: Set[int] = set()
        self.failed_signals: Set[int] = set()

        # Exchange name mapping
        self.exchange_names = {1: 'Binance', 2: 'Bybit'}

        # Performance monitoring
        self.stats = {
            'signals_processed': 0,
            'positions_opened': 0,
            'positions_failed': 0,
            'sl_set': 0,
            'sl_failed': 0,
            'start_time': datetime.now(timezone.utc)
        }

        # System control
        self.rate_limiter = RateLimiter()
        self.shutdown_event = asyncio.Event()
        self.health_check_interval = 60  # seconds
        self.last_health_check = datetime.now(timezone.utc)

        self._log_configuration()

    def _log_configuration(self):
        """Log system configuration"""
        logger.info("=" * 80)
        logger.info("TRADING SYSTEM CONFIGURATION v7.1")
        logger.info("=" * 80)
        logger.info(f"Environment: {self.trading_mode.value}")
        logger.info(f"Position Size: ${self.position_size_usd} USD")
        logger.info(f"Leverage: {self.leverage}x")
        logger.info(f"Stop Loss: {self.initial_sl_percent}%")
        logger.info(f"Max Spread: {self.spread_limit}%")
        logger.info(f"Trade Delay: {self.delay_between_trades}s")
        logger.info(f"Signal Window: {self.signal_time_window} minutes")
        logger.info("=" * 80)

    async def _init_db(self):
        """Initialize database connection pool with retry logic"""
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
                    raise Exception("Failed to connect to database after all retries")

    async def _init_binance(self):
        """Initialize Binance exchange with enhanced error handling"""
        try:
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_API_SECRET')

            if not api_key or not api_secret:
                logger.warning("Binance credentials not found")
                return

            self.binance = BinanceExchange({
                'api_key': api_key,
                'api_secret': api_secret,
                'testnet': self.testnet
            })

            await self.binance.initialize()

            # Verify connection
            balance = await self.binance.get_balance()
            logger.info(f"✅ Binance initialized. Balance: ${balance:.2f} USDT")

        except Exception as e:
            logger.error(f"Failed to initialize Binance: {e}")
            self.binance = None

    async def _init_bybit(self):
        """Initialize Bybit exchange with enhanced error handling"""
        try:
            api_key = os.getenv('BYBIT_API_KEY')
            api_secret = os.getenv('BYBIT_API_SECRET')

            if not api_key or not api_secret:
                logger.warning("Bybit credentials not found")
                return

            self.bybit = BybitExchange({
                'api_key': api_key,
                'api_secret': api_secret,
                'testnet': self.testnet
            })

            await self.bybit.initialize()

            # Verify connection
            balance = await self.bybit.get_balance()
            logger.info(f"✅ Bybit initialized. Balance: ${balance:.2f} USDT")

        except Exception as e:
            logger.error(f"Failed to initialize Bybit: {e}")
            self.bybit = None

    async def initialize(self):
        """Complete system initialization"""
        logger.info("🚀 System initialization starting...")

        # Initialize database
        await self._init_db()

        # Initialize exchanges in parallel
        tasks = []
        if os.getenv('BINANCE_API_KEY'):
            tasks.append(self._init_binance())
        if os.getenv('BYBIT_API_KEY'):
            tasks.append(self._init_bybit())

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Exchange initialization error: {result}")

        # Verify at least one exchange is available
        if not self.binance and not self.bybit:
            raise Exception("CRITICAL: No exchanges available. Cannot start trading.")

        # Log initial system health
        await self._log_system_health("main_trader", "RUNNING")

        logger.info("✅ System initialization complete")

    async def _log_system_health(self, service_name: str, status: str, error: Optional[str] = None):
        """Log system health to monitoring.system_health"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO monitoring.system_health (
                        service_name, status, binance_connected, bybit_connected,
                        database_connected, signals_processed_count, error_count,
                        last_error, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                                   service_name,
                                   status,
                                   self.binance is not None,
                                   self.bybit is not None,
                                   True,
                                   self.stats['signals_processed'],
                                   self.stats.get('errors', 0),
                                   error,
                                   json.dumps({
                                       'positions_opened': self.stats['positions_opened'],
                                       'positions_failed': self.stats['positions_failed'],
                                       'sl_set': self.stats['sl_set'],
                                       'sl_failed': self.stats['sl_failed']
                                   })
                                   )
        except Exception as e:
            logger.error(f"Failed to log system health: {e}")

    async def _check_and_reconnect(self):
        """Health check and auto-reconnection logic"""
        now = datetime.now(timezone.utc)

        # Only run health check every N seconds
        if (now - self.last_health_check).seconds < self.health_check_interval:
            return

        self.last_health_check = now
        logger.debug("Running health check...")

        # Check database
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
            except Exception as e:
                logger.error(f"Database health check failed: {e}")
                await self._log_system_health("main_trader", "DB_ERROR", str(e))
                try:
                    await self.db_pool.close()
                except:
                    pass
                self.db_pool = None
                await self._init_db()
        else:
            await self._init_db()

        # Check exchanges
        reconnected = False

        if self.binance:
            try:
                await self.binance.get_balance()
            except Exception as e:
                logger.warning(f"Binance health check failed: {e}")
                await self._init_binance()
                reconnected = True

        if self.bybit:
            try:
                await self.bybit.get_balance()
            except Exception as e:
                logger.warning(f"Bybit health check failed: {e}")
                await self._init_bybit()
                reconnected = True

        if reconnected:
            await self._log_system_health("main_trader", "RECONNECTED")

    async def get_unprocessed_signals(self) -> List[Signal]:
        """Fetch unprocessed signals from fas.scoring_history"""
        if not self.db_pool:
            logger.error("Database not available")
            return []

        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=self.signal_time_window)

        query = """
            SELECT 
                sh.id,
                sh.trading_pair_id,
                tp.pair_symbol,
                tp.exchange_id,
                e.exchange_name,
                sh.score_week,
                sh.score_month,
                sh.recommended_action,
                sh.created_at,
                sh.patterns_details,
                sh.combinations_details
            FROM fas.scoring_history sh
            JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
            JOIN public.exchanges e ON tp.exchange_id = e.id
            WHERE sh.created_at > $1
                AND sh.is_active = true
                AND sh.score_week >= $2
                AND sh.score_month >= $3
                AND sh.recommended_action IN ('BUY', 'SELL')
            ORDER BY sh.created_at DESC
            LIMIT 50
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(
                    query,
                    time_threshold,
                    self.min_score_week,
                    self.min_score_month
                )

                signals = []
                for row in rows:
                    if row['id'] not in self.processing_signals and row['id'] not in self.failed_signals:
                        signal = Signal(
                            id=row['id'],
                            trading_pair_id=row['trading_pair_id'],
                            pair_symbol=row['pair_symbol'],
                            exchange_id=row['exchange_id'],
                            exchange_name=row['exchange_name'],
                            score_week=float(row['score_week']),
                            score_month=float(row['score_month']),
                            recommended_action=row['recommended_action'],
                            timestamp=row['created_at'],
                            patterns_details=row['patterns_details'],
                            combinations_details=row['combinations_details']
                        )
                        signals.append(signal)

                return signals

        except Exception as e:
            logger.error(f"Error fetching signals: {e}")
            return []

    async def mark_signal_processed(self, signal_id: int):
        """Mark signal as processed in fas.scoring_history"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE fas.scoring_history SET is_active = false WHERE id = $1",
                    signal_id
                )
        except Exception as e:
            logger.error(f"Error marking signal {signal_id} as processed: {e}")

    async def log_trade(self, trade: TradeRecord) -> Optional[int]:
        """Log trade to monitoring.trades and return trade_id"""
        if not self.db_pool:
            return None

        try:
            async with self.db_pool.acquire() as conn:
                trade_id = await conn.fetchval("""
                    INSERT INTO monitoring.trades (
                        signal_id, trading_pair_id, symbol, exchange, side,
                        quantity, executed_qty, price, status, order_id, error_message
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id
                """,
                                               trade.signal_id,
                                               trade.trading_pair_id,
                                               trade.symbol,
                                               trade.exchange,
                                               trade.side,
                                               trade.quantity,
                                               trade.executed_qty,
                                               trade.price,
                                               trade.status,
                                               trade.order_id,
                                               trade.error_message
                                               )
                return trade_id
        except Exception as e:
            logger.error(f"Error logging trade: {e}")
            return None

    async def log_position(self, position: PositionRecord) -> Optional[int]:
        """Log position to monitoring.positions and return position_id"""
        if not self.db_pool:
            return None

        try:
            async with self.db_pool.acquire() as conn:
                position_id = await conn.fetchval("""
                    INSERT INTO monitoring.positions (
                        trade_id, symbol, exchange, side, quantity, entry_price,
                        has_stop_loss, stop_loss_price, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id
                """,
                                                  position.trade_id,
                                                  position.symbol,
                                                  position.exchange,
                                                  position.side,
                                                  position.quantity,
                                                  position.entry_price,
                                                  position.has_stop_loss,
                                                  position.stop_loss_price,
                                                  position.status
                                                  )
                return position_id
        except Exception as e:
            logger.error(f"Error logging position: {e}")
            return None

    async def update_position_stop_loss(self, position_id: int, sl_price: float):
        """Update position stop loss in monitoring.positions"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE monitoring.positions 
                    SET has_stop_loss = true, 
                        stop_loss_price = $1,
                        updated_at = now()
                    WHERE id = $2
                """, sl_price, position_id)
        except Exception as e:
            logger.error(f"Error updating position stop loss: {e}")

    async def log_protection_event(self, position_id: int, symbol: str, exchange: str,
                                   event_type: str, price_level: float, success: bool,
                                   error_message: Optional[str] = None):
        """Log protection event to monitoring.protection_events"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO monitoring.protection_events (
                        position_id, symbol, exchange, event_type, protection_type,
                        price_level, success, error_message
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                                   position_id,
                                   symbol,
                                   exchange,
                                   event_type,
                                   'STOP_LOSS',
                                   price_level,
                                   success,
                                   error_message
                                   )
        except Exception as e:
            logger.error(f"Error logging protection event: {e}")

    async def validate_spread(self, exchange: Union[BinanceExchange, BybitExchange], symbol: str) -> bool:
        """Validate bid-ask spread is within acceptable range"""
        try:
            ticker = await exchange.get_ticker(symbol)
            if not ticker or not ticker.get('bid') or not ticker.get('ask'):
                logger.warning(f"No ticker data for {symbol}")
                return False

            bid = float(ticker['bid'])
            ask = float(ticker['ask'])

            if bid <= 0 or ask <= 0:
                return False

            spread_percent = ((ask - bid) / bid) * 100

            if spread_percent > self.spread_limit:
                logger.warning(f"{symbol} spread {spread_percent:.2f}% exceeds limit {self.spread_limit}%")
                return False

            return True

        except Exception as e:
            logger.error(f"Error validating spread for {symbol}: {e}")
            return False

    async def calculate_position_size(self, exchange: Union[BinanceExchange, BybitExchange],
                                      symbol: str, price: float) -> float:
        """Calculate position size with minimum notional validation"""
        try:
            # Base calculation
            quantity = self.position_size_usd / price

            # Get minimum notional for symbol
            if isinstance(exchange, BinanceExchange):
                if symbol in exchange.exchange_info:
                    filters = exchange.exchange_info[symbol].get('filters', [])
                    min_notional_filter = next(
                        (f for f in filters if f['filterType'] == 'MIN_NOTIONAL'),
                        None
                    )
                    if min_notional_filter:
                        min_notional = float(min_notional_filter.get('notional', 5))
                        if self.position_size_usd < min_notional:
                            logger.info(
                                f"Adjusting position size from ${self.position_size_usd} to ${min_notional} for {symbol}")
                            quantity = min_notional / price

            return quantity

        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return self.position_size_usd / price

    async def set_stop_loss(self, exchange: Union[BinanceExchange, BybitExchange],
                            signal: Signal, entry_price: float, position_id: Optional[int] = None) -> bool:
        """Set stop loss for position"""
        try:
            # Calculate SL price based on side
            # BUY = LONG, SELL = SHORT
            if signal.recommended_action == 'BUY':  # LONG position
                sl_price = entry_price * (1 - self.initial_sl_percent / 100)
            else:  # SHORT position
                sl_price = entry_price * (1 + self.initial_sl_percent / 100)

            # Set Stop Loss with retries
            for attempt in range(3):
                if attempt > 0:
                    await asyncio.sleep(self.delay_between_requests * (attempt + 1))

                if await exchange.set_stop_loss(signal.pair_symbol, sl_price):
                    self.stats['sl_set'] += 1
                    logger.info(f"✅ Stop Loss set at ${sl_price:.4f}")

                    # Update position in database
                    if position_id:
                        await self.update_position_stop_loss(position_id, sl_price)

                    # Log protection event
                    await self.log_protection_event(
                        position_id, signal.pair_symbol, signal.exchange_name,
                        "STOP_LOSS_SET", sl_price, True
                    )

                    return True

            self.stats['sl_failed'] += 1
            logger.error(f"❌ Failed to set Stop Loss for {signal.pair_symbol}")

            # Log failed protection event
            await self.log_protection_event(
                position_id, signal.pair_symbol, signal.exchange_name,
                "STOP_LOSS_FAILED", sl_price, False,
                "Failed after 3 attempts"
            )

            return False

        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            self.stats['sl_failed'] += 1
            return False

    async def process_signal(self, signal: Signal):
        """Process a single trading signal with complete error handling"""
        if signal.id in self.processing_signals:
            return

        self.processing_signals.add(signal.id)
        trade_id = None
        position_id = None

        try:
            logger.info(f"{'=' * 60}")
            logger.info(
                f"Processing signal #{signal.id}: {signal.pair_symbol} {signal.recommended_action} on {signal.exchange_name}")
            logger.info(f"Scores: Week={signal.score_week:.1f}%, Month={signal.score_month:.1f}%")

            # Select exchange
            exchange = self.binance if signal.exchange_name.lower() == 'binance' else self.bybit
            if not exchange:
                logger.error(f"Exchange {signal.exchange_name} not available")
                self.failed_signals.add(signal.id)
                return

            # Validate spread
            if not await self.validate_spread(exchange, signal.pair_symbol):
                logger.warning(f"Spread validation failed for {signal.pair_symbol}")
                if self.trading_mode == TradingMode.MAINNET:
                    self.failed_signals.add(signal.id)
                    return

            # Get current price and calculate position size
            ticker = await exchange.get_ticker(signal.pair_symbol)
            if not ticker or not ticker.get('price'):
                logger.error(f"No price data for {signal.pair_symbol}")
                self.failed_signals.add(signal.id)
                return

            current_price = float(ticker['price'])
            quantity = await self.calculate_position_size(exchange, signal.pair_symbol, current_price)

            # Set leverage
            await asyncio.sleep(self.delay_between_requests)
            leverage_set = await exchange.set_leverage(signal.pair_symbol, self.leverage)
            if not leverage_set and self.trading_mode == TradingMode.MAINNET:
                logger.error(f"Failed to set leverage for {signal.pair_symbol}")
                self.failed_signals.add(signal.id)
                return

            # Convert recommended_action to side (BUY=LONG, SELL=SHORT)
            side = 'BUY' if signal.recommended_action == 'BUY' else 'SELL'

            # Open position with retry logic
            order_result = None
            for attempt in range(self.order_retry_max):
                if attempt > 0:
                    await asyncio.sleep(self.order_retry_delay * attempt)

                logger.info(f"Opening position: {quantity:.6f} {signal.pair_symbol} @ ~${current_price:.4f}")
                order_response = await exchange.create_market_order(
                    signal.pair_symbol,
                    side,
                    quantity
                )

                if order_response and order_response.get('executed_qty', 0) > 0:
                    order_result = order_response
                    break

            if not order_result or order_result.get('executed_qty', 0) == 0:
                logger.error(f"Failed to open position after {self.order_retry_max} attempts")

                # Log failed trade
                trade = TradeRecord(
                    signal_id=signal.id,
                    trading_pair_id=signal.trading_pair_id,
                    symbol=signal.pair_symbol,
                    exchange=signal.exchange_name,
                    side=signal.recommended_action,
                    quantity=quantity,
                    executed_qty=0,
                    price=0,
                    status="FAILED",
                    error_message="Failed to execute order"
                )
                await self.log_trade(trade)

                self.failed_signals.add(signal.id)
                self.stats['positions_failed'] += 1
                await self.mark_signal_processed(signal.id)
                return

            # Position opened successfully
            executed_qty = order_result.get('executed_qty', 0)
            execution_price = order_result.get('price', 0)
            order_id = str(order_result.get('orderId', ''))

            logger.info(f"✅ Position opened: {executed_qty:.6f} {signal.pair_symbol} @ ${execution_price:.4f}")
            self.stats['positions_opened'] += 1

            # Log successful trade
            trade = TradeRecord(
                signal_id=signal.id,
                trading_pair_id=signal.trading_pair_id,
                symbol=signal.pair_symbol,
                exchange=signal.exchange_name,
                side=signal.recommended_action,
                quantity=quantity,
                executed_qty=executed_qty,
                price=execution_price,
                status="FILLED",
                order_id=order_id
            )
            trade_id = await self.log_trade(trade)

            # Log position
            if trade_id:
                position = PositionRecord(
                    trade_id=trade_id,
                    symbol=signal.pair_symbol,
                    exchange=signal.exchange_name,
                    side=signal.recommended_action,
                    quantity=executed_qty,
                    entry_price=execution_price,
                    status="OPEN"
                )
                position_id = await self.log_position(position)

            # Set Stop Loss
            await asyncio.sleep(self.delay_between_requests * 2)  # Wait for position to register
            sl_set = await self.set_stop_loss(exchange, signal, execution_price, position_id)

            # Mark signal as processed
            await self.mark_signal_processed(signal.id)
            self.stats['signals_processed'] += 1

            # Log position summary
            logger.info(f"Position Summary:")
            logger.info(f"  - Filled: {executed_qty:.6f}")
            logger.info(f"  - Entry: ${execution_price:.4f}")
            logger.info(f"  - SL Set: {sl_set}")
            logger.info(f"  - Trade ID: {trade_id}")
            logger.info(f"  - Position ID: {position_id}")

        except Exception as e:
            logger.error(f"Critical error processing signal {signal.id}: {e}", exc_info=True)

            # Log error trade
            error_trade = TradeRecord(
                signal_id=signal.id,
                trading_pair_id=signal.trading_pair_id,
                symbol=signal.pair_symbol,
                exchange=signal.exchange_name,
                side=signal.recommended_action,
                quantity=0,
                executed_qty=0,
                price=0,
                status="ERROR",
                error_message=str(e)
            )
            await self.log_trade(error_trade)

            self.failed_signals.add(signal.id)
            self.stats['positions_failed'] += 1
            await self.mark_signal_processed(signal.id)

        finally:
            self.processing_signals.discard(signal.id)

    async def process_signals_batch(self, signals: List[Signal]):
        """Process batch of signals sequentially with appropriate delays"""
        if not signals:
            return

        logger.info(f"📊 Processing batch of {len(signals)} signals")

        for i, signal in enumerate(signals):
            try:
                await self.process_signal(signal)

                # Add delay between trades
                if i < len(signals) - 1:
                    logger.info(f"⏳ Waiting {self.delay_between_trades}s before next trade...")
                    await asyncio.sleep(self.delay_between_trades)

            except Exception as e:
                logger.error(f"Error processing signal {signal.id}: {e}")
                continue

        logger.info(f"✅ Batch processing complete. Processed: {self.stats['signals_processed']}, "
                    f"Opened: {self.stats['positions_opened']}, Failed: {self.stats['positions_failed']}")

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

                    await conn.execute("""
                        INSERT INTO monitoring.performance_metrics (
                            metric_date, metric_hour, exchange,
                            signals_received, orders_placed, orders_filled, orders_failed,
                            positions_opened
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (metric_date, metric_hour, exchange)
                        DO UPDATE SET
                            signals_received = monitoring.performance_metrics.signals_received + EXCLUDED.signals_received,
                            orders_placed = monitoring.performance_metrics.orders_placed + EXCLUDED.orders_placed,
                            orders_filled = monitoring.performance_metrics.orders_filled + EXCLUDED.orders_filled,
                            orders_failed = monitoring.performance_metrics.orders_failed + EXCLUDED.orders_failed,
                            positions_opened = monitoring.performance_metrics.positions_opened + EXCLUDED.positions_opened
                    """,
                                       current_hour.date(),
                                       current_hour.hour,
                                       exchange_name,
                                       0,  # Will be updated based on actual signals
                                       0,  # Will be updated based on actual orders
                                       0,  # Will be updated based on actual fills
                                       0,  # Will be updated based on actual failures
                                       0  # Will be updated based on actual positions
                                       )
        except Exception as e:
            logger.error(f"Error updating performance metrics: {e}")

    async def log_statistics(self):
        """Log trading statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        hours = runtime.total_seconds() / 3600

        logger.info("=" * 60)
        logger.info("TRADING STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Runtime: {hours:.2f} hours")
        logger.info(f"Signals Processed: {self.stats['signals_processed']}")
        logger.info(f"Positions Opened: {self.stats['positions_opened']}")
        logger.info(f"Positions Failed: {self.stats['positions_failed']}")
        logger.info(f"Stop Losses Set: {self.stats['sl_set']}")
        logger.info(f"Stop Losses Failed: {self.stats['sl_failed']}")

        if self.stats['signals_processed'] > 0:
            success_rate = (self.stats['positions_opened'] / self.stats['signals_processed']) * 100
            logger.info(f"Success Rate: {success_rate:.1f}%")

        logger.info("=" * 60)

    async def run(self):
        """Main trading loop with comprehensive error handling"""
        logger.info("🚀 Starting Main Trader v7.1 - PRODUCTION READY")
        logger.info(f"Mode: {self.trading_mode.value}")

        try:
            await self.initialize()
        except Exception as e:
            logger.critical(f"FATAL: System initialization failed: {e}")
            return

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self.shutdown_event.set())

        # Statistics logging task
        async def periodic_stats():
            while not self.shutdown_event.is_set():
                await asyncio.sleep(300)  # Log stats every 5 minutes
                await self.log_statistics()
                await self.update_performance_metrics()

        stats_task = asyncio.create_task(periodic_stats())

        try:
            while not self.shutdown_event.is_set():
                try:
                    # Health check and reconnection
                    await self._check_and_reconnect()

                    # Fetch and process signals
                    signals = await self.get_unprocessed_signals()
                    if signals:
                        await self.process_signals_batch(signals)
                    else:
                        logger.debug("No new signals found")

                    # Wait before next check
                    await asyncio.sleep(self.check_interval)

                except asyncio.CancelledError:
                    logger.info("Main loop cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    await self._log_system_health("main_trader", "ERROR", str(e))
                    await asyncio.sleep(10)

        finally:
            logger.info("Shutdown initiated...")
            stats_task.cancel()
            await self._log_system_health("main_trader", "STOPPED")
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up resources...")

        # Log final statistics
        await self.log_statistics()

        # Close database pool
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Database pool closed")

        # Close exchange connections
        if self.binance:
            await self.binance.close()
            logger.info("Binance connection closed")

        if self.bybit:
            await self.bybit.close()
            logger.info("Bybit connection closed")

        logger.info("✅ Cleanup complete. Goodbye!")


async def main():
    """Entry point"""
    trader = MainTrader()
    await trader.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)