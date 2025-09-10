#!/usr/bin/env python3
"""
Main Trading Script - Production Ready Version
High-performance async trading with proper error handling
"""

import asyncio
import asyncpg
import logging
import os
import sys
import signal
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum
import traceback
from dotenv import load_dotenv

# Add parent dir to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

load_dotenv()

# Setup logging with rotation
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO)

# File handler with rotation
file_handler = RotatingFileHandler(
    'trader.log',
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5
)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


class OrderStatus(Enum):
    """Order status enum"""
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class Signal:
    """Trading signal data class"""
    id: int
    symbol: str
    exchange_id: int
    exchange_name: str
    score_week: float
    score_month: float
    timestamp: datetime
    trading_pair_id: int


@dataclass
class OrderResult:
    """Order execution result"""
    success: bool
    order_id: Optional[str] = None
    symbol: Optional[str] = None
    side: str = "BUY"
    quantity: float = 0.0
    executed_qty: float = 0.0
    price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    error_message: Optional[str] = None
    retry_count: int = 0


class MainTrader:
    """Production-ready async trader with concurrent signal processing"""

    def __init__(self):
        # Database config
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

        # Trading config - FIXED: Using POSITION_SIZE_USD
        self.min_score_week = float(os.getenv('MIN_SCORE_WEEK', '70'))
        self.min_score_month = float(os.getenv('MIN_SCORE_MONTH', '80'))
        self.position_size_usd = float(os.getenv('POSITION_SIZE_USD', '10'))  # Fixed USD amount
        self.leverage = int(os.getenv('LEVERAGE', '10'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.signal_time_window = int(os.getenv('SIGNAL_TIME_WINDOW', '5'))

        # Performance settings
        self.max_concurrent_orders = int(os.getenv('MAX_CONCURRENT_ORDERS', '10'))
        self.order_retry_max = int(os.getenv('ORDER_RETRY_MAX', '3'))
        self.order_retry_delay = float(os.getenv('ORDER_RETRY_DELAY', '1.0'))

        # Risk management
        self.max_daily_trades = int(os.getenv('MAX_DAILY_TRADES', '5000'))
        self.max_daily_loss_usd = float(os.getenv('MAX_DAILY_LOSS_USD', '5000'))

        # Spread limits
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.spread_limit = 100.0 if self.testnet else 0.5

        # State management
        self.binance = None
        self.bybit = None
        self.db_pool = None
        self.processing_signals: Set[int] = set()
        self.daily_stats = {
            'trades_count': 0,
            'total_loss': 0.0,
            'last_reset': datetime.now(timezone.utc).date()
        }
        self.shutdown_event = asyncio.Event()

        logger.info(f"🚀 Trader Configuration:")
        logger.info(f"  - Mode: {'TESTNET' if self.testnet else 'PRODUCTION'}")
        logger.info(f"  - Position Size: ${self.position_size_usd} USD (fixed)")
        logger.info(f"  - Leverage: {self.leverage}x")
        logger.info(f"  - Min Scores: Week={self.min_score_week}%, Month={self.min_score_month}%")
        logger.info(f"  - Max Concurrent Orders: {self.max_concurrent_orders}")
        logger.info(f"  - Signal Window: {self.signal_time_window} minutes")

    async def initialize(self):
        """Initialize database and exchange connections with retry logic"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                # Create database pool
                self.db_pool = await asyncpg.create_pool(
                    **self.db_config,
                    min_size=2,
                    max_size=20,
                    command_timeout=10
                )
                logger.info("✅ Database connected")

                # Initialize exchanges concurrently
                tasks = []

                if os.getenv('BINANCE_API_KEY'):
                    tasks.append(self._init_binance())
                else:
                    logger.warning("⚠️ BINANCE_API_KEY not configured")

                if os.getenv('BYBIT_API_KEY'):
                    tasks.append(self._init_bybit())
                else:
                    logger.warning("⚠️ BYBIT_API_KEY not configured")

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"Exchange init error: {result}")

                # Verify at least one exchange is available
                if not self.binance and not self.bybit:
                    raise Exception("No exchanges available!")

                return True

            except Exception as e:
                logger.error(f"Initialization attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    raise

    async def _init_binance(self):
        """Initialize Binance with error handling"""
        try:
            self.binance = BinanceExchange({
                'api_key': os.getenv('BINANCE_API_KEY'),
                'api_secret': os.getenv('BINANCE_API_SECRET'),
                'testnet': self.testnet
            })
            await self.binance.initialize()

            # Test with balance check
            balance = await self.binance.get_balance()
            logger.info(f"✅ Binance connected - Balance: ${balance:.2f}")

        except Exception as e:
            logger.error(f"❌ Binance initialization failed: {e}")
            self.binance = None
            raise

    async def _init_bybit(self):
        """Initialize Bybit with error handling"""
        try:
            self.bybit = BybitExchange({
                'api_key': os.getenv('BYBIT_API_KEY'),
                'api_secret': os.getenv('BYBIT_API_SECRET'),
                'testnet': self.testnet
            })
            await self.bybit.initialize()

            # Test with balance check
            balance = await self.bybit.get_balance()
            logger.info(f"✅ Bybit connected - Balance: ${balance:.2f}")

        except Exception as e:
            logger.error(f"❌ Bybit initialization failed: {e}")
            self.bybit = None
            raise

    async def get_unprocessed_signals(self) -> List[Signal]:
        """Get new signals with proper filtering"""
        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=self.signal_time_window)

        query = """
            SELECT 
                sh.id,
                sh.timestamp,
                sh.score_week,
                sh.score_month,
                sh.trading_pair_id,
                tp.exchange_id,
                tp.pair_symbol as symbol,
                CASE 
                    WHEN tp.exchange_id = 1 THEN 'Binance'
                    WHEN tp.exchange_id = 2 THEN 'Bybit'
                    ELSE 'Unknown'
                END as exchange_name
            FROM fas.scoring_history sh
            JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
            WHERE sh.id NOT IN (
                SELECT signal_id FROM monitoring.trades 
                WHERE signal_id IS NOT NULL
                UNION
                SELECT unnest($1::int[])  -- Exclude currently processing
            )
            AND sh.score_week >= $2
            AND sh.score_month >= $3
            AND sh.created_at > $4
            AND tp.is_active = true
            AND tp.exchange_id IN (1, 2)
            ORDER BY 
                (sh.score_week + sh.score_month) DESC,  -- Best scores first
                sh.created_at DESC
            LIMIT $5
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(
                    query,
                    list(self.processing_signals),
                    self.min_score_week,
                    self.min_score_month,
                    time_threshold,
                    self.max_concurrent_orders
                )

                signals = []
                for row in rows:
                    signals.append(Signal(
                        id=row['id'],
                        symbol=row['symbol'],
                        exchange_id=row['exchange_id'],
                        exchange_name=row['exchange_name'],
                        score_week=float(row['score_week']),
                        score_month=float(row['score_month']),
                        timestamp=row['timestamp'],
                        trading_pair_id=row['trading_pair_id']
                    ))

                if signals:
                    binance_count = sum(1 for s in signals if s.exchange_id == 1)
                    bybit_count = sum(1 for s in signals if s.exchange_id == 2)
                    # Calculate how old the newest signal is
                    newest_signal_time = max(s.timestamp for s in signals)
                    if newest_signal_time.tzinfo is None:
                        newest_signal_time = newest_signal_time.replace(tzinfo=timezone.utc)
                    age_minutes = (datetime.now(timezone.utc) - newest_signal_time).total_seconds() / 60
                    logger.info(f"📈 Found {len(signals)} new signals to process")
                    logger.info(f"   Distribution: Binance={binance_count}, Bybit={bybit_count}")
                    logger.info(f"   Newest signal age: {age_minutes:.1f} minutes")

                return signals

        except Exception as e:
            logger.error(f"Error fetching signals: {e}")
            return []

    async def check_daily_limits(self) -> bool:
        """Check if daily trading limits are exceeded"""
        current_date = datetime.now(timezone.utc).date()

        # Reset daily stats if new day
        if current_date != self.daily_stats['last_reset']:
            self.daily_stats = {
                'trades_count': 0,
                'total_loss': 0.0,
                'last_reset': current_date
            }

        # Check trade count limit
        if self.daily_stats['trades_count'] >= self.max_daily_trades:
            logger.warning(f"Daily trade limit reached: {self.max_daily_trades}")
            return False

        # Check loss limit
        if self.daily_stats['total_loss'] >= self.max_daily_loss_usd:
            logger.warning(f"Daily loss limit reached: ${self.max_daily_loss_usd}")
            return False

        return True

    async def process_signal(self, signal: Signal) -> bool:
        """Process a single signal with full error handling"""
        self.processing_signals.add(signal.id)

        try:
            logger.info(f"🎯 Processing signal #{signal.id}: {signal.symbol} on {signal.exchange_name}")
            logger.info(f"   Scores: Week={signal.score_week:.1f}%, Month={signal.score_month:.1f}%")

            # Check daily limits
            if not await self.check_daily_limits():
                logger.warning(f"Skipping signal #{signal.id} due to daily limits")
                return False

            # Select exchange
            exchange = self._get_exchange(signal.exchange_id)
            if not exchange:
                logger.error(f"Exchange not available for signal #{signal.id}")
                await self._log_failed_trade(signal, "Exchange not available")
                return False

            # Check spread
            if not await self._check_spread(exchange, signal.symbol):
                await self._log_failed_trade(signal, "Spread too high")
                return False

            # Create order with retries
            order_result = await self._create_order_with_retry(exchange, signal)

            if order_result.success:
                # Log successful trade
                await self._log_successful_trade(signal, order_result)

                # Update daily stats
                self.daily_stats['trades_count'] += 1

                logger.info(f"✅ Signal #{signal.id} processed successfully")
                return True
            else:
                # Log failed trade
                await self._log_failed_trade(signal, order_result.error_message)
                logger.error(f"❌ Signal #{signal.id} failed: {order_result.error_message}")
                return False

        except Exception as e:
            logger.error(f"Error processing signal #{signal.id}: {e}")
            logger.error(traceback.format_exc())
            await self._log_failed_trade(signal, str(e))
            return False

        finally:
            self.processing_signals.discard(signal.id)

    def _get_exchange(self, exchange_id: int):
        """Get exchange instance by ID"""
        if exchange_id == 1:
            return self.binance
        elif exchange_id == 2:
            return self.bybit
        return None

    async def _check_spread(self, exchange, symbol: str) -> bool:
        """Check if spread is acceptable"""
        try:
            ticker = await exchange.get_ticker(symbol)
            if not ticker:
                logger.warning(f"No ticker data for {symbol}")
                return False

            bid = float(ticker.get('bid', 0))
            ask = float(ticker.get('ask', 0))
            price = float(ticker.get('price', 0))

            # Calculate spread
            if bid > 0 and ask > 0:
                spread_pct = ((ask - bid) / bid) * 100

                if spread_pct <= self.spread_limit:
                    logger.info(f"✅ {symbol} spread OK: {spread_pct:.3f}%")
                    return True
                else:
                    logger.warning(f"❌ {symbol} spread too high: {spread_pct:.3f}% > {self.spread_limit}%")
                    return False
            elif price > 0 and self.testnet:
                logger.info(f"⚠️ {symbol} using price without orderbook (testnet)")
                return True
            else:
                logger.warning(f"❌ {symbol} has no valid price data")
                return False

        except Exception as e:
            logger.error(f"Error checking spread: {e}")
            return False

    async def _create_order_with_retry(self, exchange, signal: Signal) -> OrderResult:
        """Create order with retry logic and size adjustments"""

        for attempt in range(self.order_retry_max):
            try:
                # Get current balance
                balance = await exchange.get_balance()
                if balance <= 0:
                    return OrderResult(
                        success=False,
                        error_message="Insufficient balance"
                    )

                # Use fixed USD position size
                position_size_usd = self.position_size_usd

                # Ensure minimum order value
                min_order_value = 10.0 if signal.exchange_name == 'Bybit' else 5.0
                if position_size_usd < min_order_value:
                    position_size_usd = min_order_value

                # Don't exceed available balance
                if position_size_usd > balance * 0.95:  # Leave 5% margin
                    position_size_usd = balance * 0.95
                    logger.warning(f"Adjusted position size to available balance: ${position_size_usd:.2f}")

                # Get current price
                ticker = await exchange.get_ticker(signal.symbol)
                price = float(ticker.get('price', 0))

                if price <= 0:
                    return OrderResult(
                        success=False,
                        error_message="Invalid price"
                    )

                # Calculate quantity
                quantity = position_size_usd / price

                logger.info(
                    f"Order attempt {attempt + 1}: ${position_size_usd:.2f} = {quantity:.6f} {signal.symbol} @ ${price:.4f}")

                # Set leverage with fallback
                leverage_set = await self._set_leverage_with_fallback(exchange, signal.symbol)
                if not leverage_set:
                    return OrderResult(
                        success=False,
                        error_message="Failed to set leverage"
                    )

                # Try to create order
                order = await exchange.create_market_order(signal.symbol, 'BUY', quantity)

                if order:
                    # Verify order execution
                    executed_qty = order.get('quantity', 0)
                    avg_price = order.get('price', price)

                    if executed_qty > 0:
                        return OrderResult(
                            success=True,
                            order_id=str(order.get('orderId')),
                            symbol=signal.symbol,
                            side='BUY',
                            quantity=quantity,
                            executed_qty=executed_qty,
                            price=avg_price,
                            status=OrderStatus.FILLED
                        )
                    else:
                        logger.warning(f"Order created but no execution quantity")

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Order attempt {attempt + 1} failed: {error_msg}")

                # Check if error is recoverable
                if any(x in error_msg.lower() for x in ['position', 'exceeded', 'allowable']):
                    # Try with smaller size
                    position_size_usd *= 0.5
                    logger.info(f"Retrying with reduced size: ${position_size_usd:.2f}")
                elif 'balance' in error_msg.lower() or 'insufficient' in error_msg.lower():
                    return OrderResult(
                        success=False,
                        error_message="Insufficient balance",
                        retry_count=attempt + 1
                    )
                elif 'invalid symbol' in error_msg.lower():
                    return OrderResult(
                        success=False,
                        error_message=f"Invalid symbol: {signal.symbol}",
                        retry_count=attempt + 1
                    )

                if attempt < self.order_retry_max - 1:
                    await asyncio.sleep(self.order_retry_delay * (attempt + 1))

        return OrderResult(
            success=False,
            error_message=f"Failed after {self.order_retry_max} attempts",
            retry_count=self.order_retry_max
        )

    async def _set_leverage_with_fallback(self, exchange, symbol: str) -> bool:
        """Set leverage with fallback to lower values"""
        leverage_options = [self.leverage, 10, 5, 3, 2, 1]

        for leverage in leverage_options:
            try:
                await exchange.set_leverage(symbol, leverage)
                if leverage != self.leverage:
                    logger.warning(f"Using reduced leverage {leverage}x (requested {self.leverage}x)")
                return True
            except Exception as e:
                if leverage == leverage_options[-1]:
                    logger.error(f"Could not set any leverage for {symbol}: {e}")
                    return False
                continue

        return False

    async def _log_successful_trade(self, signal: Signal, order_result: OrderResult):
        """Log successful trade to database"""
        query = """
            INSERT INTO monitoring.trades (
                signal_id,
                trading_pair_id,
                symbol,
                exchange,
                side,
                quantity,
                executed_qty,
                price,
                status,
                order_id,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    query,
                    signal.id,
                    signal.trading_pair_id,
                    signal.symbol,
                    signal.exchange_name.lower(),
                    order_result.side,
                    order_result.quantity,
                    order_result.executed_qty,
                    order_result.price,
                    order_result.status.value,
                    order_result.order_id,
                    datetime.now(timezone.utc)
                )
                logger.info(f"Trade logged to database - Order ID: {order_result.order_id}")
        except Exception as e:
            logger.error(f"Failed to log trade to database: {e}")

    async def _log_failed_trade(self, signal: Signal, error_message: str):
        """Log failed trade attempt to database"""
        query = """
            INSERT INTO monitoring.trades (
                signal_id,
                trading_pair_id,
                symbol,
                exchange,
                side,
                status,
                error_message,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    query,
                    signal.id,
                    signal.trading_pair_id,
                    signal.symbol,
                    signal.exchange_name.lower(),
                    'BUY',
                    OrderStatus.FAILED.value,
                    error_message[:500],  # Limit error message length
                    datetime.now(timezone.utc)
                )
        except Exception as e:
            logger.error(f"Failed to log failed trade: {e}")

    async def process_signals_batch(self, signals: List[Signal]):
        """Process multiple signals concurrently"""
        if not signals:
            return

        # Create tasks for concurrent processing
        tasks = []
        for signal in signals:
            if signal.id not in self.processing_signals:
                task = asyncio.create_task(self.process_signal(signal))
                tasks.append(task)

        if tasks:
            logger.info(f"⚡ Processing {len(tasks)} signals concurrently")
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Log results
            success_count = sum(1 for r in results if r is True)
            failure_count = sum(1 for r in results if r is False or isinstance(r, Exception))

            logger.info(f"📊 Batch results: {success_count} success, {failure_count} failed")

    async def health_check(self):
        """Periodic health check"""
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(60)  # Check every minute

                # Check database connection
                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        await conn.fetchval("SELECT 1")

                # Check exchange connections
                checks = []
                if self.binance:
                    checks.append(("Binance", self.binance.get_balance()))
                if self.bybit:
                    checks.append(("Bybit", self.bybit.get_balance()))

                for name, check in checks:
                    try:
                        balance = await check
                        logger.debug(f"Health check - {name}: ${balance:.2f}")
                    except Exception as e:
                        logger.error(f"Health check failed - {name}: {e}")
                        # Try to reconnect
                        if name == "Binance":
                            await self._init_binance()
                        elif name == "Bybit":
                            await self._init_bybit()

            except Exception as e:
                logger.error(f"Health check error: {e}")

    async def run(self):
        """Main trading loop with concurrent processing"""
        logger.info("🚀 Starting Production Trading System")

        try:
            await self.initialize()

            # Start health check task
            health_task = asyncio.create_task(self.health_check())

            while not self.shutdown_event.is_set():
                try:
                    # Get new signals
                    signals = await self.get_unprocessed_signals()

                    # Process signals concurrently
                    if signals:
                        await self.process_signals_batch(signals)

                    # Wait before next check
                    await asyncio.sleep(self.check_interval)

                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(10)

            # Cancel health check
            health_task.cancel()

        except KeyboardInterrupt:
            logger.info("⚠️ Shutdown signal received")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up resources...")

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

        logger.info("✅ Cleanup complete")

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.shutdown_event.set()


async def main():
    """Entry point with proper signal handling"""
    trader = MainTrader()

    # Setup signal handlers
    signal.signal(signal.SIGINT, trader.handle_shutdown)
    signal.signal(signal.SIGTERM, trader.handle_shutdown)

    await trader.run()


if __name__ == "__main__":
    asyncio.run(main())