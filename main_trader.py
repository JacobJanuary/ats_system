#!/usr/bin/env python3
"""
Main Trading Script - FIXED VERSION 3.0
Исправлены проблемы с Bybit и обработкой пустых значений
"""

import asyncio
import asyncpg
import logging
import os
import sys
import signal
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import traceback
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

load_dotenv()

from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO)

file_handler = RotatingFileHandler(
    'trader.log',
    maxBytes=10 * 1024 * 1024,
    backupCount=5
)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
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


@dataclass
class Signal:
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
    """Production-ready async trader with improved error handling"""

    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

        self.min_score_week = float(os.getenv('MIN_SCORE_WEEK', '70'))
        self.min_score_month = float(os.getenv('MIN_SCORE_MONTH', '80'))
        self.position_size_usd = float(os.getenv('POSITION_SIZE_USD', '10'))
        self.leverage = int(os.getenv('LEVERAGE', '10'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.signal_time_window = int(os.getenv('SIGNAL_TIME_WINDOW', '5'))

        self.max_concurrent_orders = int(os.getenv('MAX_CONCURRENT_ORDERS', '10'))
        self.order_retry_max = int(os.getenv('ORDER_RETRY_MAX', '3'))
        self.order_retry_delay = float(os.getenv('ORDER_RETRY_DELAY', '1.0'))

        self.max_daily_trades = int(os.getenv('MAX_DAILY_TRADES', '5000'))
        self.max_daily_loss_usd = float(os.getenv('MAX_DAILY_LOSS_USD', '5000'))
        self.min_balance_reserve = float(os.getenv('MIN_BALANCE_RESERVE', '100'))

        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.spread_limit = 100.0 if self.testnet else 0.5

        self.binance = None
        self.bybit = None
        self.db_pool = None
        self.processing_signals: Set[int] = set()
        self.failed_symbols: Dict[str, datetime] = {}
        self.symbol_cooldown_minutes = 60

        self.daily_stats = {
            'trades_count': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'total_volume': 0.0,
            'total_loss': 0.0,
            'last_reset': datetime.now(timezone.utc).date()
        }

        self.shutdown_event = asyncio.Event()
        self._log_configuration()

    def _log_configuration(self):
        logger.info("=" * 60)
        logger.info("Trading System Configuration")
        logger.info("=" * 60)
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'PRODUCTION'}")
        logger.info(f"Position Size: ${self.position_size_usd} USD")
        logger.info(f"Leverage: {self.leverage}x")
        logger.info(f"Min Scores: Week={self.min_score_week}%, Month={self.min_score_month}%")
        logger.info(f"Max Concurrent Orders: {self.max_concurrent_orders}")
        logger.info(f"Signal Window: {self.signal_time_window} minutes")
        logger.info(f"Spread Limit: {self.spread_limit}%")
        logger.info("=" * 60)

    async def initialize(self):
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                self.db_pool = await asyncpg.create_pool(
                    **self.db_config,
                    min_size=2,
                    max_size=20,
                    command_timeout=10
                )
                logger.info("✅ Database connected")

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
        try:
            self.binance = BinanceExchange({
                'api_key': os.getenv('BINANCE_API_KEY'),
                'api_secret': os.getenv('BINANCE_API_SECRET'),
                'testnet': self.testnet
            })
            await self.binance.initialize()

            balance = await self.binance.get_balance()
            logger.info(f"✅ Binance connected - Balance: ${balance:.2f}")

        except Exception as e:
            logger.error(f"❌ Binance initialization failed: {e}")
            self.binance = None
            raise

    async def _init_bybit(self):
        try:
            self.bybit = BybitExchange({
                'api_key': os.getenv('BYBIT_API_KEY'),
                'api_secret': os.getenv('BYBIT_API_SECRET'),
                'testnet': self.testnet
            })
            await self.bybit.initialize()

            balance = await self.bybit.get_balance()
            logger.info(f"✅ Bybit connected - Balance: ${balance:.2f}")

        except Exception as e:
            logger.error(f"❌ Bybit initialization failed: {e}")
            self.bybit = None
            raise

    def _is_symbol_in_cooldown(self, symbol: str) -> bool:
        if symbol not in self.failed_symbols:
            return False

        cooldown_until = self.failed_symbols[symbol] + timedelta(minutes=self.symbol_cooldown_minutes)
        if datetime.now(timezone.utc) < cooldown_until:
            return True
        else:
            del self.failed_symbols[symbol]
            return False

    async def get_unprocessed_signals(self) -> List[Signal]:
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
                SELECT unnest($1::int[])
            )
            AND sh.score_week >= $2
            AND sh.score_month >= $3
            AND sh.created_at > $4
            AND tp.is_active = true
            AND tp.exchange_id IN (1, 2)
            ORDER BY 
                (sh.score_week + sh.score_month) DESC,
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
                    symbol = row['symbol']

                    if self._is_symbol_in_cooldown(symbol):
                        logger.debug(f"Skipping {symbol} - in cooldown")
                        continue

                    signals.append(Signal(
                        id=row['id'],
                        symbol=symbol,
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
        current_date = datetime.now(timezone.utc).date()

        if current_date != self.daily_stats['last_reset']:
            self.daily_stats = {
                'trades_count': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'total_volume': 0.0,
                'total_loss': 0.0,
                'last_reset': current_date
            }
            logger.info("📊 Daily statistics reset")

        if self.daily_stats['trades_count'] >= self.max_daily_trades:
            logger.warning(f"Daily trade limit reached: {self.max_daily_trades}")
            return False

        if self.daily_stats['total_loss'] >= self.max_daily_loss_usd:
            logger.warning(f"Daily loss limit reached: ${self.max_daily_loss_usd}")
            return False

        return True

    async def _check_spread(self, exchange, symbol: str) -> bool:
        """ИСПРАВЛЕНО: Улучшенная обработка пустых значений"""
        try:
            ticker = await exchange.get_ticker(symbol)
            if not ticker or not ticker.get('price'):
                logger.warning(f"No ticker data for {symbol}")
                if self.testnet:
                    logger.info(f"⚠️ {symbol} - allowing trade on testnet despite missing ticker")
                    return True
                return False

            # ВАЖНО: Проверяем, что значения не пустые строки
            bid = ticker.get('bid', 0)
            ask = ticker.get('ask', 0)
            price = ticker.get('price', 0)

            # Конвертируем в float с проверкой
            try:
                bid = float(bid) if bid and bid != '' else 0
                ask = float(ask) if ask and ask != '' else 0
                price = float(price) if price and price != '' else 0
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting ticker values for {symbol}: bid={bid}, ask={ask}, price={price}")
                if self.testnet:
                    logger.info(f"Allowing {symbol} on testnet despite conversion error")
                    return True
                return False

            if bid > 0 and ask > 0:
                spread_pct = ((ask - bid) / bid) * 100
                is_synthetic = abs(spread_pct - 0.1) < 0.01

                if is_synthetic:
                    logger.info(f"⚠️ {symbol} using synthetic spread")
                    if self.testnet:
                        return True
                    elif spread_pct <= 1.0:
                        return True
                    else:
                        return False

                if spread_pct <= self.spread_limit:
                    logger.info(f"✅ {symbol} spread OK: {spread_pct:.3f}%")
                    return True
                else:
                    logger.warning(f"❌ {symbol} spread too high: {spread_pct:.3f}% > {self.spread_limit}%")
                    return False
            elif price > 0:
                logger.warning(f"⚠️ {symbol} has only price data: ${price:.4f}")
                if self.testnet:
                    logger.info(f"   Allowing trade on testnet")
                    return True
                else:
                    logger.warning(f"   Rejecting trade on mainnet (no orderbook)")
                    return False
            else:
                logger.warning(f"❌ {symbol} has no valid price data")
                return False

        except Exception as e:
            logger.error(f"Error checking spread for {symbol}: {e}")
            if self.testnet:
                logger.info(f"Allowing {symbol} on testnet despite error")
                return True
            return False

    async def _validate_order_size(self, exchange, symbol: str, position_size_usd: float) -> Tuple[bool, float]:
        """ИСПРАВЛЕНО: Улучшенная валидация размера ордера"""
        try:
            ticker = await exchange.get_ticker(symbol)
            if not ticker:
                logger.error(f"No ticker for {symbol}")
                return False, 0

            price = ticker.get('price', 0)

            # Проверка и конвертация цены
            try:
                price = float(price) if price and price != '' else 0
            except (ValueError, TypeError):
                logger.error(f"Invalid price for {symbol}: {price}")
                return False, 0

            if price <= 0:
                logger.error(f"Invalid price for {symbol}: {price}")
                return False, 0

            quantity = position_size_usd / price

            # Минимальные размеры ордеров для бирж
            if exchange == self.binance:
                min_notional = 5.0
                if self.testnet:
                    min_notional = 10.0
            else:  # Bybit
                min_notional = 10.0

            order_value = quantity * price
            if order_value < min_notional:
                logger.debug(f"{symbol}: Order value ${order_value:.2f} < minimum ${min_notional}")
                adjusted_size = min_notional * 1.1
                return True, adjusted_size

            return True, position_size_usd

        except Exception as e:
            logger.error(f"Error validating order size: {e}")
            return False, 0

    async def _create_order_with_retry(self, exchange, signal: Signal) -> OrderResult:
        """ИСПРАВЛЕНО: Улучшенная обработка ошибок при создании ордера"""

        for attempt in range(self.order_retry_max):
            try:
                # Получаем баланс с проверкой
                balance = await exchange.get_balance()

                # ВАЖНО: Проверяем, что баланс не пустая строка
                try:
                    balance = float(balance) if balance and balance != '' else 0
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid balance value: {balance}")
                    return OrderResult(
                        success=False,
                        error_message=f"Invalid balance: {balance}"
                    )

                if balance <= self.min_balance_reserve:
                    return OrderResult(
                        success=False,
                        error_message=f"Insufficient balance: ${balance:.2f}"
                    )

                valid, adjusted_size = await self._validate_order_size(
                    exchange,
                    signal.symbol,
                    self.position_size_usd
                )

                if not valid:
                    return OrderResult(
                        success=False,
                        error_message="Invalid order size"
                    )

                position_size_usd = adjusted_size

                max_available = balance - self.min_balance_reserve
                if position_size_usd > max_available:
                    if max_available < 10:
                        return OrderResult(
                            success=False,
                            error_message=f"Insufficient available balance: ${max_available:.2f}"
                        )
                    position_size_usd = max_available
                    logger.warning(f"Adjusted position to available balance: ${position_size_usd:.2f}")

                # Получаем текущую цену с проверкой
                ticker = await exchange.get_ticker(signal.symbol)
                if not ticker:
                    logger.error(f"No ticker for {signal.symbol}")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(self.order_retry_delay * (attempt + 1))
                        continue
                    return OrderResult(
                        success=False,
                        error_message="No ticker data"
                    )

                price = ticker.get('price', 0)

                # ВАЖНО: Проверяем и конвертируем цену
                try:
                    price = float(price) if price and price != '' else 0
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid price for {signal.symbol}: {price}")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(self.order_retry_delay * (attempt + 1))
                        continue
                    return OrderResult(
                        success=False,
                        error_message=f"Invalid price: {price}"
                    )

                if price <= 0:
                    logger.error(f"Invalid price for {signal.symbol}: {price}")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(self.order_retry_delay * (attempt + 1))
                        continue
                    return OrderResult(
                        success=False,
                        error_message=f"Invalid price: {price}"
                    )

                quantity = position_size_usd / price

                logger.info(f"📝 Order attempt {attempt + 1}/{self.order_retry_max}:")
                logger.info(f"   ${position_size_usd:.2f} = {quantity:.6f} {signal.symbol} @ ${price:.4f}")

                leverage_set = await exchange.set_leverage(signal.symbol, self.leverage)
                if not leverage_set:
                    logger.warning(f"Could not set leverage for {signal.symbol}, continuing anyway")

                # Создаем market ордер
                order = await exchange.create_market_order(signal.symbol, 'BUY', quantity)

                if order and order.get('quantity', 0) > 0:
                    executed_qty = order.get('quantity', 0)
                    avg_price = order.get('price', price)

                    self.daily_stats['successful_trades'] += 1
                    self.daily_stats['total_volume'] += executed_qty * avg_price

                    logger.info(f"✅ Order filled: {executed_qty:.6f} @ ${avg_price:.4f}")

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
                    logger.warning(f"Order attempt {attempt + 1} failed: No execution")

                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(self.order_retry_delay * (attempt + 1))

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Order attempt {attempt + 1} error: {error_msg}")

                error_lower = error_msg.lower()

                if 'insufficient' in error_lower or 'balance' in error_lower:
                    return OrderResult(
                        success=False,
                        error_message="Insufficient balance",
                        retry_count=attempt + 1
                    )
                elif 'invalid symbol' in error_lower:
                    self.failed_symbols[signal.symbol] = datetime.now(timezone.utc)
                    return OrderResult(
                        success=False,
                        error_message=f"Invalid symbol: {signal.symbol}",
                        retry_count=attempt + 1
                    )
                elif 'unable to fill' in error_lower or '-2020' in error_msg:
                    # Специфичная ошибка Binance о недостаточной ликвидности
                    logger.warning(f"{signal.symbol}: No liquidity on {signal.exchange_name}")
                    # На testnet это нормально, продолжаем попытки
                    if self.testnet and attempt < self.order_retry_max - 1:
                        await asyncio.sleep(self.order_retry_delay * (attempt + 1))
                        continue
                elif 'leverage' in error_lower:
                    logger.warning("Leverage error, retrying without leverage change")

                if attempt < self.order_retry_max - 1:
                    await asyncio.sleep(self.order_retry_delay * (attempt + 1))

        self.daily_stats['failed_trades'] += 1
        self.failed_symbols[signal.symbol] = datetime.now(timezone.utc)

        return OrderResult(
            success=False,
            error_message=f"Failed after {self.order_retry_max} attempts",
            retry_count=self.order_retry_max
        )

    async def process_signal(self, signal: Signal) -> bool:
        self.processing_signals.add(signal.id)

        try:
            logger.info(f"🎯 Processing signal #{signal.id}: {signal.symbol} on {signal.exchange_name}")
            logger.info(f"   Scores: Week={signal.score_week:.1f}%, Month={signal.score_month:.1f}%")

            if not await self.check_daily_limits():
                logger.warning(f"Skipping signal #{signal.id} due to daily limits")
                return False

            exchange = self._get_exchange(signal.exchange_id)
            if not exchange:
                logger.error(f"Exchange not available for signal #{signal.id}")
                await self._log_failed_trade(signal, "Exchange not available")
                return False

            if not await self._check_spread(exchange, signal.symbol):
                await self._log_failed_trade(signal, "Spread too high or no price data")
                return False

            order_result = await self._create_order_with_retry(exchange, signal)

            if order_result.success:
                await self._log_successful_trade(signal, order_result)
                self.daily_stats['trades_count'] += 1
                logger.info(f"✅ Signal #{signal.id} processed successfully")
                return True
            else:
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
        if exchange_id == 1:
            return self.binance
        elif exchange_id == 2:
            return self.bybit
        return None

    async def _log_successful_trade(self, signal: Signal, order_result: OrderResult):
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
                logger.info(f"📝 Trade logged to database - Order ID: {order_result.order_id}")
        except Exception as e:
            logger.error(f"Failed to log trade to database: {e}")

    async def _log_failed_trade(self, signal: Signal, error_message: str):
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
                    error_message[:500],
                    datetime.now(timezone.utc)
                )
        except Exception as e:
            logger.error(f"Failed to log failed trade: {e}")

    async def process_signals_batch(self, signals: List[Signal]):
        if not signals:
            return

        tasks = []
        for signal in signals:
            if signal.id not in self.processing_signals:
                task = asyncio.create_task(self.process_signal(signal))
                tasks.append(task)

        if tasks:
            logger.info(f"⚡ Processing {len(tasks)} signals concurrently")
            results = await asyncio.gather(*tasks, return_exceptions=True)

            success_count = sum(1 for r in results if r is True)
            failure_count = sum(1 for r in results if r is False or isinstance(r, Exception))

            logger.info(f"📊 Batch results: {success_count} success, {failure_count} failed")

    async def print_statistics(self):
        logger.info("=" * 60)
        logger.info("Daily Trading Statistics")
        logger.info("=" * 60)
        logger.info(f"Date: {self.daily_stats['last_reset']}")
        logger.info(f"Total Trades: {self.daily_stats['trades_count']}")
        logger.info(f"Successful: {self.daily_stats['successful_trades']}")
        logger.info(f"Failed: {self.daily_stats['failed_trades']}")

        if self.daily_stats['successful_trades'] > 0:
            success_rate = (self.daily_stats['successful_trades'] /
                            max(1, self.daily_stats['trades_count'])) * 100
            logger.info(f"Success Rate: {success_rate:.1f}%")

        logger.info(f"Total Volume: ${self.daily_stats['total_volume']:.2f}")
        logger.info(f"Total Loss: ${self.daily_stats['total_loss']:.2f}")
        logger.info("=" * 60)

    async def health_check(self):
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(60)

                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        await conn.fetchval("SELECT 1")

                checks = []
                if self.binance:
                    checks.append(("Binance", self.binance.get_balance()))
                if self.bybit:
                    checks.append(("Bybit", self.bybit.get_balance()))

                for name, check in checks:
                    try:
                        balance = await check
                        # ВАЖНО: Проверяем, что баланс не пустая строка
                        try:
                            balance = float(balance) if balance and balance != '' else 0
                        except (ValueError, TypeError):
                            logger.error(f"Health check - {name}: Invalid balance value")
                            balance = 0
                        logger.debug(f"Health check - {name}: ${balance:.2f}")
                    except Exception as e:
                        logger.error(f"Health check failed - {name}: {e}")
                        if name == "Binance":
                            await self._init_binance()
                        elif name == "Bybit":
                            await self._init_bybit()

                if hasattr(self, 'health_check_count'):
                    self.health_check_count += 1
                else:
                    self.health_check_count = 1

                if self.health_check_count % 30 == 0:
                    await self.print_statistics()

            except Exception as e:
                logger.error(f"Health check error: {e}")

    async def run(self):
        logger.info("🚀 Starting Production Trading System v3.0")

        try:
            await self.initialize()

            health_task = asyncio.create_task(self.health_check())

            while not self.shutdown_event.is_set():
                try:
                    signals = await self.get_unprocessed_signals()

                    if signals:
                        await self.process_signals_batch(signals)

                    await asyncio.sleep(self.check_interval)

                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(10)

            health_task.cancel()

        except KeyboardInterrupt:
            logger.info("⚠️ Shutdown signal received")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.cleanup()

    async def cleanup(self):
        logger.info("🧹 Cleaning up resources...")

        await self.print_statistics()

        if self.db_pool:
            await self.db_pool.close()
            logger.info("Database pool closed")

        if self.binance:
            await self.binance.close()
            logger.info("Binance connection closed")

        if self.bybit:
            await self.bybit.close()
            logger.info("Bybit connection closed")

        logger.info("✅ Cleanup complete")

    def handle_shutdown(self, signum, frame):
        logger.info(f"Received signal {signum}")
        self.shutdown_event.set()


async def main():
    trader = MainTrader()

    signal.signal(signal.SIGINT, trader.handle_shutdown)
    signal.signal(signal.SIGTERM, trader.handle_shutdown)

    await trader.run()


if __name__ == "__main__":
    asyncio.run(main())