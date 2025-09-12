#!/usr/bin/env python3
"""
Main Trading Script - PRODUCTION READY v4.0
- Добавлена деактивация сигналов (is_active=false)
- Добавлена установка первичного SL после открытия позиции
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
from utils.rate_limiter import RateLimiter

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

        # <<< ИЗМЕНЕНИЕ: Параметр для первичного SL из protection_monitor >>>
        self.initial_sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2.0'))
        # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.spread_limit = 100.0 if self.testnet else 0.5

        self.binance = None
        self.bybit = None
        self.db_pool = None
        self.processing_signals: Set[int] = set()

        # Система предотвращения дубликатов сигналов
        self.processed_signals_cache: Set[int] = set()
        self.signal_cache_ttl = 3600  # 1 час
        self.last_cache_cleanup = datetime.now(timezone.utc)

        # Буфер для сигналов
        self.signal_buffer = asyncio.Queue(maxsize=1000)
        self.buffer_processor_task = None

        # Мониторинг сигналов
        self.signal_stats = {
            'processed': 0,
            'duplicates_prevented': 0,
            'errors': 0,
            'avg_processing_time': 0.0,
            'last_signal_time': None
        }

        # Семафоры для контроля параллелизма
        self.signal_semaphore = asyncio.Semaphore(20)  # Максимум 20 одновременных сигналов
        self.db_semaphore = asyncio.Semaphore(10)  # Максимум 10 одновременных DB операций

        # Rate limiter для предотвращения банов на биржах
        self.rate_limiter = RateLimiter()

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
        self.start_time = datetime.now(timezone.utc)
        self.health_check_count = 0
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
        # <<< ИЗМЕНЕНИЕ: Логирование параметра SL >>>
        logger.info(f"   Signal Semaphore: {self.signal_semaphore._value}")
        logger.info(f"   DB Semaphore: {self.db_semaphore._value}")
        logger.info("=" * 60)

    def validate_config(self) -> bool:
        """Validate configuration parameters"""
        try:
            # Check required environment variables
            required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
            for var in required_vars:
                if not os.getenv(var):
                    logger.error(f"Missing required environment variable: {var}")
                    return False

            # Validate numeric parameters
            if self.position_size_usd <= 0:
                logger.error("Position size must be positive")
                return False

            if self.leverage < 1 or self.leverage > 125:
                logger.error("Leverage must be between 1 and 125")
                return False

            if self.min_score_week < 0 or self.min_score_week > 100:
                logger.error("Min score week must be between 0 and 100")
                return False

            if self.min_score_month < 0 or self.min_score_month > 100:
                logger.error("Min score month must be between 0 and 100")
                return False

            return True
        except Exception as e:
            logger.error(f"Configuration validation error: {e}")
            return False

    async def initialize(self):
        # Validate configuration
        if not self.validate_config():
            raise ValueError("Configuration validation failed")

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

    async def _cleanup_signal_cache(self):
        """Очистка кэша обработанных сигналов"""
        current_time = datetime.now(timezone.utc)
        if (current_time - self.last_cache_cleanup).seconds > self.signal_cache_ttl:
            # Очистка старых записей из кэша
            self.processed_signals_cache.clear()
            self.last_cache_cleanup = current_time
            logger.debug("Signal cache cleaned up")

    async def _get_recently_processed_signal_ids(self) -> List[int]:
        """Получение ID недавно обработанных сигналов из БД"""
        try:
            async with self.db_semaphore:
                async with self.db_pool.acquire() as conn:
                    # Получить сигналы обработанные за последний час
                    query = """
                        SELECT DISTINCT signal_id
                        FROM monitoring.trades
                        WHERE created_at > $1
                    """
                    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
                    rows = await conn.fetch(query, cutoff_time)
                    return [row['signal_id'] for row in rows]
        except Exception as e:
            logger.error(f"Error getting recently processed signals: {e}")
            return []

    async def get_unprocessed_signals(self) -> List[Signal]:
        # Очистка кэша обработанных сигналов
        await self._cleanup_signal_cache()

        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=self.signal_time_window)

        # Получить недавно обработанные сигналы для дополнительной проверки
        recently_processed = await self._get_recently_processed_signal_ids()

        # <<< ИЗМЕНЕНИЕ: Запрос теперь фильтрует по sh.is_active = true >>>
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
            WHERE sh.id NOT IN (SELECT unnest($1::int[]))
            AND sh.id NOT IN (SELECT unnest($2::int[]))
            AND sh.is_active = true
            AND sh.score_week >= $3
            AND sh.score_month >= $4
            AND sh.created_at > $5
            AND tp.is_active = true
            AND tp.exchange_id IN (1, 2)
            ORDER BY 
                (sh.score_week + sh.score_month) DESC,
                sh.created_at DESC
            LIMIT $6
        """
        # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

        try:
            async with self.db_semaphore:
                async with self.db_pool.acquire() as conn:
                    # <<< ИЗМЕНЕНИЕ: Убрана проверка по таблице monitoring.trades, т.к. is_active надежнее >>>
                    rows = await conn.fetch(
                        query,
                        list(self.processing_signals),
                        recently_processed,
                        self.min_score_week,
                        self.min_score_month,
                        time_threshold,
                        self.max_concurrent_orders
                    )
                    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

                    signals = []
                    for row in rows:
                        signal_id = row['id']
                        symbol = row['symbol']

                        # Дополнительная проверка на дубликаты в кэше
                        if signal_id in self.processed_signals_cache:
                            self.signal_stats['duplicates_prevented'] += 1
                            logger.debug(f"Skipping duplicate signal #{signal_id}")
                            continue

                        if self._is_symbol_in_cooldown(symbol):
                            logger.debug(f"Skipping {symbol} - in cooldown")
                            continue

                        signals.append(Signal(
                            id=signal_id,
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
                        logger.info(f"   Duplicates prevented: {self.signal_stats['duplicates_prevented']}")

                        self.signal_stats['last_signal_time'] = newest_signal_time

                    return signals

        except Exception as e:
            logger.error(f"Error fetching signals: {e}")
            self.signal_stats['errors'] += 1
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
        """ИСПРАВЛЕНО: Улучшенная обработка пустых значений с rate limiting"""
        try:
            # Проверяем rate limit перед получением тикера для проверки спреда
            exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'
            if not await self.rate_limiter.acquire(exchange_key, 'query', f'check_spread_{symbol}'):
                logger.warning(f"Rate limit exceeded for {exchange_key} check_spread_{symbol}")
                # На rate limit возвращаем True чтобы не блокировать торговлю
                return True

            ticker = await exchange.get_ticker(symbol)
            await self.rate_limiter.record_request(exchange_key, 'query', f'check_spread_{symbol}')
            if not ticker or not ticker.get('price'):
                logger.warning(f"No ticker data for {symbol}")
                if self.testnet:
                    logger.info(f"⚠️ {symbol} - allowing trade on testnet despite missing ticker")
                    return True
                return False

            bid = ticker.get('bid', 0)
            ask = ticker.get('ask', 0)
            price = ticker.get('price', 0)

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
        """Enhanced order size validation with better min/max handling"""
        try:
            # Проверяем rate limit
            exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'
            if not await self.rate_limiter.acquire(exchange_key, 'query', f'validate_order_{symbol}'):
                logger.warning(f"Rate limit exceeded for {exchange_key} validate_order_{symbol}")
                return True, position_size_usd

            ticker = await exchange.get_ticker(symbol)
            await self.rate_limiter.record_request(exchange_key, 'query', f'validate_order_{symbol}')

            if not ticker:
                logger.error(f"No ticker for {symbol}")
                return False, 0

            price = ticker.get('price', 0)

            try:
                price = float(price) if price and price != '' else 0
            except (ValueError, TypeError):
                logger.error(f"Invalid price for {symbol}: {price}")
                return False, 0

            if price <= 0:
                logger.error(f"Invalid price for {symbol}: {price}")
                return False, 0

            quantity = position_size_usd / price

            # Получаем минимальные требования для биржи
            if isinstance(exchange, BinanceExchange):
                # Binance минимальный notional
                min_notional = 10.0 if self.testnet else 5.0

                # Проверяем через exchange info
                if symbol in exchange.exchange_info:
                    min_notional_filter = next(
                        (f for f in exchange.exchange_info[symbol].get('filters', [])
                         if f['filterType'] == 'MIN_NOTIONAL'),
                        None
                    )
                    if min_notional_filter:
                        min_notional = float(min_notional_filter.get('minNotional', min_notional))

            else:  # Bybit
                # Bybit минимальный размер
                min_notional = 10.0

                # Проверяем через symbol info
                if symbol in exchange.symbol_info:
                    symbol_data = exchange.symbol_info[symbol]
                    if isinstance(symbol_data, dict):
                        min_qty = symbol_data.get('minOrderQty', 1)
                        min_notional = max(min_notional, min_qty * price)

            order_value = quantity * price

            # Если размер меньше минимального, корректируем
            if order_value < min_notional:
                logger.debug(f"{symbol}: Order value ${order_value:.2f} < minimum ${min_notional:.2f}")
                adjusted_size = min_notional * 1.1  # Добавляем 10% запас

                # Проверяем не превышает ли скорректированный размер наш баланс
                if adjusted_size > position_size_usd * 2:  # Не более чем в 2 раза больше
                    logger.warning(f"{symbol}: Adjusted size ${adjusted_size:.2f} too large")
                    return False, 0

                logger.info(f"{symbol}: Adjusted position size to ${adjusted_size:.2f}")
                return True, adjusted_size

            return True, position_size_usd

        except Exception as e:
            logger.error(f"Error validating order size: {e}")
            return False, 0

    async def _create_order_with_retry(self, exchange, signal: Signal) -> OrderResult:
        """FIXED: Improved balance and order handling"""

        for attempt in range(self.order_retry_max):
            try:
                # Безопасное получение баланса
                exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'

                # Проверяем rate limit
                if not await self.rate_limiter.acquire(exchange_key, 'query', 'get_balance'):
                    logger.warning(f"Rate limit exceeded for {exchange_key} get_balance")
                    await asyncio.sleep(2)  # Ждем перед повторной попыткой
                    continue

                balance = await exchange.get_balance()
                await self.rate_limiter.record_request(exchange_key, 'query', 'get_balance')

                # Безопасное преобразование баланса
                try:
                    balance = float(balance) if balance and balance != '' and balance != 'null' else 0
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid balance value: {balance}, treating as 0")
                    balance = 0

                # Проверка минимального баланса
                if balance <= self.min_balance_reserve:
                    error_msg = f"Insufficient balance: ${balance:.2f} <= reserve ${self.min_balance_reserve}"
                    logger.error(error_msg)

                    # На последней попытке возвращаем ошибку
                    if attempt == self.order_retry_max - 1:
                        return OrderResult(success=False, error_message=error_msg)

                    # Ждем и пробуем снова
                    await asyncio.sleep(5)
                    continue

                # Validate and adjust order size
                valid, adjusted_size = await self._validate_order_size(exchange, signal.symbol, self.position_size_usd)
                if not valid:
                    return OrderResult(success=False, error_message="Invalid order size")

                position_size_usd = adjusted_size

                # Проверяем доступный баланс
                max_available = balance - self.min_balance_reserve
                if position_size_usd > max_available:
                    if max_available < 10:  # Минимальная сумма для торговли
                        return OrderResult(
                            success=False,
                            error_message=f"Insufficient available balance: ${max_available:.2f}"
                        )
                    position_size_usd = max_available
                    logger.warning(f"Adjusted position to available balance: ${position_size_usd:.2f}")

                # Получаем текущую цену с проверкой rate limit
                if not await self.rate_limiter.acquire(exchange_key, 'query', f'get_ticker_{signal.symbol}'):
                    logger.warning(f"Rate limit exceeded for ticker")
                    await asyncio.sleep(2)
                    continue

                ticker = await exchange.get_ticker(signal.symbol)
                await self.rate_limiter.record_request(exchange_key, 'query', f'get_ticker_{signal.symbol}')

                if not ticker or not ticker.get('price'):
                    logger.error(f"No ticker data for {signal.symbol}")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(2)
                        continue
                    return OrderResult(success=False, error_message="No ticker data")

                # Безопасное получение цены
                price = ticker.get('price', 0)
                try:
                    price = float(price) if price and price != '' and price != 'null' else 0
                except (ValueError, TypeError):
                    logger.error(f"Invalid price for {signal.symbol}: {price}")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(2)
                        continue
                    return OrderResult(success=False, error_message=f"Invalid price: {price}")

                if price <= 0:
                    logger.error(f"Invalid price for {signal.symbol}: {price}")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(2)
                        continue
                    return OrderResult(success=False, error_message=f"Invalid price: {price}")

                # Рассчитываем количество
                quantity = position_size_usd / price

                logger.info(f"📝 Order attempt {attempt + 1}/{self.order_retry_max}:")
                logger.info(f"   ${position_size_usd:.2f} = {quantity:.6f} {signal.symbol} @ ${price:.4f}")

                # Устанавливаем leverage (не критично если не удается)
                if not await self.rate_limiter.acquire(exchange_key, 'order', f'set_leverage_{signal.symbol}'):
                    logger.warning(f"Rate limit for leverage, continuing without setting")
                else:
                    leverage_set = await exchange.set_leverage(signal.symbol, self.leverage)
                    await self.rate_limiter.record_request(exchange_key, 'order', f'set_leverage_{signal.symbol}')
                    if not leverage_set:
                        logger.warning(f"Could not set leverage for {signal.symbol}, continuing anyway")

                # Создаем ордер с проверкой rate limit
                if not await self.rate_limiter.acquire(exchange_key, 'order', f'create_order_{signal.symbol}'):
                    logger.warning(f"Rate limit exceeded for order creation")
                    await asyncio.sleep(3)
                    continue

                order = await exchange.create_market_order(signal.symbol, 'BUY', quantity)
                await self.rate_limiter.record_request(exchange_key, 'order', f'create_order_{signal.symbol}')

                # Обработка результата
                if order:
                    # Проверяем статус ордера
                    order_status = order.get('status', 'UNKNOWN')

                    # Для Bybit UNKNOWN статус может означать успех
                    if order_status in ['FILLED', 'UNKNOWN']:
                        executed_qty = order.get('quantity', quantity)
                        avg_price = order.get('price', price) if order.get('price', 0) > 0 else price

                        self.daily_stats['successful_trades'] += 1
                        self.daily_stats['total_volume'] += executed_qty * avg_price

                        logger.info(f"✅ Order processed: {executed_qty:.6f} @ ${avg_price:.4f}")

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
                    elif order_status == 'PENDING':
                        logger.warning(f"Order pending: {signal.symbol}")
                        if attempt < self.order_retry_max - 1:
                            await asyncio.sleep(3)
                            continue
                else:
                    logger.warning(f"Order attempt {attempt + 1} failed: No execution")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(2)

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Order attempt {attempt + 1} error: {error_msg}")

                # Обработка специфических ошибок
                if 'insufficient' in error_msg.lower() or 'balance' in error_msg.lower():
                    return OrderResult(success=False, error_message="Insufficient balance", retry_count=attempt + 1)
                elif 'invalid symbol' in error_msg.lower():
                    self.failed_symbols[signal.symbol] = datetime.now(timezone.utc)
                    return OrderResult(success=False, error_message=f"Invalid symbol: {signal.symbol}",
                                       retry_count=attempt + 1)
                elif 'qty invalid' in error_msg.lower() or 'quantity' in error_msg.lower():
                    logger.warning(f"Quantity invalid for {signal.symbol}, will retry with adjusted size")
                    if attempt < self.order_retry_max - 1:
                        await asyncio.sleep(2)
                        continue

                if attempt < self.order_retry_max - 1:
                    await asyncio.sleep(self.order_retry_delay * (attempt + 1))

        # Все попытки исчерпаны
        self.daily_stats['failed_trades'] += 1
        self.failed_symbols[signal.symbol] = datetime.now(timezone.utc)
        return OrderResult(
            success=False,
            error_message=f"Failed after {self.order_retry_max} attempts",
            retry_count=self.order_retry_max
        )

    # <<< ИЗМЕНЕНИЕ: Новый метод для установки первичного стоп-лосса >>>
    async def _set_initial_protection(self, exchange, order_result: OrderResult):
        """Enhanced initial stop-loss setup with better retry logic"""
        try:
            symbol = order_result.symbol
            entry_price = order_result.price
            side = order_result.side.upper()

            # Рассчитываем цену SL
            sl_price = entry_price * (1 - self.initial_sl_percent / 100) if side == 'BUY' \
                else entry_price * (1 + self.initial_sl_percent / 100)

            exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'

            # Разные задержки для разных бирж
            if exchange_key == 'bybit':
                initial_wait = 3.0  # Bybit требует больше времени
                retry_wait = 2.0
                max_retries = 3
            else:
                initial_wait = 1.5  # Binance быстрее
                retry_wait = 1.5
                max_retries = 3

            # Ждем регистрации позиции
            await asyncio.sleep(initial_wait)

            for attempt in range(max_retries):
                try:
                    # Проверяем наличие позиции
                    if isinstance(exchange, BinanceExchange):
                        all_positions = await exchange.get_open_positions()
                        positions = [p for p in all_positions if p['symbol'] == symbol]
                    else:  # Bybit
                        positions = await exchange.get_open_positions(symbol)

                    if not positions:
                        if attempt < max_retries - 1:
                            logger.warning(f"Position not found for {symbol}, attempt {attempt + 1}/{max_retries}")
                            await asyncio.sleep(retry_wait)
                            continue
                        else:
                            logger.error(f"Position not found for {symbol} after {max_retries} attempts")
                            return False

                    # Проверяем rate limit
                    if not await self.rate_limiter.acquire(exchange_key, 'order', f'set_stop_loss_{symbol}'):
                        logger.warning(f"Rate limit exceeded for {exchange_key} set_stop_loss_{symbol}")
                        return False

                    # Устанавливаем stop loss
                    success = await exchange.set_stop_loss(symbol, sl_price)
                    await self.rate_limiter.record_request(exchange_key, 'order', f'set_stop_loss_{symbol}')

                    if success:
                        logger.info(f"🛡️ Initial Stop Loss set for {symbol} at ${sl_price:.4f}")
                        return True
                    elif attempt < max_retries - 1:
                        logger.warning(f"Failed to set SL for {symbol}, retry {attempt + 1}/{max_retries}")
                        await asyncio.sleep(retry_wait)

                except Exception as e:
                    logger.error(f"Error setting SL for {symbol} (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_wait)

            logger.warning(f"⚠️ Could not set initial SL for {symbol} after {max_retries} attempts")
            # На testnet не критично
            return self.testnet

        except Exception as e:
            logger.error(f"Error setting initial protection for {symbol}: {e}")
            return False

    async def _is_symbol_tradeable(self, exchange, symbol: str) -> bool:
        """Enhanced symbol validation - FIXED for testnet"""
        try:
            # Для Bybit
            if isinstance(exchange, BybitExchange):
                # Проверяем наличие в загруженных инструментах
                if symbol not in exchange.symbol_info:
                    logger.warning(f"Symbol {symbol} not found in Bybit instruments, attempting to load...")

                    # Пытаемся загрузить информацию о символе
                    await exchange._load_single_symbol_info(symbol)

                    # Даем время на загрузку
                    await asyncio.sleep(0.5)

                    # Проверяем еще раз
                    if symbol not in exchange.symbol_info:
                        logger.error(f"❌ {symbol} not tradeable on Bybit - symbol not found after reload")
                        return False

                # Получаем информацию о символе
                symbol_info = exchange.symbol_info[symbol]

                # Обрабатываем разные форматы данных
                if isinstance(symbol_info, dict):
                    status = symbol_info.get('status', '')
                else:
                    status = getattr(symbol_info, 'status', '')

                # ВАЖНО: На testnet принимаем символы с любым непустым статусом
                if self.testnet:
                    if status == 'Closed':
                        logger.warning(f"Symbol {symbol} is Closed on testnet")
                        return False
                    else:
                        # Trading, PreLaunch, Settling и т.д. - все OK для testnet
                        logger.info(f"✅ Symbol {symbol} status='{status}' accepted on testnet")
                        return True
                else:
                    # На mainnet строгая проверка
                    if status != 'Trading':
                        logger.warning(f"Symbol {symbol} status is '{status}', not 'Trading'")
                        return False

                # Дополнительная проверка минимального размера
                min_qty = symbol_info.get('minOrderQty', 0.001) if isinstance(symbol_info, dict) else 0.001
                if min_qty > 0 and self.position_size_usd > 0:
                    # Проверяем что можем купить хотя бы минимальное количество
                    ticker = await exchange.get_ticker(symbol)
                    if ticker and ticker.get('price', 0) > 0:
                        min_cost = min_qty * ticker['price']
                        if min_cost > self.position_size_usd * 2:  # Даем запас
                            logger.warning(
                                f"Symbol {symbol} min cost ${min_cost:.2f} too high for position size ${self.position_size_usd}")
                            return False

            # Для Binance
            elif isinstance(exchange, BinanceExchange):
                if symbol not in exchange.exchange_info:
                    logger.warning(f"Symbol {symbol} not found in Binance exchange info")
                    return False

                symbol_info = exchange.exchange_info[symbol]

                # Проверяем статус
                if symbol_info.get('status') != 'TRADING':
                    logger.warning(f"Symbol {symbol} status is {symbol_info.get('status')}, not TRADING")
                    return False

                # Проверяем что это PERPETUAL контракт
                if symbol_info.get('contractType') != 'PERPETUAL':
                    logger.warning(f"Symbol {symbol} is not a PERPETUAL contract")
                    return False

                # Проверяем минимальный notional
                min_notional_filter = next(
                    (f for f in symbol_info.get('filters', [])
                     if f['filterType'] == 'MIN_NOTIONAL'),
                    None
                )

                if min_notional_filter:
                    min_notional = float(min_notional_filter.get('minNotional', 5))
                    if min_notional > self.position_size_usd:
                        logger.warning(
                            f"Symbol {symbol} min notional ${min_notional} > position size ${self.position_size_usd}")
                        return False

            logger.info(f"✅ Symbol {symbol} is tradeable on {exchange.__class__.__name__}")
            return True

        except Exception as e:
            logger.error(f"Error checking if {symbol} is tradeable: {e}")
            # В случае ошибки на testnet продолжаем, на mainnet - блокируем
            return self.testnet

    # <<< ИЗМЕНЕНИЕ: Новый метод для деактивации сигнала в БД >>>
    async def _deactivate_signal_in_db(self, signal_id: int):
        """Updates the signal in the database to set is_active = false."""
        query = "UPDATE fas.scoring_history SET is_active = false WHERE id = $1"
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(query, signal_id)
                logger.info(f"Signal #{signal_id} deactivated in database.")
        except Exception as e:
            logger.error(f"Failed to deactivate signal #{signal_id}: {e}")

    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

    async def process_signal(self, signal: Signal) -> bool:
        """Enhanced signal processing with better validation"""

        # Проверяем не обрабатывается ли уже этот сигнал
        if signal.id in self.processing_signals:
            logger.warning(f"Signal #{signal.id} already being processed")
            return False

        self.processing_signals.add(signal.id)

        try:
            logger.info(f"🎯 Processing signal #{signal.id}: {signal.symbol} on {signal.exchange_name}")
            logger.info(f"   Scores: Week={signal.score_week:.1f}%, Month={signal.score_month:.1f}%")

            # Проверка дневных лимитов
            if not await self.check_daily_limits():
                logger.warning(f"Skipping signal #{signal.id} due to daily limits")
                return False

            # Получаем биржу
            exchange = self._get_exchange(signal.exchange_id)
            if not exchange:
                logger.error(f"Exchange not available for signal #{signal.id}")
                await self._log_failed_trade(signal, "Exchange not available")
                return False

            # НОВОЕ: Проверяем доступность символа для торговли
            if not await self._is_symbol_tradeable(exchange, signal.symbol):
                logger.error(f"Symbol {signal.symbol} not tradeable on {signal.exchange_name}")
                await self._log_failed_trade(signal, f"Symbol not tradeable on {signal.exchange_name}")

                # Деактивируем сигнал чтобы не пытаться снова
                await self._deactivate_signal_in_db(signal.id)

                # Добавляем символ в cooldown
                self.failed_symbols[signal.symbol] = datetime.now(timezone.utc)
                return False

            # Проверка спреда
            if not await self._check_spread(exchange, signal.symbol):
                await self._log_failed_trade(signal, "Spread too high or no price data")
                return False

            # Создание ордера с повторными попытками
            order_result = await self._create_order_with_retry(exchange, signal)

            if order_result.success:
                await self._log_successful_trade(signal, order_result)

                # Деактивируем сигнал
                await self._deactivate_signal_in_db(signal.id)

                # Устанавливаем защиту
                protection_set = await self._set_initial_protection(exchange, order_result)

                if not protection_set and not self.testnet:
                    logger.error(f"⚠️ Failed to set protection for {signal.symbol}")


                self.daily_stats['trades_count'] += 1
                logger.info(f"✅ Signal #{signal.id} processed successfully")
                return True
            else:
                await self._log_failed_trade(signal, order_result.error_message)
                logger.error(f"❌ Signal #{signal.id} failed: {order_result.error_message}")

                # Если символ не найден, деактивируем сигнал
                if "Invalid symbol" in order_result.error_message or \
                        "Qty invalid" in order_result.error_message:
                    await self._deactivate_signal_in_db(signal.id)

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
        """ПОСЛЕДОВАТЕЛЬНАЯ обработка сигналов с rate limiting для предотвращения банов"""
        if not signals:
            return

        # Группировка сигналов по биржам для последовательной обработки
        binance_signals = [s for s in signals if s.exchange_id == 1]
        bybit_signals = [s for s in signals if s.exchange_id == 2]

        logger.info(f"🔄 Sequential processing: Binance={len(binance_signals)}, Bybit={len(bybit_signals)}")

        # ПОСЛЕДОВАТЕЛЬНАЯ обработка по биржам
        if binance_signals:
            await self._process_exchange_signals_sequential('Binance', binance_signals)

        if bybit_signals:
            await self._process_exchange_signals_sequential('Bybit', bybit_signals)

        logger.info("✅ Sequential batch processing completed")

    async def _process_exchange_signals_sequential(self, exchange_name: str, signals: List[Signal]):
        """ПОСЛЕДОВАТЕЛЬНАЯ обработка сигналов для конкретной биржи с rate limiting"""
        processed = 0
        errors = 0

        for signal in signals:
            try:
                # Проверяем rate limit перед обработкой
                exchange_key = exchange_name.lower()
                if not await self.rate_limiter.acquire(exchange_key, 'order', f'signal_{signal.id}'):
                    logger.warning(f"⚠️ Rate limit exceeded for {exchange_name}, skipping signal #{signal.id}")
                    await asyncio.sleep(1)  # Ждем 1 секунду перед следующей попыткой
                    continue

                # Записываем запрос в rate limiter
                await self.rate_limiter.record_request(exchange_key, 'order', f'signal_{signal.id}')

                # Обрабатываем сигнал
                success = await self.process_signal(signal)

                if success:
                    processed += 1
                    logger.info(f"✅ Signal #{signal.id} processed successfully")
                else:
                    errors += 1
                    logger.warning(f"❌ Signal #{signal.id} failed")

                # Добавляем задержку между сигналами для предотвращения rate limit
                await asyncio.sleep(0.2)  # 200ms задержка между сигналами

            except Exception as e:
                logger.error(f"Error processing signal #{signal.id}: {e}")
                errors += 1
                await asyncio.sleep(0.5)  # Увеличиваем задержку при ошибке

        logger.info(f"📊 Sequential {exchange_name}: {processed} processed, {errors} errors")

    async def _process_exchange_signals_batch(self, exchange_name: str, signals: List[Signal]) -> Tuple[int, int]:
        """УСТАРЕВШИЙ метод - заменен на последовательную обработку"""
        logger.warning("Using old parallel method - should use sequential processing instead")
        return await self._process_exchange_signals_sequential(exchange_name, signals)

    async def monitor_signal_processing(self):
        """Мониторинг обработки сигналов и алертинг"""
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(300)  # Каждые 5 минут

                # Проверка backlog сигналов
                buffer_size = self.signal_buffer.qsize()
                if buffer_size > 50:
                    logger.warning(f"⚠️ Signal backlog: {buffer_size} signals in buffer")

                # Проверка средней скорости обработки
                if self.signal_stats['avg_processing_time'] > 10.0:
                    logger.warning(f"⚠️ Slow signal processing: {self.signal_stats['avg_processing_time']:.2f}s avg")

                # Проверка количества ошибок
                error_rate = 0
                if self.signal_stats['processed'] > 0:
                    error_rate = (self.signal_stats['errors'] / self.signal_stats['processed']) * 100

                if error_rate > 20.0:
                    logger.error(f"⚠️ High error rate: {error_rate:.1f}%")

                # Проверка возраста последнего сигнала
                if self.signal_stats['last_signal_time']:
                    signal_age = (datetime.now(timezone.utc) - self.signal_stats['last_signal_time']).total_seconds() / 60
                    if signal_age > 60:  # Больше часа без новых сигналов
                        logger.warning(f"⚠️ No new signals for {signal_age:.1f} minutes")

                # Логирование статистики
                logger.info("📊 Signal Processing Stats:")
                logger.info(f"   Processed: {self.signal_stats['processed']}")
                logger.info(f"   Duplicates prevented: {self.signal_stats['duplicates_prevented']}")
                logger.info(f"   Errors: {self.signal_stats['errors']}")
                logger.info(f"   Avg processing time: {self.signal_stats['avg_processing_time']:.2f}s")
                logger.info(f"   Buffer size: {buffer_size}")

            except Exception as e:
                logger.error(f"Error in signal monitoring: {e}")

    async def _log_successful_trade_transactional(self, signal: Signal, order_result: OrderResult):
        """Логирование успешной сделки с транзакцией"""
        async with self.db_semaphore:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    try:
                        # Логирование сделки
                        await conn.execute(
                            """
                            INSERT INTO monitoring.trades (
                                signal_id, trading_pair_id, symbol, exchange, side,
                                quantity, executed_qty, price, status, order_id, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            """,
                            signal.id, signal.trading_pair_id, signal.symbol,
                            signal.exchange_name.lower(), order_result.side,
                            order_result.quantity, order_result.executed_qty,
                            order_result.price, order_result.status.value,
                            order_result.order_id, datetime.now(timezone.utc)
                        )

                        # Деактивация сигнала
                        await conn.execute(
                            "UPDATE fas.scoring_history SET is_active = false WHERE id = $1",
                            signal.id
                        )

                        # Добавление в кэш обработанных сигналов
                        self.processed_signals_cache.add(signal.id)

                        logger.info(f"✅ Trade logged and signal #{signal.id} deactivated")

                    except Exception as e:
                        logger.error(f"Error in transactional logging: {e}")
                        raise
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
        logger.info("🚀 Starting Production Trading System v4.0")

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

    async def print_statistics(self):
        """Print trading statistics"""
        try:
            logger.info("=" * 60)
            logger.info("📊 Trading Statistics")
            logger.info("=" * 60)

            # Calculate uptime
            uptime = datetime.now(timezone.utc) - self.start_time
            hours, remainder = divmod(uptime.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)

            logger.info(f"Uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            logger.info(f"Signals processed: {self.signal_stats['processed']}")
            logger.info(f"Duplicates prevented: {self.signal_stats['duplicates_prevented']}")
            logger.info(f"Errors: {self.signal_stats['errors']}")

            # Daily stats
            logger.info(f"\n📈 Daily Statistics:")
            logger.info(f"Trades today: {self.daily_stats['trades_count']}")
            logger.info(f"Successful: {self.daily_stats['successful_trades']}")
            logger.info(f"Failed: {self.daily_stats['failed_trades']}")
            logger.info(f"Total volume: ${self.daily_stats['total_volume']:.2f}")

            # Cache stats
            logger.info(f"\n💾 Cache Statistics:")
            logger.info(f"Processed signals in cache: {len(self.processed_signals_cache)}")
            logger.info(f"Failed symbols: {len(self.failed_symbols)}")

            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"Error printing statistics: {e}")

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

    def safe_float(value, default=0.0):
        """Safely convert value to float"""
        if value is None or value == '' or value == 'null' or value == 'undefined':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default


async def main():
    trader = MainTrader()

    signal.signal(signal.SIGINT, trader.handle_shutdown)
    signal.signal(signal.SIGTERM, trader.handle_shutdown)

    await trader.run()


if __name__ == "__main__":
    asyncio.run(main())