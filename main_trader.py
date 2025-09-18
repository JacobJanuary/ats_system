#!/usr/bin/env python3
"""
Main Trading Script - PRODUCTION READY v7.2 (FIXED)
- ИСПРАВЛЕН расчет размера позиции (notional value без учета leverage)
- ДОБАВЛЕНЫ блокировки позиций через PostgreSQL advisory locks
- УЛУЧШЕНА обработка минимальных требований к позициям
- ДОБАВЛЕНО логирование всех критических операций
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

# Enhanced Logging Configuration
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
        
        # Working hours configuration
        working_hours_str = os.getenv('WORKING_HOURS', '')
        if working_hours_str:
            try:
                self.working_hours = set(int(h.strip()) for h in working_hours_str.split(',') if h.strip())
                logger.info(f"Working hours configured: {sorted(self.working_hours)}")
            except ValueError as e:
                logger.error(f"Invalid WORKING_HOURS format: {e}. Using 24/7 mode.")
                self.working_hours = set(range(24))
        else:
            self.working_hours = set(range(24))
            logger.info("No WORKING_HOURS configured. Running 24/7.")

        # Ensure minimum position size
        if self.position_size_usd < self.min_position_size_usd:
            logger.warning(
                f"Position size ${self.position_size_usd:.2f} below minimum ${self.min_position_size_usd:.2f}. Adjusting."
            )
            self.position_size_usd = self.min_position_size_usd

        self.leverage = int(os.getenv('LEVERAGE', '10'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.signal_time_window = int(os.getenv('SIGNAL_TIME_WINDOW', '5'))
        self.max_trades_per_15m = int(os.getenv('MAX_TRADES_PER_15M', '10'))

        # Retry configuration
        self.order_retry_max = int(os.getenv('ORDER_RETRY_MAX', '3'))
        self.order_retry_delay = float(os.getenv('ORDER_RETRY_DELAY', '1.0'))

        # Risk management
        self.initial_sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2.0'))
        self.max_spread_percent = float(os.getenv('MAX_SPREAD_PERCENT', '0.5'))
        # Отдельный лимит для testnet
        self.max_spread_testnet = float(os.getenv('MAX_SPREAD_TESTNET', '50.0'))

        # Environment detection
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.trading_mode = TradingMode.TESTNET if self.testnet else TradingMode.MAINNET

        # NEW: Stop-list для исключения определенных символов
        stop_list_str = os.getenv('STOP_LIST_SYMBOLS', 'BTCDOMUSDT')
        self.stop_list = set(s.strip() for s in stop_list_str.split(',') if s.strip())
        logger.info(f"Stop-list symbols: {self.stop_list}")

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
        self.locked_positions: Set[str] = set()  # Для отслеживания заблокированных позиций

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
        logger.info("TRADING SYSTEM CONFIGURATION v7.2 (FIXED)")
        logger.info("=" * 80)
        logger.info(f"Environment: {self.trading_mode.value}")
        logger.info(f"Position Size: ${self.position_size_usd} USD (notional value)")
        logger.info(f"Leverage: {self.leverage}x")
        logger.info(f"Stop Loss: {self.initial_sl_percent}%")
        logger.info(f"Max Spread: {self.spread_limit}%")
        logger.info(f"Trade Delay: {self.delay_between_trades}s")
        logger.info(f"Signal Window: {self.signal_time_window} minutes")
        logger.info(f"Max Trades per 15min: {self.max_trades_per_15m}")
        if len(self.working_hours) == 24:
            logger.info("Working Hours: 24/7")
        else:
            logger.info(f"Working Hours: {sorted(self.working_hours)}")
        logger.info("=" * 80)
    
    def is_in_working_hours(self, signal_time: datetime) -> bool:
        """Check if signal time is within configured working hours"""
        if len(self.working_hours) == 24:
            return True
        
        hour = signal_time.hour
        return hour in self.working_hours

    async def _init_db(self):
        """Initialize database connection pool with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.db_pool = await asyncpg.create_pool(
                    **self.db_config,
                    min_size=2,
                    max_size=10,  # Reduced from 20 to prevent connection exhaustion
                    command_timeout=10,
                    max_queries=50000,
                    max_inactive_connection_lifetime=300
                )

                # Test connection
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")

                logger.info("✅ Database connected successfully (pool: min=2, max=10)")
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

    async def has_open_position(self, exchange: Union[BinanceExchange, BybitExchange],
                                symbol: str) -> bool:
        """
        HIGH PRIORITY: Проверка существующей позиции
        Предотвращает открытие дублирующих позиций по одному символу
        """
        try:
            positions = await exchange.get_open_positions()
            for pos in positions:
                if pos.get('symbol') == symbol and float(pos.get('quantity', 0)) > 0:
                    logger.info(f"Position already exists for {symbol}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking existing position for {symbol}: {e}")
            # В случае ошибки безопаснее считать, что позиция есть
            return True

    async def acquire_position_lock(self, symbol: str, exchange: str, timeout: int = 30) -> bool:
        """Получение эксклюзивной блокировки на позицию через PostgreSQL advisory locks"""
        lock_key = f"{exchange}_{symbol}"

        if lock_key in self.locked_positions:
            logger.debug(f"Position {lock_key} already locked by this instance")
            return False

        if not self.db_pool:
            return True  # Без БД работаем без блокировок

        try:
            async with self.db_pool.acquire() as conn:
                # Используем advisory lock PostgreSQL
                lock_id = hash(lock_key) % 2147483647
                result = await conn.fetchval(
                    "SELECT pg_try_advisory_lock($1)", lock_id
                )
                if result:
                    self.locked_positions.add(lock_key)
                    logger.debug(f"Acquired lock for {lock_key}")
                return result
        except Exception as e:
            logger.error(f"Failed to acquire lock for {lock_key}: {e}")
            return False

    async def release_position_lock(self, symbol: str, exchange: str):
        """Освобождение блокировки позиции"""
        lock_key = f"{exchange}_{symbol}"

        if lock_key not in self.locked_positions:
            return

        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                lock_id = hash(lock_key) % 2147483647
                await conn.execute("SELECT pg_advisory_unlock($1)", lock_id)
                self.locked_positions.discard(lock_key)
                logger.debug(f"Released lock for {lock_key}")
        except Exception as e:
            logger.error(f"Failed to release lock for {lock_key}: {e}")

    async def calculate_position_size(self, exchange: Union[BinanceExchange, BybitExchange],
                                      symbol: str, price: float) -> float:
        """
        FIXED v4: Расчет ФИКСИРОВАННОГО размера позиции с проверкой маржи
        - Проверка доступной маржи только для валидации (без изменения размера)
        - Использование фиксированного размера позиции
        - Добавлена проверка минимального размера для Bybit
        - Добавлена валидация notional value с выбросом исключения
        """
        try:
            # FIXED: Проверяем доступную маржу только для отклонения сигнала
            if isinstance(exchange, BinanceExchange):
                try:
                    # Используем API метод для получения баланса
                    account_info = await exchange.get_account_balance()
                    available_balance = float(account_info) if account_info else 0
                    
                    # Проверяем, достаточно ли маржи для ФИКСИРОВАННОЙ позиции
                    margin_required = self.position_size_usd / self.leverage
                    
                    if available_balance < margin_required:
                        error_msg = (
                            f"Insufficient margin for {symbol}: "
                            f"Available=${available_balance:.2f}, "
                            f"Required=${margin_required:.2f}"
                        )
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                        
                except Exception as e:
                    logger.error(f"Failed to check Binance margin: {e}")
                    # Продолжаем с фиксированным размером, пусть биржа сама отклонит если не хватает
            
            # Используем ФИКСИРОВАННЫЙ размер позиции
            base_quantity = self.position_size_usd / price
            formatted_qty = float(exchange.format_quantity(symbol, base_quantity))

            # CRITICAL FIX: Проверка на 0 после форматирования
            if formatted_qty == 0:
                # Для Binance
                if isinstance(exchange, BinanceExchange) and symbol in exchange.exchange_info:
                    filters = exchange.exchange_info[symbol].get('filters', [])
                    lot_size_filter = next((f for f in filters if f['filterType'] == 'LOT_SIZE'), None)
                    if lot_size_filter:
                        min_qty = float(lot_size_filter.get('minQty', 0))
                        if min_qty > 0:
                            formatted_qty = min_qty
                            logger.warning(f"Binance: Using minimum quantity {min_qty} for {symbol}")

                # NEW: Для Bybit
                elif isinstance(exchange, BybitExchange) and symbol in exchange.symbol_info:
                    min_qty = exchange.symbol_info[symbol].get('minOrderQty', 0)
                    if min_qty > 0:
                        formatted_qty = min_qty
                        logger.warning(f"Bybit: Using minimum quantity {min_qty} for {symbol}")

            # Финальная проверка на 0
            if formatted_qty == 0:
                raise ValueError(f"Cannot determine valid quantity for {symbol}")

            # NEW v2: Строгая проверка notional value
            final_notional = formatted_qty * price

            # Если notional меньше минимума даже после использования min_qty
            if final_notional < self.min_position_size_usd:
                # Пытаемся увеличить до минимального notional
                min_qty_for_notional = self.min_position_size_usd / price
                adjusted_qty = float(exchange.format_quantity(symbol, min_qty_for_notional))
                adjusted_notional = adjusted_qty * price

                # CRITICAL: Если даже после корректировки notional слишком мал - выбрасываем исключение
                if adjusted_notional < self.min_position_size_usd * 0.95:  # 5% допуск
                    error_msg = (
                        f"Cannot meet minimum position size for {symbol}: "
                        f"adjusted notional ${adjusted_notional:.2f} < "
                        f"minimum ${self.min_position_size_usd:.2f}"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                formatted_qty = adjusted_qty
                final_notional = adjusted_notional
                logger.warning(f"Adjusted quantity to meet minimum notional: {formatted_qty:.6f}")

            logger.info(
                f"📊 Position sizing for {symbol}: "
                f"Qty={formatted_qty:.6f}, "
                f"Notional=${final_notional:.2f}, "
                f"Margin required=${final_notional / self.leverage:.2f}"
            )

            return formatted_qty

        except Exception as e:
            logger.error(f"Error calculating position size for {symbol}: {e}")
            raise

    async def validate_spread(self, exchange: Union[BinanceExchange, BybitExchange], symbol: str) -> bool:
        """
        FIX v3: Финальная версия валидации спреда
        - Разные лимиты для testnet и mainnet
        - Блокировка экстремальных спредов даже на testnet
        - Детальное логирование
        """
        try:
            # Проверяем что exchange инициализирован
            if not exchange:
                logger.error(f"Exchange not initialized for spread validation of {symbol}")
                return False

            ticker = await exchange.get_ticker(symbol)
            if not ticker or not ticker.get('bid') or not ticker.get('ask'):
                logger.warning(f"No ticker data for {symbol}")
                # На testnet разрешаем если нет данных, на mainnet - блокируем
                return self.trading_mode == TradingMode.TESTNET

            bid = float(ticker['bid'])
            ask = float(ticker['ask'])

            # Проверка на валидность цен
            if bid <= 0 or ask <= 0:
                logger.error(f"Invalid prices for {symbol}: bid={bid}, ask={ask}")
                return False

            if ask <= bid:
                logger.error(f"Ask <= Bid for {symbol}: bid={bid}, ask={ask}")
                return False

            spread_percent = ((ask - bid) / bid) * 100

            # Определяем лимит в зависимости от режима
            if self.trading_mode == TradingMode.TESTNET:
                # На testnet используем отдельный, более высокий лимит
                effective_limit = self.max_spread_testnet

                # Но для ЭКСТРЕМАЛЬНЫХ спредов все равно блокируем
                if spread_percent > 100:  # Спред больше 100% - явно проблема
                    logger.error(
                        f"EXTREME spread {spread_percent:.2f}% for {symbol} on testnet. "
                        f"Blocking to prevent order errors."
                    )
                    return False
            else:
                # На mainnet используем строгий лимит
                effective_limit = self.spread_limit

            # Проверка против эффективного лимита
            if spread_percent > effective_limit:
                logger.warning(
                    f"{symbol} spread {spread_percent:.2f}% exceeds "
                    f"{'testnet' if self.trading_mode == TradingMode.TESTNET else 'mainnet'} "
                    f"limit {effective_limit}%"
                )
                return False

            logger.debug(f"{symbol} spread {spread_percent:.2f}% is acceptable")
            return True

        except Exception as e:
            logger.error(f"Error validating spread for {symbol}: {e}", exc_info=True)
            # При ошибке блокируем на mainnet, разрешаем на testnet
            return self.trading_mode == TradingMode.TESTNET

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

    # ... остальные методы остаются без изменений ...

    async def process_signal(self, signal: Signal):
        """Process a single trading signal with complete error handling"""
        if signal.id in self.processing_signals:
            return

        self.processing_signals.add(signal.id)
        # NEW: Проверка stop-list
        if signal.pair_symbol in self.stop_list:
            logger.info(f"Symbol {signal.pair_symbol} is in stop-list, skipping")
            self.processing_signals.discard(signal.id)
            return
        trade_id = None
        position_id = None

        # Пытаемся получить блокировку на позицию
        if not await self.acquire_position_lock(signal.pair_symbol, signal.exchange_name):
            logger.info(f"Cannot acquire lock for {signal.pair_symbol}, skipping signal")
            self.processing_signals.discard(signal.id)
            return

        try:
            logger.info(f"{'=' * 60}")
            logger.info(
                f"Processing signal #{signal.id}: {signal.pair_symbol} {signal.recommended_action} on {signal.exchange_name}")
            logger.info(f"Scores: Week={signal.score_week:.1f}%, Month={signal.score_month:.1f}%")

            # Select exchange
            exchange = self.binance if signal.exchange_name.lower() == 'binance' else self.bybit
            if not exchange:
                # NEW: Проверка существующей позиции
                if await self.has_open_position(exchange, signal.pair_symbol):
                    logger.warning(f"Position already exists for {signal.pair_symbol}, skipping signal")
                    self.processing_signals.discard(signal.id)
                    await self.release_position_lock(signal.pair_symbol, signal.exchange_name)
                    return
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

            # ИСПОЛЬЗУЕМ ИСПРАВЛЕННЫЙ МЕТОД
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
                # Log failed trade and other error handling...
                self.failed_signals.add(signal.id)
                self.stats['positions_failed'] += 1
                await self.mark_signal_processed(signal.id)
                return

            # Position opened successfully - continue with SL setup...
            executed_qty = order_result.get('executed_qty', 0)
            execution_price = order_result.get('price', 0)

            logger.info(f"✅ Position opened: {executed_qty:.6f} {signal.pair_symbol} @ ${execution_price:.4f}")
            self.stats['positions_opened'] += 1

            # Логируем позицию в БД
            position_id = await self.log_position_to_db(
                signal,  # Передаем весь объект signal
                signal.pair_symbol,
                signal.exchange_name,
                signal.recommended_action,
                executed_qty,
                execution_price,
                order_result.get('orderId')
            )

            # Set Stop Loss...
            await asyncio.sleep(self.delay_between_requests * 2)
            await self.set_stop_loss(exchange, signal, execution_price, position_id)

            # NEW: Верификация и восстановление защиты
            await self.verify_and_recover_position(exchange, signal.pair_symbol, position_id)

            await self.mark_signal_processed(signal.id)
            self.stats['signals_processed'] += 1

        except Exception as e:
            logger.error(f"Critical error processing signal {signal.id}: {e}", exc_info=True)
            self.failed_signals.add(signal.id)
            self.stats['positions_failed'] += 1
            await self.mark_signal_processed(signal.id)

        finally:
            # Всегда освобождаем блокировку
            await self.release_position_lock(signal.pair_symbol, signal.exchange_name)
            self.processing_signals.discard(signal.id)

    async def verify_and_recover_position(self, exchange: Union[BinanceExchange, BybitExchange],
                                          symbol: str, side: str = None,
                                          entry_price: float = None, position_id: Optional[int] = None):
        """
        HIGH PRIORITY FIX v3: Правильная проверка и восстановление защиты позиции
        - Для Bybit проверяет SL в параметрах позиции, не в ордерах
        - Для Binance проверяет SL среди открытых ордеров
        - Не пытается установить SL если он уже есть
        """
        try:
            await asyncio.sleep(2)  # Даем время на обработку ордеров

            # CRITICAL FIX: Разная логика для Bybit и Binance
            sl_exists = False
            sl_price = 0
            
            if exchange.__class__.__name__ == 'BybitExchange':
                # Для Bybit: проверяем SL в позиции
                positions = await exchange.get_open_positions(symbol)
                if positions:
                    position = positions[0]
                    sl_value = position.get('stopLoss')
                    if sl_value and float(sl_value) > 0:
                        sl_exists = True
                        sl_price = float(sl_value)
                        logger.info(f"✅ Stop Loss verified in Bybit position for {symbol} at ${sl_price:.4f}")
                
            else:  # BinanceExchange
                # Для Binance: проверяем SL среди ордеров
                orders = await exchange.get_open_orders(symbol)
                sl_orders = [
                    order for order in orders
                    if order.get('type', '').lower() in ['stop_market', 'stop', 'stop_loss']
                ]
                
                if sl_orders:
                    sl_order = sl_orders[0]
                    sl_price = float(sl_order.get('stopPrice', 0) or sl_order.get('price', 0))
                    if sl_price > 0:
                        sl_exists = True
                        logger.info(f"✅ Stop Loss verified in Binance orders for {symbol} at ${sl_price:.4f}")

            # Если SL найден - обновляем БД и выходим
            if sl_exists:
                if position_id and self.db_pool:
                    try:
                        async with self.db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE monitoring.positions 
                                SET has_stop_loss = true, stop_loss_price = $1
                                WHERE id = $2
                            """, sl_price, position_id)
                    except:
                        pass
                return True

            # SL отсутствует - пытаемся восстановить
            logger.error(f"⚠️ No Stop Loss detected for {symbol}, attempting recovery...")

            # Получаем позицию для определения параметров
            positions = await exchange.get_open_positions()
            position = next((p for p in positions if p['symbol'] == symbol), None)

            if not position:
                logger.error(f"No position found for {symbol}, cannot set SL")
                return False

            # Берем данные из позиции или переданные параметры
            actual_entry = float(position.get('entry_price', 0)) or entry_price
            actual_side = position.get('side', '').upper() or side

            if not actual_entry or not actual_side:
                logger.error(f"Cannot determine position parameters for {symbol}")
                return False

            # Получаем текущую цену
            ticker = await exchange.get_ticker(symbol)
            current_price = float(ticker.get('price', actual_entry))

            # Рассчитываем SL с учетом направления
            is_long = actual_side in ['LONG', 'BUY']

            if is_long:
                # Для LONG: SL ниже текущей цены
                sl_price = min(
                    actual_entry * (1 - self.initial_sl_percent / 100),
                    current_price * (1 - self.initial_sl_percent / 100)
                )
            else:
                # Для SHORT: SL выше текущей цены
                sl_price = max(
                    actual_entry * (1 + self.initial_sl_percent / 100),
                    current_price * (1 + self.initial_sl_percent / 100)
                )

            logger.info(
                f"Recovery SL calculation for {symbol}: "
                f"side={actual_side}, entry=${actual_entry:.4f}, "
                f"current=${current_price:.4f}, SL=${sl_price:.4f}"
            )

            # Пытаемся установить SL с retry
            for attempt in range(3):
                if attempt > 0:
                    await asyncio.sleep(1 + attempt)

                if await exchange.set_stop_loss(symbol, sl_price):
                    logger.info(f"✅ Recovery successful: SL set at ${sl_price:.4f}")
                    self.stats['sl_set'] += 1

                    # Обновляем БД
                    if position_id and self.db_pool:
                        try:
                            async with self.db_pool.acquire() as conn:
                                await conn.execute("""
                                    UPDATE monitoring.positions 
                                    SET has_stop_loss = true, stop_loss_price = $1
                                    WHERE id = $2
                                """, sl_price, position_id)
                        except:
                            pass

                    return True

            logger.critical(f"❌ Failed to recover SL for {symbol} after 3 attempts!")
            return False

        except Exception as e:
            logger.error(f"Error in verify_and_recover_position: {e}", exc_info=True)
            return False

    async def periodic_health_check(self):
        """
        HIGH PRIORITY: Периодическая проверка здоровья системы
        Логирует метрики и проверяет критические компоненты
        """
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.health_check_interval)

                # Рассчитываем метрики
                uptime = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
                success_rate = (
                        self.stats['positions_opened'] /
                        max(self.stats['positions_opened'] + self.stats['positions_failed'], 1) * 100
                )
                sl_success_rate = (
                        self.stats['sl_set'] /
                        max(self.stats['sl_set'] + self.stats['sl_failed'], 1) * 100
                )

                # Логируем метрики
                logger.info("=" * 60)
                logger.info("📊 SYSTEM HEALTH CHECK")
                logger.info(f"Uptime: {uptime / 3600:.1f} hours")
                logger.info(f"Signals processed: {self.stats['signals_processed']}")
                logger.info(f"Positions opened: {self.stats['positions_opened']}")
                logger.info(f"Success rate: {success_rate:.1f}%")
                logger.info(f"SL success rate: {sl_success_rate:.1f}%")
                logger.info(f"Failed signals: {len(self.failed_signals)}")
                logger.info(f"Active locks: {len(self.locked_positions)}")

                # Проверяем подключения
                checks = []
                if self.binance:
                    try:
                        balance = await self.binance.get_balance()
                        checks.append(f"Binance: ✅ (${balance:.2f})")
                    except:
                        checks.append("Binance: ❌")

                if self.bybit:
                    try:
                        balance = await self.bybit.get_balance()
                        checks.append(f"Bybit: ✅ (${balance:.2f})")
                    except:
                        checks.append("Bybit: ❌")

                if self.db_pool:
                    try:
                        async with self.db_pool.acquire() as conn:
                            await conn.fetchval("SELECT 1")
                        checks.append("Database: ✅")
                    except:
                        checks.append("Database: ❌")

                logger.info(f"Connections: {' | '.join(checks)}")
                logger.info("=" * 60)

                # Логируем в БД
                await self._log_system_health(
                    "main_trader",
                    "HEALTHY" if success_rate > 50 else "DEGRADED"
                )

            except Exception as e:
                logger.error(f"Health check error: {e}")

    async def get_unprocessed_signals(self) -> List[Signal]:
        """Fetch unprocessed signals from fas.scoring_history - top N by score_week"""
        if not self.db_pool:
            logger.error("Database not available")
            return []

        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=self.signal_time_window)

        # Build WHERE conditions for working hours
        if len(self.working_hours) == 24:
            # All hours - no time filtering needed
            hour_condition = ""
        else:
            # Create SQL condition for specific hours
            hours_list = ','.join(str(h) for h in sorted(self.working_hours))
            hour_condition = f"AND EXTRACT(HOUR FROM sh.created_at) IN ({hours_list})"

        query = f"""
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
                {hour_condition}
            ORDER BY sh.score_week DESC, sh.score_month DESC
            LIMIT $4
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(
                    query,
                    time_threshold,
                    self.min_score_week,
                    self.min_score_month,
                    self.max_trades_per_15m
                )

                signals = []
                for row in rows:
                    # Double-check with Python (in case of timezone differences)
                    if not self.is_in_working_hours(row['created_at']):
                        logger.debug(f"Signal {row['id']} skipped - outside working hours")
                        continue
                        
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
                
                if signals:
                    logger.info(
                        f"Found {len(signals)} signals (top {self.max_trades_per_15m} by score_week). "
                        f"Highest score: {signals[0].score_week:.1f}%"
                    )
                elif len(self.working_hours) != 24:
                    logger.debug("No signals found within working hours")

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

    async def set_stop_loss(self, exchange: Union[BinanceExchange, BybitExchange],
                            signal: Signal, entry_price: float, position_id: Optional[int] = None) -> bool:
        """
        CRITICAL FIX v3: Правильный расчет Stop Loss с валидацией
        - Использует ТЕКУЩУЮ цену если она лучше entry (учитывает проскальзывание)
        - Валидирует что SL на правильной стороне от цены
        - Добавлена защита от установки SL который приведет к немедленному срабатыванию
        """
        try:
            # Получаем актуальную цену
            ticker = await exchange.get_ticker(signal.pair_symbol)
            current_price = float(ticker.get('price', entry_price))

            # CRITICAL: Определяем направление позиции правильно
            is_long = signal.recommended_action == 'BUY'

            # Рассчитываем SL от entry price
            if is_long:
                # Для LONG: SL ниже entry на X%
                sl_from_entry = entry_price * (1 - self.initial_sl_percent / 100)

                # ВАЖНО: SL не может быть выше текущей цены для LONG
                if sl_from_entry >= current_price:
                    # Если цена упала сильно, ставим SL от текущей цены
                    sl_price = current_price * (1 - self.initial_sl_percent / 100)
                    logger.warning(
                        f"Price slippage detected for {signal.pair_symbol}: "
                        f"entry=${entry_price:.4f}, current=${current_price:.4f}. "
                        f"Adjusting SL to ${sl_price:.4f}"
                    )
                else:
                    sl_price = sl_from_entry

            else:  # SHORT
                # Для SHORT: SL выше entry на X%
                sl_from_entry = entry_price * (1 + self.initial_sl_percent / 100)

                # ВАЖНО: SL не может быть ниже текущей цены для SHORT
                if sl_from_entry <= current_price:
                    # Если цена выросла сильно, ставим SL от текущей цены
                    sl_price = current_price * (1 + self.initial_sl_percent / 100)
                    logger.warning(
                        f"Price slippage detected for {signal.pair_symbol}: "
                        f"entry=${entry_price:.4f}, current=${current_price:.4f}. "
                        f"Adjusting SL to ${sl_price:.4f}"
                    )
                else:
                    sl_price = sl_from_entry

            # ВАЛИДАЦИЯ: Проверяем что SL имеет смысл
            if is_long:
                if sl_price >= current_price:
                    logger.error(
                        f"Invalid SL for LONG {signal.pair_symbol}: "
                        f"SL ${sl_price:.4f} >= current ${current_price:.4f}"
                    )
                    # Форсируем разумный SL
                    sl_price = current_price * 0.95  # 5% ниже текущей цены
                    logger.info(f"Forced SL to ${sl_price:.4f} (5% below current)")
            else:
                if sl_price <= current_price:
                    logger.error(
                        f"Invalid SL for SHORT {signal.pair_symbol}: "
                        f"SL ${sl_price:.4f} <= current ${current_price:.4f}"
                    )
                    # Форсируем разумный SL
                    sl_price = current_price * 1.05  # 5% выше текущей цены
                    logger.info(f"Forced SL to ${sl_price:.4f} (5% above current)")

            logger.info(
                f"Setting SL for {signal.pair_symbol} {signal.recommended_action}: "
                f"entry=${entry_price:.4f}, current=${current_price:.4f}, SL=${sl_price:.4f}"
            )

            # Set Stop Loss with retries
            for attempt in range(3):
                if attempt > 0:
                    await asyncio.sleep(self.delay_between_requests * (attempt + 1))

                if await exchange.set_stop_loss(signal.pair_symbol, sl_price):
                    self.stats['sl_set'] += 1
                    logger.info(f"✅ Stop Loss set at ${sl_price:.4f}")

                    # Обновляем БД если есть position_id
                    if position_id and self.db_pool:
                        try:
                            async with self.db_pool.acquire() as conn:
                                await conn.execute("""
                                    UPDATE monitoring.positions 
                                    SET has_stop_loss = true, stop_loss_price = $1
                                    WHERE id = $2
                                """, sl_price, position_id)
                        except Exception as e:
                            logger.error(f"Failed to update DB: {e}")

                    return True

            self.stats['sl_failed'] += 1
            logger.error(f"❌ Failed to set Stop Loss for {signal.pair_symbol}")
            return False

        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            self.stats['sl_failed'] += 1
            return False

    async def run(self):
        """Main trading loop"""
        logger.info("🚀 Starting Main Trader v7.2 - FIXED")
        logger.info(f"Mode: {self.trading_mode.value}")

        try:
            await self.initialize()
            # NEW: Запуск health check в фоне
            health_task = asyncio.create_task(self.periodic_health_check())
        except Exception as e:
            logger.critical(f"FATAL: System initialization failed: {e}")
            return

        try:
            while not self.shutdown_event.is_set():
                try:
                    # Fetch and process signals
                    signals = await self.get_unprocessed_signals()
                    if signals:
                        for signal in signals:
                            await self.process_signal(signal)
                            if len(signals) > 1:
                                await asyncio.sleep(self.delay_between_trades)
                    else:
                        logger.debug("No new signals found")

                    # Wait before next check
                    await asyncio.sleep(self.check_interval)

                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    await asyncio.sleep(10)

        finally:
            logger.info("Shutdown initiated...")
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up resources...")

        # Освобождаем все блокировки
        for lock_key in list(self.locked_positions):
            exchange, symbol = lock_key.split('_', 1)
            await self.release_position_lock(symbol, exchange)

        if self.db_pool:
            await self.db_pool.close()
        if self.binance:
            await self.binance.close()
        if self.bybit:
            await self.bybit.close()

        logger.info("✅ Cleanup complete. Goodbye!")

    async def log_position_to_db(self, signal: Signal, symbol: str, exchange: str,
                                 side: str, quantity: float, price: float, order_id: str):
        """Сохраняет информацию о позиции в БД"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Используем trading_pair_id из сигнала!
                trade_id = await conn.fetchval("""
                    INSERT INTO monitoring.trades (
                        signal_id, trading_pair_id, symbol, exchange, 
                        side, quantity, executed_qty, price, status, order_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    RETURNING id
                """,
                                               signal.id,  # signal_id
                                               signal.trading_pair_id,  # trading_pair_id - ИСПРАВЛЕНО!
                                               symbol,  # symbol
                                               exchange,  # exchange
                                               side,  # side
                                               quantity,  # quantity
                                               quantity,  # executed_qty
                                               price,  # price
                                               'FILLED',  # status
                                               order_id  # order_id
                                               )

                # Создаем запись в positions с opened_at
                await conn.execute("""
                    INSERT INTO monitoring.positions (
                        trade_id, symbol, exchange, side, quantity, 
                        entry_price, opened_at, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), 'OPEN')
                """, trade_id, symbol, exchange, side, quantity, price)

                logger.info(f"✅ Position logged to DB: trade_id={trade_id}, pair_id={signal.trading_pair_id}")
                return trade_id

        except Exception as e:
            logger.error(f"Failed to log position to DB: {e}")
            return None

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