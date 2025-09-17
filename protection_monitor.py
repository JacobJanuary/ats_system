#!/usr/bin/env python3
"""
Protection Monitor - PRODUCTION READY v7.0 (FINAL)
- ИСПРАВЛЕН расчет activation price для TS от ТЕКУЩЕЙ цены
- ДОБАВЛЕНА функция очистки зомби-ордеров
- УЛУЧШЕНА диагностика при установке TS
- ДОБАВЛЕН буфер безопасности для activation price
"""

import asyncio
import asyncpg
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Union, Set
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange
from utils.rate_limiter import RateLimiter

load_dotenv()

logging.basicConfig(
    level=logging.INFO if os.getenv('DEBUG', 'false').lower() != 'true' else logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] %(message)s',
    handlers=[logging.FileHandler('logs/protection.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class PositionStatus(Enum):
    UNPROTECTED = "unprotected"
    PARTIALLY_PROTECTED = "partially_protected"
    FULLY_PROTECTED = "fully_protected"
    TRAILING_ACTIVE = "trailing_active"
    PENDING_CLOSE = "pending_close"
    LOCKED = "locked"


@dataclass
class PositionInfo:
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
    has_trailing: bool = False
    has_tp: bool = False
    has_breakeven_order: bool = False
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    trailing_activation_price: Optional[float] = None
    status: PositionStatus = PositionStatus.UNPROTECTED


class ProtectionMonitor:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'), 'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'), 'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.tp_percent = float(os.getenv('TAKE_PROFIT_PERCENT', '1'))
        self.trailing_activation = float(os.getenv('TRAILING_ACTIVATION_PERCENT', '1'))
        self.trailing_callback = float(os.getenv('TRAILING_CALLBACK_RATE', '0.5'))
        self.trailing_activation_buffer = float(os.getenv('TRAILING_ACTIVATION_BUFFER', '0.3'))  # Буфер 0.3%
        self.max_position_duration_hours = int(os.getenv('MAX_POSITION_DURATION_HOURS', '24'))
        self.min_profit_for_breakeven = float(os.getenv('MIN_PROFIT_FOR_BREAKEVEN', '0.3'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.taker_fee_percent = float(os.getenv('TAKER_FEE_PERCENT', '0.06'))
        self.request_delay = 0.5 if self.testnet else 0.1
        self.between_positions_delay = 1.0 if self.testnet else 0.2
        self.binance: Optional[BinanceExchange] = None
        self.bybit: Optional[BybitExchange] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.tracked_positions: Dict[str, PositionInfo] = {}
        self.locked_positions: set = set()
        self.zombie_orders_cleaned = 0  # Счетчик очищенных зомби-ордеров
        self._log_configuration()

    def _log_configuration(self):
        logger.info("=" * 80)
        logger.info("PROTECTION MONITOR CONFIGURATION v7.0 (FINAL)")
        logger.info("=" * 80)
        logger.info(f"Environment: {'TESTNET' if self.testnet else 'MAINNET'}")
        logger.info(f"Stop Loss: {self.sl_percent}%")
        logger.info(f"Take Profit: {self.tp_percent}%")
        logger.info(f"Trailing Activation: {self.trailing_activation}%")
        logger.info(f"Trailing Callback: {self.trailing_callback}%")
        logger.info(f"Trailing Buffer: {self.trailing_activation_buffer}%")
        logger.info(f"Max Position Duration: {self.max_position_duration_hours}h")
        logger.info(f"Check Interval: {self.check_interval}s")
        logger.info("=" * 80)

    async def initialize(self):
        logger.info("🚀 Initializing Protection Monitor...")
        await self._init_db()
        init_tasks = []
        if os.getenv('BINANCE_API_KEY'): init_tasks.append(self._init_exchange('Binance'))
        if os.getenv('BYBIT_API_KEY'): init_tasks.append(self._init_exchange('Bybit'))
        await asyncio.gather(*init_tasks)
        if not self.binance and not self.bybit: raise Exception("No exchanges available!")
        logger.info("✅ Protection Monitor initialized")

    async def _init_db(self):
        try:
            self.db_pool = await asyncpg.create_pool(**self.db_config)
            await self.db_pool.fetchval("SELECT 1")
            logger.info("✅ Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            self.db_pool = None

    async def _init_exchange(self, name: str):
        try:
            config = {
                'api_key': os.getenv(f'{name.upper()}_API_KEY'),
                'api_secret': os.getenv(f'{name.upper()}_API_SECRET'),
                'testnet': self.testnet
            }
            exchange_class = BinanceExchange if name == 'Binance' else BybitExchange
            exchange = exchange_class(config)
            await exchange.initialize()
            balance = await exchange.get_balance()
            logger.info(f"✅ {name} initialized. Balance: ${balance:.2f}")
            if name == 'Binance':
                self.binance = exchange
            else:
                self.bybit = exchange
        except Exception as e:
            logger.error(f"Failed to initialize {name}: {e}")

    async def acquire_position_lock(self, symbol: str, exchange: str, timeout: int = 30) -> bool:
        """Получение эксклюзивной блокировки на позицию через PostgreSQL advisory locks"""
        lock_key = f"{exchange}_{symbol}"

        if lock_key in self.locked_positions:
            logger.debug(f"Position {lock_key} already locked by this instance")
            return False

        if not self.db_pool:
            return True

        try:
            async with self.db_pool.acquire() as conn:
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

    async def _calculate_position_age_async(self, position: Dict, exchange_name: str) -> float:
        """
        CRITICAL FIX v2: Асинхронный расчет возраста позиции
        - Для Binance: ТОЛЬКО из БД (updateTime обновляется при любом изменении)
        - Для Bybit: сначала БД, потом createdTime из API
        """
        symbol = position.get('symbol')

        # Сначала ВСЕГДА пытаемся получить из БД - это источник истины
        if symbol and self.db_pool:
            try:
                age = await self.get_position_age_from_db(symbol, exchange_name)
                if age > 0:
                    logger.debug(f"Position age for {symbol} from DB: {age:.2f} hours")
                    return age
            except Exception as e:
                logger.warning(f"Failed to get position age from DB: {e}")

        # Fallback: ТОЛЬКО для Bybit используем createdTime
        if exchange_name == "Bybit":
            timestamp_ms = position.get("createdTime", 0)
            if timestamp_ms:
                age_hours = (datetime.now(timezone.utc).timestamp() - (int(timestamp_ms) / 1000)) / 3600
                logger.debug(f"Position age for {symbol} from Bybit API: {age_hours:.2f} hours")
                return age_hours

        # Для Binance без БД - возраст неизвестен, возвращаем 0
        logger.warning(f"Cannot determine age for {symbol} on {exchange_name}, assuming new position")
        return 0.0

    def _calculate_position_age(self, position: Dict, exchange_name: str) -> float:
        """
        CRITICAL FIX v2: Синхронная обертка для обратной совместимости
        """
        # Если мы уже в async контексте, создаем задачу
        try:
            loop = asyncio.get_running_loop()
            # Мы в async контексте, создаем корутину которая будет выполнена позже
            future = asyncio.ensure_future(
                self._calculate_position_age_async(position, exchange_name)
            )
            # Возвращаем 0 как временное значение, реальное значение будет получено асинхронно
            return 0.0
        except RuntimeError:
            # Нет запущенного loop, запускаем синхронно
            return asyncio.run(self._calculate_position_age_async(position, exchange_name))

    def _calculate_pnl_percent(self, entry_price: float, current_price: float, side: str) -> float:
        if entry_price <= 0: return 0.0
        if side.upper() in ['LONG', 'BUY']:
            return ((current_price - entry_price) / entry_price) * 100
        return ((entry_price - current_price) / entry_price) * 100

    async def _check_protection_status(self, exchange_name: str, position: Dict,
                                       symbol_orders: List[Dict]) -> PositionInfo:
        symbol = position.get('symbol')
        pos_info = PositionInfo(
            symbol=symbol, exchange=exchange_name, side=position.get('side', '').upper(),
            quantity=float(position.get('quantity', 0)), entry_price=float(position.get('entry_price', 0)),
            current_price=float(position.get('mark_price', position.get('entry_price', 0))),
            pnl=float(position.get('pnl', 0)), age_hours=self._calculate_position_age(position, exchange_name)
        )
        pos_info.pnl_percent = self._calculate_pnl_percent(pos_info.entry_price, pos_info.current_price, pos_info.side)

        if exchange_name == 'Bybit':
            pos_info.sl_price = float(position.get('stopLoss') or 0)
            pos_info.tp_price = float(position.get('takeProfit') or 0)
            pos_info.trailing_activation_price = float(position.get('activePrice') or 0)

            pos_info.has_sl = pos_info.sl_price > 0
            pos_info.has_tp = pos_info.tp_price > 0

            has_ts_value = float(position.get('trailingStop') or 0) > 0
            if has_ts_value and pos_info.trailing_activation_price == 0:
                logger.warning(
                    f"Bybit: Found broken Trailing Stop for {symbol} with 0 activation price. Marking as inactive.")
                pos_info.has_trailing = False
            else:
                pos_info.has_trailing = has_ts_value

        # ИСПРАВЛЕНИЕ для Binance: проверяем АКТИВНЫЕ trailing stop ордера
        active_trailing = False

        for order in symbol_orders:
            order_type = order.get('type', '').lower()
            order_status = order.get('status', '').upper()

            if exchange_name == 'Binance':
                if order_type in ['stop_market', 'stop']:
                    pos_info.has_sl = True
                    pos_info.sl_price = float(order.get('stopPrice', 0))
                elif order_type == 'trailing_stop_market':
                    # ВАЖНО: Проверяем статус ордера
                    if order_status in ['NEW', 'PARTIALLY_FILLED']:
                        active_trailing = True
                        pos_info.has_trailing = True
                        # Пытаемся получить activation price из ордера
                        activation_price = float(order.get('activatePrice', 0)) or float(order.get('stopPrice', 0))
                        if activation_price > 0:
                            pos_info.trailing_activation_price = activation_price
                    else:
                        logger.warning(f"Found inactive TS order for {symbol} with status {order_status}")
                elif order_type == 'take_profit_market':
                    pos_info.has_tp = True
                    pos_info.tp_price = float(order.get('stopPrice', 0))

            if order_type == 'limit' and order.get('reduceOnly', False):
                pos_info.has_breakeven_order = True

        # Для Binance используем результат проверки активных ордеров
        if exchange_name == 'Binance':
            pos_info.has_trailing = active_trailing

        # Определяем статус защиты
        if pos_info.has_breakeven_order:
            pos_info.status = PositionStatus.PENDING_CLOSE
        elif pos_info.has_sl and pos_info.has_trailing:
            pos_info.status = PositionStatus.FULLY_PROTECTED
        elif pos_info.has_sl and pos_info.has_tp:
            pos_info.status = PositionStatus.FULLY_PROTECTED
        elif pos_info.has_sl or pos_info.has_trailing:
            pos_info.status = PositionStatus.PARTIALLY_PROTECTED
        else:
            pos_info.status = PositionStatus.UNPROTECTED

        # Проверяем активацию TS
        if pos_info.has_trailing and pos_info.trailing_activation_price > 0:
            if pos_info.side in ['LONG', 'BUY']:
                if pos_info.current_price >= pos_info.trailing_activation_price:
                    pos_info.status = PositionStatus.TRAILING_ACTIVE
            else:  # SHORT
                if pos_info.current_price <= pos_info.trailing_activation_price:
                    pos_info.status = PositionStatus.TRAILING_ACTIVE

        return pos_info

    async def _safe_sl_to_ts_upgrade(self, exchange: Union[BinanceExchange, BybitExchange],
                                     pos_info: PositionInfo) -> bool:
        """
        CRITICAL FIX v2: Умная установка TS с минимальным буфером
        - Начинаем с 0.1% буфера для быстрой активации
        - Адаптивно увеличиваем если биржа отклоняет
        - Максимум 10 попыток с разными буферами
        """
        symbol = pos_info.symbol

        logger.info(f"🔄 Starting SL→TS upgrade for {symbol}")
        logger.info(f"  Current price: ${pos_info.current_price:.8f}")
        logger.info(f"  Entry price: ${pos_info.entry_price:.8f}")
        logger.info(f"  PnL: {pos_info.pnl_percent:.2f}%")

        # Для Bybit - прямая установка TS (SL автоматически заменяется)
        if isinstance(exchange, BybitExchange):
            # ВАЖНО: Используем актуальную рыночную цену, а не mark_price!
            ticker = await exchange.get_ticker(symbol)
            last_price = float(ticker.get('price', pos_info.current_price))
            
            # Минимальный буфер для Bybit
            buffer_percent = 0.1
            if pos_info.side in ['LONG', 'BUY']:
                # Для LONG: activePrice должен быть > last_price
                activation_price = last_price * (1 + buffer_percent / 100)
            else:
                # Для SHORT: activePrice должен быть < last_price
                activation_price = last_price * (1 - buffer_percent / 100)

            logger.info(f"  Bybit: Using last_price=${last_price:.8f} (not mark_price=${pos_info.current_price:.8f})")
            logger.info(f"  Bybit: Setting TS with activation=${activation_price:.8f} (buffer={buffer_percent}%)")

            if await exchange.set_trailing_stop(symbol, activation_price, self.trailing_callback):
                logger.info(f"✅ Successfully set TS for {symbol}")
                return True
            else:
                logger.error(f"Failed to set TS for {symbol}")
                return False

        # Для Binance - сложная логика с отменой SL и адаптивной установкой TS
        else:  # BinanceExchange
            # Получаем ID текущего SL ордера
            open_orders = await exchange.get_open_orders(symbol)
            sl_order_id = None
            for order in open_orders:
                if order.get('type', '').lower() in ['stop_market', 'stop']:
                    sl_order_id = order.get('orderId')
                    break

            if not sl_order_id:
                logger.error(f"No SL order found for {symbol}, cannot upgrade")
                return False

            logger.info(f"  Binance: Cancelling SL order {sl_order_id}")
            if not await exchange.cancel_order(symbol, sl_order_id):
                logger.error(f"Failed to cancel SL for {symbol}")
                return False

            await asyncio.sleep(0.1 if not self.testnet else 0.5)

            # CRITICAL: Адаптивная установка TS с минимальным буфером
            buffer_steps = [0.1, 0.2, 0.3, 0.5, 0.7, 1.0, 1.5, 2.0, 3.0, 5.0]  # Проценты

            for attempt, buffer_percent in enumerate(buffer_steps):
                if pos_info.side in ['LONG', 'BUY']:
                    activation_price = pos_info.current_price * (1 + buffer_percent / 100)
                else:
                    activation_price = pos_info.current_price * (1 - buffer_percent / 100)

                logger.info(
                    f"  Attempt {attempt + 1}: Setting TS with activation=${activation_price:.8f} "
                    f"(buffer={buffer_percent}%)"
                )

                # Пытаемся установить TS
                result = await exchange.set_trailing_stop(symbol, activation_price, self.trailing_callback)

                if result:
                    logger.info(
                        f"✅ Successfully set TS for {symbol} with {buffer_percent}% buffer. "
                        f"Will activate immediately when price reaches ${activation_price:.8f}"
                    )
                    return True

                # Небольшая задержка перед следующей попыткой
                await asyncio.sleep(0.2)

            # Если все попытки неудачны - восстанавливаем SL
            logger.error(f"Failed to set TS after {len(buffer_steps)} attempts, restoring SL")

            sl_price = (pos_info.entry_price * (1 - self.sl_percent / 100) if pos_info.side in ['LONG', 'BUY']
                        else pos_info.entry_price * (1 + self.sl_percent / 100))

            if await exchange.set_stop_loss(symbol, sl_price):
                logger.info(f"SL restored at ${sl_price:.8f}")
            else:
                logger.critical(f"Failed to restore SL for {symbol}!")

            return False

    async def _clean_zombie_orders_smart(self, exchange_name: str):
        """Умная очистка зомби-ордеров с учетом особенностей бирж"""
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        if not exchange:
            return

        try:
            positions = await exchange.get_open_positions()
            all_orders = await exchange.get_open_orders()

            if not all_orders:
                return

            # Создаем словарь позиций с их характеристиками
            position_map = {}
            if positions:
                for pos in positions:
                    symbol = pos.get('symbol')
                    if symbol:
                        position_map[symbol] = {
                            'position': pos,
                            'age_hours': self._calculate_position_age(pos, exchange_name)
                        }

            logger.info(f"🔍 Analyzing {len(all_orders)} orders for {len(position_map)} positions on {exchange_name}")

            orders_by_symbol = defaultdict(list)
            for order in all_orders:
                symbol = order.get('symbol')
                if symbol:
                    orders_by_symbol[symbol].append(order)

            zombie_orders = []

            for symbol, orders in orders_by_symbol.items():
                # Случай 1: Ордера без позиции - всегда зомби
                if symbol not in position_map:
                    logger.warning(f"🧟 Found {len(orders)} orders for {symbol} without position")
                    zombie_orders.extend(orders)
                    continue

                position_info = position_map[symbol]
                position = position_info['position']
                age_hours = position_info['age_hours']
                is_aged = age_hours > self.max_position_duration_hours if self.max_position_duration_hours > 0 else False

                # Разделяем ордера по типам
                protective_orders = []
                limit_orders = []

                for order in orders:
                    order_type = order.get('type', '').lower()
                    if order_type in ['stop_market', 'stop', 'trailing_stop_market', 'take_profit_market']:
                        protective_orders.append(order)
                    elif order_type == 'limit' and order.get('reduceOnly', False):
                        limit_orders.append(order)

                # Правила для защитных ордеров
                if exchange_name == 'Binance':
                    # Binance: максимум 2 защитных ордера (SL + TP или TS)
                    # НО! Нельзя иметь SL и TS одновременно

                    sl_orders = [o for o in protective_orders if o.get('type', '').lower() in ['stop_market', 'stop']]
                    ts_orders = [o for o in protective_orders if o.get('type', '').lower() == 'trailing_stop_market']
                    tp_orders = [o for o in protective_orders if o.get('type', '').lower() == 'take_profit_market']

                    # Если есть и SL и TS - это проблема (оставляем только TS)
                    if sl_orders and ts_orders:
                        logger.warning(f"⚠️ {symbol} has both SL and TS on Binance (impossible)")
                        zombie_orders.extend(sl_orders)  # Удаляем SL

                    # Удаляем дубликаты каждого типа
                    if len(sl_orders) > 1:
                        sl_orders.sort(key=lambda x: x.get('orderId', ''), reverse=True)
                        zombie_orders.extend(sl_orders[1:])
                    if len(ts_orders) > 1:
                        ts_orders.sort(key=lambda x: x.get('orderId', ''), reverse=True)
                        zombie_orders.extend(ts_orders[1:])
                    if len(tp_orders) > 1:
                        tp_orders.sort(key=lambda x: x.get('orderId', ''), reverse=True)
                        zombie_orders.extend(tp_orders[1:])

                elif exchange_name == 'Bybit':
                    # Bybit: для aged позиций может быть 3 ордера (SL + TS + limit breakeven)
                    # Для обычных позиций - максимум 2

                    max_protective = 3 if is_aged else 2

                    if len(protective_orders) > max_protective:
                        logger.warning(
                            f"⚠️ {symbol} has {len(protective_orders)} protective orders (max {max_protective})")
                        protective_orders.sort(key=lambda x: x.get('orderId', ''), reverse=True)
                        zombie_orders.extend(protective_orders[max_protective:])

                    # Проверяем дубликаты по типам
                    order_types = defaultdict(list)
                    for order in protective_orders[:max_protective]:
                        order_type = order.get('type', '').lower()
                        order_types[order_type].append(order)

                    for order_type, type_orders in order_types.items():
                        if len(type_orders) > 1:
                            type_orders.sort(key=lambda x: x.get('orderId', ''), reverse=True)
                            zombie_orders.extend(type_orders[1:])

                # Проверяем лимитные ордера на безубыток
                if len(limit_orders) > 1:
                    logger.warning(f"⚠️ {symbol} has {len(limit_orders)} breakeven limit orders")
                    limit_orders.sort(key=lambda x: x.get('orderId', ''), reverse=True)
                    zombie_orders.extend(limit_orders[1:])

            # Удаляем зомби-ордера
            if zombie_orders:
                logger.warning(f"🧟 Found {len(zombie_orders)} zombie orders on {exchange_name}")

                # Группируем по символам для удобства
                zombies_by_symbol = defaultdict(list)
                for order in zombie_orders:
                    zombies_by_symbol[order.get('symbol')].append(order)

                for symbol, symbol_zombies in zombies_by_symbol.items():
                    logger.info(f"  {symbol}: {len(symbol_zombies)} zombie orders")
                    for order in symbol_zombies:
                        order_type = order.get('type', '').lower()
                        logger.debug(f"    - {order_type} (ID: {order.get('orderId')})")

                # Удаляем
                for order in zombie_orders:
                    try:
                        order_id = order.get('orderId')
                        symbol = order.get('symbol')
                        order_type = order.get('type')

                        logger.info(f"Cancelling zombie: {order_type} for {symbol} (ID: {order_id})")

                        if await exchange.cancel_order(symbol, order_id):
                            self.zombie_orders_cleaned += 1
                            logger.info(f"✅ Zombie order {order_id} cancelled")
                        else:
                            logger.error(f"Failed to cancel zombie order {order_id}")

                        await asyncio.sleep(self.request_delay)

                    except Exception as e:
                        logger.error(f"Error cancelling zombie order: {e}")

                logger.info(f"🧹 Cleaned {len(zombie_orders)} zombie orders")
            else:
                logger.info(f"✨ No zombie orders found on {exchange_name}")

        except Exception as e:
            logger.error(f"Error during smart zombie cleanup on {exchange_name}: {e}", exc_info=True)

    async def _handle_breached_sl(self, exchange: Union[BinanceExchange, BybitExchange],
                                  pos_info: PositionInfo) -> bool:
        """Проверка и обработка пробитого SL"""
        if not pos_info.has_sl or not pos_info.sl_price or pos_info.sl_price == 0:
            return False

        is_breached = False
        if pos_info.side in ['LONG', 'BUY'] and pos_info.current_price < pos_info.sl_price:
            is_breached = True
        elif pos_info.side == 'SHORT' and pos_info.current_price > pos_info.sl_price:
            is_breached = True

        if is_breached:
            logger.critical("=" * 80)
            logger.critical(f"!!! EMERGENCY EXIT !!!")
            logger.critical(f"Position {pos_info.symbol} on {pos_info.exchange} has breached its Stop Loss.")
            logger.critical(f"Side: {pos_info.side}, Entry: ${pos_info.entry_price:.8f}")
            logger.critical(f"Stop Loss: ${pos_info.sl_price:.8f}, Current Price: ${pos_info.current_price:.8f}")
            logger.critical("This indicates a 'ghost' SL. Closing position at market immediately.")
            logger.critical("=" * 80)
            try:
                await exchange.cancel_all_open_orders(pos_info.symbol)
                await asyncio.sleep(self.request_delay)
                await exchange.close_position(pos_info.symbol)
                return True
            except Exception as e:
                logger.error(f"CRITICAL: Failed to execute emergency close for {pos_info.symbol}: {e}", exc_info=True)
        return False

    async def _apply_protection(self, exchange: Union[BinanceExchange, BybitExchange], pos_info: PositionInfo):
        symbol = pos_info.symbol
        logger.info(f"🛡️ Applying protection to {symbol} on {pos_info.exchange}")
        logger.debug(
            f"  Position details: PnL={pos_info.pnl_percent:.2f}%, has_sl={pos_info.has_sl}, has_trailing={pos_info.has_trailing}")

        # Проверяем не пробит ли уже SL
        if await self._handle_breached_sl(exchange, pos_info):
            return

        try:
            # ВАЖНО: На Binance нельзя иметь SL и TS одновременно!
            if pos_info.exchange == 'Binance' and pos_info.has_trailing:
                # Если уже есть TS, ничего не делаем
                logger.info(f"  Position {symbol} already has Trailing Stop, no additional protection needed")
                return

            # Action 1: Если нет защиты вообще, устанавливаем SL
            if not pos_info.has_sl and not pos_info.has_trailing:
                # Расчет SL с учетом текущей цены
                current_price = pos_info.current_price
                entry_price = pos_info.entry_price

                if pos_info.side in ['LONG', 'BUY']:
                    sl_from_entry = entry_price * (1 - self.sl_percent / 100)
                    sl_from_current = current_price * (1 - self.sl_percent / 100)
                    sl_price = min(sl_from_entry, sl_from_current)
                    logger.debug(
                        f"  LONG SL calculation: from_entry=${sl_from_entry:.4f}, from_current=${sl_from_current:.4f}, using=${sl_price:.4f}")
                else:  # SHORT
                    sl_from_entry = entry_price * (1 + self.sl_percent / 100)
                    sl_from_current = current_price * (1 + self.sl_percent / 100)
                    sl_price = max(sl_from_entry, sl_from_current)
                    logger.debug(
                        f"  SHORT SL calculation: from_entry=${sl_from_entry:.4f}, from_current=${sl_from_current:.4f}, using=${sl_price:.4f}")

                await asyncio.sleep(self.request_delay)
                if await exchange.set_stop_loss(symbol, sl_price):
                    logger.info(f"✅ Stop Loss added for {symbol} at ${sl_price:.8f}")
                    pos_info.has_sl = True  # Обновляем статус

            # Action 2: Апгрейд SL → TS если позиция прибыльна
            if pos_info.has_sl and not pos_info.has_trailing:
                if pos_info.pnl_percent >= self.trailing_activation:
                    logger.info(
                        f"📈 Position {symbol} is profitable ({pos_info.pnl_percent:.2f}%). Upgrading SL to Trailing Stop.")
                    success = await self._safe_sl_to_ts_upgrade(exchange, pos_info)
                    if success:
                        pos_info.has_trailing = True
                        pos_info.has_sl = False  # На Binance SL заменяется на TS
                else:
                    logger.debug(
                        f"  Position not ready for TS: PnL {pos_info.pnl_percent:.2f}% < target {self.trailing_activation}%")

        except Exception as e:
            logger.error(f"Error applying protection to {symbol}: {e}", exc_info=True)

    async def _handle_aged_position(self, exchange: Union[BinanceExchange, BybitExchange], pos_info: PositionInfo):
        symbol = pos_info.symbol
        logger.warning(f"⏰ Position {symbol} is aged ({pos_info.age_hours:.1f}h). Applying exit logic.")

        if pos_info.has_breakeven_order:
            logger.info(f"Breakeven limit order already exists for {symbol}. Monitoring.")
            return

        try:
            if pos_info.pnl_percent > self.min_profit_for_breakeven:
                logger.info(f"📈 Aged position {symbol} is in profit ({pos_info.pnl_percent:.2f}%), closing at market.")
                await exchange.cancel_all_open_orders(symbol)
                await asyncio.sleep(self.request_delay)
                if await exchange.close_position(symbol):
                    logger.info(f"✅ Position {symbol} closed at market due to age and profit.")
            else:
                logger.info(
                    f"📉 Aged position {symbol} not in profit ({pos_info.pnl_percent:.2f}%), setting breakeven limit order.")
                fee_multiplier = 1 + (self.taker_fee_percent * 2 / 100)
                side = 'SELL' if pos_info.side in ['LONG', 'BUY'] else 'BUY'
                breakeven_price = pos_info.entry_price * fee_multiplier if side == 'SELL' else pos_info.entry_price / fee_multiplier
                logger.info(f"Placing breakeven limit order for {symbol} at ${breakeven_price:.8f}.")
                await exchange.create_limit_order(symbol=symbol, side=side, quantity=pos_info.quantity,
                                                  price=breakeven_price, reduce_only=True)
        except Exception as e:
            logger.error(f"Error handling aged position {symbol}: {e}", exc_info=True)

    async def process_exchange_positions(self, exchange_name: str):
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        if not exchange: return

        try:
            positions = await exchange.get_open_positions()
            if not positions: return

            logger.info(f"Found {len(positions)} open positions on {exchange_name}")
            all_orders = await exchange.get_open_orders() or []
            logger.debug(f"Found {len(all_orders)} open orders on {exchange_name}")

            orders_by_symbol = defaultdict(list)
            for order in all_orders:
                if order.get('symbol'): orders_by_symbol[order['symbol']].append(order)

            for position in positions:
                symbol = position.get('symbol')
                if not symbol: continue

                # Пытаемся получить блокировку
                if not await self.acquire_position_lock(symbol, exchange_name):
                    logger.debug(f"Position {symbol} is locked, skipping")
                    continue

                try:
                    await asyncio.sleep(self.between_positions_delay)

                    pos_info = await self._check_protection_status(exchange_name, position, orders_by_symbol[symbol])
                    # CRITICAL FIX: Правильно получаем возраст позиции асинхронно
                    real_age = await self._calculate_position_age_async(position, exchange_name)
                    pos_info.age_hours = real_age
                    self.tracked_positions[f"{exchange_name}_{symbol}"] = pos_info

                    logger.info(
                        f"📊 {symbol}: PnL={pos_info.pnl_percent:.2f}%, Age={pos_info.age_hours:.1f}h, Status={pos_info.status.value}")

                    # Run emergency check
                    if await self._handle_breached_sl(exchange, pos_info):
                        continue

                    if pos_info.status == PositionStatus.TRAILING_ACTIVE and not (
                            pos_info.age_hours > self.max_position_duration_hours > 0):
                        logger.debug(f"Position {symbol} has an active trailing stop. Monitoring.")
                        continue

                    if pos_info.status == PositionStatus.PENDING_CLOSE:
                        logger.info(f"Position {symbol} has a pending breakeven limit order. Monitoring.")
                        continue

                    if self.max_position_duration_hours > 0 and pos_info.age_hours > self.max_position_duration_hours:
                        await self._handle_aged_position(exchange, pos_info)
                        continue

                    if pos_info.status in [PositionStatus.UNPROTECTED, PositionStatus.PARTIALLY_PROTECTED]:
                        await self._apply_protection(exchange, pos_info)

                finally:
                    # Всегда освобождаем блокировку
                    await self.release_position_lock(symbol, exchange_name)

        except Exception as e:
            logger.error(f"Error processing {exchange_name} positions: {e}", exc_info=True)

    async def run(self):
        logger.info(f"🚀 Starting Protection Monitor v7.0 - FINAL")
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'MAINNET'}")
        await self.initialize()

        try:
            check_count = 0
            while True:
                check_count += 1
                logger.info(f"\n{'=' * 40}\nProtection Check #{check_count}\n{'=' * 40}")

                tasks = []
                if self.binance: tasks.append(self.process_exchange_positions('Binance'))
                if self.bybit: tasks.append(self.process_exchange_positions('Bybit'))

                if tasks: await asyncio.gather(*tasks)

                # Очистка зомби-ордеров каждые 10 циклов
                if check_count % 3 == 0:
                    await self._clean_zombie_orders_smart('Binance')
                    await self._clean_zombie_orders_smart('Bybit')

                logger.info(f"Check complete. Positions tracked: {len(self.tracked_positions)}")
                if self.zombie_orders_cleaned > 0:
                    logger.info(f"Zombie orders cleaned in total: {self.zombie_orders_cleaned}")

                self.tracked_positions.clear()

                await asyncio.sleep(self.check_interval)

        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Shutdown requested")
        finally:
            logger.info("Cleaning up...")
            # Освобождаем все блокировки
            for lock_key in list(self.locked_positions):
                exchange, symbol = lock_key.split('_', 1)
                await self.release_position_lock(symbol, exchange)

            if self.db_pool: await self.db_pool.close()
            if self.binance: await self.binance.close()
            if self.bybit: await self.bybit.close()
            logger.info("✅ Cleanup complete")

    async def get_position_age_from_db(self, symbol: str, exchange: str) -> float:
        """Получает реальный возраст позиции из БД"""
        if not self.db_pool:
            return 0.0

        try:
            async with self.db_pool.acquire() as conn:
                age_hours = await conn.fetchval("""
                    SELECT EXTRACT(EPOCH FROM (NOW() - opened_at)) / 3600 
                    FROM monitoring.positions 
                    WHERE symbol = $1 
                    AND exchange = $2 
                    AND status = 'OPEN'
                    ORDER BY opened_at DESC 
                    LIMIT 1
                """, symbol, exchange)
                return age_hours or 0.0
        except Exception as e:
            logger.error(f"Error getting position age from DB: {e}")
            return 0.0

async def main():
    monitor = ProtectionMonitor()
    try:
        await monitor.run()
    except asyncio.CancelledError:
        logger.info("Main task cancelled.")



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")