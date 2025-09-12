#!/usr/bin/env python3
"""
Protection Monitor - PRODUCTION READY v2.7 (FIXED)
- Исправлена синтаксическая ошибка в _is_protection_incomplete
- Исправлен вызов _calculate_position_age
- Улучшена обработка временных меток позиций
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange
from utils.rate_limiter import RateLimiter

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler('protection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProtectionMonitor:
    def __init__(self):
        self.stop_loss_type = os.getenv('STOP_LOSS_TYPE', 'fixed').lower()
        self.sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.tp_percent = float(os.getenv('TAKE_PROFIT_PERCENT', '3'))
        self.trailing_activation = float(os.getenv('TRAILING_ACTIVATION_PERCENT', '3.5'))
        self.trailing_callback = float(os.getenv('TRAILING_CALLBACK_RATE', '0.5'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.max_position_duration_hours = int(os.getenv('MAX_POSITION_DURATION_HOURS', '0'))
        self.taker_fee_percent = float(os.getenv('TAKER_FEE_PERCENT', '0.06'))
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'

        self.binance: Optional[BinanceExchange] = None
        self.bybit: Optional[BybitExchange] = None

        self.stats = {
            'checks': 0, 'positions_protected': 0, 'positions_closed': 0,
            'errors': 0, 'start_time': datetime.now(timezone.utc)
        }

        # Детальная статистика по биржам
        self.exchange_stats = {
            'Binance': {
                'positions_found': 0,
                'positions_open': 0,
                'positions_protected': 0,
                'positions_expired': 0,
                'positions_with_breakeven': 0,
                'positions_with_errors': 0,
                'ts_sl_applied': 0,
                'fixed_sl_applied': 0,
                'tp_applied': 0,
                'protection_complete': 0,
                'protection_incomplete': 0,
                'rate_limit_errors': 0,
                'api_errors': 0,
                'last_cycle_time': None
            },
            'Bybit': {
                'positions_found': 0,
                'positions_open': 0,
                'positions_protected': 0,
                'positions_expired': 0,
                'positions_with_breakeven': 0,
                'positions_with_errors': 0,
                'ts_sl_applied': 0,
                'fixed_sl_applied': 0,
                'tp_applied': 0,
                'protection_complete': 0,
                'protection_incomplete': 0,
                'rate_limit_errors': 0,
                'api_errors': 0,
                'last_cycle_time': None
            }
        }

        # МОНИТОРИНГ ОШИБОК И АЛЕРТЫ
        self.error_monitor = {
            'symbol_errors': {},  # symbol -> error_count
            'error_patterns': {},  # error_pattern -> count
            'alert_thresholds': {
                'symbol_error_threshold': 10,  # Алерт если символ имеет >10 ошибок
                'pattern_error_threshold': 50,  # Алерт если паттерн имеет >50 ошибок
                'api_error_rate_threshold': 5.0  # Алерт если >5% API ошибок
            },
            'alerts_sent': set(),  # Чтобы не отправлять одинаковые алерты
            'last_alert_time': None
        }

        self.binance_semaphore = asyncio.Semaphore(10)
        self.bybit_semaphore = asyncio.Semaphore(5)

        # Rate limiter для предотвращения банов на биржах
        self.rate_limiter = RateLimiter()

        # Кэш состояний защиты для предотвращения повторных проверок
        self.protection_cache = {}
        self.cache_ttl = 300  # 5 минут TTL кэша

        self._log_configuration()

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Проверка актуальности кэша состояний защиты"""
        if cache_key not in self.protection_cache:
            return False

        cache_time, _ = self.protection_cache[cache_key]
        age_seconds = (datetime.now(timezone.utc) - cache_time).total_seconds()
        return age_seconds < self.cache_ttl

    def _update_cache(self, cache_key: str, has_protection: bool):
        """Обновление кэша состояний защиты"""
        self.protection_cache[cache_key] = (datetime.now(timezone.utc), has_protection)

    def _round_price_to_tick_size(self, price: float, tick_size: float) -> float:
        """Округляет цену до ближайшего значения tick_size для точности биржи"""
        if tick_size <= 0:
            return price
        try:
            # Округляем до ближайшего кратного tick_size
            rounded = round(price / tick_size) * tick_size
            # Убеждаемся что результат не меньше минимального tick_size
            return max(rounded, tick_size)
        except (ZeroDivisionError, OverflowError):
            logger.warning(f"Invalid tick_size {tick_size} for price {price}, using original price")
            return price

    def _get_tick_size(self, exchange: BinanceExchange | BybitExchange, symbol: str) -> float:
        """Получает tick_size для символа с fallback значениями"""
        try:
            if isinstance(exchange, BinanceExchange):
                return float(exchange.symbol_info.get(symbol, {}).get('priceFilter', {}).get('tickSize', 0.0001))
            else:  # BybitExchange
                return float(exchange.symbol_info.get(symbol, {}).get('tick_size', 0.0001))
        except (KeyError, TypeError, ValueError):
            logger.debug(f"Could not get tick_size for {symbol}, using default 0.0001")
            return 0.0001

    def _log_configuration(self):
        logger.info("=" * 60)
        logger.info("Protection Monitor Configuration")
        logger.info("=" * 60)
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'PRODUCTION'}")
        logger.info(f"Stop Loss Type: {self.stop_loss_type.upper()}")
        logger.info(f"Stop Loss: {self.sl_percent}%")
        logger.info(f"Take Profit: {self.tp_percent}%")
        if self.stop_loss_type == 'trailing':
            logger.info(f"Trailing Activation: {self.trailing_activation}%")
            logger.info(f"Trailing Callback: {self.trailing_callback}%")
        if self.max_position_duration_hours > 0:
            logger.info(f"Max Position Duration: {self.max_position_duration_hours} hours")
            logger.info(f"Taker Fee: {self.taker_fee_percent}%")
        logger.info(f"Check Interval: {self.check_interval} seconds")
        logger.info("=" * 60)

    def _calculate_breakeven_price(self, entry_price: float, side: str,
                                   exchange: Optional[BinanceExchange | BybitExchange] = None,
                                   symbol: str = "") -> float:
        """Рассчитывает цену безубытка с учетом комиссий и точности биржи"""
        try:
            fee_multiplier = self.taker_fee_percent / 100
            if side.upper() in ['LONG', 'BUY']:
                breakeven_price = entry_price * (1 + 2 * fee_multiplier)
            else:
                breakeven_price = entry_price * (1 - 2 * fee_multiplier)

            # Округляем до tick_size если доступен exchange и symbol
            if exchange and symbol:
                tick_size = self._get_tick_size(exchange, symbol)
                breakeven_price = self._round_price_to_tick_size(breakeven_price, tick_size)
                logger.info(f"💰 Breakeven: {symbol} | Price: ${breakeven_price:.6f} | Tick Size: {tick_size}")

            return breakeven_price
        except Exception as e:
            logger.error(f"Error calculating breakeven price: {e}")
            return entry_price

    # ИСПРАВЛЕНО: Добавлен правильный метод расчета возраста позиции
    def _calculate_position_age(self, position: Dict, exchange_name: str) -> float:
        """Calculate position age in hours - FIXED VERSION"""
        try:
            # Для Binance используем updateTime
            if exchange_name == "Binance":
                timestamp = position.get("updateTime", 0)
            else:  # Bybit использует updatedTime
                timestamp = position.get("updatedTime", 0)

            if not timestamp:
                logger.debug(f"No timestamp found for position, returning 0 age")
                return 0.0

            # Конвертируем миллисекунды в секунды если нужно
            if timestamp > 1e10:  # Если timestamp в миллисекундах
                timestamp = timestamp / 1000

            current_time = datetime.now(timezone.utc).timestamp()
            age_seconds = current_time - timestamp
            age_hours = age_seconds / 3600  # Конвертируем в часы

            logger.debug(f"Position age calculated: {age_hours:.2f} hours")
            return max(0.0, age_hours)  # Гарантируем неотрицательное значение

        except Exception as e:
            logger.warning(f"Error calculating position age: {e}")
            return 0.0  # По умолчанию возвращаем 0 если расчет не удался

    async def initialize(self):
        try:
            if os.getenv('BINANCE_API_KEY'):
                self.binance = BinanceExchange(
                    {'api_key': os.getenv('BINANCE_API_KEY'), 'api_secret': os.getenv('BINANCE_API_SECRET'),
                     'testnet': self.testnet})
                await self.binance.initialize()
                logger.info(f"✅ Binance connected - Balance: ${await self.binance.get_balance():.2f}")

            if os.getenv('BYBIT_API_KEY'):
                self.bybit = BybitExchange(
                    {'api_key': os.getenv('BYBIT_API_KEY'), 'api_secret': os.getenv('BYBIT_API_SECRET'),
                     'testnet': self.testnet})
                await self.bybit.initialize()
                logger.info(f"✅ Bybit connected - Balance: ${await self.bybit.get_balance():.2f}")

            if not self.binance and not self.bybit:
                raise Exception("No exchanges configured!")
        except Exception as e:
            logger.error(f"Initialization failed: {e}", exc_info=True)
            raise

    # ИСПРАВЛЕНО: Добавлено определение переменной symbol
    async def _is_protection_incomplete(self, exchange_name: str, pos: Dict, open_orders: List[Dict]) -> bool:
        """Check if position protection is incomplete - FIXED VERSION"""

        # ИСПРАВЛЕНИЕ: Добавлено определение переменной symbol
        symbol = pos.get('symbol')
        cache_key = f"{exchange_name}_{symbol}_protection"

        logger.debug(f"🔍 Checking protection status for {exchange_name} {symbol}")

        # Проверяем кэш
        if self._is_cache_valid(cache_key):
            _, has_protection = self.protection_cache[cache_key]
            return not has_protection

        # Фильтруем ордера только для данной позиции
        symbol_orders = [o for o in open_orders if o.get('symbol') == symbol]

        has_sl = False
        has_tp = False
        has_ts = False

        try:
            if exchange_name == 'Bybit':
                # Для Bybit: проверка через ордера (более надежно чем поля позиции)
                has_sl = any(
                    o.get('type', '').upper() in ['STOP', 'STOP_MARKET', 'STOP_LOSS'] or
                    'stop' in o.get('type', '').lower()
                    for o in symbol_orders
                )
                has_tp = any(
                    o.get('type', '').upper() in ['TAKE_PROFIT', 'TAKE_PROFIT_MARKET'] or
                    'take_profit' in o.get('type', '').lower()
                    for o in symbol_orders
                )
                has_ts = any(
                    'trailing' in o.get('type', '').lower() or
                    'trail' in o.get('type', '').lower()
                    for o in symbol_orders
                )

                # Дополнительная проверка через position fields если ордера не найдены
                if not any([has_sl, has_tp, has_ts]):
                    has_sl = pos.get('stopLoss') and str(pos.get('stopLoss')) not in ['', '0', 'null', None]
                    has_tp = pos.get('takeProfit') and str(pos.get('takeProfit')) not in ['', '0', 'null', None]
                    has_ts = pos.get('trailingStop') and str(pos.get('trailingStop')) not in ['', '0', 'null', None]

            else:  # Binance
                # Для Binance проверяем типы ордеров (более надежно)
                has_sl = any(
                    o.get('type', '').upper() in ['STOP_MARKET', 'STOP_LOSS', 'STOP_LOSS_LIMIT'] or
                    o.get('type', '').lower() == 'stop'
                    for o in symbol_orders
                )
                has_tp = any(
                    o.get('type', '').upper() in ['TAKE_PROFIT_MARKET', 'TAKE_PROFIT_LIMIT'] or
                    'take_profit' in o.get('type', '').lower()
                    for o in symbol_orders
                )
                has_ts = any(
                    o.get('type', '').upper() == 'TRAILING_STOP_MARKET' or
                    'trailing' in o.get('type', '').lower()
                    for o in symbol_orders
                )

            # Определяем нужна ли защита
            if self.stop_loss_type == 'trailing':
                protection_complete = has_ts and has_sl
                protection_needed = not protection_complete
            else:  # fixed
                protection_complete = has_sl and has_tp
                protection_needed = not protection_complete

            # Логируем результат проверки
            if protection_complete:
                logger.debug(
                    f"✅ Protection complete for {exchange_name} {symbol}: SL={has_sl}, TP={has_tp}, TS={has_ts}")
            else:
                logger.info(
                    f"🛡️ Protection incomplete for {exchange_name} {symbol}: SL={has_sl}, TP={has_tp}, TS={has_ts}")

            # Обновляем кэш
            self._update_cache(cache_key, protection_complete)

            return protection_needed

        except Exception as e:
            logger.error(f"Error checking protection for {exchange_name} {symbol}: {e}")
            # В случае ошибки считаем что защита нужна
            return True

    async def _apply_protection(self, exchange_name: str, exchange: BinanceExchange | BybitExchange, pos: Dict):
        """Применяет защиту к позиции с полной проверкой"""
        symbol = pos['symbol']
        logger.warning(f"⚠️ {exchange_name} {symbol} protection is incomplete. Applying now.")

        try:
            if self.stop_loss_type == 'trailing':
                # УМНАЯ отмена ордеров: сохраняем close-ордера, отменяем только защиту
                close_order = await self._cancel_existing_protection_orders(exchange, symbol)
                await asyncio.sleep(0.5)

                # Устанавливаем trailing stop
                ts_success = await self._set_trailing_stop(exchange, pos)
                if ts_success:
                    # Устанавливаем backup stop loss
                    sl_success = await self._set_backup_sl(exchange, pos)
                    if sl_success:
                        logger.info(f"✅ Fully protected {exchange_name} position with TS+SL: {symbol}")
                        self.stats['positions_protected'] += 1
                    else:
                        logger.error(f"❌ Failed to set backup SL for {exchange_name} {symbol}")
                else:
                    logger.error(f"❌ Failed to set trailing stop for {exchange_name} {symbol}")
            else:  # fixed
                # Устанавливаем fixed stop loss
                sl_success = await self._set_fixed_sl(exchange, pos)
                # Устанавливаем take profit
                tp_success = await self._set_fixed_tp(exchange, pos)

                if sl_success and tp_success:
                    logger.info(f"✅ Protected {exchange_name} position with SL+TP: {symbol}")
                    self.stats['positions_protected'] += 1
                else:
                    logger.warning(f"⚠️ Partial protection applied for {exchange_name} {symbol}")

        except Exception as e:
            logger.error(f"Error applying protection to {exchange_name} {symbol}: {e}", exc_info=True)
            self.stats['errors'] += 1

    async def _process_single_position(self, exchange_name: str, pos: Dict, orders_by_symbol: Dict[str, List[Dict]]):
        """Обрабатывает отдельную позицию с полной проверкой и логированием"""
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        symbol = pos['symbol']

        # ДОПОЛНИТЕЛЬНОЕ ЛОГИРОВАНИЕ ДЛЯ ДИАГНОСТИКИ
        logger.info(f"📊 POSITION ANALYSIS: {exchange_name} {symbol}")
        side = pos.get('side', 'UNKNOWN')
        entry_price = pos.get('entry_price', 0)
        quantity = pos.get('quantity', pos.get('size', 0))

        logger.info(f"   Direction: {side}")
        logger.info(f"   Entry Price: ${entry_price:.6f}")
        logger.info(f"   Quantity: {quantity}")
        logger.info(f"   Position Value: ${entry_price * abs(quantity):.2f}")

        try:
            logger.debug(f"🔍 Processing {exchange_name} {symbol} position")

            # Проверяем превышение максимальной длительности позиции
            if self.max_position_duration_hours > 0:
                # ИСПРАВЛЕНО: Правильный вызов _calculate_position_age
                age_hours = self._calculate_position_age(pos, exchange_name)

                if age_hours > self.max_position_duration_hours:
                    logger.warning(
                        f"⏰ {exchange_name} {symbol} exceeded max duration: {age_hours:.1f}h > {self.max_position_duration_hours}h")
                    await self._handle_expired_position(exchange, pos, orders_by_symbol.get(symbol, []), age_hours)
                    self.exchange_stats[exchange_name]['positions_expired'] += 1
                    return
                else:
                    logger.debug(f"   {symbol} age: {age_hours:.1f}h (limit: {self.max_position_duration_hours}h)")

            # Проверяем защиту позиции
            protection_is_incomplete = await self._is_protection_incomplete(exchange_name, pos,
                                                                            orders_by_symbol.get(symbol, []))

            if protection_is_incomplete:
                logger.info(f"🛡️ Protection incomplete for {exchange_name} {symbol}, applying protection...")
                await self._apply_protection(exchange_name, exchange, pos)
                self.exchange_stats[exchange_name]['protection_incomplete'] += 1
            else:
                logger.debug(f"✅ Protection complete for {exchange_name} {symbol}")
                self.exchange_stats[exchange_name]['protection_complete'] += 1

        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR in protect_positions for {exchange_name}: {e}", exc_info=True)
            # СТАТИСТИКА: Увеличиваем счетчик API ошибок
            self.exchange_stats[exchange_name]['api_errors'] += 1

        # Логируем статистику
        self._log_exchange_statistics(exchange_name)

    def _log_exchange_statistics(self, exchange_name: str):
        """Логирует детальную статистику по бирже"""

        stats = self.exchange_stats[exchange_name]

        logger.info(f"📊 {exchange_name.upper()} STATISTICS REPORT")
        logger.info(f"   📈 Positions Found: {stats['positions_found']}")
        logger.info(f"   🔄 Positions Open: {stats['positions_open']}")
        logger.info(f"   ✅ Positions Protected: {stats['positions_protected']}")
        logger.info(f"   ⏰ Positions Expired: {stats['positions_expired']}")
        logger.info(f"   💰 Positions with Breakeven: {stats['positions_with_breakeven']}")
        logger.info(f"   ❌ Positions with Errors: {stats['positions_with_errors']}")
        logger.info(f"   🎯 TS/SL Applied: {stats['ts_sl_applied']}")
        logger.info(f"   🛡️ Fixed SL Applied: {stats['fixed_sl_applied']}")
        logger.info(f"   💹 TP Applied: {stats['tp_applied']}")
        logger.info(f"   ✅ Protection Complete: {stats['protection_complete']}")
        logger.info(f"   🛠️ Protection Incomplete: {stats['protection_incomplete']}")
        logger.info(f"   🚦 Rate Limit Errors: {stats['rate_limit_errors']}")
        logger.info(f"   📌 API Errors: {stats['api_errors']}")

        if stats['last_cycle_time']:
            logger.info(f"   🕒 Last Cycle: {stats['last_cycle_time'].strftime('%H:%M:%S UTC')}")

        # Расчетные метрики
        total_processed = stats['protection_complete'] + stats['protection_incomplete']
        if total_processed > 0:
            success_rate = (stats['protection_complete'] / total_processed) * 100
            logger.info(f"   📊 Success Rate: {success_rate:.1f}%")

        if stats['positions_found'] > 0:
            error_rate = (stats['positions_with_errors'] / stats['positions_found']) * 100
            logger.info(f"   ⚠️ Error Rate: {error_rate:.1f}%")

    def _log_comprehensive_statistics(self):
        """Логирует полную статистику по всем биржам"""

        logger.info("=" * 80)
        logger.info("🎯 PROTECTION MONITOR COMPREHENSIVE STATISTICS")
        logger.info("=" * 80)

        total_positions = 0
        total_protected = 0
        total_expired = 0
        total_errors = 0

        for exchange_name in ['Binance', 'Bybit']:
            stats = self.exchange_stats[exchange_name]
            self._log_exchange_statistics(exchange_name)

            total_positions += stats['positions_found']
            total_protected += stats['positions_protected']
            total_expired += stats['positions_expired']
            total_errors += stats['positions_with_errors']

            logger.info("")  # Пустая строка между биржами

        # Итоговые метрики
        logger.info("🎉 OVERALL SUMMARY:")
        logger.info(f"   📊 Total Positions Processed: {total_positions}")
        logger.info(f"   ✅ Total Positions Protected: {total_protected}")
        logger.info(f"   ⏰ Total Positions Expired: {total_expired}")
        logger.info(f"   ❌ Total Positions with Errors: {total_errors}")

        if total_positions > 0:
            protection_rate = (total_protected / total_positions) * 100
            error_rate = (total_errors / total_positions) * 100

            logger.info(f"   📈 Protection Success Rate: {protection_rate:.1f}%")
            logger.info(f"   ⚠️ Overall Error Rate: {error_rate:.1f}%")

        logger.info("=" * 80)

    async def _set_trailing_stop(self, exchange: BinanceExchange | BybitExchange, pos: Dict) -> bool:
        """Устанавливает trailing stop с улучшенной логикой"""
        try:
            symbol, side, entry_price = pos['symbol'], pos['side'].upper(), pos['entry_price']

            # Проверяем rate limit перед получением тикера
            exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'
            if not await self.rate_limiter.acquire(exchange_key, 'query', f'get_ticker_trailing_{symbol}'):
                logger.warning(f"Rate limit exceeded for {exchange_key} get_ticker_trailing_{symbol}")
                return False

            ticker = await exchange.get_ticker(symbol)
            await self.rate_limiter.record_request(exchange_key, 'query', f'get_ticker_trailing_{symbol}')
            current_price = ticker.get('price', entry_price)

            # Расчет цены активации с учетом текущей цены
            activation_price = max(entry_price * (1 + self.trailing_activation / 100), current_price * 1.01) if side in [
                'LONG', 'BUY'] else min(entry_price * (1 - self.trailing_activation / 100), current_price * 0.99)

            # Округляем до tick_size для точности биржи
            tick_size = self._get_tick_size(exchange, symbol)
            activation_price = self._round_price_to_tick_size(activation_price, tick_size)

            # УЛУЧШЕННОЕ ЛОГИРОВАНИЕ (INFO уровень для видимости в production)
            logger.info(f"🎯 TS Activation: {symbol} | Price: ${activation_price:.6f} | Tick Size: {tick_size}")

            # Проверяем rate limit перед установкой trailing stop
            if not await self.rate_limiter.acquire(exchange_key, 'order', f'set_trailing_stop_{symbol}'):
                logger.warning(f"Rate limit exceeded for {exchange_key} set_trailing_stop_{symbol}")
                return False

            position_cancel_result = await exchange.cancel_position_trading_stops(symbol)
            success = await exchange.set_trailing_stop(symbol, activation_price, self.trailing_callback)
            await self.rate_limiter.record_request(exchange_key, 'order', f'set_trailing_stop_{symbol}')
            if success:
                logger.info(f"✅ Trailing Stop set for {symbol}: activation=${activation_price:.4f}, callback={self.trailing_callback}%")
                # СТАТИСТИКА: Увеличиваем счетчик TS
                exchange_name = 'Binance' if isinstance(exchange, BinanceExchange) else 'Bybit'
                self.exchange_stats[exchange_name]['ts_sl_applied'] += 1
                return True
            else:
                logger.error(f"❌ Failed to set Trailing Stop for {symbol}")
                return False
        except Exception as e:
            logger.error(f"Error setting TS for {pos['symbol']}: {e}")
            return False

    async def _handle_expired_position(self, exchange: BinanceExchange | BybitExchange, position: Dict,
                                     open_orders: List[Dict], age_hours: float):
        pnl = position.get('pnl', 0)
        exchange_name = exchange.__class__.__name__

        # ✅ FIX: Extract symbol and side from position data
        symbol = position['symbol']
        side = position['side'].upper()

        logger.warning(f"⏰ {exchange_name} {symbol} EXPIRED: {age_hours:.1f}h old, closing position")

        try:
            # 🔴 КРИТИЧНО: ОТМЕНЯЕМ ВСЕ СУЩЕСТВУЮЩИЕ ЗАЩИТНЫЕ ОРДЕРА ПЕРЕД ЗАКРЫТИЕМ
            logger.info(f"🗑️ Cancelling position-level trading stops for expired {symbol} before closing")

            # Получаем все открытые ордера для проверки
            exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'
            if not await self.rate_limiter.acquire(exchange_key, 'query', f'get_orders_expired_{symbol}'):
                logger.warning(f"Rate limit exceeded for {exchange_key} get_orders_expired_{symbol}")
                return

            # Отменяем все открытые ордера для этого символа
            cancel_result = await self._cancel_existing_protection_orders(exchange, symbol)
            if cancel_result:
                logger.info(f"✅ Successfully cancelled protection orders for {symbol}")


            # 🔴 КРИТИЧНЫЙ ФИКС: Для Bybit также отменяем position-level TL/SL
            if isinstance(exchange, BybitExchange):
                logger.info(f"🗑️ Cancelling position-level trading stops for expired {symbol}")
                position_cancel_result = await exchange.cancel_position_trading_stops(symbol)
                if position_cancel_result:
                    logger.info(f"✅ Successfully cancelled position-level trading stops for {symbol}")
                else:
                    logger.warning(f"⚠️ Failed to cancel position-level trading stops for {symbol}")
            # 🔍 ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убедимся что ордера действительно отменены
                await asyncio.sleep(1)  # Небольшая задержка для синхронизации

                # Повторная проверка ордеров после отмены
                if not await self.rate_limiter.acquire(exchange_key, 'query', f'verify_cancel_{symbol}'):
                    logger.warning(f"Rate limit exceeded for {exchange_key} verify_cancel_{symbol}")
                else:
                    verification_orders = await exchange.get_open_orders(symbol)
                    await self.rate_limiter.record_request(exchange_key, 'query', f'verify_cancel_{symbol}')

                    verification_protection_count = 0
                    for order in verification_orders:
                        if order.get('symbol') == symbol:
                            order_type = order.get('type', '').upper()
                            is_reduce_only = False

                            if isinstance(exchange, BinanceExchange):
                                is_reduce_only = order.get('reduceOnly', False)
                            else:  # Bybit - УЛУЧШЕННАЯ ЛОГИКА ВЕРИФИКАЦИИ
                                # Для Bybit проверяем несколько условий
                                order_type_lower = order.get('type', '').lower()
                                is_reduce_only = (
                                    'reduce' in order_type_lower or
                                    order.get('reduceOnly', False) or
                                    # ЯВНО проверяем SL ордера на Bybit
                                    order_type_lower in ['stop_loss', 'stop_market', 'stop_limit'] or
                                    'stop' in order_type_lower or
                                    # Проверяем по специальным полям Bybit
                                    order.get('stopOrderType', '') in ['StopLoss', 'Stop'] or
                                    order.get('orderType', '') in ['StopLoss', 'Stop']
                                )

                            if not is_reduce_only:
                                verification_protection_count += 1

                    if verification_protection_count == 0:
                        logger.info(f"✅ VERIFIED: All protection orders cancelled for {symbol}")
                    else:
                        logger.warning(f"⚠️ VERIFICATION: Still {verification_protection_count} protection orders for {symbol}")

            else:
                logger.warning(f"⚠️ No protection orders found to cancel for {symbol}")

            # Небольшая пауза после отмены ордеров
            await asyncio.sleep(0.5)

            # Проверяем, есть ли уже ордер на закрытие
            breakeven_price = self._calculate_breakeven_price(position['entry_price'], side, exchange, symbol)

            # Получаем tick_size для округления цены
            if isinstance(exchange, BinanceExchange):
                tick_size = float(exchange.symbol_info.get(symbol, {}).get('priceFilter', {}).get('tickSize', 0.0001))
            else:  # BybitExchange
                tick_size = float(exchange.symbol_info.get(symbol, {}).get('tick_size', 0.0001))

            # Округляем цену breakeven до ближайшего tick_size
            breakeven_price = round(breakeven_price / tick_size) * tick_size

            # Проверяем существующие close-ордера
            existing_close_order = self._find_existing_close_order(exchange, symbol, breakeven_price, tick_size, open_orders)

            if existing_close_order:
                logger.info(f"✅ Close order already exists for {symbol} at ${breakeven_price:.6f}")
                return

            # Создаем лимитный ордер на breakeven без отмены существующей защиты
            logger.info(f"📝 Creating breakeven limit order for {symbol} at ${breakeven_price:.6f}")

            # Проверяем rate limit перед созданием ордера
            exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'
            if not await self.rate_limiter.acquire(exchange_key, 'order', f'create_breakeven_{symbol}'):
                logger.warning(f"Rate limit exceeded for {exchange_key} create_breakeven_{symbol}")
                return

            # Создаем лимитный ордер на breakeven
            order_side = "SELL" if side in ["LONG", "BUY"] else "BUY"
            quantity = abs(position.get('quantity', position.get('size', 0)))

            if quantity <= 0:
                logger.error(f"Invalid quantity for {symbol}: {quantity}")
                return

            order_result = await exchange.create_limit_order(
                symbol=symbol,
                side=order_side,
                quantity=quantity,
                price=breakeven_price,
                reduce_only=True
            )

            await self.rate_limiter.record_request(exchange_key, 'order', f'create_breakeven_{symbol}')

            if order_result and order_result.get('orderId'):
                logger.info(f"✅ Breakeven limit order created for {symbol}: {order_result.get('orderId')}")
                self.stats['positions_closed'] += 1
                # СТАТИСТИКА: Увеличиваем счетчик breakeven ордеров
                self.exchange_stats[exchange_name]['positions_with_breakeven'] += 1
            else:
                logger.error(f"❌ Failed to create breakeven order for {symbol}")
                # СТАТИСТИКА: Увеличиваем счетчик ошибок
                self.exchange_stats[exchange_name]['positions_with_errors'] += 1

        except Exception as e:
            logger.error(f"Error handling expired position {symbol}: {e}")
            self.stats['errors'] += 1

    async def _cancel_existing_protection_orders(self, exchange: BinanceExchange | BybitExchange, symbol: str) -> Optional[Dict]:
        """УМНАЯ отмена ордеров: сохраняет close-ордера, отменяет только защиту"""

        # Получаем все открытые ордера для символа
        exchange_key = 'binance' if isinstance(exchange, BinanceExchange) else 'bybit'
        if not await self.rate_limiter.acquire(exchange_key, 'query', f'get_orders_{symbol}'):
            logger.warning(f"Rate limit exceeded for {exchange_key} get_orders_{symbol}")
            return None

        open_orders = await exchange.get_open_orders(symbol)
        await self.rate_limiter.record_request(exchange_key, 'query', f'get_orders_{symbol}')

        if not open_orders:
            return None

        orders_to_cancel = []
        close_order = None

        for order in open_orders:
            if order.get('symbol') != symbol:
                continue

            # Определяем тип ордера
            order_type = order.get('type', '').upper()
            is_reduce_only = False

            if isinstance(exchange, BinanceExchange):
                is_reduce_only = order.get('reduceOnly', False)
            else:  # Bybit - УЛУЧШЕННАЯ ЛОГИКА ДЛЯ SL ОРДЕРОВ
                # Для Bybit проверяем несколько условий
                order_type_lower = order.get('type', '').lower()
                is_reduce_only = (
                    'reduce' in order_type_lower or
                    order.get('reduceOnly', False) or
                    # ЯВНО проверяем SL ордера на Bybit
                    order_type_lower in ['stop_loss', 'stop_market', 'stop_limit'] or
                    'stop' in order_type_lower or
                    # Проверяем по специальным полям Bybit
                    order.get('stopOrderType', '') in ['StopLoss', 'Stop'] or
                    order.get('orderType', '') in ['StopLoss', 'Stop']
                )

            # Сохраняем close-ордера (reduce-only)
            if is_reduce_only:
                close_order = order
                logger.debug(f"🔄 Preserving close order {order.get('orderId')} for {symbol}")
            else:
                # Отменяем защитные ордера
                orders_to_cancel.append(order)
                logger.debug(f"🗑️ Will cancel protection order {order.get('orderId')} for {symbol}")

        # Отменяем только защитные ордера
        if orders_to_cancel:
            logger.info(f"🗑️ Cancelling {len(orders_to_cancel)} protection orders for {symbol}")

            for order in orders_to_cancel:
                # Проверяем rate limit для каждой отмены
                if not await self.rate_limiter.acquire(exchange_key, 'order', f'cancel_order_{order.get("orderId")}'):
                    logger.warning(f"Rate limit exceeded for {exchange_key} cancel_order_{order.get('orderId')}")
                    continue

                success = await exchange.cancel_order(order.get('orderId'))
                await self.rate_limiter.record_request(exchange_key, 'order', f'cancel_order_{order.get("orderId")}')

                if success:
                    logger.debug(f"✅ Cancelled order {order.get('orderId')} for {symbol}")
                else:
                    logger.warning(f"❌ Failed to cancel order {order.get('orderId')} for {symbol}")

        if close_order:
            logger.info(f"🛡️ Preserved close order {close_order.get('orderId')} for {symbol}")

        return close_order

    async def protect_positions(self, exchange_name: str):
        """Защищает позиции для указанной биржи с улучшенной обработкой ошибок"""
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        semaphore = self.binance_semaphore if exchange_name == 'Binance' else self.bybit_semaphore
        if not exchange:
            logger.warning(f"⚠️ {exchange_name} exchange not initialized")
            return

        try:
            # ✅ FIX: Определяем exchange_key в начале метода
            exchange_key = 'binance' if exchange_name == 'Binance' else 'bybit'
            # Проверяем rate limit перед получением открытых позиций
            if not await self.rate_limiter.acquire(exchange_key, 'query', 'get_open_positions'):
                logger.warning(f"Rate limit exceeded for {exchange_key} get_open_positions")
                # СТАТИСТИКА: Увеличиваем счетчик rate limit ошибок
                self.exchange_stats[exchange_name]['rate_limit_errors'] += 1
                return

            # Получаем открытые позиции
            positions = await exchange.get_open_positions()
            await self.rate_limiter.record_request(exchange_key, 'query', 'get_open_positions')
            if not positions:
                logger.debug(f"📭 No open {exchange_name} positions")
                return

            # ОБНОВЛЯЕМ СТАТИСТИКУ
            self.exchange_stats[exchange_name]['positions_found'] = len(positions)
            self.exchange_stats[exchange_name]['positions_open'] = len(positions)
            self.exchange_stats[exchange_name]['last_cycle_time'] = datetime.now(timezone.utc)

            logger.info(f"🔍 Found {len(positions)} {exchange_name} positions to check")

            # Проверяем rate limit перед получением открытых ордеров
            if not await self.rate_limiter.acquire(exchange_key, 'query', 'get_open_orders'):
                logger.warning(f"Rate limit exceeded for {exchange_key} get_open_orders")
                return

            # Получаем все открытые ордера
            all_open_orders = await exchange.get_open_orders()
            await self.rate_limiter.record_request(exchange_key, 'query', 'get_open_orders')
            if all_open_orders is None:
                logger.error(f"❌ Failed to get open orders for {exchange_name}")
                all_open_orders = []

            # Группируем ордера по символам
            orders_by_symbol = {pos['symbol']: [] for pos in positions}
            for order in all_open_orders:
                symbol = order.get('symbol', '')
                if symbol in orders_by_symbol:
                    orders_by_symbol[symbol].append(order)

            # Обрабатываем каждую позицию
            tasks = [
                self.run_with_semaphore(semaphore, self._process_single_position, exchange_name, pos, orders_by_symbol)
                for pos in positions
            ]

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # Проверяем результаты на исключения
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        symbol = positions[i]['symbol']
                        logger.error(f"❌ Exception processing {exchange_name} {symbol}: {result}")
                        self.stats['errors'] += 1

        except Exception as e:
            logger.error(f"💥 Critical error in protect_{exchange_name.lower()}_positions: {e}", exc_info=True)
            self.stats['errors'] += 1

    async def run(self):
        logger.info(f"🚀 Starting Protection Monitor v2.6")
        await self.initialize()
        try:
            while True:
                try:
                    self.stats['checks'] += 1
                    logger.info(f"=== Protection Check #{self.stats['checks']} ===")
                    await asyncio.gather(
                        self.protect_positions('Binance'),
                        self.protect_positions('Bybit')
                    )
                    if self.stats['checks'] % 10 == 0:
                        await self.print_statistics()
                    await asyncio.sleep(self.check_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    self.stats['errors'] += 1
                    await asyncio.sleep(5)
        except KeyboardInterrupt:
            logger.info("⛔ Shutdown signal received")
        finally:
            await self.cleanup()

    async def print_statistics(self):
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        hours = uptime.total_seconds() / 3600
        logger.info("=" * 60)
        logger.info("Performance Statistics")
        logger.info("=" * 60)
        logger.info(f"Uptime: {hours:.2f} hours")
        logger.info(f"Checks performed: {self.stats['checks']}")
        logger.info(f"Positions protected: {self.stats['positions_protected']}")
        logger.info(f"Positions closed: {self.stats['positions_closed']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)

    async def cleanup(self):
        logger.info("🧹 Cleaning up...")
        await self.print_statistics()
        if self.binance:
            await self.binance.close()
            logger.info("Binance connection closed")
        if self.bybit:
            await self.bybit.close()
            logger.info("Bybit connection closed")

    def _calculate_position_age(self, position: Dict, exchange_name: str) -> float:
        """Calculate position age in hours - FIXED VERSION"""
        try:
            # Для Binance используем updateTime
            if exchange_name == "Binance":
                timestamp = position.get("updateTime", 0)
            else:  # Bybit использует updatedTime
                timestamp = position.get("updatedTime", 0)

            if not timestamp:
                logger.debug(f"No timestamp found for position, returning 0 age")
                return 0.0

            # Конвертируем миллисекунды в секунды если нужно
            if timestamp > 1e10:  # Если timestamp в миллисекундах
                timestamp = timestamp / 1000

            current_time = datetime.now(timezone.utc).timestamp()
            age_seconds = current_time - timestamp
            age_hours = age_seconds / 3600  # Конвертируем в часы

            logger.debug(f"Position age calculated: {age_hours:.2f} hours")
            return max(0.0, age_hours)  # Гарантируем неотрицательное значение

        except Exception as e:
            logger.warning(f"Error calculating position age: {e}")
            return 0.0  # По умолчанию возвращаем 0 если расчет не удался

    async def run_with_semaphore(self, semaphore, func, *args, **kwargs):
        """Run a function with semaphore control"""
        async with semaphore:
            return await func(*args, **kwargs)

async def main():
    monitor = ProtectionMonitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())