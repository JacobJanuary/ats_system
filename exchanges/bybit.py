#!/usr/bin/env python3
"""
Bybit Exchange Implementation - PRODUCTION READY v2.4 (FIXED)
- ДОБАВЛЕНА поддержка set_take_profit
- УЛУЧШЕНА обработка Trailing Stop с правильной активацией
- ИСПРАВЛЕНА проверка createdTime для корректного расчета возраста позиции
"""

import asyncio
import logging
from typing import Dict, Optional, List, Any
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP

# Official Bybit Python SDK
from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError

# Assuming base is in the same directory structure
from .base import BaseExchange

logger = logging.getLogger(__name__)


def safe_float(value, default=0.0):
    """Safely convert value to float"""
    if value is None: return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


class BybitExchange(BaseExchange):
    """Bybit futures exchange implementation using official pybit library"""

    def __init__(self, config: Dict):
        """Initialize Bybit exchange with configuration"""
        super().__init__(config)
        self.api_key = config.get('api_key', '').strip()
        self.api_secret = config.get('api_secret', '').strip()
        self.client = HTTP(testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret)
        self.symbol_info = {}
        self.last_error = None
        # Кэш для trailing stop параметров, чтобы избежать повторных установок
        # Формат: {symbol: {'distance': value, 'activePrice': value}}
        self.trailing_stop_cache = {}
        logger.info(f"Bybit {'testnet' if self.testnet else 'mainnet'} client created")

    async def initialize(self):
        """Initialize Bybit connection and load market info"""
        try:
            await self._async_request(self.client.get_server_time)
            await self._load_instruments_info()
            logger.info(
                f"Bybit {'testnet' if self.testnet else 'mainnet'} initialized with {len(self.symbol_info)} symbols")
        except Exception as e:
            logger.error(f"Failed to initialize Bybit: {e}")
            raise

    async def _async_request(self, method, *args, **kwargs):
        """Wrapper to run synchronous pybit methods asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: method(*args, **kwargs))

    async def _load_instruments_info(self):
        """Load all active trading instruments"""
        self.symbol_info = {}
        cursor = ""
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor: params["cursor"] = cursor

            result = await self._async_request(self.client.get_instruments_info, **params)

            if result and result.get('retCode') == 0:
                data = result.get('result', {})
                for instrument in data.get('list', []):
                    if instrument.get('status') == 'Trading':
                        self._store_instrument_info(instrument['symbol'], instrument)

                cursor = data.get('nextPageCursor', '')
                if not cursor: break
            else:
                logger.error(f"Failed to load instruments: {result}")
                break

    def _store_instrument_info(self, symbol: str, instrument: Dict):
        lot_size_filter = instrument.get('lotSizeFilter', {})
        price_filter = instrument.get('priceFilter', {})
        self.symbol_info[symbol] = {
            'minOrderQty': float(lot_size_filter.get('minOrderQty', 0.001)),
            'qtyStep': float(lot_size_filter.get('qtyStep', 0.001)),
            'tickSize': float(price_filter.get('tickSize', 0.0001)),
        }

    def format_quantity(self, symbol: str, quantity: float) -> str:
        """
        Форматирует количество согласно требованиям Bybit (TICK_SIZE mode)
        Основано на логике CCXT для правильной обработки qtyStep
        """
        if symbol not in self.symbol_info: 
            logger.warning(f"No symbol info for {symbol}, using default rounding")
            return str(round(quantity, 4))
        
        try:
            info = self.symbol_info[symbol]
            step_size = Decimal(str(info['qtyStep']))
            min_order_qty = Decimal(str(info['minOrderQty']))
            qty_decimal = Decimal(str(quantity))
            
            # TICK_SIZE MODE: округляем количество до ближайшего кратного qtyStep
            # Для reduce-only ордеров важно не округлять вверх больше позиции
            remainder = qty_decimal % step_size
            
            if remainder == 0:
                # Количество уже кратно step_size - не меняем
                rounded_qty = qty_decimal
            else:
                # Округляем к ближайшему кратному
                # ВАЖНО: используем ROUND_DOWN для reduce-only чтобы не превысить позицию
                steps = (qty_decimal / step_size).quantize(Decimal('0'), rounding=ROUND_DOWN)
                rounded_qty = steps * step_size
                
                # Если округлили до 0, пробуем минимальное количество
                if rounded_qty == 0:
                    rounded_qty = step_size
            
            # Проверяем минимальное количество
            if rounded_qty < min_order_qty:
                # Если меньше минимума, используем минимум
                rounded_qty = min_order_qty
                logger.debug(
                    f"{symbol}: Quantity {quantity} rounded to {rounded_qty} (using minOrderQty)"
                )
            
            # КРИТИЧЕСКАЯ ПРОВЕРКА: Для целочисленных qtyStep (>=1) 
            # убеждаемся что результат тоже целое число
            if step_size >= 1:
                # Форматируем как целое число без десятичной точки
                formatted = str(int(rounded_qty))
            else:
                # Определяем количество знаков после запятой из qtyStep
                step_str = str(step_size)
                if '.' in step_str:
                    # Считаем значащие цифры после точки
                    after_dot = step_str.split('.')[1].rstrip('0')
                    decimals = len(after_dot)
                else:
                    decimals = 0
                
                # Форматируем с нужным количеством знаков
                # ВАЖНО: НЕ удаляем trailing zeros для Bybit
                formatted = format(rounded_qty, f'.{decimals}f')
            
            # Финальная валидация
            # Проверяем что не получился 0 после форматирования
            if float(formatted) == 0:
                logger.warning(f"Formatted quantity is 0 for {symbol}, using minimum: {min_order_qty}")
                if min_order_qty >= 1:
                    formatted = str(int(min_order_qty))
                else:
                    formatted = str(min_order_qty)
            
            # Дополнительная проверка для больших qtyStep
            final_value = Decimal(formatted)
            if final_value == 0:
                logger.error(
                    f"{symbol}: CRITICAL - Formatted quantity is 0! "
                    f"Original: {quantity}, Step: {step_size}, Min: {min_order_qty}"
                )
                # Возвращаем минимальное количество
                if step_size >= 1:
                    formatted = str(int(min_order_qty))
                else:
                    formatted = str(min_order_qty)
            
            # Логирование для отладки
            if float(formatted) != quantity:
                logger.debug(
                    f"{symbol}: Quantity formatted from {quantity} to {formatted} "
                    f"(qtyStep={step_size}, minOrderQty={min_order_qty})"
                )
            
            return formatted
            
        except Exception as e:
            logger.error(f"Error formatting quantity for {symbol}: {e}", exc_info=True)
            return str(round(quantity, 4))

    def format_price(self, symbol: str, price: float) -> str:
        if symbol not in self.symbol_info: return f"{price:.4f}"
        try:
            info = self.symbol_info[symbol]
            tick_size = Decimal(str(info['tickSize']))
            price_decimal = Decimal(str(price))
            rounded_price = (price_decimal / tick_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size
            return str(rounded_price).rstrip('0').rstrip('.')
        except Exception:
            return f"{price:.4f}"

    async def get_balance(self) -> float:
        try:
            result = await self._async_request(self.client.get_wallet_balance, accountType="UNIFIED")
            if result and result.get('retCode') == 0:
                for coin in result['result']['list'][0]['coin']:
                    if coin['coin'] == 'USDT':
                        return safe_float(coin.get('walletBalance', 0))
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
        return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        try:
            result = await self._async_request(self.client.get_tickers, category="linear", symbol=symbol)
            if result and result.get('retCode') == 0 and result['result']['list']:
                ticker = result['result']['list'][0]
                return {
                    'symbol': ticker.get('symbol'), 
                    'price': safe_float(ticker.get('lastPrice')),
                    'last': safe_float(ticker.get('lastPrice')),  # Добавляем поле last для совместимости
                    'bid': safe_float(ticker.get('bid1Price')), 
                    'ask': safe_float(ticker.get('ask1Price')),
                }
        except Exception as e:
            logger.error(f"Error getting ticker for {symbol}: {e}")
        return {}

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        try:
            await self._async_request(
                self.client.set_leverage, category="linear", symbol=symbol,
                buyLeverage=str(leverage), sellLeverage=str(leverage)
            )
            logger.info(f"✅ Leverage set to {leverage}x for {symbol}")
            return True
        except InvalidRequestError as e:
            if "110043" in str(e):  # Leverage not modified
                logger.info(f"Leverage for {symbol} is already set to {leverage}x.")
                return True
            logger.error(f"Error setting leverage for {symbol}: {e}")
            return self.testnet
        except Exception as e:
            logger.error(f"Unexpected error setting leverage for {symbol}: {e}")
            return self.testnet

    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """Creates an order with testnet-aware fallback strategy"""
        try:
            ticker = await self.get_ticker(symbol)
            if not ticker or not ticker.get('ask') or not ticker.get('bid'):
                logger.error(f"Cannot place order for {symbol}: invalid ticker data.")
                return None

            # Проверка минимального размера ордера перед отправкой
            # Предотвращаем ошибку "orderQty will be truncated to zero"
            if symbol in self.symbol_info:
                min_order_qty = self.symbol_info[symbol].get('minOrderQty', 0.001)
                if quantity < min_order_qty:
                    logger.warning(
                        f"Market order quantity {quantity} for {symbol} is below minimum {min_order_qty}. "
                        f"Skipping order creation."
                    )
                    return None

            formatted_qty = self.format_quantity(symbol, quantity)
            
            # Дополнительная проверка после форматирования
            if float(formatted_qty) == 0:
                logger.warning(
                    f"Formatted quantity became 0 for {symbol} market order (original: {quantity}). "
                    f"Order creation skipped."
                )
                return None

            # Всегда используем IOC лимитный ордер для избежания ошибки 30208
            # Особенно важно для токенов с префиксом "1000" (1000TURBOUSDT, 1000PEPEUSDT и др.)
            
            # Проверяем ликвидность и корректность спреда
            ask_price = float(ticker.get('ask', 0))
            bid_price = float(ticker.get('bid', 0))
            last_price = float(ticker.get('last', 0))
            
            # Если спред слишком большой (больше 10%), используем last price с запасом
            if ask_price > 0 and bid_price > 0:
                spread_pct = ((ask_price - bid_price) / bid_price) * 100
                if spread_pct > 10:
                    logger.warning(f"Large spread detected for {symbol}: {spread_pct:.2f}%. Using last price.")
                    # Используем last price с большим запасом
                    if side.upper() == "BUY":
                        limit_price = last_price * 1.05  # 5% выше последней цены для покупки
                    else:
                        limit_price = last_price * 0.95  # 5% ниже последней цены для продажи
                else:
                    # Нормальный спред - используем bid/ask с небольшим запасом
                    price_multiplier = 1.01 if side.upper() == "BUY" else 0.99
                    limit_price = ask_price * price_multiplier if side.upper() == "BUY" else bid_price * price_multiplier
            else:
                # Нет данных о bid/ask - используем last price
                logger.warning(f"No bid/ask data for {symbol}. Using last price.")
                if side.upper() == "BUY":
                    limit_price = last_price * 1.05
                else:
                    limit_price = last_price * 0.95
            
            formatted_price = self.format_price(symbol, limit_price)

            result = await self._async_request(
                self.client.place_order,
                category="linear",
                symbol=symbol,
                side=side.capitalize(),
                orderType="Limit",
                qty=formatted_qty,
                price=formatted_price,
                timeInForce="IOC",
                positionIdx=0
            )

            if result and result.get('retCode') == 0:
                order_id = result.get('result', {}).get('orderId')
                await asyncio.sleep(0.75)

                # Используем get_order_history вместо реального времени для тестнета
                order_status = await self._check_order_status(order_id, symbol)

                if order_status and order_status.get('executedQty', 0) > 0:
                    logger.info(f"✅ Order {order_id} executed. Qty: {order_status['executedQty']}")
                    return {
                        'orderId': order_id,
                        'symbol': symbol,
                        'status': 'FILLED',
                        'executed_qty': order_status.get('executedQty', 0.0),
                        'price': order_status.get('avgPrice', 0.0)
                    }

            logger.warning(f"Order was not executed for {symbol}")
            return None

        except Exception as e:
            logger.error(f"Critical error in create_market_order: {e}", exc_info=True)
            return None

    async def _check_order_status(self, order_id: str, symbol: str) -> Optional[Dict]:
        try:
            # For IOC orders, history is sufficient and more reliable
            result = await self._async_request(self.client.get_order_history, category="linear", orderId=order_id)
            if result and result.get('retCode') == 0 and result['result']['list']:
                order = result['result']['list'][0]
                return {
                    'status': order.get('orderStatus'),
                    'avgPrice': safe_float(order.get('avgPrice', 0)),
                    'executedQty': safe_float(order.get('cumExecQty', 0))
                }
            return None
        except Exception as e:
            logger.error(f"Error checking order status for {order_id}: {e}")
            return None

    async def get_open_positions(self, symbol: str = None) -> List[Dict]:
        try:
            params = {"category": "linear", "settleCoin": "USDT"}
            if symbol:
                params["symbol"] = symbol

            result = await self._async_request(self.client.get_positions, **params)

            positions = []
            if result and result.get('retCode') == 0:
                for pos in result['result']['list']:
                    if safe_float(pos.get('size', 0)) > 0:
                        positions.append({
                            'symbol': pos.get('symbol'),
                            'quantity': safe_float(pos.get('size')),
                            'entry_price': safe_float(pos.get('avgPrice')),
                            'mark_price': safe_float(pos.get('markPrice', 0)),
                            'pnl': safe_float(pos.get('unrealisedPnl')),
                            'side': 'LONG' if pos.get('side') == 'Buy' else 'SHORT',
                            'updatedTime': int(pos.get('updatedTime', 0)),
                            'createdTime': int(pos.get('createdTime', 0)),  # Для корректного расчета возраста
                            'stopLoss': pos.get('stopLoss'),
                            'takeProfit': pos.get('takeProfit'),
                            'trailingStop': pos.get('trailingStop'),
                            'activePrice': pos.get('activePrice')  # Для проверки активации TS
                        })
            return positions
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        try:
            # Форматируем цену перед отправкой
            formatted_price = self.format_price(symbol, stop_price)
            
            result = await self._async_request(
                self.client.set_trading_stop, category="linear", symbol=symbol,
                stopLoss=formatted_price, positionIdx=0
            )
            
            error_code = result.get('retCode') if result else None
            error_msg = result.get('retMsg', '') if result else ''
            
            if result and result.get('retCode') == 0:
                logger.info(f"✅ Stop loss set for {symbol} at ${stop_price:.4f}")
                return True
            elif error_code == 34040:  # "not modified" - SL уже установлен с таким же значением
                logger.info(f"Stop loss already set at ${stop_price:.4f} for {symbol} (no modification needed)")
                return True
            elif error_code == 110008:  # "cannot replace_so" - ордер уже triggered
                logger.warning(f"Cannot modify SL for {symbol}: order already triggered")
                return False
            elif error_code == 10001:
                if "should lower than" in error_msg:
                    logger.error(f"Invalid SL price for LONG {symbol}: SL ${stop_price:.4f} must be lower than current price")
                elif "should higher than" in error_msg:
                    logger.error(f"Invalid SL price for SHORT {symbol}: SL ${stop_price:.4f} must be higher than current price")
                else:
                    logger.error(f"Invalid parameters for {symbol}: {error_msg}")
                return False
            else:
                logger.error(f"Failed to set stop loss for {symbol}: {error_msg} (code: {error_code})")
                return False
                
        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Устанавливает Take Profit для позиции"""
        try:
            result = await self._async_request(
                self.client.set_trading_stop,
                category="linear",
                symbol=symbol,
                takeProfit=self.format_price(symbol, take_profit_price),
                positionIdx=0
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Take profit set for {symbol} at ${take_profit_price:.4f}")
                return True
            logger.error(f"Failed to set take profit: {result}")
            return False
        except Exception as e:
            logger.error(f"Error setting take profit: {e}")
            return False

    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """Устанавливает Trailing Stop с правильной активацией"""
        try:
            # Рассчитываем trailing distance от activation price
            distance = activation_price * (callback_rate / 100)
            formatted_distance = self.format_price(symbol, distance)
            formatted_active_price = self.format_price(symbol, activation_price)
            
            # Проверяем кэш на идентичные параметры
            # Это предотвращает ошибку "not modified" при повторных попытках
            cached_params = self.trailing_stop_cache.get(symbol, {})
            if (cached_params.get('distance') == formatted_distance and 
                cached_params.get('activePrice') == formatted_active_price):
                logger.debug(
                    f"Trailing stop for {symbol} already set with same parameters. "
                    f"Skipping to avoid 'not modified' error."
                )
                return True
            
            # Проверяем текущие параметры позиции
            positions = await self.get_open_positions(symbol)
            if positions:
                pos = positions[0]
                # Проверяем, не установлены ли уже такие же параметры
                if (pos.get('trailingStop') == formatted_distance and 
                    pos.get('activePrice') == formatted_active_price):
                    logger.debug(
                        f"Position {symbol} already has identical trailing stop parameters. "
                        f"Updating cache and skipping API call."
                    )
                    # Обновляем кэш
                    self.trailing_stop_cache[symbol] = {
                        'distance': formatted_distance,
                        'activePrice': formatted_active_price
                    }
                    return True

            result = await self._async_request(
                self.client.set_trading_stop,
                category="linear",
                symbol=symbol,
                trailingStop=formatted_distance,
                activePrice=formatted_active_price,
                positionIdx=0
            )
            if result and result.get('retCode') == 0:
                # Сохраняем успешно установленные параметры в кэш
                self.trailing_stop_cache[symbol] = {
                    'distance': formatted_distance,
                    'activePrice': formatted_active_price
                }
                logger.info(
                    f"✅ Trailing stop for {symbol} set with activation at ${activation_price:.4f} "
                    f"and distance ${formatted_distance}"
                )
                return True
            
            # Обработка ошибки "not modified" - это не критическая ошибка
            if result and result.get('retCode') == 34040:
                logger.debug(f"Trailing stop for {symbol} already has these parameters (34040).")
                # Обновляем кэш даже при этой ошибке
                self.trailing_stop_cache[symbol] = {
                    'distance': formatted_distance,
                    'activePrice': formatted_active_price
                }
                return True
                
            logger.error(f"Failed to set trailing stop: {result}")
            return False
        except Exception as e:
            logger.error(f"Error setting trailing stop: {e}")
            return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        try:
            params = {"category": "linear", "settleCoin": "USDT"}
            if symbol:
                params["symbol"] = symbol

            result = await self._async_request(self.client.get_open_orders, **params)

            orders = []
            if result and result.get('retCode') == 0:
                for order in result['result']['list']:
                    orders.append({
                        'orderId': order.get('orderId'),
                        'symbol': order.get('symbol'),
                        'side': order.get('side', '').lower(),
                        'quantity': safe_float(order.get('qty')),
                        'price': safe_float(order.get('price')),
                        'status': order.get('orderStatus'),
                        'type': order.get('orderType', '').lower(),
                        'reduceOnly': order.get('reduceOnly', False)
                    })
            return orders
        except Exception as e:
            logger.error(f"Error getting open orders: {e}")
            return []

    async def cancel_all_open_orders(self, symbol: str) -> bool:
        try:
            result = await self._async_request(self.client.cancel_all_orders, category="linear", symbol=symbol)
            return result and result.get('retCode') == 0
        except Exception as e:
            logger.error(f"Error cancelling orders for {symbol}: {e}")
            return False

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancels a specific order by its ID."""
        try:
            result = await self._async_request(
                self.client.cancel_order,
                category="linear",
                symbol=symbol,
                orderId=order_id
            )
            if result and result.get('retCode') == 0:
                logger.info(f"✅ Order {order_id} for {symbol} cancelled successfully.")
                return True
            logger.error(f"Failed to cancel order {order_id}: {result}")
            return False
        except Exception as e:
            logger.error(f"Error cancelling order {order_id} for {symbol}: {e}")
            return False

    async def create_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                                 reduce_only: bool = False) -> Optional[Dict]:
        try:
            # Для reduce-only ордеров сначала получаем актуальную позицию
            if reduce_only:
                positions = await self.get_open_positions(symbol=symbol)
                actual_position = None
                for pos in positions:
                    if pos.get('symbol') == symbol:
                        actual_position = pos
                        break
                
                if actual_position:
                    # Используем точное количество из позиции
                    actual_qty = actual_position.get('quantity', 0)
                    if actual_qty > 0:
                        logger.debug(f"Using actual position size for reduce-only: {actual_qty} (requested: {quantity})")
                        quantity = actual_qty
                    else:
                        logger.warning(f"No position to reduce for {symbol}")
                        return None
                else:
                    logger.warning(f"No position found for {symbol} to reduce")
                    return None
                
                # WORKAROUND: Многие символы на testnet имеют проблемы с reduce-only limit orders
                # Основано на тестировании: ALEOUSDT, COREUSDT работают с reduce-only
                # Остальные требуют обходного решения
                working_symbols = ['ALEOUSDT', 'COREUSDT']
                
                if self.testnet and symbol not in working_symbols:
                    logger.warning(
                        f"Testnet workaround for {symbol}: reduce-only limit orders fail with 110017. "
                        f"Will use regular limit order without reduce-only flag."
                    )
                    reduce_only = False
            
            # Проверка минимального размера ордера перед отправкой на биржу
            if symbol in self.symbol_info:
                min_order_qty = self.symbol_info[symbol].get('minOrderQty', 0.001)
                if quantity < min_order_qty:
                    logger.warning(
                        f"Order quantity {quantity} for {symbol} is below minimum {min_order_qty}. "
                        f"Skipping order creation to avoid 'truncated to zero' error."
                    )
                    return None
            
            # Форматирование количества с учетом step size
            formatted_qty = self.format_quantity(symbol, quantity)
            
            # Логирование для отладки
            logger.debug(f"Quantity formatting for {symbol}: original={quantity}, formatted={formatted_qty}, type={type(formatted_qty)}")
            
            # Дополнительная проверка после форматирования
            # Убеждаемся, что отформатированное количество не стало 0
            if float(formatted_qty) == 0:
                logger.error(
                    f"CRITICAL: Formatted quantity became 0 for {symbol} "
                    f"(original: {quantity}, formatted: {formatted_qty}). "
                    f"Order creation skipped to avoid error 110017."
                )
                return None
            
            # НОВАЯ ПРОВЕРКА: Для reduce-only ордеров проверяем соответствие позиции
            if reduce_only and symbol in self.symbol_info:
                step_size = self.symbol_info[symbol].get('qtyStep', 1)
                if step_size >= 1000 and float(formatted_qty) < step_size:
                    logger.error(
                        f"Reduce-only order for {symbol}: formatted qty {formatted_qty} < step {step_size}. "
                        f"This will cause error 110017. Adjusting to minimum step."
                    )
                    formatted_qty = str(int(step_size))
            
            params = {
                "category": "linear", "symbol": symbol, "side": side.capitalize(),
                "orderType": "Limit", "qty": formatted_qty,
                "price": self.format_price(symbol, price), "timeInForce": "GTC",
                "positionIdx": 0
            }
            if reduce_only: params["reduceOnly"] = True
            
            # DEBUG: Логирование параметров ордера перед отправкой
            logger.debug(
                f"Creating limit order for {symbol}: qty={formatted_qty} (original={quantity}), "
                f"price={self.format_price(symbol, price)}, side={side}, reduce_only={reduce_only}"
            )

            result = await self._async_request(self.client.place_order, **params)
            if result and result.get('retCode') == 0:
                return {'orderId': result['result'].get('orderId')}
            
            # Детальное логирование ошибки для диагностики
            error_code = result.get('retCode') if result else None
            error_msg = result.get('retMsg', '') if result else 'No error message'
            
            if error_code == 110017:
                # Специальная диагностика для ошибки 110017
                logger.error(
                    f"Error 110017 for {symbol}: {error_msg}. "
                    f"Order params: qty={formatted_qty}, original_qty={quantity}, "
                    f"qtyStep={self.symbol_info.get(symbol, {}).get('qtyStep', 'unknown')}, "
                    f"minOrderQty={self.symbol_info.get(symbol, {}).get('minOrderQty', 'unknown')}"
                )
            else:
                logger.error(f"Failed to create limit order: {result}")
            
            return None
        except Exception as e:
            logger.error(f"Error creating limit order: {e}")
            return None

    async def close_position(self, symbol: str) -> bool:
        """Closes a position using a robust reduce-only market order."""
        try:
            positions = await self.get_open_positions(symbol)
            if not positions:
                logger.warning(f"No position found to close for {symbol}")
                return True

            pos_to_close = positions[0]
            side_to_close = "Sell" if pos_to_close['side'] == 'LONG' else "Buy"
            qty_to_close = pos_to_close['quantity']

            logger.info(f"Attempting to close {qty_to_close} {symbol} with a reduce-only market order.")

            params = {
                "category": "linear", "symbol": symbol, "side": side_to_close.capitalize(),
                "orderType": "Market", "qty": self.format_quantity(symbol, qty_to_close),
                "reduceOnly": True, "positionIdx": 0
            }
            result = await self._async_request(self.client.place_order, **params)

            if result and result.get('retCode') == 0:
                await asyncio.sleep(0.75)  # Give time for fill
                logger.info(f"✅ Position for {symbol} close order submitted successfully.")
                # Очищаем кэш trailing stop для закрытого символа
                # Это важно для корректной работы при открытии новой позиции
                if symbol in self.trailing_stop_cache:
                    del self.trailing_stop_cache[symbol]
                    logger.debug(f"Cleared trailing stop cache for {symbol}")
                return True
            else:
                logger.error(f"Failed to submit close order for {symbol}. Response: {result}")
                return False

        except Exception as e:
            logger.error(f"Critical error closing position for {symbol}: {e}", exc_info=True)
            return False

    async def close(self):
        logger.info("Bybit connection wrapper closed")