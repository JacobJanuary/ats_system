#!/usr/bin/env python3
"""
Binance Exchange Implementation - PRODUCTION READY v2.5 (FIXED)
- ВОССТАНОВЛЕН activationPrice для отложенной активации Trailing Stop
- ДОБАВЛЕН set_take_profit для установки TP
- УЛУЧШЕНА обработка ошибок при переходе SL → TS
- ИСПРАВЛЕН cancel_order для точного управления ордерами
"""

import asyncio
import logging
from typing import Dict, Optional, List
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hmac
import hashlib
import time

try:
    from ..api_error_handler import get_error_handler
except ImportError:
    from api_error_handler import get_error_handler
import aiohttp
import json
from urllib.parse import urlencode

from .base import BaseExchange

logger = logging.getLogger(__name__)


class BinanceExchange(BaseExchange):
    """Binance futures exchange implementation with all fixes"""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')
        if self.testnet:
            self.base_url = "https://testnet.binancefuture.com"
            self.ws_url = "wss://stream.binancefuture.com"
        else:
            self.base_url = "https://fapi.binance.com"
            self.ws_url = "wss://fstream.binance.com"

        self.session = None
        self.exchange_info = {}
        self.symbol_info = {}
        self.symbol_leverage_limits = {}
        self.last_error = None
        # Кэш для trailing stop параметров, чтобы избежать повторных установок
        # Формат: {symbol: {'activation_price': value, 'callback_rate': value}}
        self.trailing_stop_cache = {}

    async def initialize(self):
        self.session = aiohttp.ClientSession()
        await self._make_request("GET", "/fapi/v1/ping")
        exchange_info = await self._make_request("GET", "/fapi/v1/exchangeInfo")
        if exchange_info:
            self.exchange_info = {}
            self.symbol_info = {}
            self.symbol_leverage_limits = {}
            for symbol_info in exchange_info.get('symbols', []):
                if (symbol_info.get('status') == 'TRADING' and
                        symbol_info.get('contractType') == 'PERPETUAL'):
                    symbol = symbol_info['symbol']
                    self.exchange_info[symbol] = symbol_info
                    self.symbol_info[symbol] = symbol_info
                    leverage_bracket = next(
                        (f for f in symbol_info.get('filters', [])
                         if f['filterType'] == 'LEVERAGE_BRACKET'),
                        None
                    )
                    if leverage_bracket and leverage_bracket.get('brackets'):
                        max_leverage = int(leverage_bracket['brackets'][0].get('initialLeverage', 20))
                        self.symbol_leverage_limits[symbol] = max_leverage
                    else:
                        self.symbol_leverage_limits[symbol] = 20
            logger.info(f"Binance {'testnet' if self.testnet else 'mainnet'} initialized")
            logger.info(f"Loaded {len(self.exchange_info)} active perpetual contracts")

    async def close(self):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, endpoint: str, data: Dict = None, signed: bool = False):
        if data is None: data = {}
        headers = {'X-MBX-APIKEY': self.api_key}
        url = f"{self.base_url}{endpoint}"
        try:
            if signed:
                timestamp = int(time.time() * 1000)
                data['timestamp'] = timestamp
                filtered_data = {k: v for k, v in data.items() if v is not None}
                query_string = urlencode(filtered_data)
                signature = hmac.new(
                    self.api_secret.encode('utf-8'),
                    query_string.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                query_string += f'&signature={signature}'
                url += f"?{query_string}"
            else:
                filtered_data = {k: v for k, v in data.items() if v is not None} if data else {}
                query_string = urlencode(filtered_data)
                if query_string:
                    url += f"?{query_string}"
            async with self.session.request(method.upper(), url, headers=headers) as response:
                response_text = await response.text()
                if response.status >= 400:
                    logger.error(f"HTTP Error {response.status}: {response_text}")
                    self.last_error = response_text
                    return None
                return json.loads(response_text) if response_text else {}
        except Exception as e:
            logger.error(f"Request failed: {e}")
            self.last_error = str(e)
            raise

    def get_max_leverage(self, symbol: str) -> int:
        return self.symbol_leverage_limits.get(symbol, 20)

    def format_price(self, symbol: str, price: float) -> str:
        try:
            if symbol in self.exchange_info:
                price_filter = next(
                    (f for f in self.exchange_info[symbol]['filters'] if f['filterType'] == 'PRICE_FILTER'), None)
                if price_filter:
                    tick_size = Decimal(price_filter['tickSize'])
                    price_decimal = Decimal(str(price))
                    return str((price_decimal / tick_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size)
        except Exception as e:
            logger.error(f"Error formatting price for {symbol}: {e}")
        return str(price)

    def format_quantity(self, symbol: str, quantity: float) -> str:
        try:
            if symbol in self.exchange_info:
                lot_size_filter = next(
                    (f for f in self.exchange_info[symbol]['filters'] if f['filterType'] == 'LOT_SIZE'), None)
                if lot_size_filter:
                    step_size = Decimal(lot_size_filter['stepSize'])
                    min_qty = Decimal(lot_size_filter.get('minQty', '0'))
                    quantity_decimal = Decimal(str(quantity))

                    # Округляем к ближайшему step_size
                    quantized_qty = (quantity_decimal / step_size).quantize(Decimal('1'),
                                                                            rounding=ROUND_DOWN) * step_size

                    # ВАЖНО: Проверяем минимальное количество
                    if quantized_qty < min_qty:
                        quantized_qty = min_qty

                    # Правильное определение точности
                    precision = abs(step_size.as_tuple().exponent)
                    return f"{quantized_qty:.{precision}f}"
        except Exception as e:
            logger.error(f"Error formatting quantity for {symbol}: {e}")
        return str(round(quantity, 8))  # Увеличиваем дефолтную точность

    async def get_open_positions(self) -> List[Dict]:
        try:
            positions_data = await self._make_request("GET", "/fapi/v2/positionRisk", signed=True)
            if not positions_data: return []
            open_positions = []
            for pos in positions_data:
                quantity = float(pos.get('positionAmt', 0))
                if quantity != 0:
                    open_positions.append({
                        'symbol': pos.get('symbol'),
                        'quantity': abs(quantity),
                        'side': 'LONG' if quantity > 0 else 'SHORT',
                        'entry_price': float(pos.get('entryPrice', 0)),
                        'mark_price': float(pos.get('markPrice', 0)),
                        'pnl': float(pos.get('unRealizedProfit', 0)),
                        'updateTime': int(pos.get('updateTime', 0))
                    })
            return open_positions
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    async def get_balance(self) -> float:
        account = await self._make_request("GET", "/fapi/v2/account", signed=True)
        if not account: return 0.0
        for asset in account.get('assets', []):
            if asset['asset'] == 'USDT':
                return float(asset.get('availableBalance', 0))
        return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        ticker = await self._make_request("GET", "/fapi/v1/ticker/bookTicker", {'symbol': symbol})
        if ticker:
            bid = float(ticker.get('bidPrice', 0))
            ask = float(ticker.get('askPrice', 0))
            price = (bid + ask) / 2 if bid and ask else 0
            return {'symbol': symbol, 'bid': bid, 'ask': ask, 'price': price}
        return {}

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        try:
            max_leverage = self.get_max_leverage(symbol)
            target_leverage = min(leverage, max_leverage)
            result = await self._make_request("POST", "/fapi/v1/leverage",
                                              {'symbol': symbol, 'leverage': target_leverage}, signed=True)
            if result and result.get('leverage') == target_leverage:
                logger.info(f"✅ {symbol}: Leverage set to {target_leverage}x")
                return True
            elif result and "No need to modify leverage" in result.get('msg', ''):
                logger.info(f"Leverage for {symbol} already at {target_leverage}x.")
                return True
            else:
                logger.error(f"Failed to set leverage for {symbol}: {result}")
                return False
        except Exception as e:
            logger.error(f"Exception setting leverage for {symbol}: {e}")
            return self.testnet

    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        # Проверка минимального размера ордера перед отправкой
        if symbol in self.symbol_info:
            min_qty = self.symbol_info[symbol].get('minQty', 0.001)
            if quantity < min_qty:
                logger.warning(
                    f"Market order quantity {quantity} for {symbol} is below minimum {min_qty}. "
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
            
        params = {
            'symbol': symbol, 'side': side.upper(), 'type': 'MARKET',
            'quantity': formatted_qty
        }
        try:
            result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
            if result and 'orderId' in result:
                await asyncio.sleep(0.5)
                order_status = await self._make_request("GET", "/fapi/v1/order",
                                                        {'symbol': symbol, 'orderId': result['orderId']}, signed=True)
                if order_status and order_status.get('status') == 'FILLED':
                    executed_qty = float(order_status.get('executedQty', 0))
                    avg_price = float(order_status.get('avgPrice', 0))
                    logger.info(f"Order {result['orderId']} filled: {executed_qty} {symbol} @ {avg_price}")
                    return {
                        'orderId': str(result['orderId']), 'symbol': symbol, 'side': side,
                        'executed_qty': executed_qty, 'price': avg_price, 'status': 'FILLED'
                    }
            logger.error(f"Market order failed or not filled: {result}")
            return None
        except Exception as e:
            logger.error(f"Failed to create market order: {e}")
            return None

    async def create_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                                 reduce_only: bool = False) -> Optional[Dict]:
        # Проверка минимального размера ордера перед отправкой на биржу
        # Это предотвращает ошибки с недостаточным размером ордера
        if symbol in self.symbol_info:
            min_qty = self.symbol_info[symbol].get('minQty', 0.001)
            if quantity < min_qty:
                logger.warning(
                    f"Order quantity {quantity} for {symbol} is below minimum {min_qty}. "
                    f"Skipping order creation."
                )
                return None
        
        # Форматирование количества и цены
        formatted_qty = self.format_quantity(symbol, quantity)
        
        # Дополнительная проверка после форматирования
        if float(formatted_qty) == 0:
            logger.warning(
                f"Formatted quantity became 0 for {symbol} (original: {quantity}). "
                f"Order creation skipped."
            )
            return None
            
        params = {
            'symbol': symbol, 'side': side.upper(), 'type': 'LIMIT',
            'quantity': formatted_qty,
            'price': self.format_price(symbol, price),
            'timeInForce': 'GTC'
        }
        if reduce_only: params['reduceOnly'] = 'true'
        result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
        if result and 'orderId' in result:
            return result
        logger.error(f"Failed to create limit order: {result}")
        return None

    async def cancel_all_open_orders(self, symbol: str) -> bool:
        try:
            result = await self._make_request("DELETE", "/fapi/v1/allOpenOrders", {'symbol': symbol}, signed=True)
            return result and result.get('code') == 200
        except Exception as e:
            logger.error(f"Failed to cancel orders for {symbol}: {e}")
            return False

    async def get_account_balance(self) -> float:
        """Get available balance from futures account"""
        try:
            account_info = await self._make_request("GET", "/fapi/v2/account", signed=True)
            if account_info:
                return float(account_info.get('availableBalance', 0))
            return 0.0
        except Exception as e:
            logger.error(f"Failed to get account balance: {e}")
            return 0.0

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancels a specific order by its ID."""
        try:
            params = {'symbol': symbol, 'orderId': order_id}
            result = await self._make_request("DELETE", "/fapi/v1/order", params, signed=True)
            # Check for 'CANCELED' status for success
            if result and str(result.get('orderId')) == order_id and result.get('status') == 'CANCELED':
                logger.info(f"✅ Order {order_id} for {symbol} cancelled successfully.")
                return True
            logger.error(f"Failed to cancel order {order_id} for {symbol}: {result}")
            return False
        except Exception as e:
            logger.error(f"Exception cancelling order {order_id} for {symbol}: {e}")
            return False

    async def close_position(self, symbol: str) -> bool:
        positions = await self.get_open_positions()
        pos_to_close = next((p for p in positions if p['symbol'] == symbol), None)
        if pos_to_close:
            side = 'SELL' if pos_to_close['side'] == 'LONG' else 'BUY'
            result = await self.create_market_order(symbol, side, pos_to_close['quantity'])
            if result and result.get('executed_qty', 0) > 0:
                # Очищаем кэш trailing stop для закрытого символа
                # Это важно для корректной работы при открытии новой позиции
                if symbol in self.trailing_stop_cache:
                    del self.trailing_stop_cache[symbol]
                    logger.debug(f"Cleared trailing stop cache for {symbol}")
                return True
            return False
        logger.warning(f"No position found to close for {symbol}")
        return True

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        try:
            positions = await self.get_open_positions()
            pos = next((p for p in positions if p['symbol'] == symbol), None)
            if not pos:
                logger.warning(f"No position found for {symbol} to set SL.")
                return False
            
            # CRITICAL FIX: Cancel existing SL orders before creating new one
            open_orders = await self.get_open_orders(symbol)
            for order in open_orders:
                if order.get('type') == 'STOP_MARKET' and order.get('status') in ['NEW', 'PARTIALLY_FILLED']:
                    order_id = order.get('orderId')
                    logger.info(f"Cancelling existing SL order {order_id} for {symbol}")
                    await self.cancel_order(symbol, order_id)
                    await asyncio.sleep(0.1)  # Small delay to ensure cancellation
            
            side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
            params = {
                'symbol': symbol, 'side': side, 'type': 'STOP_MARKET',
                'stopPrice': self.format_price(symbol, stop_price),
                'closePosition': 'true'
            }
            result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
            if result and result.get('orderId'):
                logger.info(f"✅ Stop Loss set for {symbol} at {stop_price}")
                return True
            logger.error(f"Failed to set SL for {symbol}: {result}")
            return False
        except Exception as e:
            logger.error(f"Exception setting SL for {symbol}: {e}")
            return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Устанавливает Take Profit для позиции"""
        try:
            positions = await self.get_open_positions()
            pos = next((p for p in positions if p['symbol'] == symbol), None)
            if not pos:
                logger.warning(f"No position found for {symbol} to set TP.")
                return False

            # Для LONG позиции TP это SELL ордер, для SHORT - BUY
            side = 'SELL' if pos['side'] == 'LONG' else 'BUY'

            params = {
                'symbol': symbol,
                'side': side,
                'type': 'TAKE_PROFIT_MARKET',
                'stopPrice': self.format_price(symbol, take_profit_price),
                'closePosition': 'true'
            }

            result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
            if result and result.get('orderId'):
                logger.info(f"✅ Take Profit set for {symbol} at {take_profit_price}")
                return True
            logger.error(f"Failed to set TP for {symbol}: {result}")
            return False
        except Exception as e:
            logger.error(f"Exception setting TP for {symbol}: {e}")
            return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        params = {'symbol': symbol} if symbol else {}
        orders_raw = await self._make_request("GET", "/fapi/v1/openOrders", params, signed=True)
        if not orders_raw: return []

        parsed_orders = []
        for o in orders_raw:
            try:
                parsed_orders.append({
                    'orderId': str(o.get('orderId')),
                    'symbol': o.get('symbol'),
                    'side': o.get('side', '').lower(),
                    'quantity': float(o.get('origQty', 0)),
                    'price': float(o.get('price', 0)),
                    'stopPrice': float(o.get('stopPrice', 0)) if o.get('stopPrice') else 0,
                    'status': o.get('status'),
                    'type': o.get('type', '').lower(),
                    'reduceOnly': o.get('reduceOnly', False)
                })
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(
                    f"Could not parse order data for order {o.get('orderId')}. Skipping. Error: {e}. Data: {o}")
                continue
        return parsed_orders

    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """
        CRITICAL FIX v2: Умная установка TS с обработкой ошибок
        - Автоматически корректирует activation_price при ошибке
        - Логирует детальную информацию для отладки
        - Кэширует параметры для избежания повторных установок
        """
        try:
            # Проверяем кэш на идентичные параметры
            # Это предотвращает повторные попытки с одинаковыми параметрами
            cached_params = self.trailing_stop_cache.get(symbol, {})
            if (cached_params.get('activation_price') == activation_price and 
                cached_params.get('callback_rate') == callback_rate):
                logger.debug(
                    f"Trailing stop for {symbol} already set with same parameters. "
                    f"Skipping to avoid duplicate API call."
                )
                return True
            
            positions = await self.get_open_positions()
            pos = next((p for p in positions if p['symbol'] == symbol), None)
            if not pos:
                logger.warning(f"No position for {symbol} to set Trailing Stop.")
                return False

            side = 'SELL' if pos['side'] == 'LONG' else 'BUY'

            # Форматируем параметры
            formatted_activation = self.format_price(symbol, activation_price)
            callback_rate = max(0.1, min(5.0, callback_rate))
            formatted_qty = self.format_quantity(symbol, pos['quantity'])

            params = {
                'symbol': symbol,
                'side': side,
                'type': 'TRAILING_STOP_MARKET',
                'activationPrice': formatted_activation,
                'callbackRate': callback_rate,
                'quantity': formatted_qty
            }

            logger.debug(f"TS params for {symbol}: {params}")

            result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)

            if result and 'orderId' in result:
                # Сохраняем успешно установленные параметры в кэш
                self.trailing_stop_cache[symbol] = {
                    'activation_price': activation_price,
                    'callback_rate': callback_rate
                }
                logger.info(
                    f"✅ Trailing stop set for {symbol}: "
                    f"activation=${formatted_activation}, "
                    f"callback={callback_rate}%. "
                    f"Will activate IMMEDIATELY when price reaches target."
                )
                return True

            # Обработка специфических ошибок
            if result:
                error_code = result.get('code')
                error_msg = result.get('msg', '')

                if error_code == -2021:  # Order would immediately trigger
                    logger.warning(
                        f"TS would trigger immediately for {symbol}. "
                        f"This means price already passed activation level - good!"
                    )
                    # Пробуем без activationPrice (активируется сразу)
                    params.pop('activationPrice', None)

                    result2 = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
                    if result2 and 'orderId' in result2:
                        # Сохраняем в кэш даже при установке без activation price
                        self.trailing_stop_cache[symbol] = {
                            'activation_price': activation_price,
                            'callback_rate': callback_rate
                        }
                        logger.info(f"✅ TS set without activation price - active immediately")
                        return True

                elif error_code == -4131:  # PERCENT_PRICE filter
                    logger.error(f"Price filter violation for {symbol}: {error_msg}")
                    return False

                logger.error(f"Failed to set TS for {symbol}: {result}")

            return False

        except Exception as e:
            logger.error(f"Exception setting Trailing Stop for {symbol}: {e}")
            return False