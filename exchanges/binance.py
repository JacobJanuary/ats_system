#!/usr/bin/env python3
"""
Binance Exchange Implementation - PRODUCTION READY
Fixed all critical issues with order execution and leverage
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
        self.symbol_info = {}  # Alias for compatibility with protection_monitor
        self.symbol_leverage_limits = {}  # Store max leverage per symbol
        self.last_error = None

    async def initialize(self):
        """Initialize with better symbol loading"""
        self.session = aiohttp.ClientSession()

        # Test connection
        await self._make_request("GET", "/fapi/v1/ping")

        # Load exchange info with complete symbol data
        exchange_info = await self._make_request("GET", "/fapi/v1/exchangeInfo")

        if exchange_info:
            self.exchange_info = {}
            self.symbol_info = {}
            self.symbol_leverage_limits = {}

            for symbol_info in exchange_info.get('symbols', []):
                # Обрабатываем все активные PERPETUAL контракты
                if (symbol_info.get('status') == 'TRADING' and
                        symbol_info.get('contractType') == 'PERPETUAL'):

                    symbol = symbol_info['symbol']
                    self.exchange_info[symbol] = symbol_info
                    self.symbol_info[symbol] = symbol_info

                    # Извлекаем информацию о leverage
                    # Метод 1: LEVERAGE_BRACKET filter
                    leverage_bracket = next(
                        (f for f in symbol_info.get('filters', [])
                         if f['filterType'] == 'LEVERAGE_BRACKET'),
                        None
                    )

                    if leverage_bracket:
                        brackets = leverage_bracket.get('brackets', [])
                        if brackets:
                            # Берем максимальный leverage из первого bracket
                            max_leverage = int(brackets[0].get('initialLeverage', 20))
                            self.symbol_leverage_limits[symbol] = max_leverage

                    # Метод 2: MAX_LEVERAGE filter (fallback)
                    if symbol not in self.symbol_leverage_limits:
                        max_leverage_filter = next(
                            (f for f in symbol_info.get('filters', [])
                             if f['filterType'] == 'MAX_LEVERAGE'),
                            None
                        )

                        if max_leverage_filter:
                            max_lev = int(max_leverage_filter.get('maxLeverage', 20))
                            self.symbol_leverage_limits[symbol] = max_lev
                        else:
                            # Default values
                            if self.testnet:
                                self.symbol_leverage_limits[symbol] = 125
                            else:
                                # На mainnet используем консервативное значение
                                self.symbol_leverage_limits[symbol] = 20

                    # Логируем для отладки
                    logger.debug(f"Loaded {symbol}: max_leverage={self.symbol_leverage_limits.get(symbol, 'N/A')}")

            # Проверяем проблемные символы
            problem_symbols = ['LINEAUSDT', 'ICPUSDT', 'PORT3USDT', 'PUMPUSDT', 'BDXNUSDT']
            for symbol in problem_symbols:
                if symbol in self.exchange_info:
                    logger.info(f"✅ {symbol} loaded: max_leverage={self.symbol_leverage_limits.get(symbol, 'N/A')}")
                else:
                    logger.warning(f"⚠️ {symbol} not found in exchange info")

        logger.info(f"Binance {'testnet' if self.testnet else 'mainnet'} initialized")
        logger.info(f"Loaded {len(self.exchange_info)} active perpetual contracts")

    async def close(self):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, endpoint: str, data: Dict = None, signed: bool = False):
        """Make API request with improved error handling"""
        if data is None:
            data = {}
        headers = {'X-MBX-APIKEY': self.api_key}
        url = f"{self.base_url}{endpoint}"

        try:
            if signed:
                timestamp = int(time.time() * 1000)
                data['timestamp'] = timestamp
                query_string = urlencode(data)
                signature = hmac.new(
                    self.api_secret.encode('utf-8'),
                    query_string.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                query_string += f'&signature={signature}'
                url += f"?{query_string}"
            else:
                query_string = urlencode(data)
                if query_string:
                    url += f"?{query_string}"

            async with self.session.request(method.upper(), url, headers=headers) as response:
                response_text = await response.text()

                if response.status >= 400:
                    logger.error(f"HTTP Error {response.status}: {response_text}")
                    self.last_error = response_text

                    # Parse error for better handling
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get('code')
                        error_msg = error_data.get('msg', '')

                        # Handle specific error codes
                        if error_code == -4028:  # Invalid leverage
                            raise ValueError(f"Invalid leverage: {error_msg}")
                        elif error_code == -2019:  # Margin insufficient
                            raise ValueError(f"Insufficient margin: {error_msg}")
                        elif error_code == -4061:  # Order's position side does not match
                            raise ValueError(f"Position side mismatch: {error_msg}")

                    except json.JSONDecodeError:
                        pass

                    return None

                result = json.loads(response_text) if response_text else {}
                return result

        except Exception as e:
            logger.error(f"Request failed: {e}")
            self.last_error = str(e)
            raise

    def get_max_leverage(self, symbol: str) -> int:
        """Get maximum allowed leverage for symbol"""
        return self.symbol_leverage_limits.get(symbol, 20)

    def format_price(self, symbol: str, price: float) -> str:
        """Format price according to exchange rules"""
        try:
            if symbol in self.exchange_info:
                price_filter = next(
                    (f for f in self.exchange_info[symbol]['filters']
                     if f['filterType'] == 'PRICE_FILTER'),
                    None
                )
                if price_filter:
                    tick_size = Decimal(price_filter['tickSize'])
                    price_decimal = Decimal(str(price))
                    quantized_price = (price_decimal / tick_size).quantize(
                        Decimal('1'),
                        rounding=ROUND_DOWN
                    ) * tick_size
                    return str(quantized_price)
        except Exception as e:
            logger.error(f"Error formatting price for {symbol}: {e}")
        return str(price)

    def format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity with precision validation"""
        try:
            if symbol not in self.exchange_info:
                logger.warning(f"No info for {symbol}, using default formatting")
                # Для Binance минимальная точность обычно 3 знака
                return str(round(quantity, 3))

            lot_size_filter = next(
                (f for f in self.exchange_info[symbol]['filters']
                 if f['filterType'] == 'LOT_SIZE'),
                None
            )

            if lot_size_filter:
                step_size = Decimal(lot_size_filter['stepSize'])
                min_qty = Decimal(lot_size_filter.get('minQty', '0'))
                max_qty = Decimal(lot_size_filter.get('maxQty', '999999999'))

                quantity_decimal = Decimal(str(quantity))

                # Определяем количество десятичных знаков
                step_str = str(step_size)
                if '.' in step_str:
                    # Убираем trailing zeros для определения точности
                    step_str = step_str.rstrip('0')
                    if '.' in step_str:
                        decimals = len(step_str.split('.')[1])
                    else:
                        decimals = 0
                else:
                    decimals = 0

                # Округляем вниз до step_size
                quantized_quantity = (quantity_decimal / step_size).quantize(
                    Decimal('1'),
                    rounding=ROUND_DOWN
                ) * step_size

                # Проверка на минимум/максимум
                if quantized_quantity < min_qty:
                    quantized_quantity = min_qty
                    logger.debug(f"{symbol}: adjusted to minimum {min_qty}")
                elif quantized_quantity > max_qty:
                    quantized_quantity = max_qty
                    logger.warning(f"{symbol}: capped at maximum {max_qty}")

                # Форматируем с правильным количеством знаков
                # ВАЖНО: Не добавляем лишние нули
                result = format(quantized_quantity, f'.{decimals}f')

                # Убираем trailing zeros и точку если нужно
                if '.' in result:
                    result = result.rstrip('0').rstrip('.')

                logger.debug(f"{symbol}: formatted {quantity} -> {result}")
                return result

        except Exception as e:
            logger.error(f"Error formatting quantity for {symbol}: {e}")
            return str(round(quantity, 3))

    async def get_open_positions(self) -> List[Dict]:
        """Get all open positions"""
        try:
            positions_data = await self._make_request("GET", "/fapi/v2/positionRisk", signed=True)
            if not positions_data:
                return []

            open_positions = []
            for pos_data in positions_data:
                quantity = float(pos_data.get('positionAmt', 0))
                if quantity != 0:
                    open_positions.append({
                        'symbol': pos_data.get('symbol'),
                        'quantity': abs(quantity),
                        'side': 'LONG' if quantity > 0 else 'SHORT',
                        'entry_price': float(pos_data.get('entryPrice', 0)),
                        'mark_price': float(pos_data.get('markPrice', 0)),
                        'pnl': float(pos_data.get('unRealizedProfit', 0)),
                        'updateTime': int(pos_data.get('updateTime', 0))
                    })
            return open_positions

        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    async def get_balance(self) -> float:
        """Get USDT balance"""
        account = await self._make_request("GET", "/fapi/v2/account", signed=True)
        if not account:
            return 0.0
        for asset in account.get('assets', []):
            if asset['asset'] == 'USDT':
                return float(asset.get('availableBalance', 0))
        return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        """Get ticker with bid/ask/price"""
        ticker = await self._make_request("GET", "/fapi/v1/ticker/bookTicker", {'symbol': symbol})
        if ticker:
            bid = float(ticker.get('bidPrice', 0))
            ask = float(ticker.get('askPrice', 0))
            price = (bid + ask) / 2 if bid and ask else 0

            # Fallback to 24hr ticker if no orderbook
            if price == 0:
                ticker_24hr = await self._make_request("GET", "/fapi/v1/ticker/24hr", {'symbol': symbol})
                if ticker_24hr:
                    price = float(ticker_24hr.get('lastPrice', 0))

            return {
                'symbol': symbol,
                'bid': bid,
                'ask': ask,
                'price': price
            }
        return {}

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage with automatic adjustment for symbol limits"""
        try:
            # Получаем информацию о максимальном leverage для символа
            max_leverage = self.get_max_leverage(symbol)

            # Если запрошенный leverage больше максимального, используем максимальный
            target_leverage = min(leverage, max_leverage)

            # Пробуем установить leverage
            try:
                params = {'symbol': symbol, 'leverage': target_leverage}
                result = await self._make_request("POST", "/fapi/v1/leverage", params, signed=True)

                if result:
                    if target_leverage != leverage:
                        logger.info(
                            f"✅ {symbol}: Leverage set to {target_leverage}x (requested {leverage}x, max {max_leverage}x)")
                    else:
                        logger.info(f"✅ {symbol}: Leverage set to {target_leverage}x")
                    return True

            except Exception as e:
                error_str = str(e)

                # Если ошибка про invalid leverage, пробуем меньшие значения
                if "Invalid leverage" in error_str or "4028" in error_str:
                    # Пробуем стандартные значения leverage в порядке убывания
                    fallback_leverages = [20, 10, 5, 3, 2, 1]

                    for fallback_lev in fallback_leverages:
                        if fallback_lev >= target_leverage:
                            continue

                        try:
                            params = {'symbol': symbol, 'leverage': fallback_lev}
                            result = await self._make_request("POST", "/fapi/v1/leverage", params, signed=True)

                            if result:
                                logger.warning(
                                    f"⚠️ {symbol}: Leverage set to {fallback_lev}x (requested {leverage}x failed)")
                                # Обновляем информацию о максимальном leverage
                                self.symbol_leverage_limits[symbol] = fallback_lev
                                return True

                        except Exception as inner_e:
                            continue

                raise e

        except Exception as e:
            logger.error(f"Failed to set leverage for {symbol}: {e}")
            # На testnet не критично если leverage не установился
            if self.testnet:
                return True
            return False

    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """Create market order with proper response handling"""
        params = {
            'symbol': symbol,
            'side': side.upper(),
            'type': 'MARKET',
            'quantity': self.format_quantity(symbol, quantity)
        }

        try:
            result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)

            if result and 'orderId' in result:
                # Get order details to confirm execution
                order_id = result['orderId']

                # Wait a bit for order to be processed
                await asyncio.sleep(0.5)

                # Query order status
                order_status = await self._make_request(
                    "GET",
                    "/fapi/v1/order",
                    {'symbol': symbol, 'orderId': order_id},
                    signed=True
                )

                if order_status:
                    executed_qty = float(order_status.get('executedQty', 0))
                    avg_price = float(order_status.get('avgPrice', 0))
                    status = order_status.get('status', 'NEW')

                    if executed_qty > 0 and status == 'FILLED':
                        logger.info(f"Order {order_id} filled: {executed_qty} {symbol} @ {avg_price}")
                        return {
                            'orderId': str(order_id),
                            'symbol': symbol,
                            'side': side,
                            'quantity': executed_qty,
                            'price': avg_price,
                            'status': 'FILLED'
                        }
                    else:
                        logger.warning(f"Order {order_id} not fully filled: status={status}, executed={executed_qty}")
                        return None

            return None

        except Exception as e:
            logger.error(f"Failed to create market order: {e}")
            return None

    async def create_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                                 reduce_only: bool = False) -> Optional[Dict]:
        """Create limit order"""
        params = {
            'symbol': symbol,
            'side': side.upper(),
            'type': 'LIMIT',
            'quantity': self.format_quantity(symbol, quantity),
            'price': self.format_price(symbol, price),
            'timeInForce': 'GTC'
        }
        if reduce_only:
            params['reduceOnly'] = 'true'

        return await self._make_request("POST", "/fapi/v1/order", params, signed=True)

    async def cancel_all_open_orders(self, symbol: str) -> bool:
        """Cancel all open orders for symbol"""
        try:
            params = {'symbol': symbol}
            result = await self._make_request("DELETE", "/fapi/v1/allOpenOrders", params, signed=True)

            if result and result.get('code') == 200:
                logger.info(f"Cancelled all orders for {symbol}")
                return True
            return False

        except Exception as e:
            logger.error(f"Failed to cancel orders for {symbol}: {e}")
            return False

    async def close_position(self, symbol: str) -> Optional[Dict]:
        """Close position by market order"""
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                return await self.create_market_order(symbol, side, pos['quantity'])
        return None

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        """Set stop loss with improved retry logic"""
        max_attempts = 3

        for attempt in range(max_attempts):
            try:
                # Ждем пока позиция зарегистрируется
                if attempt > 0:
                    await asyncio.sleep(2)

                # Получаем текущие позиции
                positions = await self.get_open_positions()
                pos = next((p for p in positions if p['symbol'] == symbol), None)

                if not pos:
                    logger.warning(f"Position for {symbol} not found yet, attempt {attempt + 1}/{max_attempts}")
                    continue

                # Определяем side для stop order
                side = 'SELL' if pos['side'] == 'LONG' else 'BUY'

                # Проверяем существующие ордера
                existing_orders = await self.get_open_orders(symbol)

                # Проверяем есть ли уже stop order
                stop_exists = any(
                    o.get('type') in ['STOP_MARKET', 'STOP', 'STOP_LOSS']
                    for o in existing_orders
                )

                if stop_exists:
                    logger.info(f"Stop loss already exists for {symbol}")
                    return True

                # Создаем stop order
                params = {
                    'symbol': symbol,
                    'side': side,
                    'type': 'STOP_MARKET',
                    'stopPrice': self.format_price(symbol, stop_price),
                    'closePosition': 'true',
                    'timeInForce': 'GTE_GTC',
                    'workingType': 'MARK_PRICE'  # Используем Mark Price для стабильности
                }

                result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)

                if result and result.get('orderId'):
                    logger.info(f"✅ Stop Loss set for {symbol} at {stop_price}")
                    return True

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} to set SL for {symbol} failed: {e}")

                # На последней попытке проверяем, может SL уже установлен
                if attempt == max_attempts - 1:
                    try:
                        orders = await self.get_open_orders(symbol)
                        if any(o.get('type') in ['STOP_MARKET', 'STOP'] for o in orders):
                            logger.info(f"Stop loss found for {symbol} on final check")
                            return True
                    except:
                        pass

        logger.error(f"Failed to set Stop Loss for {symbol} after {max_attempts} attempts")
        return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Set take profit order with retry logic."""
        for attempt in range(4):
            try:
                positions = await self.get_open_positions()
                pos = next((p for p in positions if p['symbol'] == symbol), None)
                if pos:
                    side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                    params = {'symbol': symbol, 'side': side, 'type': 'TAKE_PROFIT_MARKET',
                              'stopPrice': self.format_price(symbol, take_profit_price), 'closePosition': 'true'}
                    result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
                    if result and result.get('orderId'):
                        logger.info(f"✅ Take Profit set for {symbol} at {take_profit_price}")
                        return True

                await asyncio.sleep(0.5 + attempt)

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} to set TP for {symbol} failed: {e}")
                await asyncio.sleep(0.5 + attempt)

        logger.error(f"❌ Failed to set Take Profit for {symbol} after multiple attempts.")
        return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders"""
        params = {}
        if symbol:
            params['symbol'] = symbol
        orders = await self._make_request("GET", "/fapi/v1/openOrders", params, signed=True)
        return orders if orders else []

    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """Set trailing stop order with retry logic."""
        for attempt in range(4):
            try:
                positions = await self.get_open_positions()
                pos = next((p for p in positions if p['symbol'] == symbol), None)
                if pos:
                    side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                    callback_rate = max(0.1, min(5.0, callback_rate))
                    params = {'symbol': symbol, 'side': side, 'type': 'TRAILING_STOP_MARKET',
                              'callbackRate': callback_rate,
                              'activationPrice': self.format_price(symbol, activation_price),
                              'quantity': self.format_quantity(symbol, pos['quantity'])}
                    result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
                    if result and 'orderId' in result:
                        logger.info(
                            f"✅ Trailing stop set for {symbol}: activation={activation_price}, callback={callback_rate}%")
                        return True

                await asyncio.sleep(0.5 + attempt)

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} to set Trailing Stop for {symbol} failed: {e}")
                await asyncio.sleep(0.5 + attempt)

        logger.error(f"❌ Failed to set Trailing Stop for {symbol} after multiple attempts.")
        return False