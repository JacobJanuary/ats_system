#!/usr/bin/env python3
"""
Bybit Exchange Implementation - V5 API ИСПРАВЛЕННАЯ ВЕРСИЯ
Исправлены все проблемы с подписью и проверкой ордеров
"""

import asyncio
import logging
from typing import Dict, Optional, List, Any
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hmac
import hashlib
import time
import aiohttp
import json
from urllib.parse import urlencode  # ВАЖНО: для правильного кодирования параметров
from .base import BaseExchange

logger = logging.getLogger(__name__)


class BybitExchange(BaseExchange):
    """
    Bybit futures exchange implementation with FIXED signature generation
    """

    def __init__(self, config: Dict):
        """Initialize Bybit exchange with configuration"""
        super().__init__(config)

        # КРИТИЧНО: Убедимся, что ключи не обрезаны
        self.api_key = config.get('api_key', '').strip()
        self.api_secret = config.get('api_secret', '').strip()

        if self.testnet:
            self.base_url = "https://api-testnet.bybit.com"
            self.ws_url = "wss://stream-testnet.bybit.com"
        else:
            self.base_url = "https://api.bybit.com"
            self.ws_url = "wss://stream.bybit.com"

        self.session = None
        self.recv_window = 5000
        self.symbol_info = {}
        self.position_mode = None

    async def initialize(self):
        """Initialize Bybit connection and load market info"""
        self.session = aiohttp.ClientSession()

        # Test connectivity
        server_time = await self._make_request("GET", "/v5/market/time")
        if not server_time:
            raise Exception("Failed to connect to Bybit")

        # Detect position mode
        await self._detect_position_mode()

        # Load instrument info
        await self._load_instruments_info()

        logger.info(f"Bybit {'testnet' if self.testnet else 'mainnet'} initialized")
        logger.info(f"Position mode: {self.position_mode}")
        logger.info(f"Loaded {len(self.symbol_info)} active symbols")

    async def _detect_position_mode(self):
        """Detect account position mode"""
        try:
            result = await self._make_request(
                "GET",
                "/v5/account/info",
                signed=True
            )

            if result:
                unified_margin_status = result.get('unifiedMarginStatus', 0)
                if unified_margin_status in [1, 2, 3, 4]:
                    self.position_mode = "hedge"
                else:
                    self.position_mode = "one-way"
            else:
                self.position_mode = "one-way"

        except Exception as e:
            logger.warning(f"Could not detect position mode, defaulting to one-way: {e}")
            self.position_mode = "one-way"

    async def _load_instruments_info(self):
        """Load all active trading instruments"""
        try:
            cursor = None
            while True:
                params = {'category': 'linear', 'limit': 1000}
                if cursor:
                    params['cursor'] = cursor

                result = await self._make_request(
                    "GET",
                    "/v5/market/instruments-info",
                    params
                )

                if not result or 'list' not in result:
                    break

                for instrument in result['list']:
                    if instrument.get('status') == 'Trading':
                        symbol = instrument['symbol']
                        self.symbol_info[symbol] = {
                            'min_qty': float(instrument['lotSizeFilter']['minOrderQty']),
                            'max_qty': float(instrument['lotSizeFilter']['maxOrderQty']),
                            'qty_step': float(instrument['lotSizeFilter']['qtyStep']),
                            'min_price': float(instrument['priceFilter']['minPrice']),
                            'max_price': float(instrument['priceFilter']['maxPrice']),
                            'tick_size': float(instrument['priceFilter']['tickSize']),
                            'max_leverage': float(instrument['leverageFilter']['maxLeverage']),
                            'min_leverage': float(instrument['leverageFilter']['minLeverage']),
                        }

                cursor = result.get('nextPageCursor')
                if not cursor:
                    break

        except Exception as e:
            logger.error(f"Error loading instruments: {e}")

    def _generate_signature(self, timestamp: str, params: str) -> str:
        """
        Generate HMAC SHA256 signature for Bybit API
        ИСПРАВЛЕНО: Убедимся, что используем полные ключи
        """
        # Формат: timestamp + api_key + recv_window + params
        param_str = f"{timestamp}{self.api_key}{self.recv_window}{params}"

        # Для отладки (закомментировать в production)
        # logger.debug(f"Signature string: {param_str}")

        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return signature

    async def _make_request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False):
        """
        Make HTTP request to Bybit API
        ИСПРАВЛЕНО: Правильная обработка параметров и подписи
        """
        if not self.session:
            return None

        url = self.base_url + endpoint
        timestamp = str(int(time.time() * 1000))

        headers = {}

        # Для не подписанных запросов
        if not signed:
            try:
                if method == "GET":
                    if params:
                        query_string = urlencode(sorted(params.items()))
                        url = f"{url}?{query_string}"
                    async with self.session.get(url, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)
                elif method == "POST":
                    async with self.session.post(url, json=params, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)
            except Exception as e:
                logger.error(f"Request error: {e}")
                return None

        # Для подписанных запросов
        if signed and self.api_key and self.api_secret:
            headers = {
                'X-BAPI-API-KEY': self.api_key,
                'X-BAPI-TIMESTAMP': timestamp,
                'X-BAPI-RECV-WINDOW': str(self.recv_window),
            }

            try:
                if method == "GET":
                    # КРИТИЧНО: Используем urlencode для правильного кодирования
                    if params:
                        # Сортируем параметры по алфавиту
                        sorted_params = sorted(params.items())
                        query_string = urlencode(sorted_params)
                    else:
                        query_string = ""

                    # Генерируем подпись
                    signature = self._generate_signature(timestamp, query_string)
                    headers['X-BAPI-SIGN'] = signature

                    # Добавляем параметры к URL
                    if query_string:
                        url = f"{url}?{query_string}"

                    async with self.session.get(url, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)

                elif method == "POST":
                    # Для POST используем JSON body
                    body_string = json.dumps(params) if params else ""
                    signature = self._generate_signature(timestamp, body_string)
                    headers['X-BAPI-SIGN'] = signature
                    headers['Content-Type'] = 'application/json'

                    async with self.session.post(url, json=params, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)

            except asyncio.TimeoutError:
                logger.error(f"Request timeout for {endpoint}")
                return None
            except Exception as e:
                logger.error(f"Bybit request error: {e}")
                return None

    async def _handle_response(self, response):
        """Handle API response"""
        try:
            data = await response.json()

            if response.status == 200:
                if data.get('retCode') == 0:
                    return data.get('result', data)
                else:
                    ret_code = data.get('retCode')
                    ret_msg = data.get('retMsg', 'Unknown error')

                    if ret_code == 10001:
                        logger.error(f"Bybit: Parameter error - {ret_msg}")
                    elif ret_code == 10004:
                        logger.error(f"Bybit: Sign error - {ret_msg}")
                    elif ret_code == 110043:
                        logger.debug(f"Bybit: Leverage not modified - {ret_msg}")
                        return data  # Not really an error
                    else:
                        logger.error(f"Bybit API error {ret_code}: {ret_msg}")

                    return None
            else:
                logger.error(f"Bybit HTTP error {response.status}: {data}")
                return None

        except Exception as e:
            logger.error(f"Error parsing Bybit response: {e}")
            return None

    def format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol specifications"""
        if symbol not in self.symbol_info:
            logger.warning(f"No symbol info for {symbol}, using default formatting")
            if quantity < 1:
                return str(round(quantity, 4))
            else:
                return str(int(quantity))

        info = self.symbol_info[symbol]
        qty_step = Decimal(str(info['qty_step']))
        min_qty = Decimal(str(info['min_qty']))
        max_qty = Decimal(str(info['max_qty']))

        qty_decimal = Decimal(str(quantity))

        # Round DOWN to step size
        if qty_step > 0:
            step_str = str(qty_step)
            if '.' in step_str:
                decimal_places = len(step_str.split('.')[1].rstrip('0'))
            else:
                decimal_places = 0

            rounded_qty = (qty_decimal / qty_step).quantize(Decimal('1'), rounding=ROUND_DOWN) * qty_step

            if rounded_qty < min_qty:
                rounded_qty = min_qty
                logger.debug(f"{symbol}: Adjusted to minimum {min_qty}")

            if rounded_qty > max_qty:
                rounded_qty = max_qty
                logger.warning(f"{symbol}: Capped at maximum {max_qty}")

            if decimal_places > 0:
                format_str = f"{{:.{decimal_places}f}}"
                result = format_str.format(float(rounded_qty))
            else:
                result = str(int(rounded_qty))

            if '.' in result:
                result = result.rstrip('0').rstrip('.')

            logger.debug(f"{symbol}: Formatted {quantity} -> {result}")
            return result
        else:
            logger.error(f"{symbol}: Invalid qty_step: {qty_step}")
            return str(quantity)

    def format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol tick size"""
        if symbol not in self.symbol_info:
            return str(round(price, 2))

        info = self.symbol_info[symbol]
        tick_size = Decimal(str(info['tick_size']))

        price_decimal = Decimal(str(price))
        rounded_price = (price_decimal / tick_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size

        result = str(rounded_price)
        if '.' in result:
            result = result.rstrip('0').rstrip('.')

        return result

    async def get_balance(self) -> float:
        """Get USDT balance"""
        wallet = await self._make_request(
            "GET",
            "/v5/account/wallet-balance",
            {'accountType': 'UNIFIED'},
            signed=True
        )

        if not wallet:
            wallet = await self._make_request(
                "GET",
                "/v5/account/wallet-balance",
                {'accountType': 'CONTRACT'},
                signed=True
            )

        if wallet and 'list' in wallet:
            for account in wallet['list']:
                for coin in account.get('coin', []):
                    if coin['coin'] == 'USDT':
                        balance_str = coin.get('walletBalance', '0')
                        if balance_str and balance_str != '':
                            try:
                                return float(balance_str)
                            except (ValueError, TypeError):
                                logger.error(f"Invalid balance value: {balance_str}")
                                return 0.0
        return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        """Get ticker info"""
        ticker = await self._make_request(
            "GET",
            "/v5/market/tickers",
            {'category': 'linear', 'symbol': symbol}
        )

        if not ticker or 'list' not in ticker or not ticker['list']:
            logger.warning(f"No ticker data for {symbol}")
            return {}

        ticker_data = ticker['list'][0]

        # ИСПРАВЛЕНО: Безопасная конвертация с проверкой на пустые строки
        bid_raw = ticker_data.get('bid1Price', 0)
        ask_raw = ticker_data.get('ask1Price', 0)
        last_raw = ticker_data.get('lastPrice', 0)

        try:
            bid = float(bid_raw) if bid_raw and bid_raw != '' else 0
        except (ValueError, TypeError):
            bid = 0

        try:
            ask = float(ask_raw) if ask_raw and ask_raw != '' else 0
        except (ValueError, TypeError):
            ask = 0

        try:
            last = float(last_raw) if last_raw and last_raw != '' else 0
        except (ValueError, TypeError):
            last = 0

        if bid > 0 and ask > 0:
            price = (bid + ask) / 2
        elif last > 0:
            price = last
            if bid == 0:
                bid = last * 0.9995
            if ask == 0:
                ask = last * 1.0005
        else:
            return {}

        return {
            'symbol': ticker_data.get('symbol'),
            'bid': bid,
            'ask': ask,
            'price': price
        }

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol"""
        if symbol in self.symbol_info:
            max_leverage = int(self.symbol_info[symbol]['max_leverage'])
            min_leverage = int(self.symbol_info[symbol]['min_leverage'])
            leverage = max(min_leverage, min(leverage, max_leverage))

        params = {
            'category': 'linear',
            'symbol': symbol,
            'buyLeverage': str(leverage),
            'sellLeverage': str(leverage)
        }

        result = await self._make_request("POST", "/v5/position/set-leverage", params, signed=True)

        if result or result is None:
            logger.debug(f"Leverage set/checked for {symbol}: {leverage}x")
            return True

        return False

    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """
        Create market order with COMPLETE empty value handling
        """
        formatted_qty = self.format_quantity(symbol, quantity)

        position_idx = 0
        if self.position_mode == "hedge":
            position_idx = 1 if side.upper() == 'BUY' else 2

        params = {
            'category': 'linear',
            'symbol': symbol,
            'side': 'Buy' if side.upper() == 'BUY' else 'Sell',
            'orderType': 'Market',
            'qty': formatted_qty,
            'positionIdx': position_idx,
            'timeInForce': 'GTC',
        }

        logger.info(f"Creating market order for {symbol}: {formatted_qty} @ Market")
        order_result = await self._make_request("POST", "/v5/order/create", params, signed=True)

        if not order_result or 'orderId' not in order_result:
            logger.error(f"Failed to create market order for {symbol}")
            return None

        order_id = order_result['orderId']
        logger.info(f"Order created: {order_id} for {symbol}")

        await asyncio.sleep(1.0)

        # Check order status
        order_status = None
        try:
            result = await self._make_request(
                "GET",
                "/v5/order/realtime",
                {
                    'category': 'linear',
                    'orderId': order_id,
                    'openOnly': 0
                },
                signed=True
            )

            if result and 'list' in result and len(result['list']) > 0:
                order_status = result['list'][0]
                logger.debug(f"Order status: {order_status.get('orderStatus')}")

        except Exception as e:
            logger.error(f"Error checking order status: {e}")

        # Check executions
        execution_details = None
        try:
            exec_result = await self._make_request(
                "GET",
                "/v5/execution/list",
                {
                    'category': 'linear',
                    'orderId': order_id,
                    'limit': 100
                },
                signed=True
            )

            if exec_result and 'list' in exec_result:
                executions = exec_result['list']

                if executions:
                    total_qty = 0
                    weighted_sum = 0

                    for e in executions:
                        # Safe conversion with empty string check
                        exec_qty = e.get('execQty', 0)
                        exec_price = e.get('execPrice', 0)

                        try:
                            exec_qty = float(exec_qty) if exec_qty and exec_qty != '' else 0
                            exec_price = float(exec_price) if exec_price and exec_price != '' else 0
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid execution data: qty={exec_qty}, price={exec_price}")
                            continue

                        if exec_qty > 0 and exec_price > 0:
                            total_qty += exec_qty
                            weighted_sum += exec_qty * exec_price

                    if total_qty > 0:
                        avg_price = weighted_sum / total_qty
                        execution_details = {
                            'quantity': total_qty,
                            'price': avg_price
                        }
                        logger.info(f"Execution confirmed: {total_qty} @ {avg_price}")

        except Exception as e:
            logger.debug(f"Execution check error: {e}")

        # Check position
        position_exists = False
        position_data = None
        try:
            positions = await self.get_open_positions()

            for pos in positions:
                if pos['symbol'] == symbol:
                    position_exists = True
                    position_data = {
                        'quantity': pos['quantity'],
                        'price': pos['entry_price']
                    }
                    logger.info(f"Position confirmed for {symbol}: {pos['quantity']} @ {pos['entry_price']}")

                    if not execution_details:
                        execution_details = position_data
                    break

        except Exception as e:
            logger.error(f"Position check error: {e}")

        # Process results
        if order_status:
            # Safe conversion of all values
            executed_qty_raw = order_status.get('cumExecQty', 0)
            avg_price_raw = order_status.get('avgPrice', 0)
            cum_exec_value_raw = order_status.get('cumExecValue', 0)

            try:
                executed_qty = float(executed_qty_raw) if executed_qty_raw and executed_qty_raw != '' else 0
            except (ValueError, TypeError):
                executed_qty = 0
                logger.warning(f"Invalid cumExecQty: {executed_qty_raw}")

            try:
                avg_price = float(avg_price_raw) if avg_price_raw and avg_price_raw != '' else 0
            except (ValueError, TypeError):
                avg_price = 0
                logger.warning(f"Invalid avgPrice: {avg_price_raw}")

            try:
                cum_exec_value = float(cum_exec_value_raw) if cum_exec_value_raw and cum_exec_value_raw != '' else 0
            except (ValueError, TypeError):
                cum_exec_value = 0
                logger.warning(f"Invalid cumExecValue: {cum_exec_value_raw}")

            order_status_str = order_status.get('orderStatus', '')

            # Fix avgPrice if zero
            if avg_price == 0 and executed_qty > 0 and cum_exec_value > 0:
                avg_price = cum_exec_value / executed_qty

            # Use execution details if available
            if execution_details:
                executed_qty = execution_details['quantity']
                avg_price = execution_details['price']

            if order_status_str in ['Filled', 'PartiallyFilled'] and executed_qty > 0 and avg_price > 0:
                logger.info(f"✅ Order {order_id} {order_status_str.lower()}: {executed_qty} @ {avg_price}")

                return {
                    'orderId': order_id,
                    'symbol': symbol,
                    'side': side,
                    'quantity': executed_qty,
                    'price': avg_price,
                    'status': 'FILLED' if order_status_str == 'Filled' else 'PARTIALLY_FILLED'
                }

            elif order_status_str == 'Cancelled':
                logger.warning(f"Order {order_id} was cancelled - likely no liquidity")
                if position_exists and position_data:
                    logger.info(f"But position exists, using position data")
                    return {
                        'orderId': order_id,
                        'symbol': symbol,
                        'side': side,
                        'quantity': position_data['quantity'],
                        'price': position_data['price'],
                        'status': 'FILLED'
                    }
                return None

            elif order_status_str == 'Rejected':
                logger.error(f"Order {order_id} was rejected: {order_status.get('rejectReason')}")
                return None

        # If position exists but no order status
        if position_exists and position_data:
            logger.warning(f"Order status unknown but position exists")
            return {
                'orderId': order_id,
                'symbol': symbol,
                'side': side,
                'quantity': position_data['quantity'],
                'price': position_data['price'],
                'status': 'FILLED'
            }

        # Final check after delay
        logger.info(f"Final check for {symbol}...")
        await asyncio.sleep(2.0)

        try:
            positions = await self.get_open_positions()
            for pos in positions:
                if pos['symbol'] == symbol:
                    logger.info(f"Position found on final check: {pos['quantity']} @ {pos['entry_price']}")
                    return {
                        'orderId': order_id,
                        'symbol': symbol,
                        'side': side,
                        'quantity': pos['quantity'],
                        'price': pos['entry_price'],
                        'status': 'FILLED'
                    }
        except Exception as e:
            logger.error(f"Final position check error: {e}")

        # Last resort fallback
        ticker = await self.get_ticker(symbol)
        if ticker and ticker.get('price'):
            try:
                price = float(ticker['price']) if ticker['price'] and ticker['price'] != '' else 0
            except (ValueError, TypeError):
                price = 0

            if price > 0:
                logger.warning(f"Using current price as fallback: ${price}")
                return {
                    'orderId': order_id,
                    'symbol': symbol,
                    'side': side,
                    'quantity': float(formatted_qty),
                    'price': price,
                    'status': 'UNKNOWN'
                }

        logger.error(f"Order {order_id} verification failed completely")
        return None

    async def get_open_positions(self) -> List[Dict]:
        """Get all open positions"""
        positions = await self._make_request(
            "GET",
            "/v5/position/list",
            {'category': 'linear', 'settleCoin': 'USDT'},
            signed=True
        )

        if not positions or 'list' not in positions:
            return []

        open_positions = []
        for pos in positions['list']:
            size = float(pos.get('size', 0))
            if size > 0:
                open_positions.append({
                    'symbol': pos['symbol'],
                    'side': pos['side'].upper(),
                    'quantity': size,
                    'entry_price': float(pos.get('avgPrice', 0)),
                    'mark_price': float(pos.get('markPrice', 0)),
                    'pnl': float(pos.get('unrealisedPnl', 0)),
                    'pnl_percent': (float(pos.get('unrealisedPnl', 0)) / float(pos.get('positionValue', 1)) * 100)
                    if float(pos.get('positionValue', 0)) > 0 else 0,
                    'created_time': int(pos.get('createdTime', 0))
                })

        return open_positions

    async def close_position(self, symbol: str) -> bool:
        """Close position"""
        positions = await self.get_open_positions()

        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'Sell' if pos['side'] == 'BUY' else 'Buy'

                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'side': side,
                    'orderType': 'Market',
                    'qty': self.format_quantity(symbol, pos['quantity']),
                    'positionIdx': 0,
                    'reduceOnly': True,
                    'timeInForce': 'GTC'  # Изменено с IOC
                }

                result = await self._make_request("POST", "/v5/order/create", params, signed=True)

                if result and 'orderId' in result:
                    logger.info(f"Position close order created for {symbol}: {result['orderId']}")
                    return True
                else:
                    logger.error(f"Failed to close position for {symbol}")
                    return False

        logger.warning(f"No open position found for {symbol}")
        return False

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        """Set stop loss"""
        positions = await self.get_open_positions()

        for pos in positions:
            if pos['symbol'] == symbol:
                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'stopLoss': self.format_price(symbol, stop_price),
                    'slTriggerBy': 'MarkPrice',
                    'positionIdx': 0
                }

                result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)

                if result:
                    logger.info(f"Stop loss set for {symbol} at {stop_price}")
                    return True
                else:
                    logger.error(f"Failed to set stop loss for {symbol}")
                    return False

        return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Set take profit"""
        positions = await self.get_open_positions()

        for pos in positions:
            if pos['symbol'] == symbol:
                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'takeProfit': self.format_price(symbol, take_profit_price),
                    'tpTriggerBy': 'MarkPrice',
                    'positionIdx': 0
                }

                result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)

                if result:
                    logger.info(f"Take profit set for {symbol} at {take_profit_price}")
                    return True
                else:
                    logger.error(f"Failed to set take profit for {symbol}")
                    return False

        return False

    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """Set trailing stop"""
        positions = await self.get_open_positions()

        for pos in positions:
            if pos['symbol'] == symbol:
                trailing_stop_distance = pos['entry_price'] * (callback_rate / 100)

                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'trailingStop': self.format_price(symbol, trailing_stop_distance),
                    'activePrice': self.format_price(symbol, activation_price),
                    'positionIdx': 0
                }

                result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)

                if result:
                    logger.info(f"Trailing stop set for {symbol}")
                    return True
                else:
                    logger.error(f"Failed to set trailing stop for {symbol}")
                    return False

        return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders"""
        params = {'category': 'linear'}
        if symbol:
            params['symbol'] = symbol

        result = await self._make_request("GET", "/v5/order/realtime", params, signed=True)

        if result and 'list' in result:
            return result['list']

        return []

    async def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all orders"""
        params = {
            'category': 'linear',
            'symbol': symbol
        }

        result = await self._make_request("POST", "/v5/order/cancel-all", params, signed=True)

        if result:
            logger.info(f"All orders cancelled for {symbol}")
            return True
        else:
            logger.error(f"Failed to cancel orders for {symbol}")
            return False

    async def close(self):
        """Close session"""
        if self.session:
            await self.session.close()