#!/usr/bin/env python3
"""
Bybit Exchange Implementation - V5 API PRODUCTION READY v2.0
- Упрощена и усилена логика верификации market-ордеров
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
from urllib.parse import urlencode
from .base import BaseExchange

logger = logging.getLogger(__name__)


class BybitExchange(BaseExchange):
    """
    Bybit futures exchange implementation with FIXED signature and ROBUST order verification
    """

    def __init__(self, config: Dict):
        """Initialize Bybit exchange with configuration"""
        super().__init__(config)

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

        server_time = await self._make_request("GET", "/v5/market/time")
        if not server_time:
            raise Exception("Failed to connect to Bybit")

        await self._detect_position_mode()
        await self._load_instruments_info()

        logger.info(f"Bybit {'testnet' if self.testnet else 'mainnet'} initialized")
        logger.info(f"Position mode: {self.position_mode}")
        logger.info(f"Loaded {len(self.symbol_info)} active symbols")

    async def _detect_position_mode(self):
        """Detect account position mode"""
        try:
            result = await self._make_request("GET", "/v5/account/info", signed=True)
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
                result = await self._make_request("GET", "/v5/market/instruments-info", params)
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
        param_str = f"{timestamp}{self.api_key}{self.recv_window}{params}"
        signature = hmac.new(self.api_secret.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    async def _make_request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False):
        if not self.session:
            return None
        url = self.base_url + endpoint
        timestamp = str(int(time.time() * 1000))
        headers = {}
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
        if signed and self.api_key and self.api_secret:
            headers = {'X-BAPI-API-KEY': self.api_key, 'X-BAPI-TIMESTAMP': timestamp,
                       'X-BAPI-RECV-WINDOW': str(self.recv_window)}
            try:
                if method == "GET":
                    if params:
                        sorted_params = sorted(params.items())
                        query_string = urlencode(sorted_params)
                    else:
                        query_string = ""
                    signature = self._generate_signature(timestamp, query_string)
                    headers['X-BAPI-SIGN'] = signature
                    if query_string:
                        url = f"{url}?{query_string}"
                    async with self.session.get(url, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)
                elif method == "POST":
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
                        return data
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
        if symbol not in self.symbol_info:
            logger.warning(f"No symbol info for {symbol}, using default formatting")
            return str(round(quantity, 4)) if quantity < 1 else str(int(quantity))
        info = self.symbol_info[symbol]
        qty_step = Decimal(str(info['qty_step']))
        min_qty = Decimal(str(info['min_qty']))
        max_qty = Decimal(str(info['max_qty']))
        qty_decimal = Decimal(str(quantity))
        if qty_step > 0:
            step_str = str(qty_step)
            decimal_places = len(step_str.split('.')[1].rstrip('0')) if '.' in step_str else 0
            rounded_qty = (qty_decimal / qty_step).quantize(Decimal('1'), rounding=ROUND_DOWN) * qty_step
            if rounded_qty < min_qty:
                rounded_qty = min_qty
            if rounded_qty > max_qty:
                rounded_qty = max_qty
            result = f"{{:.{decimal_places}f}}".format(float(rounded_qty)) if decimal_places > 0 else str(
                int(rounded_qty))
            if '.' in result:
                result = result.rstrip('0').rstrip('.')
            return result
        else:
            return str(quantity)

    def format_price(self, symbol: str, price: float) -> str:
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
        for account_type in ['UNIFIED', 'CONTRACT']:
            wallet = await self._make_request("GET", "/v5/account/wallet-balance", {'accountType': account_type},
                                              signed=True)
            if wallet and 'list' in wallet:
                for account in wallet['list']:
                    for coin in account.get('coin', []):
                        if coin['coin'] == 'USDT':
                            balance_str = coin.get('walletBalance', '0')
                            try:
                                return float(balance_str) if balance_str else 0.0
                            except (ValueError, TypeError):
                                return 0.0
        return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        ticker = await self._make_request("GET", "/v5/market/tickers", {'category': 'linear', 'symbol': symbol})
        if not ticker or 'list' not in ticker or not ticker['list']:
            return {}
        ticker_data = ticker['list'][0]
        try:
            bid = float(ticker_data.get('bid1Price', 0))
        except (ValueError, TypeError):
            bid = 0
        try:
            ask = float(ticker_data.get('ask1Price', 0))
        except (ValueError, TypeError):
            ask = 0
        try:
            last = float(ticker_data.get('lastPrice', 0))
        except (ValueError, TypeError):
            last = 0
        price = (bid + ask) / 2 if bid > 0 and ask > 0 else last
        if price <= 0: return {}
        if bid == 0: bid = price * 0.9995
        if ask == 0: ask = price * 1.0005
        return {'symbol': ticker_data.get('symbol'), 'bid': bid, 'ask': ask, 'price': price}

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        if symbol in self.symbol_info:
            max_leverage = int(self.symbol_info[symbol]['max_leverage'])
            min_leverage = int(self.symbol_info[symbol]['min_leverage'])
            leverage = max(min_leverage, min(leverage, max_leverage))
        params = {'category': 'linear', 'symbol': symbol, 'buyLeverage': str(leverage), 'sellLeverage': str(leverage)}
        result = await self._make_request("POST", "/v5/position/set-leverage", params, signed=True)
        return result is not None

    # <<< ИЗМЕНЕНИЕ: Полностью переписанная, упрощенная и более надежная логика верификации ордера >>>
    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """
        Creates a market order and verifies execution by checking the position state.
        This is the most reliable way to confirm execution and get the average entry price.
        """
        formatted_qty = self.format_quantity(symbol, quantity)
        position_idx = 1 if side.upper() == 'BUY' and self.position_mode == "hedge" else \
            2 if side.upper() == 'SELL' and self.position_mode == "hedge" else 0

        params = {
            'category': 'linear',
            'symbol': symbol,
            'side': 'Buy' if side.upper() == 'BUY' else 'Sell',
            'orderType': 'Market',
            'qty': formatted_qty,
            'positionIdx': position_idx
        }

        logger.info(f"Creating market order for {symbol}: {formatted_qty} @ Market")
        order_result = await self._make_request("POST", "/v5/order/create", params, signed=True)

        if not order_result or 'orderId' not in order_result:
            logger.error(f"Failed to create market order for {symbol}. Result: {order_result}")
            return None

        order_id = order_result['orderId']
        logger.info(f"Order {order_id} for {symbol} placed. Verifying execution by position state...")

        # Verify execution by checking the position after a short delay
        for attempt in range(5):  # 5 attempts over ~5 seconds
            await asyncio.sleep(1.0 + attempt * 0.5)  # Increasing delay
            try:
                positions = await self.get_open_positions()
                for pos in positions:
                    if pos['symbol'] == symbol:
                        # Position found, this is our confirmation
                        logger.info(
                            f"✅ Position for {symbol} confirmed. Qty: {pos['quantity']}, AvgPrice: {pos['entry_price']}")
                        return {
                            'orderId': order_id,
                            'symbol': symbol,
                            'side': side,
                            'quantity': pos['quantity'],
                            'price': pos['entry_price'],
                            'status': 'FILLED'
                        }
            except Exception as e:
                logger.error(f"Error during position check (attempt {attempt + 1}): {e}")

        logger.error(f"❌ Order {order_id} verification failed for {symbol}. Could not find resulting position.")
        return None

    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

    async def get_open_positions(self) -> List[Dict]:
        positions = await self._make_request("GET", "/v5/position/list", {'category': 'linear', 'settleCoin': 'USDT'},
                                             signed=True)
        if not positions or 'list' not in positions: return []
        open_positions = []
        for pos in positions['list']:
            try:
                size = float(pos.get('size', 0))
                if size > 0:
                    open_positions.append({
                        'symbol': pos['symbol'],
                        'side': pos['side'].upper(),
                        'quantity': size,
                        'entry_price': float(pos.get('avgPrice', 0)),
                        'mark_price': float(pos.get('markPrice', 0)),
                        'pnl': float(pos.get('unrealisedPnl', 0)),
                        'leverage': float(pos.get('leverage', 1)),
                        'created_time': int(pos.get('createdTime', 0))
                    })
            except (ValueError, TypeError):
                logger.warning(f"Could not parse position data for {pos.get('symbol')}")
                continue
        return open_positions

    async def close_position(self, symbol: str) -> bool:
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'Sell' if pos['side'] == 'BUY' else 'Buy'
                params = {
                    'category': 'linear', 'symbol': symbol, 'side': side,
                    'orderType': 'Market', 'qty': self.format_quantity(symbol, pos['quantity']),
                    'reduceOnly': True
                }
                result = await self._make_request("POST", "/v5/order/create", params, signed=True)
                if result and 'orderId' in result:
                    logger.info(f"Position close order created for {symbol}: {result['orderId']}")
                    return True
        return False

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        params = {
            'category': 'linear', 'symbol': symbol,
            'stopLoss': self.format_price(symbol, stop_price),
            'slTriggerBy': 'MarkPrice', 'positionIdx': 0
        }
        result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)
        if result:
            logger.info(f"Stop loss set for {symbol} at {stop_price}")
            return True
        logger.error(f"Failed to set stop loss for {symbol}")
        return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        params = {
            'category': 'linear', 'symbol': symbol,
            'takeProfit': self.format_price(symbol, take_profit_price),
            'tpTriggerBy': 'MarkPrice', 'positionIdx': 0
        }
        result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)
        if result:
            logger.info(f"Take profit set for {symbol} at {take_profit_price}")
            return True
        logger.error(f"Failed to set take profit for {symbol}")
        return False

    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        positions = await self.get_open_positions()
        pos_found = next((p for p in positions if p['symbol'] == symbol), None)

        if pos_found:
            trailing_stop_distance = pos_found['entry_price'] * (callback_rate / 100)
            params = {
                'category': 'linear', 'symbol': symbol,
                'trailingStop': self.format_price(symbol, trailing_stop_distance),
                'activePrice': self.format_price(symbol, activation_price),
                'positionIdx': 0
            }
            result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)
            if result:
                logger.info(f"Trailing stop set for {symbol}")
                return True
        logger.error(f"Failed to set trailing stop for {symbol}")
        return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        params = {'category': 'linear'}
        if symbol:
            params['symbol'] = symbol
        result = await self._make_request("GET", "/v5/order/realtime", params, signed=True)
        return result['list'] if result and 'list' in result else []

    async def cancel_all_orders(self, symbol: str) -> bool:
        params = {'category': 'linear', 'symbol': symbol}
        result = await self._make_request("POST", "/v5/order/cancel-all", params, signed=True)
        if result:
            logger.info(f"All orders cancelled for {symbol}")
            return True
        return False

    async def create_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                                 reduce_only: bool = False) -> Optional[Dict]:
        """Create limit order"""
        params = {
            'category': 'linear', 'symbol': symbol, 'side': side, 'orderType': 'Limit',
            'qty': self.format_quantity(symbol, quantity),
            'price': self.format_price(symbol, price),
            'timeInForce': 'GTC', 'reduceOnly': reduce_only
        }
        return await self._make_request("POST", "/v5/order/create", params, signed=True)

    async def close(self):
        if self.session:
            await self.session.close()