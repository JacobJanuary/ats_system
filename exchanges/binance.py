#!/usr/bin/env python3
# exchanges/binance.py - ФИНАЛЬНАЯ ПОЛНАЯ ВЕРСИЯ

import asyncio
import logging
from typing import Dict, Optional, List
from decimal import Decimal, ROUND_DOWN
import hmac
import hashlib
import time
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
        self.last_error = None

    async def initialize(self):
        self.session = aiohttp.ClientSession()
        await self._make_request("GET", "/fapi/v1/ping")
        exchange_info = await self._make_request("GET", "/fapi/v1/exchangeInfo")
        if exchange_info:
            for symbol_info in exchange_info.get('symbols', []):
                self.exchange_info[symbol_info['symbol']] = symbol_info
        logger.info(f"Binance {'testnet' if self.testnet else 'mainnet'} initialized")

    async def close(self):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, endpoint: str, data: Dict = None, signed: bool = False):
        if data is None:
            data = {}
        headers = {'X-MBX-APIKEY': self.api_key}
        url = f"{self.base_url}{endpoint}"
        try:
            if signed:
                timestamp = int(time.time() * 1000)
                data['timestamp'] = timestamp
                query_string = urlencode(data)
                signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'),
                                     hashlib.sha256).hexdigest()
                query_string += f'&signature={signature}'
                url += f"?{query_string}"
            else:
                query_string = urlencode(data)
                if query_string:
                    url += f"?{query_string}"

            async with self.session.request(method.upper(), url, headers=headers) as response:
                response_text = await response.text()
                if response.status >= 400:
                    logger.error(f"HTTP Error: {response.status} for URL: {url}")
                    logger.error(f"Binance Response Body: {response_text}")
                    return None
                result = json.loads(response_text)
                return result
        except Exception as e:
            logger.error(f"An unexpected exception occurred in _make_request: {e}")
            return None

    def format_price(self, symbol: str, price: float) -> str:
        try:
            if symbol in self.exchange_info:
                price_filter = next(
                    (f for f in self.exchange_info[symbol]['filters'] if f['filterType'] == 'PRICE_FILTER'), None)
                if price_filter:
                    tick_size = Decimal(price_filter['tickSize'])
                    price_decimal = Decimal(str(price))
                    quantized_price = (price_decimal / tick_size).quantize(Decimal('1'),
                                                                           rounding=ROUND_DOWN) * tick_size
                    return str(quantized_price)
        except Exception as e:
            logger.error(f"Error formatting price for {symbol}: {e}")
        return str(price)

    def format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to exchange rules with minimum quantity check"""
        try:
            if symbol in self.exchange_info:
                lot_size_filter = next(
                    (f for f in self.exchange_info[symbol]['filters'] if f['filterType'] == 'LOT_SIZE'),
                    None
                )

                if lot_size_filter:
                    step_size = Decimal(lot_size_filter['stepSize'])
                    min_qty = Decimal(lot_size_filter.get('minQty', '0'))
                    max_qty = Decimal(lot_size_filter.get('maxQty', '999999999'))

                    quantity_decimal = Decimal(str(quantity))

                    # Округляем вниз до step_size
                    quantized_quantity = (quantity_decimal / step_size).quantize(
                        Decimal('1'),
                        rounding=ROUND_DOWN
                    ) * step_size

                    # ИСПРАВЛЕНИЕ: Если получилось меньше минимума
                    if quantized_quantity < min_qty:
                        # Проверяем, можем ли округлить вверх
                        quantized_up = (quantity_decimal / step_size).quantize(
                            Decimal('1'),
                            rounding=ROUND_UP if quantity_decimal > min_qty * Decimal('0.9') else ROUND_DOWN
                        ) * step_size

                        # Используем большее из: округление вверх или минимум
                        quantized_quantity = max(quantized_up, min_qty)

                        logger.debug(f"{symbol}: quantity {quantity:.8f} adjusted to minimum {quantized_quantity}")

                    # Проверка максимального количества
                    if quantized_quantity > max_qty:
                        quantized_quantity = max_qty
                        logger.warning(f"{symbol}: quantity capped at maximum {max_qty}")

                    # Удаляем trailing zeros для чистоты
                    result = str(quantized_quantity)
                    if '.' in result:
                        result = result.rstrip('0').rstrip('.')

                    return result

        except Exception as e:
            logger.error(f"Error formatting quantity for {symbol}: {e}")

        return str(quantity)

    async def get_open_positions(self):
        try:
            all_positions_data = await self._make_request("GET", "/fapi/v2/positionRisk", signed=True)
            if not all_positions_data: return []
            open_positions = []
            for pos_data in all_positions_data:
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
            logger.error(f"Error fetching Binance open positions: {e}")
            return []

    async def create_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                                 reduce_only: bool = False):
        params = {'symbol': symbol, 'side': side.upper(), 'type': 'LIMIT',
                  'quantity': self.format_quantity(symbol, quantity),
                  'price': self.format_price(symbol, price),
                  'timeInForce': 'GTC'}
        if reduce_only:
            params['reduceOnly'] = 'true'
        return await self._make_request("POST", "/fapi/v1/order", params, signed=True)

    async def close_position(self, symbol: str):
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                return await self.create_market_order(symbol, side, pos['quantity'])
        return None

    async def cancel_all_open_orders(self, symbol: str):
        try:
            params = {'symbol': symbol}
            logger.info(f"Cancelling all open orders for {symbol}...")
            result = await self._make_request("DELETE", "/fapi/v1/allOpenOrders", params, signed=True)
            if result and result.get('code') == 200:
                logger.info(f"Successfully cancelled orders for {symbol}.")
                return True
            else:
                logger.error(f"Failed to cancel orders for {symbol}: {result}")
                return False
        except Exception as e:
            logger.error(f"Exception while cancelling orders for {symbol}: {e}")
            return False

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
            price = (float(ticker.get('bidPrice', 0)) + float(ticker.get('askPrice', 0))) / 2
            return {'symbol': symbol, 'bid': float(ticker.get('bidPrice', 0)), 'ask': float(ticker.get('askPrice', 0)),
                    'price': price}
        return {}

    async def set_leverage(self, symbol: str, leverage: int):
        params = {'symbol': symbol, 'leverage': leverage}
        await self._make_request("POST", "/fapi/v1/leverage", params, signed=True)
        return True

    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        params = {'symbol': symbol, 'side': side, 'type': 'MARKET', 'quantity': self.format_quantity(symbol, quantity)}
        return await self._make_request("POST", "/fapi/v1/order", params, signed=True)

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                params = {'symbol': symbol, 'side': side, 'type': 'STOP_MARKET',
                          'stopPrice': self.format_price(symbol, stop_price), 'closePosition': 'true'}
                result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
                return result is not None
        return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                params = {'symbol': symbol, 'side': side, 'type': 'TAKE_PROFIT_MARKET',
                          'stopPrice': self.format_price(symbol, take_profit_price), 'closePosition': 'true'}
                result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
                return result is not None
        return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        params = {}
        if symbol: params['symbol'] = symbol
        orders = await self._make_request("GET", "/fapi/v1/openOrders", params, signed=True)
        return orders if orders else []

    # --- ВОССТАНОВЛЕННЫЙ И УЛУЧШЕННЫЙ МЕТОД ---
    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
                params = {
                    'symbol': symbol,
                    'side': side,
                    'type': 'TRAILING_STOP_MARKET',
                    'callbackRate': callback_rate,
                    'activationPrice': self.format_price(symbol, activation_price),
                    'quantity': self.format_quantity(symbol, pos['quantity'])
                }
                result = await self._make_request("POST", "/fapi/v1/order", params, signed=True)
                if result and 'orderId' in result:
                    logger.info(f"Successfully placed Trailing Stop for {symbol}")
                    return True
                logger.error(f"Failed to place Trailing Stop for {symbol}: {result}")
                return False
        return False