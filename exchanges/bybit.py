#!/usr/bin/env python3
"""
Bybit Exchange Implementation - CLEAN PRODUCTION VERSION
Created from scratch using BinanceExchange as template
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

try:
    from ..api_error_handler import get_error_handler
except ImportError:
    from api_error_handler import get_error_handler

from .base import BaseExchange

logger = logging.getLogger(__name__)


class BybitExchange(BaseExchange):
    """Bybit futures exchange implementation - clean version"""

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
        # Initialize error handler
        self.error_handler = get_error_handler("Bybit")

    async def initialize(self):
        """Initialize Bybit connection and load market info"""
        self.session = aiohttp.ClientSession()

        server_time = await self._make_request("GET", "/v5/market/time")
        if not server_time:
            raise Exception("Failed to connect to Bybit")

        await self._detect_position_mode()
        # Advanced error handling
        self.error_handler = get_error_handler("Bybit")
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
            result = await self._make_request("GET", "/v5/market/instruments-info")
            if result and 'result' in result and 'list' in result['result']:
                for symbol_info in result['result']['list']:
                    if symbol_info.get('status') == 'Trading':
                        symbol = symbol_info['symbol']
                        self.symbol_info[symbol] = symbol_info
            logger.info(f"Loaded {len(self.symbol_info)} trading instruments")
        except Exception as e:
            logger.error(f"Error loading instruments: {e}")

    def _generate_signature(self, timestamp: str, params: str) -> str:
        """Generate HMAC SHA256 signature for Bybit API"""
        param_str = f"{timestamp}{self.api_key}{self.recv_window}{params}"
        signature = hmac.new(self.api_secret.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    async def _make_request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False):
        """Make API request to Bybit"""
        try:
            if not self.session:
                return None
            url = self.base_url + endpoint
            timestamp = str(int(time.time() * 1000))
            headers = {}

            if signed and self.api_key and self.api_secret:
                headers = {
                    'X-BAPI-API-KEY': self.api_key,
                    'X-BAPI-TIMESTAMP': timestamp,
                    'X-BAPI-RECV-WINDOW': str(self.recv_window)
                }

                if method == "GET":
                    if params:
                        query_string = urlencode(sorted(params.items()))
                        url = f"{url}?{query_string}"
                        signature = self._generate_signature(timestamp, query_string)
                    else:
                        # Even without params, we need to generate signature for authenticated endpoints
                        signature = self._generate_signature(timestamp, "")
                    headers['X-BAPI-SIGN'] = signature
                    async with self.session.get(url, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)
                elif method == "POST":
                    body_string = json.dumps(params) if params else ""
                    signature = self._generate_signature(timestamp, body_string)
                    headers['X-BAPI-SIGN'] = signature
                    headers['Content-Type'] = 'application/json'
                    async with self.session.post(url, json=params, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)
                return None
            else:
                if method == "GET":
                    if params:
                        query_string = urlencode(sorted(params.items()))
                        url = f"{url}?{query_string}"
                    async with self.session.get(url, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)
                elif method == "POST":
                    async with self.session.post(url, json=params, headers=headers, timeout=10) as response:
                        return await self._handle_response(response)
                return None
        except Exception as e:
            logger.error(f"Error making Bybit request: {e}")
            return None

    async def _handle_response(self, response):
        """Handle API response"""
        try:
            data = await response.json()
            if response.status == 200:
                ret_code = data.get('retCode')
                if ret_code == 0:
                    return data.get('result', data)

                # Use advanced error handler
                error_type = self.error_handler.classify_error(ret_code, data.get('retMsg', 'Unknown error'), response.status)
                logger.error(f"Bybit API error {ret_code}: {data.get('retMsg', 'Unknown error')} (type: {error_type.value})")

                # Provide specific suggestions
                suggestion = self.error_handler.suggest_fix(error_type, data.get('retMsg', 'Unknown error'))
                if suggestion:
                    logger.info(f"💡 Suggestion: {suggestion}")

                return None
            else:
                logger.error(f"Bybit HTTP error {response.status}: {await response.text()}")
                return None
        except Exception as e:
            logger.error(f"Error parsing Bybit response: {e}")
            return None

    async def _handle_response_with_error_handler(self, response):
        """Handle API response with advanced error classification"""
        try:
            data = await response.json()
            if response.status == 200:
                ret_code = data.get('retCode')
                if ret_code == 0:
                    return data.get('result', data)

                # Use advanced error handler
                error_type = self.error_handler.classify_error(ret_code, data.get('retMsg', 'Unknown error'), response.status)
                logger.error(f"Bybit API error {ret_code}: {data.get('retMsg', 'Unknown error')} (type: {error_type.value})")

                # Provide specific suggestions - ensure variable is always defined
                suggestion = self.error_handler.suggest_fix(error_type, data.get('retMsg', 'Unknown error'))
                if suggestion:
                    logger.info(f"💡 Suggestion: {suggestion}")

                return None
            else:
                logger.error(f"Bybit HTTP error {response.status}: {await response.text()}")
                return None
        except Exception as e:
            logger.error(f"Error parsing Bybit response: {e}")
            return None

    def format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol precision"""
        if symbol not in self.symbol_info:
            logger.warning(f"No symbol info for {symbol}, using default formatting")
            return ".8f".format(quantity)
        precision = self.symbol_info[symbol].get('lotSize', {}).get('stepSize', "0.00000001")
        decimals = len(precision.split(".")[1].rstrip("0")) if "." in precision else 0
        return f".{decimals}f".format(quantity)

    def format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol precision"""
        if symbol not in self.symbol_info:
            logger.warning(f"No symbol info for {symbol}, using default formatting")
            return ".8f".format(price)
        precision = self.symbol_info[symbol].get('priceFilter', {}).get('tickSize', "0.00000001")
        decimals = len(precision.split(".")[1].rstrip("0")) if "." in precision else 0
        return f".{decimals}f".format(price)

    async def get_balance(self) -> float:
        """Get account balance"""
        try:
            result = await self._make_request("GET", "/v5/account/wallet-balance", signed=True)
            if result and 'result' in result and 'list' in result['result']:
                for balance_info in result['result']['list']:
                    if balance_info.get('accountType') == 'UNIFIED':
                        return float(balance_info.get('totalWalletBalance', 0))
            return 0.0
        except Exception as e:
            logger.error(f"Error getting Bybit balance: {e}")
            return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        """Get ticker information"""
        try:
            result = await self._make_request("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol})
            if result and 'result' in result and 'list' in result['result']:
                ticker = result['result']['list'][0]
                return {
                    'symbol': ticker.get('symbol'),
                    'price': float(ticker.get('lastPrice', 0)),
                    'bid': float(ticker.get('bid1Price', 0)),
                    'ask': float(ticker.get('ask1Price', 0)),
                    'volume': float(ticker.get('volume24h', 0))
                }
        except Exception as e:
            logger.error(f"Error getting Bybit open positions: {e}")
            return []

    async def set_stop_loss(self, symbol: str, stop_price: float, position_side: str = "long") -> bool:
        """Set stop loss for position"""
        try:
            # First check if position exists
            positions = await self.get_open_positions(symbol)
            position_found = False
            for pos in positions:
                if pos['symbol'] == symbol and pos['quantity'] > 0:
                    position_found = True
                    break

            if not position_found:
                logger.warning(f"No open position found for {symbol}, cannot set stop loss")
                return False

            # Format stop price according to symbol precision
            formatted_stop_price = self.format_price(symbol, stop_price)

            # Set stop loss
            stop_params = {
                "category": "linear",
                "symbol": symbol,
                "stopLoss": formatted_stop_price,
                "tpslMode": "Full"
            }

            result = await self._make_request("POST", "/v5/position/trading-stop", stop_params, signed=True)

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Bybit stop loss set for {symbol} at {stop_price}")
                return True
            else:
                if result and 'retMsg' in result:
                    logger.error(f"Bybit API message: {result['retMsg']}")
                if result and 'retCode' in result:
                    logger.error(f"Bybit API code: {result['retCode']}")
                return False
        except Exception as e:
            logger.error(f"Error setting Bybit stop loss for {symbol}: {e}")
            return False

    async def _check_order_status(self, order_id: str, symbol: str) -> Optional[Dict]:
        """Check order status by orderId"""
        try:
            logger.debug(f"Checking order status for {order_id} on {symbol}")

            # Bybit API v5 doesn't support orderId filter in history/realtime endpoints
            # We need to get all recent orders and find our order by orderId

            # Try to get recent orders (both open and closed) from realtime endpoint
            result = await self._make_request("GET", "/v5/order/realtime", {
                "category": "linear",
                "symbol": symbol,
                "openOnly": 0,  # Get both open and recent closed orders
                "limit": "50"   # Maximum allowed
            }, signed=True)

            if result and 'result' in result and 'list' in result['result']:
                orders = result['result']['list']
                logger.debug(f"Got {len(orders)} orders from realtime for {symbol}")

                # Search for our order by orderId or orderLinkId
                for order in orders:
                    if order.get('orderId') == order_id or order.get('orderLinkId') == order_id:
                        status = order.get('orderStatus')
                        logger.debug(f"✅ Found order {order_id} in realtime: status={status}, executedQty={order.get('cumExecQty', '0')}")
                        return {
                            'status': status,
                            'avgPrice': order.get('avgPrice', '0'),
                            'executedQty': order.get('cumExecQty', '0')
                        }

            # If not found in realtime, try history endpoint
            result = await self._make_request("GET", "/v5/order/history", {
                "category": "linear",
                "symbol": symbol,
                "limit": "50"  # Maximum allowed
            }, signed=True)

            if result and 'result' in result and 'list' in result['result']:
                orders = result['result']['list']
                logger.debug(f"Got {len(orders)} orders from history for {symbol}")

                # Search for our order by orderId or orderLinkId
                for order in orders:
                    if order.get('orderId') == order_id or order.get('orderLinkId') == order_id:
                        status = order.get('orderStatus')
                        logger.debug(f"✅ Found order {order_id} in history: status={status}, executedQty={order.get('cumExecQty', '0')}")
                        return {
                            'status': status,
                            'avgPrice': order.get('avgPrice', '0'),
                            'executedQty': order.get('cumExecQty', '0')
                        }

            # Try without symbol filter (get all symbols)
            result = await self._make_request("GET", "/v5/order/realtime", {
                "category": "linear",
                "openOnly": 0,
                "limit": "50"
            }, signed=True)

            if result and 'result' in result and 'list' in result['result']:
                orders = result['result']['list']
                logger.debug(f"Got {len(orders)} orders from realtime (all symbols)")

                # Search for our order by orderId or orderLinkId
                for order in orders:
                    if (order.get('orderId') == order_id or order.get('orderLinkId') == order_id) and order.get('symbol') == symbol:
                        status = order.get('orderStatus')
                        logger.debug(f"✅ Found order {order_id} in realtime (all symbols): status={status}, executedQty={order.get('cumExecQty', '0')}")
                        return {
                            'status': status,
                            'avgPrice': order.get('avgPrice', '0'),
                            'executedQty': order.get('cumExecQty', '0')
                        }

            # Last attempt - get all history without symbol filter
            result = await self._make_request("GET", "/v5/order/history", {
                "category": "linear",
                "limit": "50"
            }, signed=True)

            if result and 'result' in result and 'list' in result['result']:
                orders = result['result']['list']
                logger.debug(f"Got {len(orders)} orders from history (all symbols)")

                # Search for our order by orderId or orderLinkId
                for order in orders:
                    if (order.get('orderId') == order_id or order.get('orderLinkId') == order_id) and order.get('symbol') == symbol:
                        status = order.get('orderStatus')
                        logger.debug(f"✅ Found order {order_id} in history (all symbols): status={status}, executedQty={order.get('cumExecQty', '0')}")
                        return {
                            'status': status,
                            'avgPrice': order.get('avgPrice', '0'),
                            'executedQty': order.get('cumExecQty', '0')
                        }

            logger.warning(f"❌ Order {order_id} for {symbol} not found in any endpoint after extensive search")
            return None

        except Exception as e:
            logger.error(f"Error checking Bybit order status for {order_id}: {e}")
            return None

    async def close_position(self, symbol: str) -> bool:
        """Close position"""
        try:
            positions = await self.get_open_positions()
            for pos in positions:
                if pos['symbol'] == symbol:
                    side = "Sell" if pos['side'] == 'long' else "Buy"
                    qty = abs(pos['quantity'])

                    result = await self._make_request("POST", "/v5/order/create", {
                        "category": "linear",
                        "symbol": symbol,
                        "side": side,
                        "orderType": "Market",
                        "qty": str(qty),
                        "timeInForce": "GTC",
                        "reduceOnly": True
                    }, signed=True)
                    return result is not None
            return False
        except Exception as e:
            logger.error(f"Error closing Bybit position for {symbol}: {e}")
            return False

    async def cleanup(self):
        """Cleanup resources"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def get_balance(self) -> float:
        """Get account balance"""
        try:
            # Bybit v5 requires accountType parameter
            result = await self._make_request("GET", "/v5/account/wallet-balance", 
                                             {"accountType": "UNIFIED"}, signed=True)
            if result and 'list' in result:
                for balance_info in result['list']:
                    if balance_info.get('accountType') == 'UNIFIED':
                        # totalWalletBalance is at the same level as accountType
                        return float(balance_info.get('totalWalletBalance', 0))
            return 0.0
        except Exception as e:
            logger.error(f"Error getting Bybit balance: {e}")
            return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        """Get ticker information"""
        try:
            result = await self._make_request("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol})
            if result and 'list' in result:
                if len(result['list']) > 0:
                    ticker = result['list'][0]
                    # Handle empty price values
                    last_price = ticker.get('lastPrice', '0')
                    if not last_price or last_price == '':
                        return {}
                    
                    return {
                        'symbol': ticker.get('symbol'),
                        'price': float(last_price) if last_price else 0,
                        'bid': float(ticker.get('bid1Price', 0) or 0),
                        'ask': float(ticker.get('ask1Price', 0) or 0),
                        'volume': float(ticker.get('volume24h', 0) or 0)
                    }
            return {}
        except Exception as e:
            logger.error(f"Error getting Bybit ticker for {symbol}: {e}")
            return {}

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol"""
        try:
            result = await self._make_request("POST", "/v5/position/set-leverage", {
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": str(leverage),
                "sellLeverage": str(leverage)
            }, signed=True)
            return result is not None
        except Exception as e:
            logger.error(f"Error setting Bybit leverage for {symbol}: {e}")
            return False

    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """Create market order - try simple approach first, fallback to marketUnit if needed"""
        try:
            # Fix Bybit API side parameter - Bybit expects "Buy"/"Sell" not "BUY"/"SELL"
            bybit_side = "Buy" if side.upper() == "BUY" else "Sell"

            # Try simple market order first (works for most symbols)
            formatted_qty = self.format_quantity(symbol, quantity)

            order_params = {
                "category": "linear",
                "symbol": symbol,
                "side": bybit_side,
                "orderType": "Market",
                "qty": formatted_qty,
                "timeInForce": "GTC"
            }

            result = await self._make_request("POST", "/v5/order/create", order_params, signed=True)
            
            # Bybit API returns orderId directly when successful, not in 'result' field
            if result and 'orderId' in result and result['orderId']:
                # Success! Order was created
                order_id = result['orderId']
                logger.info(f"✅ Bybit order created successfully: {order_id} for {symbol}")
                
                # For BUY orders, wait longer before checking status (market orders may take time to fill)
                if side.upper() == "BUY":
                    await asyncio.sleep(2.0)  # Longer wait for BUY orders
                else:
                    await asyncio.sleep(0.5)  # Shorter wait for SELL orders
                
                order_status = await self._check_order_status(order_id, symbol)
                
                if order_status:
                    status = order_status.get('status')
                    executed_qty = float(order_status.get('executedQty', '0'))
                    avg_price = float(order_status.get('avgPrice', '0'))
                    
                    if status == 'Filled' and executed_qty > 0:
                        logger.info(f"✅ Bybit order executed successfully: {order_id}")
                        return {
                            'orderId': order_id,
                            'symbol': symbol,
                            'side': side,
                            'quantity': quantity,
                            'price': avg_price,
                            'status': 'FILLED'
                        }
                    elif status in ['New', 'PartiallyFilled']:
                        logger.warning(f"⚠️ Bybit order created but not fully filled: {order_id}, status: {status}")
                        return {
                            'orderId': order_id,
                            'symbol': symbol,
                            'side': side,
                            'quantity': quantity,
                            'price': 0,
                            'status': 'PENDING'
                        }
                    else:
                        logger.error(f"❌ Bybit order failed: {order_id}, status: {status}")
                        return None
                else:
                    logger.warning(f"⚠️ Bybit order created but status unknown: {order_id}")
                    return {
                        'orderId': order_id,
                        'symbol': symbol,
                        'side': side,
                        'quantity': quantity,
                        'price': 0,
                        'status': 'UNKNOWN'
                    }
            elif result and 'result' in result:
                # Alternative success format
                order_info = result['result']
                return {
                    'orderId': order_info.get('orderId'),
                    'symbol': symbol,
                    'side': side,
                    'quantity': quantity,
                    'price': float(order_info.get('avgPrice', 0)),
                    'status': 'FILLED' if order_info.get('orderStatus') == 'Filled' else 'NEW'
                }
            else:
                # Add detailed logging for debugging
                logger.error(f"Bybit market order failed for {symbol}: {result}")
                if result and 'retMsg' in result:
                    logger.error(f"Bybit API message: {result['retMsg']}")
                if result and 'retCode' in result:
                    logger.error(f"Bybit API code: {result['retCode']}")
                return None
        except Exception as e:
            logger.error(f"Error creating Bybit market order: {e}")
            return None

    async def get_open_positions(self, symbol: str = None) -> List[Dict]:
        """Get open positions"""
        try:
            # Bybit v5 requires category and either symbol or settleCoin
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
            else:
                params["settleCoin"] = "USDT"

            result = await self._make_request("GET", "/v5/position/list", params, signed=True)
            positions = []
            if result and 'result' in result and 'list' in result['result']:
                for pos in result['result']['list']:
                    if float(pos.get('size', 0)) > 0:
                        positions.append({
                            'symbol': pos.get('symbol'),
                            'quantity': float(pos.get('size', 0)),
                            'entry_price': float(pos.get('avgPrice', 0)),
                            'pnl': float(pos.get('unrealisedPnl', 0)),
                            'side': 'long' if pos.get('side') == 'Buy' else 'short'
                        })
            return positions
        except Exception as e:
            logger.error(f"Error getting Bybit open positions: {e}")
            return []

    async def close_position(self, symbol: str) -> bool:
        """Close position"""
        try:
            positions = await self.get_open_positions()
            for pos in positions:
                if pos['symbol'] == symbol:
                    side = "Sell" if pos['side'] == 'long' else "Buy"
                    qty = abs(pos['quantity'])
                    
                    result = await self._make_request("POST", "/v5/order/create", {
                        "category": "linear",
                        "symbol": symbol,
                        "side": side,
                        "orderType": "Market",
                        "qty": str(qty),
                        "timeInForce": "GTC",
                        "reduceOnly": True
                    }, signed=True)
                    return result is not None
            return False
        except Exception as e:
            logger.error(f"Error closing Bybit position for {symbol}: {e}")
            return False

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        """Set stop loss"""
        try:
            # First check if position exists
            positions = await self.get_open_positions()
            position_exists = any(pos['symbol'] == symbol for pos in positions)

            if not position_exists:
                logger.warning(f"No open position found for {symbol}, cannot set stop loss")
                return False

            # Format stop price properly
            formatted_price = self.format_price(symbol, stop_price)

            result = await self._make_request("POST", "/v5/position/trading-stop", {
                "category": "linear",
                "symbol": symbol,
                "stopLoss": formatted_price,
                "positionIdx": 0
            }, signed=True)

            if result and 'retCode' in result and result['retCode'] == 0:
                logger.info(f"✅ Bybit stop loss set successfully for {symbol} at {formatted_price}")
                return True
            else:
                logger.error(f"Failed to set Bybit stop loss for {symbol}: {result}")
                return False

        except Exception as e:
            logger.error(f"Error setting Bybit stop loss for {symbol}: {e}")
            return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Set take profit"""
        try:
            result = await self._make_request("POST", "/v5/position/trading-stop", {
                "category": "linear",
                "symbol": symbol,
                "takeProfit": str(take_profit_price),
                "positionIdx": 0
            }, signed=True)
            return result is not None
        except Exception as e:
            logger.error(f"Error setting Bybit take profit for {symbol}: {e}")
            return False

    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """Set trailing stop"""
        try:
            result = await self._make_request("POST", "/v5/position/trading-stop", {
                "category": "linear",
                "symbol": symbol,
                "trailingStop": str(callback_rate),
                "positionIdx": 0
            }, signed=True)
            return result is not None
        except Exception as e:
            logger.error(f"Error setting Bybit trailing stop for {symbol}: {e}")
            return False

    async def cancel_position_trading_stops(self, symbol: str) -> bool:
        """Cancel position-level trading stops (TL/SL/TP) by sending '0' values"""
        try:
            result = await self._make_request("POST", "/v5/position/trading-stop", {
                "category": "linear",
                "symbol": symbol,
                "takeProfit": "0",
                "stopLoss": "0",
                "trailingStop": "0",
                "positionIdx": 0
            }, signed=True)
            if result:
                logger.info(f"✅ Cancelled trading stops for {symbol}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error cancelling Bybit trading stops for {symbol}: {e}")
            return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders"""
        try:
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
            
            result = await self._make_request("GET", "/v5/order/realtime", params, signed=True)
            orders = []
            if result and 'result' in result and 'list' in result['result']:
                for order in result['result']['list']:
                    orders.append({
                        'orderId': order.get('orderId'),
                        'symbol': order.get('symbol'),
                        'side': order.get('side').lower(),
                        'quantity': float(order.get('qty', 0)),
                        'price': float(order.get('price', 0)),
                        'status': order.get('orderStatus'),
                        'type': order.get('orderType').lower()
                    })
            return orders
        except Exception as e:
            logger.error(f"Error getting Bybit open orders: {e}")
            return []

    async def cancel_all_open_orders(self, symbol: str) -> bool:
        """Cancel all open orders for symbol"""
        try:
            result = await self._make_request("POST", "/v5/order/cancel-all", {
                "category": "linear",
                "symbol": symbol
            }, signed=True)
            return result is not None
        except Exception as e:
            logger.error(f"Error cancelling Bybit orders for {symbol}: {e}")
            return False

    async def create_limit_order(self, symbol: str, side: str, quantity: float, price: float, reduce_only: bool = False) -> Optional[Dict]:
        """Create limit order"""
        try:
            order_params = {
                "category": "linear",
                "symbol": symbol,
                "side": side.upper(),
                "orderType": "Limit",
                "qty": str(quantity),
                "price": str(price),
                "timeInForce": "GTC"
            }
            
            if reduce_only:
                order_params["reduceOnly"] = True
            
            result = await self._make_request("POST", "/v5/order/create", order_params, signed=True)
            if result and 'result' in result:
                order_info = result['result']
                return {
                    'orderId': order_info.get('orderId'),
                    'symbol': symbol,
                    'side': side,
                    'quantity': quantity,
                    'price': price,
                    'status': 'NEW'
                }
            return None
        except Exception as e:
            logger.error(f"Error creating Bybit limit order: {e}")
            return None

    async def close(self):
        """Close connections"""
        if self.session:
            await self.session.close()
            self.session = None
