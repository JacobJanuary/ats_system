#!/usr/bin/env python3
"""
Bybit Exchange Implementation - V5 API PRODUCTION READY
Complete rewrite using official pybit library for reliability
"""

import asyncio
import logging
from typing import Dict, Optional, List, Any
from decimal import Decimal, ROUND_DOWN
import time
from datetime import datetime, timezone

# Official Bybit Python SDK
from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError, FailedRequestError

try:
    from ..api_error_handler import get_error_handler
except ImportError:
    from api_error_handler import get_error_handler

from .base import BaseExchange

logger = logging.getLogger(__name__)


def safe_float(value, default=0.0):
    """Safely convert value to float"""
    if value is None or value == '' or value == 'null' or value == 'undefined':
        return default

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in ['null', 'none', 'undefined', 'nan']:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.debug(f"Could not convert '{value}' to float, using default {default}")
            return default

    try:
        return float(value)
    except (ValueError, TypeError):
        logger.debug(f"Could not convert {type(value)} to float, using default {default}")
        return default


def safe_int(value, default=0):
    """Safely convert value to integer"""
    float_val = safe_float(value, default)
    return int(float_val)


class BybitExchange(BaseExchange):
    """Bybit futures exchange implementation using official pybit library"""

    def __init__(self, config: Dict):
        """Initialize Bybit exchange with configuration"""
        super().__init__(config)

        self.api_key = config.get('api_key', '').strip()
        self.api_secret = config.get('api_secret', '').strip()

        # Initialize pybit client
        if self.testnet:
            self.client = HTTP(
                testnet=True,
                api_key=self.api_key,
                api_secret=self.api_secret
            )
            self.base_url = "https://api-testnet.bybit.com"
        else:
            self.client = HTTP(
                testnet=False,
                api_key=self.api_key,
                api_secret=self.api_secret
            )
            self.base_url = "https://api.bybit.com"

        self.symbol_info = {}
        self.position_mode = None
        self.last_error = None
        self.error_handler = get_error_handler("Bybit")

    async def initialize(self):
        """Initialize Bybit connection and load market info"""
        try:
            # Test connection with server time
            server_time = await self._async_request(self.client.get_server_time)
            if not server_time or server_time.get('retCode') != 0:
                raise Exception("Failed to connect to Bybit")

            logger.info(f"Connected to Bybit {'testnet' if self.testnet else 'mainnet'}")

            # Get account info to detect position mode
            await self._detect_position_mode()

            # Load all active instruments
            await self._load_instruments_info()

            logger.info(f"Bybit initialized - Position mode: {self.position_mode}")
            logger.info(f"Loaded {len(self.symbol_info)} active symbols")

        except Exception as e:
            logger.error(f"Failed to initialize Bybit: {e}")
            raise

    async def _async_request(self, method, *args, **kwargs):
        """Wrapper to run synchronous pybit methods asynchronously"""
        loop = asyncio.get_event_loop()
        # Fix: run_in_executor doesn't accept keyword arguments for the function
        # We need to call the method directly with its arguments
        try:
            return await loop.run_in_executor(None, lambda: method(*args, **kwargs))
        except Exception as e:
            logger.error(f"Error in _async_request: {e}")
            raise

    async def _detect_position_mode(self):
        """Detect account position mode"""
        try:
            result = await self._async_request(self.client.get_account_info)

            if result and result.get('retCode') == 0:
                data = result.get('result', {})
                unified_margin_status = data.get('unifiedMarginStatus', 0)

                if unified_margin_status in [1, 2, 3, 4]:
                    self.position_mode = "hedge"
                else:
                    self.position_mode = "one-way"
            else:
                self.position_mode = "one-way"

        except Exception as e:
            logger.warning(f"Could not detect position mode: {e}")
            self.position_mode = "one-way"

    async def _load_instruments_info(self):
        """Load all active trading instruments"""
        try:
            self.symbol_info = {}
            cursor = ""

            while True:
                if cursor:
                    result = await self._async_request(
                        self.client.get_instruments_info,
                        category="linear",
                        limit=1000,
                        cursor=cursor
                    )
                else:
                    result = await self._async_request(
                        self.client.get_instruments_info,
                        category="linear",
                        limit=1000
                    )

                if result and result.get('retCode') == 0:
                    data = result.get('result', {})
                    instruments = data.get('list', [])

                    for instrument in instruments:
                        symbol = instrument.get('symbol', '')
                        status = instrument.get('status', '')

                        # On testnet accept various statuses, on mainnet only Trading
                        if self.testnet:
                            if symbol and status and status != 'Closed':
                                self._store_instrument_info(symbol, instrument)
                        else:
                            if status == 'Trading':
                                self._store_instrument_info(symbol, instrument)

                    cursor = data.get('nextPageCursor', '')
                    if not cursor:
                        break
                else:
                    logger.error(f"Failed to load instruments: {result}")
                    break

        except Exception as e:
            logger.error(f"Error loading instruments: {e}")

    def _store_instrument_info(self, symbol: str, instrument: Dict):
        """Store instrument information"""
        lot_size_filter = instrument.get('lotSizeFilter', {})
        price_filter = instrument.get('priceFilter', {})

        self.symbol_info[symbol] = {
            'symbol': symbol,
            'status': instrument.get('status', ''),
            'minOrderQty': float(lot_size_filter.get('minOrderQty', 0.001)),
            'maxOrderQty': float(lot_size_filter.get('maxOrderQty', 999999999)),
            'qtyStep': float(lot_size_filter.get('qtyStep', 0.001)),
            'tickSize': float(price_filter.get('tickSize', 0.0001)),
            'minPrice': float(price_filter.get('minPrice', 0)),
            'maxPrice': float(price_filter.get('maxPrice', 999999)),
            'settleCoin': instrument.get('settleCoin', 'USDT'),
            'quoteCoin': instrument.get('quoteCoin', 'USDT'),
            'baseCoin': instrument.get('baseCoin', ''),
            'contractType': instrument.get('contractType', 'LinearPerpetual')
        }

    def format_quantity(self, symbol: str, quantity: float) -> str:
        """Format quantity according to symbol rules"""
        if symbol not in self.symbol_info:
            logger.warning(f"No symbol info for {symbol}, using default")
            return str(round(quantity, 4))

        try:
            symbol_data = self.symbol_info[symbol]
            step_size = symbol_data.get('qtyStep', 0.001)
            min_qty = symbol_data.get('minOrderQty', 0.001)
            max_qty = symbol_data.get('maxOrderQty', 999999999)

            # Round to step size
            qty_decimal = Decimal(str(quantity))
            step_decimal = Decimal(str(step_size))

            # Determine decimal places
            step_str = str(step_size)
            if '.' in step_str:
                decimals = len(step_str.split('.')[1])
            else:
                decimals = 0

            # Round down to step size
            rounded_qty = (qty_decimal / step_decimal).quantize(
                Decimal('1'), rounding=ROUND_DOWN
            ) * step_decimal

            # Check min/max
            if rounded_qty < Decimal(str(min_qty)):
                rounded_qty = Decimal(str(min_qty))
            elif rounded_qty > Decimal(str(max_qty)):
                rounded_qty = Decimal(str(max_qty))

            result = format(rounded_qty, f'.{decimals}f')
            return result

        except Exception as e:
            logger.error(f"Error formatting quantity for {symbol}: {e}")
            return str(round(quantity, 4))

    def format_price(self, symbol: str, price: float) -> str:
        """Format price according to symbol precision"""
        if symbol not in self.symbol_info:
            logger.warning(f"No symbol info for {symbol}")
            return f"{price:.4f}"

        try:
            tick_size = self.symbol_info[symbol].get('tickSize', 0.0001)

            price_decimal = Decimal(str(price))
            tick_decimal = Decimal(str(tick_size))

            rounded_price = (price_decimal / tick_decimal).quantize(
                Decimal('1'), rounding=ROUND_DOWN
            ) * tick_decimal

            result = str(rounded_price)
            if '.' in result:
                result = result.rstrip('0').rstrip('.')

            return result

        except Exception as e:
            logger.error(f"Error formatting price for {symbol}: {e}")
            return f"{price:.4f}"

    async def get_balance(self) -> float:
        """Get USDT balance"""
        try:
            result = await self._async_request(
                self.client.get_wallet_balance,
                accountType="UNIFIED"
            )

            if result and result.get('retCode') == 0:
                data = result.get('result', {})
                accounts = data.get('list', [])

                for account in accounts:
                    if account.get('accountType') == 'UNIFIED':
                        coins = account.get('coin', [])
                        for coin in coins:
                            if coin.get('coin') == 'USDT':
                                available = safe_float(coin.get('availableToWithdraw', 0))
                                wallet_balance = safe_float(coin.get('walletBalance', 0))
                                return available if available > 0 else wallet_balance

            return 0.0

        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            return 0.0

    async def get_ticker(self, symbol: str) -> Dict:
        """Get ticker information"""
        try:
            result = await self._async_request(
                self.client.get_tickers,
                category="linear",
                symbol=symbol
            )

            if result and result.get('retCode') == 0:
                data = result.get('result', {})
                tickers = data.get('list', [])

                if tickers:
                    ticker = tickers[0]
                    return {
                        'symbol': ticker.get('symbol'),
                        'price': safe_float(ticker.get('lastPrice', 0)),
                        'bid': safe_float(ticker.get('bid1Price', 0)),
                        'ask': safe_float(ticker.get('ask1Price', 0)),
                        'volume': safe_float(ticker.get('volume24h', 0))
                    }

            return {}

        except Exception as e:
            logger.error(f"Error getting ticker for {symbol}: {e}")
            return {}

    # В файле bybit.py

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol, handling 'not modified' error as success."""
        try:
            result = await self._async_request(
                self.client.set_leverage,
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Leverage set to {leverage}x for {symbol}")
                return True
            else:
                # Неудачная попытка, но без исключения от pybit
                logger.error(f"Failed to set leverage: {result}")
                return False

        except InvalidRequestError as e:
            # pybit выбрасывает исключение для некоторых кодов ошибок. Ловим его.
            if "110043" in str(e):  # <--- ИСПРАВЛЕНИЕ
                logger.info(f"Leverage for {symbol} is already set to {leverage}x. Continuing.")
                return True
            else:
                logger.error(f"Error setting leverage for {symbol}: {e}")
                return self.testnet  # On testnet, continue anyway

        except Exception as e:
            logger.error(f"Unexpected error setting leverage for {symbol}: {e}")
            return self.testnet

    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """
        Creates a robust smart market order using an aggressive IOC limit order.
        Final version with increased price aggression for illiquid markets.
        """
        try:
            ticker = await self.get_ticker(symbol)
            if not ticker or not ticker.get('ask') or not ticker.get('bid') or float(ticker['ask']) == 0:
                logger.error(f"Cannot place order for {symbol}: invalid ticker data.")
                return None

            # Увеличиваем агрессивность цены до 1% для большей надежности на тестнете
            if side.upper() == "BUY":
                limit_price = float(ticker['ask']) * 1.01  # +1%
                order_side = "Buy"
            else:
                limit_price = float(ticker['bid']) * 0.99  # -1%
                order_side = "Sell"

            formatted_price = self.format_price(symbol, limit_price)
            formatted_qty = self.format_quantity(symbol, quantity)

            logger.info(f"Placing SMART market order: {order_side} {formatted_qty} {symbol} at ~${formatted_price}")

            result = await self._async_request(
                self.client.place_order,
                category="linear", symbol=symbol, side=order_side, orderType="Limit",
                qty=formatted_qty, price=formatted_price, timeInForce="IOC", positionIdx=0
            )

            if not (result and result.get('retCode') == 0):
                logger.error(f"IOC order placement failed: {result}")
                self.last_error = str(result.get('retMsg', 'Placement error'))
                return None

            order_id = result.get('result', {}).get('orderId')
            logger.info(f"IOC Order submitted. ID: {order_id}. Checking status...")

            await asyncio.sleep(0.75)
            order_status = await self._check_order_status(order_id, symbol)

            final_result = {
                'orderId': order_id, 'symbol': symbol, 'status': 'UNKNOWN',
                'executed_qty': 0.0, 'price': 0.0
            }

            if order_status:
                final_result['status'] = order_status.get('status', 'UNKNOWN')
                final_result['executed_qty'] = order_status.get('executedQty', 0.0)
                final_result['price'] = order_status.get('avgPrice', 0.0)

                if final_result['executed_qty'] > 0:
                    logger.info(f"✅ Order {order_id} confirmed executed. Qty: {final_result['executed_qty']}")
                else:
                    logger.warning(f"Order {order_id} was NOT executed. Status: {final_result['status']}")
            else:
                logger.warning(f"Could not retrieve status for order {order_id} after placing.")

            return final_result

        except Exception as e:
            logger.error(f"Critical error in create_market_order: {e}", exc_info=True)
            self.last_error = str(e)
            return None

    async def _check_order_status(self, order_id: str, symbol: str) -> Optional[Dict]:
        """Check order status"""
        try:
            # Check real-time orders
            result = await self._async_request(
                self.client.get_open_orders,
                category="linear",
                symbol=symbol,
                orderId=order_id
            )

            if result and result.get('retCode') == 0:
                orders = result.get('result', {}).get('list', [])
                if orders:
                    order = orders[0]
                    return {
                        'status': order.get('orderStatus'),
                        'avgPrice': safe_float(order.get('avgPrice', 0)),
                        'executedQty': safe_float(order.get('cumExecQty', 0))
                    }

            # Check order history if not in open orders
            result = await self._async_request(
                self.client.get_order_history,
                category="linear",
                symbol=symbol,
                orderId=order_id
            )

            if result and result.get('retCode') == 0:
                orders = result.get('result', {}).get('list', [])
                if orders:
                    order = orders[0]
                    return {
                        'status': order.get('orderStatus'),
                        'avgPrice': safe_float(order.get('avgPrice', 0)),
                        'executedQty': safe_float(order.get('cumExecQty', 0))
                    }

            return None

        except Exception as e:
            logger.error(f"Error checking order status: {e}")
            return None

    async def get_open_positions(self, symbol: str = None) -> List[Dict]:
        """Get open positions"""
        try:
            params = {
                "category": "linear",
                "settleCoin": "USDT"
            }

            if symbol:
                params["symbol"] = symbol

            result = await self._async_request(
                self.client.get_positions,
                **params
            )

            positions = []
            if result and result.get('retCode') == 0:
                data = result.get('result', {})
                position_list = data.get('list', [])

                for pos in position_list:
                    size = safe_float(pos.get('size', 0))

                    if size > 0:
                        positions.append({
                            'symbol': pos.get('symbol'),
                            'quantity': size,
                            'entry_price': safe_float(pos.get('avgPrice', 0)),
                            'pnl': safe_float(pos.get('unrealisedPnl', 0)),
                            'side': 'long' if pos.get('side') == 'Buy' else 'short',
                            'size': size,
                            'updatedTime': safe_int(pos.get('updatedTime', 0)),
                            'stopLoss': pos.get('stopLoss'),
                            'takeProfit': pos.get('takeProfit'),
                            'trailingStop': pos.get('trailingStop')
                        })

            return positions

        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []

    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        """Set stop loss for position"""
        try:
            formatted_price = self.format_price(symbol, stop_price)

            result = await self._async_request(
                self.client.set_trading_stop,
                category="linear",
                symbol=symbol,
                stopLoss=formatted_price,
                tpslMode="Full",
                slTriggerBy="LastPrice",
                positionIdx=0
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Stop loss set for {symbol} at {formatted_price}")
                return True
            elif result and result.get('retCode') == 34040:
                # Not modified - already exists
                logger.info(f"Stop loss already exists for {symbol}")
                return True
            else:
                logger.error(f"Failed to set stop loss: {result}")
                self.last_error = str(result.get('retMsg', 'Unknown error'))
                return False

        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            self.last_error = str(e)
            return False

    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Set take profit for position"""
        try:
            formatted_price = self.format_price(symbol, take_profit_price)

            result = await self._async_request(
                self.client.set_trading_stop,
                category="linear",
                symbol=symbol,
                takeProfit=formatted_price,
                tpslMode="Full",
                tpTriggerBy="LastPrice",
                positionIdx=0
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Take profit set for {symbol} at {formatted_price}")
                return True
            else:
                logger.error(f"Failed to set take profit: {result}")
                return False

        except Exception as e:
            logger.error(f"Error setting take profit: {e}")
            return False

    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """Set trailing stop for position"""
        try:
            formatted_price = self.format_price(symbol, activation_price)

            result = await self._async_request(
                self.client.set_trading_stop,
                category="linear",
                symbol=symbol,
                trailingStop=str(callback_rate),
                activePrice=formatted_price,
                tpslMode="Full",
                positionIdx=0
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Trailing stop set for {symbol}")
                return True
            else:
                logger.error(f"Failed to set trailing stop: {result}")
                return False

        except Exception as e:
            logger.error(f"Error setting trailing stop: {e}")
            return False

    async def cancel_position_trading_stops(self, symbol: str) -> bool:
        """Cancel all position-level trading stops"""
        try:
            result = await self._async_request(
                self.client.set_trading_stop,
                category="linear",
                symbol=symbol,
                takeProfit="0",
                stopLoss="0",
                trailingStop="0",
                positionIdx=0
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Cancelled trading stops for {symbol}")
                return True
            else:
                logger.warning(f"Could not cancel trading stops: {result}")
                return False

        except Exception as e:
            logger.error(f"Error cancelling trading stops: {e}")
            return False

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        """Get open orders"""
        try:
            params = {
                "category": "linear",
                "settleCoin": "USDT"
            }

            if symbol:
                params["symbol"] = symbol

            result = await self._async_request(
                self.client.get_open_orders,
                **params
            )

            orders = []
            if result and result.get('retCode') == 0:
                data = result.get('result', {})
                order_list = data.get('list', [])

                for order in order_list:
                    orders.append({
                        'orderId': order.get('orderId'),
                        'symbol': order.get('symbol'),
                        'side': order.get('side', '').lower(),
                        'quantity': safe_float(order.get('qty', 0)),
                        'price': safe_float(order.get('price', 0)),
                        'status': order.get('orderStatus'),
                        'type': order.get('orderType', '').lower(),
                        'reduceOnly': order.get('reduceOnly', False)
                    })

            return orders

        except Exception as e:
            logger.error(f"Error getting open orders: {e}")
            return []

    async def cancel_all_open_orders(self, symbol: str) -> bool:
        """Cancel all open orders for symbol"""
        try:
            result = await self._async_request(
                self.client.cancel_all_orders,
                category="linear",
                symbol=symbol,
                settleCoin="USDT"
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Cancelled all orders for {symbol}")
                return True
            else:
                logger.error(f"Failed to cancel orders: {result}")
                return False

        except Exception as e:
            logger.error(f"Error cancelling orders: {e}")
            return False

    async def create_limit_order(self, symbol: str, side: str, quantity: float, price: float,
                                 reduce_only: bool = False) -> Optional[Dict]:
        """Create limit order"""
        try:
            order_side = "Buy" if side.upper() == "BUY" else "Sell"
            formatted_qty = self.format_quantity(symbol, quantity)
            formatted_price = self.format_price(symbol, price)

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": order_side,
                "orderType": "Limit",
                "qty": formatted_qty,
                "price": formatted_price,
                "timeInForce": "GTC",
                "positionIdx": 0
            }

            if reduce_only:
                params["reduceOnly"] = True

            result = await self._async_request(
                self.client.place_order,
                **params
            )

            if result and result.get('retCode') == 0:
                order_data = result.get('result', {})
                return {
                    'orderId': order_data.get('orderId'),
                    'symbol': symbol,
                    'side': side,
                    'quantity': float(formatted_qty),
                    'price': float(formatted_price),
                    'status': 'NEW'
                }
            else:
                logger.error(f"Failed to create limit order: {result}")
                return None

        except Exception as e:
            logger.error(f"Error creating limit order: {e}")
            return None

    async def close_position(self, symbol: str) -> bool:
        """
        Closes a position robustly, checking for partial or full execution.
        Final version with case-insensitive status check.
        """
        try:
            positions = await self.get_open_positions(symbol)
            if not positions:
                logger.warning(f"No position found to close for {symbol}")
                return True  # Считаем успешным, т.к. позиции уже нет

            pos_to_close = positions[0]
            side_to_close = "Sell" if pos_to_close['side'] == 'long' else "Buy"
            qty_to_close = abs(pos_to_close['quantity'])

            logger.info(f"Attempting to close {qty_to_close} {symbol} position.")

            close_order_result = await self.create_market_order(symbol, side_to_close, qty_to_close)

            # ФИНАЛЬНЫЙ ИСПРАВЛЕННЫЙ КОД
            # Проверяем, что ордер был отправлен и исполнен хотя бы частично.
            if close_order_result and close_order_result.get('executed_qty', 0.0) > 0:
                # Проверяем, полностью ли закрыта позиция
                if abs(close_order_result['executed_qty'] - qty_to_close) < float(self.symbol_info[symbol]['qtyStep']):
                    logger.info(f"✅ Position for {symbol} fully closed.")
                    return True
                else:
                    logger.warning(f"Position for {symbol} was only partially closed. "
                                   f"Closed {close_order_result['executed_qty']} of {qty_to_close}.")
                    # В реальной системе здесь может быть логика повторного закрытия остатка
                    return False  # Возвращаем False, так как позиция закрыта не полностью
            else:
                logger.error(f"Failed to close position for {symbol}. Close order was not executed.")
                return False

        except Exception as e:
            logger.error(f"Critical error closing position for {symbol}: {e}", exc_info=True)
            return False

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel specific order"""
        try:
            result = await self._async_request(
                self.client.cancel_order,
                category="linear",
                orderId=order_id
            )

            if result and result.get('retCode') == 0:
                logger.info(f"✅ Order {order_id} cancelled")
                return True
            else:
                logger.error(f"Failed to cancel order: {result}")
                return False

        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return False

    async def close(self):
        """Close connection (cleanup)"""
        # pybit doesn't require explicit connection closing
        logger.info("Bybit connection closed")