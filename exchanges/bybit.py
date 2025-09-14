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
from decimal import Decimal, ROUND_DOWN

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
        if symbol not in self.symbol_info: return str(round(quantity, 4))
        try:
            info = self.symbol_info[symbol]
            step_size = Decimal(str(info['qtyStep']))
            qty_decimal = Decimal(str(quantity))
            rounded_qty = (qty_decimal / step_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * step_size

            step_str = str(step_size).rstrip('0')
            decimals = len(step_str.split('.')[1]) if '.' in step_str else 0
            return format(rounded_qty, f'.{decimals}f')
        except Exception:
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
                    'symbol': ticker.get('symbol'), 'price': safe_float(ticker.get('lastPrice')),
                    'bid': safe_float(ticker.get('bid1Price')), 'ask': safe_float(ticker.get('ask1Price')),
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
        """Creates an order to OPEN a position (uses aggressive IOC for slippage control)."""
        try:
            ticker = await self.get_ticker(symbol)
            if not ticker or not ticker.get('ask') or not ticker.get('bid') or not float(ticker['ask']) > 0:
                logger.error(f"Cannot place order for {symbol}: invalid ticker data.")
                return None

            price_multiplier = 1.01 if side.upper() == "BUY" else 0.99
            limit_price = float(ticker['ask' if side.upper() == "BUY" else 'bid']) * price_multiplier

            formatted_qty = self.format_quantity(symbol, quantity)
            formatted_price = self.format_price(symbol, limit_price)

            result = await self._async_request(
                self.client.place_order, category="linear", symbol=symbol,
                side=side.capitalize(), orderType="Limit", qty=formatted_qty,
                price=formatted_price, timeInForce="IOC", positionIdx=0
            )

            if not (result and result.get('retCode') == 0):
                logger.error(f"IOC order placement failed: {result}")
                return None

            order_id = result.get('result', {}).get('orderId')
            await asyncio.sleep(0.75)
            order_status = await self._check_order_status(order_id, symbol)

            if order_status and order_status.get('executedQty', 0) > 0:
                logger.info(f"✅ Order {order_id} executed. Qty: {order_status['executedQty']}")
                return {
                    'orderId': order_id, 'symbol': symbol, 'status': 'FILLED',
                    'executed_qty': order_status.get('executedQty', 0.0),
                    'price': order_status.get('avgPrice', 0.0)
                }
            logger.warning(
                f"Order {order_id} was not executed. Status: {order_status.get('status') if order_status else 'UNKNOWN'}")
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
            result = await self._async_request(
                self.client.set_trading_stop, category="linear", symbol=symbol,
                stopLoss=self.format_price(symbol, stop_price), positionIdx=0
            )
            if result and result.get('retCode') == 0:
                logger.info(f"✅ Stop loss set for {symbol} at ${stop_price:.4f}")
                return True
            logger.error(f"Failed to set stop loss: {result}")
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

            result = await self._async_request(
                self.client.set_trading_stop,
                category="linear",
                symbol=symbol,
                trailingStop=formatted_distance,
                activePrice=self.format_price(symbol, activation_price),
                positionIdx=0
            )
            if result and result.get('retCode') == 0:
                logger.info(
                    f"✅ Trailing stop for {symbol} set with activation at ${activation_price:.4f} "
                    f"and distance ${formatted_distance}"
                )
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
            params = {
                "category": "linear", "symbol": symbol, "side": side.capitalize(),
                "orderType": "Limit", "qty": self.format_quantity(symbol, quantity),
                "price": self.format_price(symbol, price), "timeInForce": "GTC",
                "positionIdx": 0
            }
            if reduce_only: params["reduceOnly"] = True

            result = await self._async_request(self.client.place_order, **params)
            if result and result.get('retCode') == 0:
                return {'orderId': result['result'].get('orderId')}
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
                return True
            else:
                logger.error(f"Failed to submit close order for {symbol}. Response: {result}")
                return False

        except Exception as e:
            logger.error(f"Critical error closing position for {symbol}: {e}", exc_info=True)
            return False

    async def close(self):
        logger.info("Bybit connection wrapper closed")