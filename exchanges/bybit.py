"""
ATS 2.0 - Bybit Exchange Implementation
Full implementation for Bybit Futures trading
"""
import logging
import asyncio
from decimal import Decimal
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import os

from pybit.unified_trading import HTTP
from exchanges.base import (
    ExchangeBase, SymbolInfo, MarketData, AccountBalance, ExchangePosition
)
from core.retry import retry, CircuitBreaker, RetryConfig
from core.security import safe_log

logger = logging.getLogger(__name__)


class BybitExchange(ExchangeBase):
    """Bybit Futures exchange implementation"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "bybit"
        self.client = None
        self._rate_limit_delay = 0.1  # 100ms between requests
        self._last_request_time = 0

    async def initialize(self) -> None:
        """Initialize Bybit client"""
        try:
            # Получаем настройки из конфига
            api_key = self.config.get('api_key') or os.getenv('BYBIT_API_KEY')
            api_secret = self.config.get('api_secret') or os.getenv('BYBIT_API_SECRET')
            testnet = self.config.get('testnet', True)

            # Инициализируем клиент
            self.client = HTTP(
                testnet=testnet,
                api_key=api_key,
                api_secret=api_secret
            )

            # Проверяем подключение
            account_info = self.client.get_wallet_balance(accountType="UNIFIED")
            if account_info['retCode'] == 0:
                logger.info(f"Bybit {'testnet' if testnet else 'mainnet'} connected successfully")
                self._initialized = True
            else:
                raise Exception(f"Failed to connect: {account_info}")

        except Exception as e:
            logger.error(f"Failed to initialize Bybit: {e}")
            raise

    async def _rate_limit_check(self):
        """Rate limiting check"""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self._last_request_time

        if time_since_last < self._rate_limit_delay:
            await asyncio.sleep(self._rate_limit_delay - time_since_last)

        self._last_request_time = asyncio.get_event_loop().time()

    async def get_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        """Get symbol trading information"""
        try:
            await self._rate_limit_check()

            response = self.client.get_instruments_info(
                category="linear",
                symbol=symbol
            )

            if response['retCode'] != 0:
                return None

            info = response['result']['list'][0]

            return SymbolInfo(
                symbol=symbol,
                base_asset=info['baseCoin'],
                quote_asset=info['quoteCoin'],
                status=info['status'],
                min_quantity=Decimal(info['lotSizeFilter']['minOrderQty']),
                max_quantity=Decimal(info['lotSizeFilter']['maxOrderQty']),
                step_size=Decimal(info['lotSizeFilter']['qtyStep']),
                min_notional=Decimal(info['lotSizeFilter'].get('minNotionalValue', '5')),
                price_precision=int(info['priceScale']),
                quantity_precision=len(info['lotSizeFilter']['qtyStep'].split('.')[-1]) if '.' in info['lotSizeFilter'][
                    'qtyStep'] else 0,
                max_leverage=int(float(info['leverageFilter']['maxLeverage'])),
                is_trading=info['status'] == 'Trading'
            )

        except Exception as e:
            logger.error(f"Error getting symbol info: {e}")
            return None

    async def get_account_balance(self) -> AccountBalance:
        """Get account balance"""
        try:
            await self._rate_limit_check()

            response = self.client.get_wallet_balance(accountType="UNIFIED")

            if response['retCode'] != 0:
                raise Exception(f"Failed to get balance: {response}")

            # Находим USDT баланс
            usdt_free = 0
            usdt_locked = 0

            for coin in response['result']['list'][0]['coin']:
                if coin['coin'] == 'USDT':
                    usdt_free = float(coin['walletBalance']) - float(coin.get('locked', '0'))
                    usdt_locked = float(coin.get('locked', '0'))
                    break

            return AccountBalance(
                asset='USDT',
                free=Decimal(str(usdt_free)),
                locked=Decimal(str(usdt_locked))
            )

        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            return AccountBalance(
                asset='USDT',
                free=Decimal('0'),
                locked=Decimal('0')
            )

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        reduce_only: bool = False
    ) -> Dict[str, Any]:
        """Place market order"""
        try:
            await self._rate_limit_check()

            # Конвертируем side для Bybit
            order_side = "Buy" if side == "BUY" else "Sell"

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": order_side,
                "orderType": "Market",
                "qty": str(quantity),
                "timeInForce": "IOC",
                "reduceOnly": reduce_only,
                "closeOnTrigger": reduce_only
            }

            response = self.client.place_order(**params)

            if response['retCode'] != 0:
                raise Exception(f"Order failed: {response}")

            return {
                'orderId': response['result']['orderId'],
                'status': 'FILLED',
                'executedQty': str(quantity),
                'avgPrice': '0'  # Market orders don't have price
            }

        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def place_trailing_stop_order(
        self,
        symbol: str,
        side: str,
        callback_rate: Decimal,
        activation_price: Optional[Decimal] = None,
        quantity: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """Place trailing stop order"""
        try:
            await self._rate_limit_check()

            # Конвертируем параметры для Bybit
            trigger_side = "Buy" if side == "BUY" else "Sell"

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": trigger_side,
                "orderType": "Market",
                "qty": str(quantity),
                "triggerBy": "MarkPrice",
                "trailingStop": str(callback_rate * 100),  # Bybit uses basis points
                "reduceOnly": True,
                "closeOnTrigger": True
            }

            if activation_price:
                params["activePrice"] = str(activation_price)

            response = self.client.place_order(**params)

            if response['retCode'] != 0:
                raise Exception(f"Trailing stop failed: {response}")

            logger.info(f"Bybit trailing stop created: {response['result']['orderId']}")

            return {
                'orderId': response['result']['orderId'],
                'status': 'NEW'
            }

        except Exception as e:
            logger.error(f"Error placing trailing stop: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def place_stop_market_order(
            self,
            symbol: str,
            side: str,
            quantity: Decimal,
            stop_price: Decimal,
            reduce_only: bool = True,
            close_position: bool = False
    ) -> Dict[str, Any]:
        """Place stop market order"""
        try:
            await self._rate_limit_check()

            trigger_side = "Buy" if side == "BUY" else "Sell"

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": trigger_side,
                "orderType": "Market",
                "qty": str(quantity),
                "triggerPrice": str(stop_price),
                "triggerBy": "MarkPrice",
                "triggerDirection": "Below" if trigger_side == "Buy" else "Above",
                "reduceOnly": reduce_only,
                "closeOnTrigger": True
            }

            response = self.client.place_order(**params)

            if response['retCode'] != 0:
                raise Exception(f"Stop order failed: {response}")

            return {
                'orderId': response['result']['orderId'],
                'status': 'NEW'
            }

        except Exception as e:
            logger.error(f"Error placing stop order: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def place_take_profit_order(
            self,
            symbol: str,
            side: str,
            quantity: Decimal,
            stop_price: Decimal,
            reduce_only: bool = True,
            close_position: bool = False
    ) -> Dict[str, Any]:
        """Place take profit market order"""
        try:
            await self._rate_limit_check()

            trigger_side = "Buy" if side == "BUY" else "Sell"

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": trigger_side,
                "orderType": "Market",
                "qty": str(quantity),
                "triggerPrice": str(stop_price),
                "triggerBy": "MarkPrice",
                "triggerDirection": "Above" if trigger_side == "Sell" else "Below",
                "reduceOnly": reduce_only,
                "closeOnTrigger": True
            }

            response = self.client.place_order(**params)

            if response['retCode'] != 0:
                raise Exception(f"Take profit failed: {response}")

            return {
                'orderId': response['result']['orderId'],
                'status': 'NEW'
            }

        except Exception as e:
            logger.error(f"Error placing take profit: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def get_positions(self) -> List[ExchangePosition]:
        """Get all open positions"""
        try:
            await self._rate_limit_check()

            response = self.client.get_positions(
                category="linear",
                settleCoin="USDT"
            )

            if response['retCode'] != 0:
                return []

            positions = []
            for pos in response['result']['list']:
                if float(pos['size']) > 0:
                    positions.append(ExchangePosition(
                        symbol=pos['symbol'],
                        side="LONG" if pos['side'] == "Buy" else "SHORT",
                        quantity=Decimal(pos['size']),
                        entry_price=Decimal(pos['avgPrice']) if pos['avgPrice'] else Decimal('0'),
                        mark_price=Decimal(pos['markPrice']) if pos['markPrice'] else Decimal('0'),
                        unrealized_pnl=Decimal(pos['unrealisedPnl']) if pos['unrealisedPnl'] else Decimal('0'),
                        realized_pnl=Decimal(pos.get('realisedPnl', '0')),
                        margin_type=pos.get('marginMode', 'cross'),
                        leverage=int(pos.get('leverage', '1')),
                        liquidation_price=Decimal(pos['liqPrice']) if pos.get('liqPrice') else None
                    ))

            return positions

        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []

    async def get_market_data(self, symbol: str) -> Optional[MarketData]:
        """Get current market data"""
        try:
            await self._rate_limit_check()

            response = self.client.get_tickers(
                category="linear",
                symbol=symbol
            )

            if response['retCode'] != 0:
                return None

            ticker = response['result']['list'][0]

            # Рассчитываем изменение цены
            last_price = Decimal(ticker['lastPrice'])
            prev_price = Decimal(ticker['prevPrice24h'])
            price_change = last_price - prev_price
            price_change_percent = (price_change / prev_price * 100) if prev_price > 0 else Decimal('0')

            return MarketData(
                symbol=symbol,
                bid_price=Decimal(ticker['bid1Price']),
                bid_quantity=Decimal(ticker['bid1Size']),
                ask_price=Decimal(ticker['ask1Price']),
                ask_quantity=Decimal(ticker['ask1Size']),
                last_price=last_price,
                volume_24h=Decimal(ticker['volume24h']),
                quote_volume_24h=Decimal(ticker['turnover24h']),
                open_price_24h=prev_price,
                high_price_24h=Decimal(ticker['highPrice24h']),
                low_price_24h=Decimal(ticker['lowPrice24h']),
                price_change_24h=price_change,
                price_change_percent_24h=price_change_percent,
                timestamp=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"Error getting market data: {e}")
            return None

    async def get_orderbook(self, symbol: str, limit: int = 10) -> Dict[str, Any]:
        """Get order book"""
        try:
            await self._rate_limit_check()

            response = self.client.get_orderbook(
                category="linear",
                symbol=symbol,
                limit=limit
            )

            if response['retCode'] != 0:
                return {}

            return {
                'bids': [[float(b[0]), float(b[1])] for b in response['result']['b']],
                'asks': [[float(a[0]), float(a[1])] for a in response['result']['a']]
            }

        except Exception as e:
            logger.error(f"Error getting orderbook: {e}")
            return {}

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel order"""
        try:
            await self._rate_limit_check()

            response = self.client.cancel_order(
                category="linear",
                symbol=symbol,
                orderId=order_id
            )

            return response['retCode'] == 0

        except Exception as e:
            logger.error(f"Error canceling order: {e}")
            return False

    async def get_order_status(self, symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order status"""
        try:
            await self._rate_limit_check()

            response = self.client.get_order_history(
                category="linear",
                symbol=symbol,
                orderId=order_id
            )

            if response['retCode'] != 0 or not response['result']['list']:
                return None

            order = response['result']['list'][0]

            return {
                'orderId': order['orderId'],
                'status': order['orderStatus'],
                'side': order['side'],
                'executedQty': order['cumExecQty'],
                'avgPrice': order['avgPrice']
            }

        except Exception as e:
            logger.error(f"Error getting order status: {e}")
            return None

    async def place_take_profit_market_order(
            self,
            symbol: str,
            side: str,
            quantity: Decimal,
            stop_price: Decimal,
            reduce_only: bool = True,
            close_position: bool = False
    ) -> Dict[str, Any]:
        """Alias for place_take_profit_order"""
        return await self.place_take_profit_order(
            symbol, side, quantity, stop_price, reduce_only, close_position
        )

    async def place_limit_order(
            self,
            symbol: str,
            side: str,
            quantity: Decimal,
            price: Decimal,
            reduce_only: bool = False
    ) -> Dict[str, Any]:
        """Place limit order"""
        try:
            await self._rate_limit_check()

            order_side = "Buy" if side == "BUY" else "Sell"

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": order_side,
                "orderType": "Limit",
                "qty": str(quantity),
                "price": str(price),
                "timeInForce": "GTC",
                "reduceOnly": reduce_only
            }

            response = self.client.place_order(**params)

            if response['retCode'] != 0:
                raise Exception(f"Limit order failed: {response}")

            return {
                'orderId': response['result']['orderId'],
                'status': 'NEW'
            }

        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def get_position(self, symbol: str) -> Optional[ExchangePosition]:
        """Get position for specific symbol"""
        try:
            positions = await self.get_positions()
            for pos in positions:
                if pos.symbol == symbol:
                    return pos
            return None
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return None

    async def get_all_positions(self) -> List[ExchangePosition]:
        """Alias for get_positions"""
        return await self.get_positions()

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders"""
        try:
            await self._rate_limit_check()

            params = {
                "category": "linear",
                "settleCoin": "USDT"
            }
            if symbol:
                params["symbol"] = symbol

            response = self.client.get_open_orders(**params)

            if response['retCode'] != 0:
                return []

            orders = []
            for order in response['result']['list']:
                orders.append({
                    'orderId': order['orderId'],
                    'symbol': order['symbol'],
                    'side': order['side'],
                    'type': order['orderType'],
                    'quantity': order['qty'],
                    'price': order.get('price', '0'),
                    'status': order['orderStatus']
                })

            return orders

        except Exception as e:
            logger.error(f"Error getting open orders: {e}")
            return []

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> bool:
        """Cancel all open orders"""
        try:
            await self._rate_limit_check()

            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
            else:
                params["settleCoin"] = "USDT"

            response = self.client.cancel_all_orders(**params)

            return response['retCode'] == 0

        except Exception as e:
            logger.error(f"Error canceling all orders: {e}")
            return False

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol"""
        try:
            await self._rate_limit_check()

            response = self.client.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )

            return response['retCode'] == 0

        except Exception as e:
            logger.error(f"Error setting leverage: {e}")
            return False

    async def set_stop_loss(
        self,
        position: 'Position',
        stop_loss_percent: float,
        is_trailing: bool = False,
        callback_rate: Optional[float] = None
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Set stop loss for a position
        
        Args:
            position: Position object
            stop_loss_percent: Stop loss percentage from entry price
            is_trailing: If True, creates trailing stop loss
            callback_rate: Trailing callback rate (required if is_trailing=True)
            
        Returns:
            Tuple of (success, order_id, error_message)
        """
        try:
            # Calculate stop price (convert to Decimal for proper arithmetic)
            entry_price = Decimal(str(position.entry_price))
            sl_factor = Decimal(str(1 - stop_loss_percent / 100)) if position.side == "LONG" else Decimal(str(1 + stop_loss_percent / 100))
            stop_price = entry_price * sl_factor
            
            # Round stop price to appropriate precision
            symbol_info = await self.get_symbol_info(position.symbol)
            if symbol_info:
                stop_price = round(float(stop_price), symbol_info.price_precision)
            
            if is_trailing and callback_rate:
                # Place trailing stop
                logger.info(f"Setting trailing stop for {position.symbol}: "
                          f"callback_rate={callback_rate}%")
                
                result = await self.place_trailing_stop_order(
                    symbol=position.symbol,
                    side="SELL" if position.side == "LONG" else "BUY",
                    quantity=Decimal(str(position.quantity)),
                    callback_rate=Decimal(str(callback_rate / 100)),  # Convert to decimal
                    activation_price=None  # Use current price as activation
                )
                
                if result.get('status') == 'NEW':
                    return True, result.get('orderId'), None
                else:
                    return False, None, result.get('msg', 'Failed to set trailing stop')
                    
            else:
                # Place fixed stop loss
                logger.info(f"Setting fixed stop loss for {position.symbol}: "
                          f"stop_price={stop_price}")
                
                result = await self.place_stop_market_order(
                    symbol=position.symbol,
                    side="SELL" if position.side == "LONG" else "BUY",
                    quantity=Decimal(str(position.quantity)),
                    stop_price=Decimal(str(stop_price)),
                    reduce_only=True,
                    close_position=True
                )
                
                if result.get('status') == 'NEW':
                    return True, result.get('orderId'), None
                else:
                    return False, None, result.get('msg', 'Failed to set stop loss')
                    
        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            return False, None, str(e)

    async def close(self) -> None:
        """Close connection"""
        logger.info("Bybit connection closed")
        self._initialized = False