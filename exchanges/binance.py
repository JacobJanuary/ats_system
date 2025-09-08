"""
ATS 2.0 - Binance Exchange Implementation
Binance Futures implementation of the exchange interface
"""
import asyncio
import logging
import time
import hmac
import hashlib
import aiohttp
from decimal import Decimal
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

from binance import AsyncClient
from binance.enums import *
from binance.exceptions import BinanceAPIException

from exchanges.base import (
    ExchangeBase, SymbolInfo, MarketData, AccountBalance, ExchangePosition
)

logger = logging.getLogger(__name__)


class BinanceExchange(ExchangeBase):
    """Binance Futures exchange implementation"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.name = "binance"
        self.client: Optional[AsyncClient] = None
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')

        # Testnet URLs
        self.testnet_base = "https://testnet.binancefuture.com"
        self.testnet_ws = "wss://stream.binancefuture.com"
        
        # Set base URL based on testnet flag
        if config.get('testnet'):
            self.base_url = self.testnet_base
        else:
            self.base_url = "https://fapi.binance.com"

        # Rate limiting
        self._request_times = []
        self._max_requests_per_minute = config.get('max_requests_per_minute', 1200)

    async def initialize(self) -> None:
        """Initialize Binance client"""
        try:
            if self.testnet:
                # Testnet configuration
                self.client = await AsyncClient.create(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    testnet=True
                )
                logger.info("Connected to Binance Futures Testnet")
            else:
                # Production
                self.client = await AsyncClient.create(
                    api_key=self.api_key,
                    api_secret=self.api_secret
                )
                logger.info("Connected to Binance Futures Production")

            # Load exchange info
            await self._load_exchange_info()
            self._initialized = True

        except Exception as e:
            logger.error(f"Failed to initialize Binance client: {e}")
            raise

    async def close(self) -> None:
        """Close Binance client"""
        if self.client:
            await self.client.close_connection()
            logger.info("Binance connection closed")

    async def _load_exchange_info(self) -> None:
        """Load and cache symbol information"""
        try:
            info = await self.client.futures_exchange_info()

            for symbol_data in info['symbols']:
                if symbol_data['status'] != 'TRADING':
                    continue

                # Extract filters
                min_qty = Decimal('0')
                max_qty = Decimal('99999999')
                step_size = Decimal('0.001')
                min_notional = Decimal('5')
                price_precision = 2
                quantity_precision = 3

                for filter in symbol_data['filters']:
                    if filter['filterType'] == 'LOT_SIZE':
                        min_qty = Decimal(filter['minQty'])
                        max_qty = Decimal(filter['maxQty'])
                        step_size = Decimal(filter['stepSize'])
                    elif filter['filterType'] == 'MIN_NOTIONAL':
                        min_notional = Decimal(filter.get('notional', filter.get('minNotional', '5')))
                    elif filter['filterType'] == 'PRICE_FILTER':
                        # Calculate precision from tick size
                        tick_size = filter['tickSize']
                        if '.' in tick_size:
                            price_precision = len(tick_size.rstrip('0').split('.')[-1])
                        else:
                            price_precision = 0

                # Calculate quantity precision
                step_str = str(step_size)
                if '.' in step_str:
                    quantity_precision = len(step_str.rstrip('0').split('.')[-1])
                else:
                    quantity_precision = 0

                symbol_info = SymbolInfo(
                    symbol=symbol_data['symbol'],
                    base_asset=symbol_data['baseAsset'],
                    quote_asset=symbol_data['quoteAsset'],
                    status=symbol_data['status'],
                    min_quantity=min_qty,
                    max_quantity=max_qty,
                    step_size=step_size,
                    min_notional=min_notional,
                    price_precision=price_precision,
                    quantity_precision=quantity_precision,
                    max_leverage=125,  # Default, will be updated per symbol
                    is_trading=symbol_data['status'] == 'TRADING'
                )

                self._symbol_cache[symbol_data['symbol']] = symbol_info

            logger.info(f"Loaded {len(self._symbol_cache)} tradeable symbols")

        except Exception as e:
            logger.error(f"Failed to load exchange info: {e}")

    async def _rate_limit_check(self) -> None:
        """Check and enforce rate limits"""
        now = datetime.now().timestamp()
        # Remove requests older than 1 minute
        self._request_times = [t for t in self._request_times if now - t < 60]

        if len(self._request_times) >= self._max_requests_per_minute:
            # Wait until oldest request is more than 1 minute old
            sleep_time = 60 - (now - self._request_times[0]) + 0.1
            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)

        self._request_times.append(now)

    # Market Data Methods

    async def get_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        """Get symbol trading information"""
        return self._symbol_cache.get(symbol)

    async def get_market_data(self, symbol: str) -> Optional[MarketData]:
        """Get current market data for symbol"""
        try:
            await self._rate_limit_check()

            # Get ticker data
            ticker = await self.client.futures_ticker(symbol=symbol)

            # Get order book for best bid/ask
            depth = await self.client.futures_order_book(symbol=symbol, limit=5)

            best_bid = Decimal(depth['bids'][0][0]) if depth['bids'] else Decimal('0')
            best_bid_qty = Decimal(depth['bids'][0][1]) if depth['bids'] else Decimal('0')
            best_ask = Decimal(depth['asks'][0][0]) if depth['asks'] else Decimal('0')
            best_ask_qty = Decimal(depth['asks'][0][1]) if depth['asks'] else Decimal('0')

            market = MarketData(
                symbol=symbol,
                bid_price=best_bid,
                bid_quantity=best_bid_qty,
                ask_price=best_ask,
                ask_quantity=best_ask_qty,
                last_price=Decimal(ticker['lastPrice']),
                volume_24h=Decimal(ticker['volume']),
                quote_volume_24h=Decimal(ticker['quoteVolume']),
                open_price_24h=Decimal(ticker['openPrice']),
                high_price_24h=Decimal(ticker['highPrice']),
                low_price_24h=Decimal(ticker['lowPrice']),
                price_change_24h=Decimal(ticker['priceChange']),
                price_change_percent_24h=Decimal(ticker['priceChangePercent']),
                timestamp=datetime.fromtimestamp(ticker['closeTime'] / 1000)
            )

            return market

        except BinanceAPIException as e:
            logger.error(f"Binance API error getting market data for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting market data for {symbol}: {e}")
            return None

    async def get_orderbook(self, symbol: str, limit: int = 10) -> Dict[str, Any]:
        """Get order book for symbol"""
        try:
            await self._rate_limit_check()
            depth = await self.client.futures_order_book(symbol=symbol, limit=limit)
            return depth
        except Exception as e:
            logger.error(f"Error getting orderbook for {symbol}: {e}")
            return {}

    # Account Methods

    async def get_account_balance(self) -> Dict[str, AccountBalance]:
        """Get account balances"""
        try:
            await self._rate_limit_check()
            account = await self.client.futures_account()

            balances = {}
            for asset_data in account['assets']:
                balance = AccountBalance(
                    asset=asset_data['asset'],
                    free=Decimal(asset_data['availableBalance']),
                    locked=Decimal(asset_data['initialMargin'])
                )
                if balance.total > 0:
                    balances[asset_data['asset']] = balance

            return balances

        except Exception as e:
            logger.error(f"Error getting account balance: {e}")
            return {}

    async def get_position(self, symbol: str) -> Optional[ExchangePosition]:
        """Get position for symbol"""
        try:
            await self._rate_limit_check()
            positions = await self.client.futures_position_information(symbol=symbol)

            for pos_data in positions:
                qty = Decimal(pos_data['positionAmt'])
                if qty == 0:
                    continue

                position = ExchangePosition(
                    symbol=symbol,
                    side="LONG" if qty > 0 else "SHORT",
                    position_amount=qty,  # Add raw position amount
                    quantity=abs(qty),
                    entry_price=Decimal(pos_data['entryPrice']),
                    mark_price=Decimal(pos_data['markPrice']),
                    unrealized_pnl=Decimal(pos_data['unRealizedProfit']),
                    realized_pnl=Decimal('0'),  # Not provided directly
                    margin_type=pos_data.get('marginType', 'cross'),
                    leverage=int(pos_data.get('leverage', 1)),
                    liquidation_price=Decimal(pos_data['liquidationPrice']) if pos_data.get('liquidationPrice') else None
                )
                return position

            return None

        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}")
            return None

    async def get_all_positions(self) -> List[ExchangePosition]:
        """Get all open positions"""
        try:
            await self._rate_limit_check()
            positions_data = await self.client.futures_position_information()

            positions = []
            for pos_data in positions_data:
                qty = Decimal(pos_data['positionAmt'])
                if qty == 0:
                    continue

                position = ExchangePosition(
                    symbol=pos_data['symbol'],
                    side="LONG" if qty > 0 else "SHORT",
                    position_amount=qty,  # Add raw position amount
                    quantity=abs(qty),
                    entry_price=Decimal(pos_data['entryPrice']),
                    mark_price=Decimal(pos_data['markPrice']),
                    unrealized_pnl=Decimal(pos_data['unRealizedProfit']),
                    realized_pnl=Decimal('0'),
                    margin_type=pos_data.get('marginType', 'cross'),
                    leverage=int(pos_data.get('leverage', 1)),
                    liquidation_price=Decimal(pos_data['liquidationPrice']) if pos_data['liquidationPrice'] else None
                )
                positions.append(position)

            return positions

        except Exception as e:
            logger.error(f"Error getting all positions: {e}")
            return []

    # Trading Methods

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol"""
        try:
            await self._rate_limit_check()
            result = await self.client.futures_change_leverage(
                symbol=symbol,
                leverage=leverage
            )
            return True
        except BinanceAPIException as e:
            if e.code == -4028:  # Leverage not changed
                return True
            logger.error(f"Error setting leverage for {symbol}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error setting leverage for {symbol}: {e}")
            return False

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

            # Get symbol info to round quantity properly
            symbol_info = self._symbol_cache.get(symbol)
            if symbol_info:
                quantity = symbol_info.round_quantity(quantity)

            params = {
                'symbol': symbol,
                'side': side,  # BUY or SELL
                'type': 'MARKET',
                'quantity': str(quantity)
            }

            if reduce_only:
                params['reduceOnly'] = 'true'

            order = await self.client.futures_create_order(**params)

            # For testnet, sometimes executedQty is 0, use the requested quantity
            if self.testnet and order.get('executedQty') == '0':
                # Get current price for testnet simulation
                ticker = await self.client.futures_ticker(symbol=symbol)
                order['executedQty'] = str(quantity)
                order['avgPrice'] = ticker['lastPrice']
                logger.warning(f"Testnet order simulation for {symbol}: qty={quantity}, price={ticker['lastPrice']}")

            return order

        except BinanceAPIException as e:
            logger.error(f"Binance API error placing market order: {e}")
            # Special handling for PERCENT_PRICE filter
            if e.code == -4131:
                return {'status': 'REJECTED', 'msg': 'Price filter error - market too volatile'}
            return {'status': 'REJECTED', 'msg': str(e)}
        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force: str = "GTC"
    ) -> Dict[str, Any]:
        """Place limit order"""
        try:
            await self._rate_limit_check()

            params = {
                'symbol': symbol,
                'side': side,
                'type': 'LIMIT',
                'quantity': str(quantity),
                'price': str(price),
                'timeInForce': time_in_force
            }

            if reduce_only:
                params['reduceOnly'] = 'true'

            order = await self.client.futures_create_order(**params)
            return order

        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
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

            params = {
                'symbol': symbol,
                'side': side,
                'type': 'STOP_MARKET',
                'stopPrice': str(stop_price),
                'closePosition': 'true' if close_position else 'false'
            }

            if not close_position:
                params['quantity'] = str(quantity)

            if reduce_only and not close_position:
                params['reduceOnly'] = 'true'

            order = await self.client.futures_create_order(**params)
            return order

        except Exception as e:
            logger.error(f"Error placing stop market order: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def place_take_profit_market_order(
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

            params = {
                'symbol': symbol,
                'side': side,
                'type': 'TAKE_PROFIT_MARKET',
                'stopPrice': str(stop_price),
                'closePosition': 'true' if close_position else 'false'
            }

            if not close_position:
                params['quantity'] = str(quantity)

            if reduce_only and not close_position:
                params['reduceOnly'] = 'true'

            order = await self.client.futures_create_order(**params)
            return order

        except Exception as e:
            logger.error(f"Error placing take profit order: {e}")
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

            # Quantity обязателен для Binance Futures API
            if not quantity:
                logger.error(f"Quantity is required for trailing stop order")
                return {'status': 'REJECTED', 'msg': 'Quantity is required'}

            # Округляем quantity и activation_price
            symbol_info = await self.get_symbol_info(symbol)
            if symbol_info:
                quantity = symbol_info.round_quantity(quantity)
                if activation_price:
                    activation_price = symbol_info.round_price(activation_price)

            # Формируем параметры для API
            # Важно: quantity должен быть float, activationPrice - строка
            order_params = {
                'symbol': symbol,
                'side': side,
                'type': 'TRAILING_STOP_MARKET',
                'quantity': float(quantity),  # Обязательный параметр!
                'callbackRate': float(callback_rate),
                'reduceOnly': True
            }

            # Добавляем activationPrice если указан
            if activation_price:
                order_params['activationPrice'] = str(activation_price)
                logger.info(f"Creating trailing stop with activation at {activation_price}")

            # Создаем ордер
            order = await self.client.futures_create_order(**order_params)

            logger.info(f"Trailing stop order placed successfully: orderId={order.get('orderId')}, "
                        f"symbol={symbol}, quantity={quantity}, callbackRate={callback_rate}%, "
                        f"activationPrice={activation_price if activation_price else 'immediate'}")
            return order

        except Exception as e:
            logger.error(f"Error placing trailing stop order: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel order"""
        try:
            await self._rate_limit_check()
            result = await self.client.futures_cancel_order(
                symbol=symbol,
                orderId=order_id
            )
            return result.get('status') == 'CANCELED'
        except Exception as e:
            logger.error(f"Error canceling order {order_id}: {e}")
            return False

    async def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all orders for symbol"""
        try:
            await self._rate_limit_check()
            result = await self.client.futures_cancel_all_open_orders(symbol=symbol)
            return True
        except Exception as e:
            logger.error(f"Error canceling all orders for {symbol}: {e}")
            return False

    async def get_order_status(self, symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order status"""
        try:
            await self._rate_limit_check()
            order = await self.client.futures_get_order(
                symbol=symbol,
                orderId=order_id
            )
            return order
        except Exception as e:
            logger.error(f"Error getting order status: {e}")
            return None

    async def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Get open orders"""
        try:
            await self._rate_limit_check()
            if symbol:
                orders = await self.client.futures_get_open_orders(symbol=symbol)
            else:
                orders = await self.client.futures_get_open_orders()
            return orders
        except Exception as e:
            logger.error(f"Error getting open orders: {e}")
            return []
    
    async def set_stop_loss(
        self,
        position: 'Position',
        stop_loss_percent: float,
        is_trailing: bool = False,
        callback_rate: Optional[float] = None
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Set stop loss for position - supports both fixed and trailing
        Returns: (success, order_id, error_message)
        """
        try:
            # Validate position data
            if not position.entry_price or position.entry_price <= 0:
                return False, None, f"Invalid entry price: {position.entry_price}"
            
            if not position.quantity or position.quantity <= 0:
                return False, None, f"Invalid quantity: {position.quantity}"
            
            # Calculate stop price for fixed stop loss
            if position.side == "LONG":
                stop_price = position.entry_price * (Decimal('1') - Decimal(str(stop_loss_percent)) / Decimal('100'))
                order_side = "SELL"
            else:  # SHORT
                stop_price = position.entry_price * (Decimal('1') + Decimal(str(stop_loss_percent)) / Decimal('100'))
                order_side = "BUY"
            
            stop_price = stop_price.quantize(Decimal('0.01'))
            
            # Place appropriate order type
            if is_trailing and callback_rate:
                # Use trailing stop for Binance
                logger.info(f"Setting trailing stop for {position.symbol}: callback={callback_rate}%")
                
                # Calculate activation price (3.5% profit by default)
                from core.config import config as system_config
                activation_percent = system_config.risk.trailing_activation_percent
                
                if position.side == "LONG":
                    # For LONG: activate when price rises X%
                    activation_price = position.entry_price * Decimal(1 + activation_percent / 100)
                else:
                    # For SHORT: activate when price falls X%
                    activation_price = position.entry_price * Decimal(1 - activation_percent / 100)
                
                order_result = await self.place_trailing_stop_order(
                    symbol=position.symbol,
                    side=order_side,
                    callback_rate=Decimal(str(callback_rate)),
                    activation_price=activation_price,
                    quantity=position.quantity
                )
            else:
                # Use fixed stop loss
                logger.info(f"Setting fixed stop loss for {position.symbol} at {stop_price}")
                
                order_result = await self.place_stop_market_order(
                    symbol=position.symbol,
                    side=order_side,
                    quantity=position.quantity,
                    stop_price=stop_price,
                    reduce_only=True
                )
            
            # Check result
            if order_result.get('status') == 'REJECTED':
                error_msg = order_result.get('msg', 'Order rejected')
                logger.error(f"Failed to set stop loss: {error_msg}")
                return False, None, error_msg
            
            order_id = str(order_result.get('orderId', ''))
            logger.info(f"✅ Stop loss set successfully: order_id={order_id}")
            
            return True, order_id, None
            
        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            return False, None, str(e)
    
    async def get_positions(self) -> List['ExchangePosition']:
        """Get all open positions from Binance Futures"""
        try:
            endpoint = "/fapi/v2/positionRisk"
            
            params = {
                'timestamp': int(time.time() * 1000)
            }
            
            # Generate signature
            query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            params['signature'] = signature
            
            headers = {'X-MBX-APIKEY': self.api_key}
            url = f"{self.base_url}{endpoint}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        positions = []
                        for pos in data:
                            position_amt = float(pos.get('positionAmt', 0))
                            if position_amt != 0:
                                from exchanges.base import ExchangePosition
                                
                                positions.append(ExchangePosition(
                                    symbol=pos.get('symbol'),
                                    side="LONG" if position_amt > 0 else "SHORT",
                                    quantity=Decimal(str(abs(position_amt))),
                                    entry_price=Decimal(pos.get('entryPrice', '0')),
                                    mark_price=Decimal(pos.get('markPrice', '0')),
                                    unrealized_pnl=Decimal(pos.get('unRealizedProfit', '0')),
                                    realized_pnl=Decimal('0'),
                                    margin_type=pos.get('marginType', 'cross'),
                                    leverage=int(pos.get('leverage', '1')),
                                    liquidation_price=Decimal(pos.get('liquidationPrice', '0')) if pos.get('liquidationPrice') else None
                                ))
                        
                        logger.debug(f"Found {len(positions)} open positions on Binance")
                        return positions
                    else:
                        logger.error(f"Failed to get Binance positions: {response.status}")
                        return []
                        
        except Exception as e:
            logger.error(f"Error getting Binance positions: {e}")
            return []