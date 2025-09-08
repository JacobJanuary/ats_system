"""
ATS 2.0 - Exchange Base Class
Abstract base class for all exchange implementations
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import logging

from database.models import Position, Order, OrderType, OrderPurpose, OrderStatus
from core.retry import retry, CircuitBreaker, RetryConfig, retry_with_backoff
from core.security import safe_log

logger = logging.getLogger(__name__)


@dataclass
class SymbolInfo:
    """Exchange symbol information"""
    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    min_quantity: Decimal
    max_quantity: Decimal
    step_size: Decimal  # Quantity precision
    min_notional: Decimal  # Minimum order value
    price_precision: int
    quantity_precision: int
    max_leverage: int
    is_trading: bool

    def round_quantity(self, quantity: Decimal) -> Decimal:
        """Round quantity to correct precision"""
        return Decimal(str(quantity)).quantize(self.step_size)

    def round_price(self, price: Decimal) -> Decimal:
        """Round price to correct precision"""
        precision = Decimal(10) ** -self.price_precision
        return Decimal(str(price)).quantize(precision)


@dataclass
class MarketData:
    """Current market data for symbol"""
    symbol: str
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    last_price: Decimal
    volume_24h: Decimal
    quote_volume_24h: Decimal
    open_price_24h: Decimal
    high_price_24h: Decimal
    low_price_24h: Decimal
    price_change_24h: Decimal
    price_change_percent_24h: Decimal
    timestamp: datetime

    @property
    def spread(self) -> Decimal:
        """Calculate bid-ask spread"""
        return self.ask_price - self.bid_price

    @property
    def spread_percent(self) -> Decimal:
        """Calculate spread as percentage"""
        if self.bid_price == 0:
            return Decimal(100)
        return (self.spread / self.bid_price) * 100

    @property
    def mid_price(self) -> Decimal:
        """Calculate mid price"""
        return (self.bid_price + self.ask_price) / 2


@dataclass
class AccountBalance:
    """Account balance information"""
    asset: str
    free: Decimal
    locked: Decimal

    @property
    def total(self) -> Decimal:
        return self.free + self.locked


@dataclass
class ExchangePosition:
    """Position information from exchange"""
    symbol: str
    side: str  # LONG or SHORT
    quantity: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    margin_type: str
    leverage: int
    liquidation_price: Optional[Decimal]
    position_amount: Decimal = Decimal('0')  # Raw position amount (positive for long, negative for short)

    @property
    def unrealized_pnl_percent(self) -> Decimal:
        """Calculate unrealized PNL percentage"""
        if self.entry_price == 0:
            return Decimal(0)

        if self.side == "LONG":
            return ((self.mark_price - self.entry_price) / self.entry_price) * 100
        else:  # SHORT
            return ((self.entry_price - self.mark_price) / self.entry_price) * 100


class ExchangeBase(ABC):
    """Abstract base class for exchange implementations"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = "base"
        self.testnet = config.get('testnet', True)
        self._symbol_cache: Dict[str, SymbolInfo] = {}
        self._initialized = False

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize exchange connection"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close exchange connection"""
        pass

    # Market Data Methods

    @abstractmethod
    async def get_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        """Get symbol trading information"""
        pass

    @abstractmethod
    async def get_market_data(self, symbol: str) -> Optional[MarketData]:
        """Get current market data for symbol"""
        pass

    @abstractmethod
    async def get_orderbook(self, symbol: str, limit: int = 10) -> Dict[str, Any]:
        """Get order book for symbol"""
        pass

    async def check_symbol_tradeable(self, symbol: str) -> Tuple[bool, str]:
        """Check if symbol is tradeable with reason if not"""
        try:
            info = await self.get_symbol_info(symbol)
            if not info:
                return False, "Symbol not found"

            if not info.is_trading:
                return False, "Symbol not trading"

            market = await self.get_market_data(symbol)
            if not market:
                return False, "No market data available"

            # Check spread
            if market.spread_percent > self.config.get('max_spread_percent', 2.0):
                return False, f"Spread too high: {market.spread_percent:.2f}%"

            # Check volume - lower threshold for testnet
            if self.testnet:
                min_volume = self.config.get('min_volume_24h_usd', 10_000)  # $10k for testnet
            else:
                min_volume = self.config.get('min_volume_24h_usd', 1_000_000)  # $1M for production

            if market.quote_volume_24h < min_volume:
                return False, f"Low volume: ${market.quote_volume_24h:,.0f}"

            return True, "OK"

        except Exception as e:
            logger.error(f"Error checking symbol {symbol}: {e}")
            return False, str(e)

    # Account Methods

    @abstractmethod
    async def get_account_balance(self) -> Dict[str, AccountBalance]:
        """Get account balances"""
        pass

    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[ExchangePosition]:
        """Get position for symbol"""
        pass

    @abstractmethod
    async def get_all_positions(self) -> List[ExchangePosition]:
        """Get all open positions"""
        pass

    # Trading Methods

    @abstractmethod
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for symbol"""
        pass

    @abstractmethod
    async def place_market_order(
        self,
        symbol: str,
        side: str,  # BUY or SELL
        quantity: Decimal,
        reduce_only: bool = False
    ) -> Dict[str, Any]:
        """Place market order"""
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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

            # ВАЖНО: Binance Futures использует параметры в camelCase
            params = {
                'symbol': symbol,
                'side': side,
                'type': 'TRAILING_STOP_MARKET',
                'callbackRate': float(callback_rate),  # Преобразуем в float
                'reduceOnly': True  # Добавляем reduceOnly для futures
            }

            # Activation price нужно округлить и передать как строку
            if activation_price:
                symbol_info = await self.get_symbol_info(symbol)
                if symbol_info:
                    activation_price = symbol_info.round_price(activation_price)
                params['activationPrice'] = str(activation_price)  # activationPrice с большой P

            if quantity:
                symbol_info = await self.get_symbol_info(symbol)
                if symbol_info:
                    quantity = symbol_info.round_quantity(quantity)
                params['quantity'] = float(quantity)  # Преобразуем в float
            else:
                # Close entire position - для futures не используем closePosition
                pass  # quantity обязателен для trailing stop

            # Для futures используем новый метод API
            order = await self.client.futures_create_order(**params)
            logger.info(f"Trailing stop order placed: {order}")
            return order

        except Exception as e:
            logger.error(f"Error placing trailing stop order: {e}")
            return {'status': 'REJECTED', 'msg': str(e)}

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel order"""
        pass

    @abstractmethod
    async def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all orders for symbol"""
        pass

    @abstractmethod
    async def get_order_status(self, symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order status"""
        pass

    @abstractmethod
    async def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Get open orders"""
        pass

    # Helper Methods

    async def open_position(
        self,
        position: Position,
        size_usd: float
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Open a new position
        Returns: (success, order_result, error_message)
        """
        try:
            symbol = position.symbol
            side = position.side  # LONG or SHORT

            # Get symbol info
            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info:
                return False, None, f"Symbol {symbol} not found"

            # Get current price
            market = await self.get_market_data(symbol)
            if not market:
                return False, None, "Failed to get market data"

            # Calculate quantity
            entry_price = market.ask_price if side == "LONG" else market.bid_price

            # Position size in contracts (considering leverage)
            # size_usd is the margin we want to use
            # With leverage, the actual position size = margin * leverage
            position_value_usd = Decimal(str(size_usd * position.leverage))
            raw_quantity = position_value_usd / entry_price
            quantity = symbol_info.round_quantity(raw_quantity)

            # Check minimum quantity
            if quantity < symbol_info.min_quantity:
                quantity = symbol_info.min_quantity

            # Check minimum notional value
            notional = quantity * entry_price
            if notional < symbol_info.min_notional:
                # Adjust quantity to meet minimum
                min_quantity = (symbol_info.min_notional / entry_price) * Decimal('1.05')  # 5% buffer
                quantity = symbol_info.round_quantity(min_quantity)
                notional = quantity * entry_price

                if notional < symbol_info.min_notional:
                    return False, None, f"Cannot meet minimum order value ${symbol_info.min_notional}"

            # Set leverage
            leverage_set = await self.set_leverage(symbol, position.leverage)
            if not leverage_set:
                return False, None, "Failed to set leverage"

            # Determine order side (BUY for LONG, SELL for SHORT)
            order_side = "BUY" if side == "LONG" else "SELL"

            # Place market order
            order_result = await self.place_market_order(
                symbol=symbol,
                side=order_side,
                quantity=quantity
            )

            if not order_result or order_result.get('status') == 'REJECTED':
                error = order_result.get('msg', 'Order rejected') if order_result else 'Order failed'
                return False, order_result, error

            # Update position with actual values
            executed_qty = order_result.get('executedQty', '0')
            avg_price = order_result.get('avgPrice', '0')

            # Convert and check values
            if executed_qty and float(executed_qty) > 0:
                position.quantity = Decimal(str(executed_qty))
            else:
                # Fallback to requested quantity for testnet
                position.quantity = quantity
                logger.warning(f"Using requested quantity {quantity} as executed quantity was {executed_qty}")

            if avg_price and float(avg_price) > 0:
                position.entry_price = Decimal(str(avg_price))
            else:
                # Fallback to market price
                position.entry_price = entry_price
                logger.warning(f"Using market price {entry_price} as avg price was {avg_price}")

            # Validate position data
            if position.quantity <= 0 or position.entry_price <= 0:
                logger.error(f"Invalid position data: qty={position.quantity}, price={position.entry_price}")
                return False, order_result, "Invalid order execution data"

            logger.info(f"Opened {side} position for {symbol}: "
                       f"qty={position.quantity}, price={position.entry_price}")

            # ОБЯЗАТЕЛЬНАЯ АВТОМАТИЧЕСКАЯ ЗАЩИТА
            # ВСЕГДА устанавливаем и Stop Loss, и Trailing Stop
            from core.config import config as system_config
            import asyncio
            
            logger.info(f"🛡️ SETTING MANDATORY PROTECTION for {symbol} position...")
            
            # Параметры защиты из системной конфигурации
            stop_loss_percent = system_config.risk.stop_loss_percent
            trailing_callback_rate = system_config.risk.trailing_callback_rate
            
            protection_tasks = []
            protection_results = {}
            
            # 1. ВСЕГДА устанавливаем Fixed Stop Loss
            logger.info(f"Setting fixed stop loss at {stop_loss_percent}%")
            sl_success, sl_order_id, sl_error = await self.set_stop_loss(
                position=position,
                stop_loss_percent=stop_loss_percent,
                is_trailing=False
            )
            
            if sl_success:
                logger.info(f"✅ Stop loss set successfully: order_id={sl_order_id}")
                position.has_stop_loss = True
                protection_results['stop_loss'] = True
            else:
                logger.error(f"❌ Failed to set stop loss: {sl_error}")
                protection_results['stop_loss'] = False
            
            # 2. ВСЕГДА устанавливаем Trailing Stop
            logger.info(f"Setting trailing stop: callback={trailing_callback_rate}%")
            
            # Ждем немного перед установкой trailing stop
            await asyncio.sleep(1)
            
            ts_success, ts_order_id, ts_error = await self.set_stop_loss(
                position=position,
                stop_loss_percent=stop_loss_percent,
                is_trailing=True,
                callback_rate=trailing_callback_rate
            )
            
            if ts_success:
                logger.info(f"✅ Trailing stop set successfully: order_id={ts_order_id}")
                position.stop_loss_type = "BOTH"  # Both fixed and trailing
                protection_results['trailing_stop'] = True
            else:
                logger.error(f"❌ Failed to set trailing stop: {ts_error}")
                protection_results['trailing_stop'] = False
                # Keep trying in background
                logger.info("Will retry trailing stop via protection monitor...")
            
            # Log protection summary
            if all(protection_results.values()):
                logger.info(f"✅ FULL PROTECTION established for {symbol}")
            else:
                logger.warning(f"⚠️ PARTIAL PROTECTION for {symbol}: {protection_results}")
                logger.info("Protection monitor will complete the setup...")

            return True, order_result, None

        except Exception as e:
            logger.error(f"Error opening position: {e}")
            return False, None, str(e)

    async def close_position(
        self,
        position: Position,
        reason: str = "MANUAL"
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Close an existing position
        Returns: (success, order_result, error_message)
        """
        try:
            # Cancel all orders first
            await self.cancel_all_orders(position.symbol)

            # Determine close side (opposite of position side)
            close_side = "SELL" if position.side == "LONG" else "BUY"

            # Place market order to close
            order_result = await self.place_market_order(
                symbol=position.symbol,
                side=close_side,
                quantity=position.quantity,
                reduce_only=True
            )

            if not order_result or order_result.get('status') == 'REJECTED':
                error = order_result.get('msg', 'Close order rejected') if order_result else 'Close failed'
                return False, order_result, error

            logger.info(f"Closed {position.side} position for {position.symbol}: reason={reason}")
            return True, order_result, None

        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return False, None, str(e)

    async def set_stop_loss(
        self,
        position: Position,
        stop_loss_percent: float,
        is_trailing: bool = False,
        callback_rate: Optional[float] = None
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Set stop loss for position
        Returns: (success, order_id, error_message)
        """
        try:
            # Validate position data
            if not position.entry_price or position.entry_price <= 0:
                return False, None, f"Invalid entry price: {position.entry_price}"

            symbol_info = await self.get_symbol_info(position.symbol)
            if not symbol_info:
                return False, None, "Symbol info not found"

            # В методе set_stop_loss, блок if is_trailing and callback_rate:
            if is_trailing and callback_rate:
                # Рассчитываем activation_price для trailing stop
                from core.config import config as system_config
                activation_percent = system_config.risk.trailing_activation_percent

                if position.side == "LONG":
                    # Для LONG: активация когда цена поднимется на X%
                    activation_price = position.entry_price * Decimal(1 + activation_percent / 100)
                else:
                    # Для SHORT: активация когда цена упадет на X%
                    activation_price = position.entry_price * Decimal(1 - activation_percent / 100)

                # Округляем activation_price
                symbol_info = await self.get_symbol_info(position.symbol)
                if symbol_info:
                    activation_price = symbol_info.round_price(activation_price)

                logger.info(f"Setting trailing stop for {position.symbol}: "
                            f"callback_rate={callback_rate}%, "
                            f"activation_price={activation_price} "
                            f"(+{activation_percent}% from {position.entry_price})")

                # Trailing stop
                result = await self.place_trailing_stop_order(
                    symbol=position.symbol,
                    side="SELL" if position.side == "LONG" else "BUY",
                    callback_rate=Decimal(str(callback_rate)),
                    activation_price=activation_price,
                    quantity=position.quantity  # ← Убедитесь, что quantity передается!
                )

            else:
                # Fixed stop loss
                if position.side == "LONG":
                    stop_price = position.entry_price * (1 - Decimal(str(stop_loss_percent)) / 100)
                    order_side = "SELL"
                else:  # SHORT
                    stop_price = position.entry_price * (1 + Decimal(str(stop_loss_percent)) / 100)
                    order_side = "BUY"

                stop_price = symbol_info.round_price(stop_price)

                result = await self.place_stop_market_order(
                    symbol=position.symbol,
                    side=order_side,
                    quantity=position.quantity,
                    stop_price=stop_price,
                    reduce_only=True,
                    close_position=True
                )

            if result and result.get('orderId'):
                order_id = str(result['orderId'])
                logger.info(f"Set {'trailing' if is_trailing else 'fixed'} stop loss for {position.symbol}: "
                           f"order_id={order_id}")
                return True, order_id, None

            return False, None, result.get('msg', 'Failed to set stop loss') if result else 'Unknown error'

        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            return False, None, str(e)

    async def set_take_profit(
        self,
        position: Position,
        take_profit_percent: float,
        partial_targets: Optional[Dict[str, Dict[str, float]]] = None
    ) -> Tuple[bool, List[str], Optional[str]]:
        """
        Set take profit for position
        Returns: (success, order_ids, error_message)
        """
        try:
            # Validate position data
            if not position.entry_price or position.entry_price <= 0:
                return False, [], f"Invalid entry price: {position.entry_price}"

            symbol_info = await self.get_symbol_info(position.symbol)
            if not symbol_info:
                return False, [], "Symbol info not found"

            order_ids = []

            if partial_targets:
                # Multiple take profit levels
                remaining_qty = position.quantity

                for level_name, level_config in partial_targets.items():
                    tp_percent = level_config['percent']
                    tp_size_percent = level_config['size']

                    # Calculate TP price
                    if position.side == "LONG":
                        tp_price = position.entry_price * (1 + Decimal(str(tp_percent)) / 100)
                        order_side = "SELL"
                    else:  # SHORT
                        tp_price = position.entry_price * (1 - Decimal(str(tp_percent)) / 100)
                        order_side = "BUY"

                    tp_price = symbol_info.round_price(tp_price)

                    # Calculate quantity for this level
                    level_qty = position.quantity * Decimal(str(tp_size_percent)) / 100
                    level_qty = symbol_info.round_quantity(level_qty)
                    level_qty = min(level_qty, remaining_qty)

                    if level_qty > 0:
                        result = await self.place_take_profit_market_order(
                            symbol=position.symbol,
                            side=order_side,
                            quantity=level_qty,
                            stop_price=tp_price,
                            reduce_only=True
                        )

                        if result and result.get('orderId'):
                            order_ids.append(str(result['orderId']))
                            remaining_qty -= level_qty
                            logger.info(f"Set TP {level_name} for {position.symbol}: "
                                       f"price={tp_price}, qty={level_qty}")
            else:
                # Single take profit
                if position.side == "LONG":
                    tp_price = position.entry_price * (1 + Decimal(str(take_profit_percent)) / 100)
                    order_side = "SELL"
                else:  # SHORT
                    tp_price = position.entry_price * (1 - Decimal(str(take_profit_percent)) / 100)
                    order_side = "BUY"

                tp_price = symbol_info.round_price(tp_price)

                result = await self.place_take_profit_market_order(
                    symbol=position.symbol,
                    side=order_side,
                    quantity=position.quantity,
                    stop_price=tp_price,
                    reduce_only=True,
                    close_position=True
                )

                if result and result.get('orderId'):
                    order_ids.append(str(result['orderId']))
                    logger.info(f"Set take profit for {position.symbol}: price={tp_price}")

            if order_ids:
                return True, order_ids, None

            return False, [], "Failed to set take profit orders"

        except Exception as e:
            logger.error(f"Error setting take profit: {e}")
            return False, [], str(e)