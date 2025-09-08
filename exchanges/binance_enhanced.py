"""
Enhanced Binance Futures Manager with Full Protection
Implements automatic Stop Loss and Trailing Stop for all positions
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import os
from dotenv import load_dotenv

from exchanges.binance import BinanceExchange
from exchanges.base import ExchangePosition
from database.models import Position

load_dotenv()

logger = logging.getLogger(__name__)


class BinanceEnhancedManager(BinanceExchange):
    """
    Enhanced Binance Futures manager with mandatory protection
    Extends the base BinanceExchange with automatic SL and TS
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Load protection settings from .env (same as Bybit)
        self.use_stop_loss = os.getenv('USE_STOP_LOSS', 'true').lower() == 'true'
        self.stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', 6.5))
        self.stop_loss_type = os.getenv('STOP_LOSS_TYPE', 'fixed')
        
        self.use_trailing_stop = True  # ALWAYS use trailing stop
        self.trailing_callback_rate = float(os.getenv('TRAILING_CALLBACK_RATE', 0.5))
        self.trailing_activation_percent = float(os.getenv('TRAILING_ACTIVATION_PERCENT', 3.5))
        
        self.use_take_profit = os.getenv('USE_TAKE_PROFIT', 'false').lower() == 'true'
        self.take_profit_percent = float(os.getenv('TAKE_PROFIT_PERCENT', 10.0))
        
        logger.info(f"BinanceEnhancedManager initialized with protection settings:")
        logger.info(f"  Stop Loss: {self.use_stop_loss} ({self.stop_loss_percent}%)")
        logger.info(f"  Trailing Stop: {self.trailing_callback_rate}% callback, {self.trailing_activation_percent}% activation")
        logger.info(f"  Take Profit: {self.use_take_profit} ({self.take_profit_percent}%)")
    
    async def create_order_with_protection(
        self,
        symbol: str,
        side: str,  # "BUY" or "SELL"
        quantity: Decimal,
        order_type: str = "MARKET",
        price: Optional[Decimal] = None,
        leverage: int = 10,
        reduce_only: bool = False
    ) -> Dict[str, Any]:
        """
        Create order with automatic protection setup
        This is the main entry point for signal processing
        """
        try:
            logger.info(f"🔧 Creating Binance order with protection: {symbol} {side} {quantity}")
            
            # Step 1: Set leverage
            await self.set_leverage(symbol, leverage)
            
            # Step 2: Create the main order
            if order_type == "MARKET":
                order_result = await self.place_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    reduce_only=reduce_only
                )
            else:
                order_result = await self.place_limit_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=price,
                    reduce_only=reduce_only
                )
            
            # Check if order was successful
            if order_result.get('status') == 'REJECTED':
                logger.error(f"Order rejected: {order_result}")
                return {
                    'success': False,
                    'error': order_result.get('msg', 'Order rejected'),
                    'order': order_result
                }
            
            order_id = order_result.get('orderId')
            logger.info(f"✅ Order created successfully: {order_id}")
            
            # Step 3: Wait for market order execution
            if order_type == "MARKET":
                await asyncio.sleep(2)
            
            # Step 4: Get position info for protection setup
            position_info = await self.get_position(symbol)
            
            if not position_info:
                logger.warning("Position not found after order creation")
                return {
                    'success': True,
                    'order': order_result,
                    'protection': None,
                    'warning': 'Position not found for protection setup'
                }
            
            position_amt = abs(position_info.position_amount)
            entry_price = position_info.entry_price
            
            if position_amt == 0 or entry_price == 0:
                logger.warning(f"Invalid position data: amt={position_amt}, price={entry_price}")
                return {
                    'success': True,
                    'order': order_result,
                    'protection': None,
                    'warning': 'Invalid position data for protection'
                }
            
            # Step 5: Set up protection
            protection_results = await self._setup_protection(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                position_amount=position_amt
            )
            
            return {
                'success': True,
                'order': order_result,
                'protection': protection_results
            }
            
        except Exception as e:
            logger.error(f"Error creating order with protection: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _setup_protection(
        self,
        symbol: str,
        side: str,
        entry_price: Decimal,
        position_amount: Decimal
    ) -> Dict[str, Any]:
        """
        Set up all protection orders for a position
        """
        protection_results = {
            'stop_loss': None,
            'trailing_stop': None,
            'take_profit': None
        }
        
        logger.info(f"🛡️ Setting up protection for {symbol} position...")
        
        # 1. Set Stop Loss (if enabled)
        if self.use_stop_loss:
            try:
                sl_result = await self._set_stop_loss(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    position_amount=position_amount
                )
                protection_results['stop_loss'] = sl_result
                if sl_result.get('success'):
                    logger.info(f"✅ Stop Loss set at {sl_result.get('stop_price')}")
                else:
                    logger.error(f"❌ Failed to set Stop Loss: {sl_result.get('error')}")
            except Exception as e:
                logger.error(f"Exception setting Stop Loss: {e}")
                protection_results['stop_loss'] = {'success': False, 'error': str(e)}
        
        # 2. ALWAYS set Trailing Stop
        await asyncio.sleep(0.5)  # Small delay between orders
        try:
            ts_result = await self._set_trailing_stop(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                position_amount=position_amount
            )
            protection_results['trailing_stop'] = ts_result
            if ts_result.get('success'):
                logger.info(f"✅ Trailing Stop set: activation={ts_result.get('activation_price')}, callback={ts_result.get('callback_rate')}%")
            else:
                logger.error(f"❌ Failed to set Trailing Stop: {ts_result.get('error')}")
        except Exception as e:
            logger.error(f"Exception setting Trailing Stop: {e}")
            protection_results['trailing_stop'] = {'success': False, 'error': str(e)}
        
        # 3. Set Take Profit (if enabled)
        if self.use_take_profit:
            await asyncio.sleep(0.5)
            try:
                tp_result = await self._set_take_profit(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    position_amount=position_amount
                )
                protection_results['take_profit'] = tp_result
                if tp_result.get('success'):
                    logger.info(f"✅ Take Profit set at {tp_result.get('stop_price')}")
                else:
                    logger.error(f"❌ Failed to set Take Profit: {tp_result.get('error')}")
            except Exception as e:
                logger.error(f"Exception setting Take Profit: {e}")
                protection_results['take_profit'] = {'success': False, 'error': str(e)}
        
        # Log protection summary
        successful = sum(1 for r in protection_results.values() if r and r.get('success'))
        total = len([r for r in protection_results.values() if r is not None])
        
        if successful == total:
            logger.info(f"✅ FULL PROTECTION established for {symbol}")
        else:
            logger.warning(f"⚠️ PARTIAL PROTECTION for {symbol}: {successful}/{total} successful")
        
        return protection_results
    
    async def _set_stop_loss(
        self,
        symbol: str,
        side: str,
        entry_price: Decimal,
        position_amount: Decimal
    ) -> Dict[str, Any]:
        """
        Set Stop Loss order
        """
        try:
            # Calculate stop loss price
            if side == "BUY":
                # For long position, stop loss is below entry
                stop_price = entry_price * (Decimal('1') - Decimal(str(self.stop_loss_percent)) / Decimal('100'))
                stop_side = "SELL"
            else:
                # For short position, stop loss is above entry
                stop_price = entry_price * (Decimal('1') + Decimal(str(self.stop_loss_percent)) / Decimal('100'))
                stop_side = "BUY"
            
            # Get price precision
            stop_price = self._round_price(symbol, stop_price)
            
            logger.info(f"Setting Stop Loss for {symbol}: price={stop_price}, side={stop_side}, qty={position_amount}")
            
            # Place stop market order
            order = await self.place_stop_market_order(
                symbol=symbol,
                side=stop_side,
                quantity=position_amount,
                stop_price=stop_price,
                reduce_only=True
            )
            
            if order.get('status') != 'REJECTED':
                return {
                    'success': True,
                    'order_id': order.get('orderId'),
                    'stop_price': stop_price
                }
            else:
                return {
                    'success': False,
                    'error': order.get('msg', 'Order rejected')
                }
                
        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _set_trailing_stop(
        self,
        symbol: str,
        side: str,
        entry_price: Decimal,
        position_amount: Decimal
    ) -> Dict[str, Any]:
        """
        Set Trailing Stop order for Binance
        Binance uses callback rate (percentage) for trailing stops
        """
        try:
            # Calculate activation price
            if side == "BUY":
                # For long, activate when price goes up
                activation_price = entry_price * (Decimal('1') + Decimal(str(self.trailing_activation_percent)) / Decimal('100'))
            else:
                # For short, activate when price goes down
                activation_price = entry_price * (Decimal('1') - Decimal(str(self.trailing_activation_percent)) / Decimal('100'))
            
            activation_price = self._round_price(symbol, activation_price)
            
            logger.info(f"Setting Trailing Stop for {symbol}: activation={activation_price}, callback={self.trailing_callback_rate}%")
            
            # Place trailing stop order
            order = await self.place_trailing_stop_order(
                symbol=symbol,
                side="SELL" if side == "BUY" else "BUY",
                callback_rate=Decimal(str(self.trailing_callback_rate)),
                activation_price=activation_price,
                quantity=position_amount
            )
            
            if order.get('status') != 'REJECTED':
                return {
                    'success': True,
                    'order_id': order.get('orderId'),
                    'activation_price': activation_price,
                    'callback_rate': self.trailing_callback_rate
                }
            else:
                return {
                    'success': False,
                    'error': order.get('msg', 'Order rejected')
                }
                
        except Exception as e:
            logger.error(f"Error setting trailing stop: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _set_take_profit(
        self,
        symbol: str,
        side: str,
        entry_price: Decimal,
        position_amount: Decimal
    ) -> Dict[str, Any]:
        """
        Set Take Profit order
        """
        try:
            # Calculate take profit price
            if side == "BUY":
                # For long position, take profit is above entry
                tp_price = entry_price * (Decimal('1') + Decimal(str(self.take_profit_percent)) / Decimal('100'))
                tp_side = "SELL"
            else:
                # For short position, take profit is below entry
                tp_price = entry_price * (Decimal('1') - Decimal(str(self.take_profit_percent)) / Decimal('100'))
                tp_side = "BUY"
            
            tp_price = self._round_price(symbol, tp_price)
            
            logger.info(f"Setting Take Profit for {symbol}: price={tp_price}, side={tp_side}, qty={position_amount}")
            
            # Place take profit order
            order = await self.place_take_profit_market_order(
                symbol=symbol,
                side=tp_side,
                quantity=position_amount,
                stop_price=tp_price,
                reduce_only=True
            )
            
            if order.get('status') != 'REJECTED':
                return {
                    'success': True,
                    'order_id': order.get('orderId'),
                    'stop_price': tp_price
                }
            else:
                return {
                    'success': False,
                    'error': order.get('msg', 'Order rejected')
                }
                
        except Exception as e:
            logger.error(f"Error setting take profit: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _round_price(self, symbol: str, price: Decimal) -> Decimal:
        """
        Round price according to symbol's tick size
        """
        # Get symbol info for precision if available
        if hasattr(self, 'symbols') and self.symbols:
            symbol_info = self.symbols.get(symbol)
            if symbol_info:
                return symbol_info.round_price(price)
        
        # Default rounding if symbol info not found
        if price >= 1000:
            return price.quantize(Decimal('0.1'))
        elif price >= 100:
            return price.quantize(Decimal('0.01'))
        elif price >= 10:
            return price.quantize(Decimal('0.001'))
        elif price >= 1:
            return price.quantize(Decimal('0.0001'))
        else:
            return price.quantize(Decimal('0.00001'))
    
    async def process_signal_for_binance(
        self,
        signal: Dict[str, Any],
        position: Optional[Position] = None
    ) -> Dict[str, Any]:
        """
        Process a trading signal specifically for Binance
        This is the main entry point from signal processor
        """
        try:
            logger.info(f"📨 Processing Binance signal: {signal}")
            
            symbol = signal.get('pair_symbol', signal.get('symbol'))
            side = signal.get('action', 'BUY').upper()
            
            # Determine quantity
            if position and position.quantity:
                quantity = position.quantity
            else:
                # Calculate quantity based on risk management
                quantity = await self._calculate_position_size(symbol, signal)
            
            if not symbol or quantity <= 0:
                return {
                    'success': False,
                    'error': 'Invalid signal parameters'
                }
            
            # Get leverage from signal or use default
            leverage = signal.get('leverage', 10)
            
            # Create order with protection
            result = await self.create_order_with_protection(
                symbol=symbol,
                side=side,
                quantity=quantity,
                leverage=leverage
            )
            
            if result.get('success'):
                logger.info(f"✅ Binance order processed successfully for {symbol}")
            else:
                logger.error(f"❌ Failed to process Binance order: {result.get('error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing Binance signal: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _calculate_position_size(
        self,
        symbol: str,
        signal: Dict[str, Any]
    ) -> Decimal:
        """
        Calculate position size based on risk management rules
        """
        try:
            # Get account balance
            balance = await self.get_account_balance()
            if not balance:
                logger.error("Could not get account balance")
                return Decimal('0')
            
            # Get USDT balance
            usdt_balance = Decimal(str(balance.usdt_balance))
            
            # Risk per trade (from config or default 1%)
            risk_percent = Decimal(str(os.getenv('RISK_PER_TRADE', '1.0')))
            risk_amount = usdt_balance * (risk_percent / Decimal('100'))
            
            # Get current price
            ticker = await self.get_market_data(symbol)
            if not ticker:
                logger.error(f"Could not get ticker for {symbol}")
                return Decimal('0')
            
            current_price = ticker.last_price
            
            # Calculate position size based on stop loss distance
            sl_distance = current_price * (Decimal(str(self.stop_loss_percent)) / Decimal('100'))
            position_size = risk_amount / sl_distance
            
            # Round to symbol's quantity precision
            symbol_info = None
            if hasattr(self, 'symbols') and self.symbols:
                symbol_info = self.symbols.get(symbol)
                if symbol_info:
                    position_size = symbol_info.round_quantity(position_size)
            
            # Check minimum notional
            notional = position_size * current_price
            min_notional = Decimal('5')  # Default minimum
            if symbol_info:
                min_notional = symbol_info.min_notional
            
            if notional < min_notional:
                position_size = (min_notional / current_price) * Decimal('1.1')  # Add 10% buffer
                if symbol_info:
                    position_size = symbol_info.round_quantity(position_size)
                else:
                    # Default rounding for quantity
                    position_size = position_size.quantize(Decimal('0.001'))
            
            logger.info(f"Calculated position size for {symbol}: {position_size} (notional: {position_size * current_price} USDT)")
            
            return position_size
            
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return Decimal('0')
    
    # Add helper methods for compatibility
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get ticker data for a symbol (compatibility method)"""
        try:
            market_data = await self.get_market_data(symbol)
            if market_data:
                return {
                    'last': str(market_data.last_price),
                    'bid': str(market_data.bid_price),
                    'ask': str(market_data.ask_price),
                    'volume': str(market_data.volume_24h)
                }
            return None
        except Exception as e:
            logger.error(f"Error getting ticker: {e}")
            return None
    
    async def get_balance(self):
        """Get account balance (compatibility method)"""
        return await self.get_account_balance()
    
    async def get_positions(self) -> List[ExchangePosition]:
        """Get all positions (compatibility method)"""
        return await self.get_all_positions()