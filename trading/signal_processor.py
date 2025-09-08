"""
ATS 2.0 - Signal Processor
Processes trading signals with duplicate protection and risk checks
"""
import asyncio
import logging
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Dict, Any, List, Set
from collections import defaultdict

from core.config import SystemConfig
from database.connection import DatabaseManager
from database.models import (
    TradingSignal, Position, Order, AuditLog,
    SignalStatus, PositionStatus, OrderStatus, OrderPurpose, CloseReason
)
from exchanges.base import ExchangeBase
from exchanges.binance import BinanceExchange

logger = logging.getLogger(__name__)


class DuplicateProtection:
    """Manages duplicate signal and position protection"""

    def __init__(self, cooldown_seconds: int = 30):
        self.cooldown_seconds = cooldown_seconds
        self._processing_signals: Set[int] = set()  # prediction_ids being processed
        self._processing_symbols: Set[str] = set()  # symbols being processed
        self._recent_signals: Dict[str, datetime] = {}  # symbol -> last signal time
        self._lock = asyncio.Lock()

    async def can_process_signal(self, signal: TradingSignal) -> tuple[bool, str]:
        """Check if signal can be processed"""
        async with self._lock:
            # Check if signal is already being processed
            if signal.prediction_id in self._processing_signals:
                return False, "Signal already being processed"

            # Check if symbol is being processed
            if signal.pair_symbol in self._processing_symbols:
                return False, f"Symbol {signal.pair_symbol} already being processed"

            # Check cooldown period
            if signal.pair_symbol in self._recent_signals:
                time_since_last = datetime.now(timezone.utc) - self._recent_signals[signal.pair_symbol]
                if time_since_last.total_seconds() < self.cooldown_seconds:
                    remaining = self.cooldown_seconds - time_since_last.total_seconds()
                    return False, f"Cooldown active, {remaining:.0f}s remaining"

            # Mark as processing
            self._processing_signals.add(signal.prediction_id)
            self._processing_symbols.add(signal.pair_symbol)
            self._recent_signals[signal.pair_symbol] = datetime.now(timezone.utc)

            return True, "OK"

    async def release_signal(self, signal: TradingSignal):
        """Release signal from processing"""
        async with self._lock:
            self._processing_signals.discard(signal.prediction_id)
            self._processing_symbols.discard(signal.pair_symbol)

    async def clear_old_entries(self):
        """Clear old cooldown entries"""
        async with self._lock:
            now = datetime.now(timezone.utc)
            expired = []

            for symbol, last_time in self._recent_signals.items():
                if (now - last_time).total_seconds() > self.cooldown_seconds * 2:
                    expired.append(symbol)

            for symbol in expired:
                del self._recent_signals[symbol]


class SignalFilter:
    """Filters signals based on configuration"""

    def __init__(self, config: SystemConfig):
        self.config = config

    def should_process(self, signal: TradingSignal) -> tuple[bool, Optional[str]]:
        """Check if signal passes all filters"""

        # Check if signal is valid
        if not signal.is_valid:
            return False, "Invalid signal format"

        # Check prediction value
        if not signal.prediction:
            return False, "Prediction is False"

        # Check confidence level filter
        min_confidence = self.config.signal.min_confidence_level
        if min_confidence:
            confidence_levels = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
            signal_level = confidence_levels.get(signal.confidence_level, 0)
            min_level = confidence_levels.get(min_confidence, 0)

            if signal_level < min_level:
                return False, f"Confidence {signal.confidence_level} below minimum {min_confidence}"

        # Check prediction probability
        min_proba = self.config.signal.min_prediction_proba
        if min_proba > 0 and signal.prediction_proba < min_proba:
            return False, f"Probability {signal.prediction_proba:.4f} below minimum {min_proba}"

        # Check if symbol is blacklisted
        if signal.pair_symbol in self.config.position.skip_symbols:
            return False, f"Symbol {signal.pair_symbol} is blacklisted"

        # Check signal age
        # Handle both timezone-aware and naive datetimes
        if hasattr(signal.created_at, 'tzinfo') and signal.created_at.tzinfo is None:
            # Make created_at timezone-aware if it's naive
            signal_created_at = signal.created_at.replace(tzinfo=timezone.utc)
        else:
            signal_created_at = signal.created_at
        
        signal_age = (datetime.now(timezone.utc) - signal_created_at).total_seconds()
        if signal_age > self.config.signal.signal_timeout_seconds:
            return False, f"Signal too old: {signal_age:.0f}s"

        return True, None


class RiskManager:
    """Manages risk limits and position checks"""

    def __init__(self, config: SystemConfig, db: DatabaseManager):
        self.config = config
        self.db = db
        self._daily_trades = 0
        self._daily_loss = Decimal(0)
        self._last_reset = datetime.now(timezone.utc).date()

    async def can_open_position(self, symbol: str) -> tuple[bool, Optional[str]]:
        """Check if new position can be opened"""

        # Reset daily counters if new day
        today = datetime.now(timezone.utc).date()
        if today != self._last_reset:
            self._daily_trades = 0
            self._daily_loss = Decimal(0)
            self._last_reset = today

        # Check daily trade limit
        if self._daily_trades >= self.config.risk.max_daily_trades:
            return False, f"Daily trade limit reached: {self._daily_trades}"

        # Check daily loss limit
        if self._daily_loss >= self.config.risk.max_daily_loss_usd:
            return False, f"Daily loss limit reached: ${self._daily_loss:.2f}"

        # Check if position already exists for symbol
        exists = await self.db.positions.check_position_exists(symbol)
        if exists:
            return False, f"Position already exists for {symbol}"

        # Check total open positions
        open_count = await self.db.positions.count_open_positions()
        if open_count >= self.config.position.max_open_positions:
            return False, f"Max open positions reached: {open_count}"

        return True, None

    def record_trade(self):
        """Record a new trade"""
        self._daily_trades += 1

    def record_loss(self, amount: Decimal):
        """Record a loss"""
        if amount < 0:
            self._daily_loss += abs(amount)


class SignalProcessor:
    """Main signal processing engine"""

    def __init__(
        self,
        config: SystemConfig,
        db: DatabaseManager,
        exchange: ExchangeBase
    ):
        self.config = config
        self.db = db
        self.exchange = exchange

        # Components
        self.duplicate_protection = DuplicateProtection(
            config.signal.signal_cooldown_seconds
        )
        self.signal_filter = SignalFilter(config)
        self.risk_manager = RiskManager(config, db)

        # Statistics
        self.stats = defaultdict(int)
        self._running = False

    async def process_signal(self, signal: TradingSignal) -> bool:
        """
        Process a single trading signal
        Returns: True if signal was executed, False otherwise
        """
        start_time = datetime.now(timezone.utc)

        try:
            # 1. Create signal record in database
            signal.id = await self.db.signals.create_signal(signal)

            # Log signal received
            signal_data = signal.to_dict()
            # Ensure all Decimal values are converted to float for JSON
            signal_data_json = json.loads(json.dumps(signal_data, default=str))

            await self.db.audit.log_event(
                event_type="SIGNAL_RECEIVED",
                event_category="SIGNAL",
                event_data=signal_data_json,
                entity_id=signal.id,
                entity_type="signals"
            )

            # 2. Check if signal is for supported exchange and route accordingly
            if signal.exchange_id == 1:  # Binance
                logger.info(f"Processing Binance signal for {signal.pair_symbol}")
                # Continue with normal flow for Binance
            elif signal.exchange_id == 2:  # Bybit
                logger.info(f"Processing Bybit signal for {signal.pair_symbol}")
                # Bybit processing enabled - continue with normal flow
            else:
                await self._skip_signal(signal, f"Unknown exchange_id: {signal.exchange_id}")
                return False

            # 3. Check duplicate protection
            can_process, reason = await self.duplicate_protection.can_process_signal(signal)
            if not can_process:
                await self._skip_signal(signal, reason)
                return False

            try:
                # 4. Apply signal filters
                should_process, skip_reason = self.signal_filter.should_process(signal)
                if not should_process:
                    await self._skip_signal(signal, skip_reason)
                    return False

                # 5. Check if symbol is tradeable
                is_tradeable, trade_reason = await self.exchange.check_symbol_tradeable(
                    signal.pair_symbol
                )
                if not is_tradeable:
                    await self._skip_signal(signal, f"Not tradeable: {trade_reason}")
                    return False

                # 6. Risk management checks
                can_open, risk_reason = await self.risk_manager.can_open_position(
                    signal.pair_symbol
                )
                if not can_open:
                    await self._skip_signal(signal, f"Risk check failed: {risk_reason}")
                    return False

                # 7. Mark signal as processing
                await self.db.signals.update_signal_status(
                    signal.prediction_id,
                    SignalStatus.PROCESSING
                )

                # 8. Open position
                success = await self._open_position(signal)

                if success:
                    # Update signal status
                    await self.db.signals.update_signal_status(
                        signal.prediction_id,
                        SignalStatus.EXECUTED
                    )

                    # Record trade
                    self.risk_manager.record_trade()
                    self.stats['executed'] += 1

                    # Log success
                    processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    logger.info(f"Signal {signal.prediction_id} executed in {processing_time:.2f}s")

                    return True
                else:
                    await self._error_signal(signal, "Failed to open position")
                    return False

            finally:
                # Always release signal from processing
                await self.duplicate_protection.release_signal(signal)

        except Exception as e:
            logger.error(f"Error processing signal {signal.prediction_id}: {e}")
            await self._error_signal(signal, str(e))
            return False

    async def _open_position(self, signal: TradingSignal) -> bool:
        """Open a new position from signal"""
        try:
            # Create position object
            position = Position(
                signal_id=signal.id,
                prediction_id=signal.prediction_id,
                exchange=self.exchange.name,
                symbol=signal.pair_symbol,
                side=signal.side,
                leverage=self.config.position.leverage
            )

            # Open position on exchange
            success, order_result, error = await self.exchange.open_position(
                position,
                self.config.position.size_usd
            )

            if not success:
                logger.error(f"Failed to open position: {error}")
                await self.db.audit.log_event(
                    event_type="POSITION_OPEN_FAILED",
                    event_category="POSITION",
                    event_data={
                        'signal_id': signal.id,
                        'symbol': signal.pair_symbol,
                        'error': error
                    },
                    severity="ERROR"
                )
                return False

            # Save position to database
            position_id = await self.db.positions.create_position(position)

            # Log position opened
            position_data = position.to_dict()
            position_data_json = json.loads(json.dumps(position_data, default=str))

            await self.db.audit.log_event(
                event_type="POSITION_OPENED",
                event_category="POSITION",
                event_data=position_data_json,
                entity_id=position_id,
                entity_type="positions"
            )

            # Create order record
            if order_result:
                order = Order(
                    position_id=position_id,
                    exchange=self.exchange.name,
                    exchange_order_id=str(order_result.get('orderId', '')),
                    order_purpose=OrderPurpose.ENTRY,
                    side="BUY" if signal.side == "LONG" else "SELL",
                    quantity=position.quantity,
                    avg_price=position.entry_price,
                    status=OrderStatus.FILLED
                )
                await self.db.orders.create_order(order)

            # Set stop loss
            if self.config.risk.stop_loss_enabled:
                await self._set_stop_loss(position)

            # Set take profit
            if self.config.risk.take_profit_enabled:
                await self._set_take_profit(position)

            logger.info(f"Opened {position.side} position for {position.symbol}: "
                       f"qty={position.quantity}, price={position.entry_price}")

            return True

        except Exception as e:
            logger.error(f"Error opening position: {e}")
            return False

    async def _set_stop_loss(self, position: Position) -> bool:
        """Set stop loss for position"""
        try:
            success, order_id, error = await self.exchange.set_stop_loss(
                position,
                self.config.risk.stop_loss_percent,
                is_trailing=self.config.risk.stop_loss_type == 'trailing',
                callback_rate=self.config.risk.trailing_callback_rate
            )

            if success and order_id:
                # Update position with SL info
                position.stop_loss_order_id = order_id
                position.stop_loss_type = self.config.risk.stop_loss_type

                # ВАЖНО: Сохраняем обновления в БД!
                await self.db.positions.update_position(position)

                logger.info(f"Set {position.stop_loss_type} stop loss for {position.symbol}: order_id={order_id}")
                return True
            else:
                logger.error(f"Failed to set stop loss: {error}")
                return False

        except Exception as e:
            logger.error(f"Error setting stop loss: {e}")
            return False

    async def _set_take_profit(self, position: Position) -> bool:
        """Set take profit for position"""
        try:
            partial_config = None
            if self.config.risk.take_profit_type == 'partial':
                partial_config = self.config.risk.take_profit_levels

            success, order_ids, error = await self.exchange.set_take_profit(
                position,
                self.config.risk.take_profit_percent,
                partial_targets=partial_config
            )

            if success and order_ids:
                # Update position with TP info
                if order_ids:
                    position.take_profit_order_id = order_ids[0]  # Store first TP order

                if position.side == "LONG":
                    position.take_profit_price = position.entry_price * (
                            1 + Decimal(str(self.config.risk.take_profit_percent)) / 100
                    )
                else:
                    position.take_profit_price = position.entry_price * (
                            1 - Decimal(str(self.config.risk.take_profit_percent)) / 100
                    )

                # ⚠️ ДОБАВИТЬ ЭТУ СТРОКУ (перед созданием order records):
                await self.db.positions.update_position(position)

                # Create order records
                for order_id in order_ids:
                    order = Order(
                        position_id=position.id,
                        exchange=self.exchange.name,
                        exchange_order_id=order_id,
                        order_purpose=OrderPurpose.TAKE_PROFIT,
                        side="SELL" if position.side == "LONG" else "BUY"
                    )
                    await self.db.orders.create_order(order)

                logger.info(f"Set take profit for {position.symbol}: {len(order_ids)} orders")
                return True
            else:
                logger.error(f"Failed to set take profit: {error}")
                return False

        except Exception as e:
            logger.error(f"Error setting take profit: {e}")
            return False

    async def _skip_signal(self, signal: TradingSignal, reason: str):
        """Mark signal as skipped"""
        await self.db.signals.update_signal_status(
            signal.prediction_id,
            SignalStatus.SKIPPED,
            skip_reason=reason
        )

        await self.db.audit.log_event(
            event_type="SIGNAL_SKIPPED",
            event_category="SIGNAL",
            event_data={
                'signal_id': signal.id,
                'symbol': signal.pair_symbol,
                'reason': reason
            },
            entity_id=signal.id,
            entity_type="signals"
        )

        self.stats['skipped'] += 1
        logger.info(f"Skipped signal {signal.prediction_id}: {reason}")

    async def _error_signal(self, signal: TradingSignal, error: str):
        """Mark signal as error"""
        await self.db.signals.update_signal_status(
            signal.prediction_id,
            SignalStatus.ERROR,
            error_message=error
        )

        await self.db.audit.log_event(
            event_type="SIGNAL_ERROR",
            event_category="SIGNAL",
            event_data={
                'signal_id': signal.id,
                'symbol': signal.pair_symbol,
                'error': error
            },
            entity_id=signal.id,
            entity_type="signals",
            severity="ERROR"
        )

        self.stats['errors'] += 1
        logger.error(f"Error processing signal {signal.prediction_id}: {error}")

    async def process_batch(self, signals: List[TradingSignal]) -> Dict[str, int]:
        """Process a batch of signals"""
        results = {
            'total': len(signals),
            'executed': 0,
            'skipped': 0,
            'errors': 0
        }

        for signal in signals:
            try:
                executed = await self.process_signal(signal)
                if executed:
                    results['executed'] += 1
                else:
                    # Check last status
                    # This is simplified - in production would query DB
                    results['skipped'] += 1
            except Exception as e:
                logger.error(f"Error processing signal {signal.prediction_id}: {e}")
                results['errors'] += 1

        return results

    async def cleanup(self):
        """Cleanup old entries"""
        await self.duplicate_protection.clear_old_entries()