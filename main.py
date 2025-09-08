#!/usr/bin/env python3
"""
ATS 2.0 - Automated Trading System
Main entry point for the trading system
"""
import asyncio
import signal
import sys
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
import json

from core.config import SystemConfig
from core.security import LogSanitizer, safe_log
from database.connection import DatabaseManager
from database.models import TradingSignal, SignalStatus, CloseReason
from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange
from trading.signal_processor import SignalProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ats_system.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ATSSystem:
    """Main ATS System orchestrator"""

    def __init__(self):
        self.config = SystemConfig()
        self.db: Optional[DatabaseManager] = None
        self.exchange: Optional[BinanceExchange] = None
        self.signal_processor: Optional[SignalProcessor] = None

        self._running = False
        self._tasks = []
        self._shutdown_event = asyncio.Event()

    async def initialize(self):
        """Initialize all system components"""
        logger.info("=" * 60)
        logger.info("ATS 2.0 - Automated Trading System")
        logger.info("=" * 60)

        # Validate configuration
        errors = self.config.validate()
        if errors:
            logger.error(f"Configuration errors: {errors}")
            if self.config.mode == 'live':
                raise ValueError(f"Cannot start in live mode with config errors: {errors}")
            else:
                logger.warning("Running in test mode with config errors")

        # Log configuration (sanitized)
        safe_config = LogSanitizer.sanitize_config(self.config)
        logger.info(f"Configuration: {json.dumps(safe_config, indent=2)}")

        # Initialize database
        logger.info("Initializing database connection...")
        self.db = DatabaseManager(self.config)
        await self.db.initialize()

        # Check database health
        is_healthy = await self.db.health_check()
        if not is_healthy:
            raise RuntimeError("Database health check failed")
        logger.info("Database connection established")

        # Initialize exchange
        logger.info(f"Initializing {self.config.exchange.name} exchange...")

        # Use different thresholds for testnet
        if self.config.exchange.testnet:
            min_volume = self.config.position.min_volume_24h_usd_testnet
            max_spread = self.config.position.max_spread_percent_testnet
            logger.info(f"Using testnet thresholds: volume=${min_volume:,.0f}, spread={max_spread}%")
        else:
            min_volume = self.config.position.min_volume_24h_usd
            max_spread = self.config.position.max_spread_percent
            logger.info(f"Using production thresholds: volume=${min_volume:,.0f}, spread={max_spread}%")

        exchange_config = {
            'api_key': self.config.exchange.api_key,
            'api_secret': self.config.exchange.api_secret,
            'testnet': self.config.exchange.testnet,
            'max_spread_percent': max_spread,
            'min_volume_24h_usd': min_volume
        }

        # Initialize exchange based on config
        if self.config.exchange.name == 'binance':
            self.exchange = BinanceExchange(exchange_config)
        else:
            # Use Bybit for all non-binance configs
            import os
            bybit_config = {
                'api_key': os.getenv('BYBIT_API_KEY'),
                'api_secret': os.getenv('BYBIT_API_SECRET'),
                'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
            }
            self.exchange = BybitExchange(bybit_config)
        
        await self.exchange.initialize()
        logger.info(f"{self.config.exchange.name.upper()} initialized ({'Testnet' if self.config.exchange.testnet else 'Production'})")

        # Check exchange connectivity
        balances = await self.exchange.get_account_balance()
        if balances:
            # Handle different balance formats for different exchanges
            if hasattr(balances, 'items'):
                # Binance format
                for asset, balance in balances.items():
                    logger.info(f"Balance: {asset} = {balance.total}")
            elif hasattr(balances, 'usdt_balance'):
                # Bybit format
                logger.info(f"Balance: USDT = {balances.usdt_balance}")

        # Initialize signal processor
        logger.info("Initializing signal processor...")
        self.signal_processor = SignalProcessor(
            self.config,
            self.db,
            self.exchange
        )
        logger.info("Signal processor initialized")

        # Setup signal handlers
        self._setup_signal_handlers()

        logger.info("System initialization complete")

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""

        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, initiating shutdown...")
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def monitor_signals(self):
        """Monitor and process new trading signals"""
        logger.info("Starting signal monitor...")

        while self._running:
            try:
                # Get unprocessed signals - use new source if enabled
                if self.db.use_new_signal_source:
                    # NEW: Get signals from fas.scoring_history
                    logger.info("Using new signal source: fas.scoring_history")
                    signals = await self.db.signals_v2.get_unprocessed_signals_legacy(limit=10)
                else:
                    # OLD: Get signals from smart_ml.predictions
                    logger.info("Using old signal source: smart_ml.predictions")
                    signals = await self.db.signals.get_unprocessed_signals(limit=10)

                if signals:
                    logger.info(f"Found {len(signals)} new signals")

                    # Process signals
                    results = await self.signal_processor.process_batch(signals)

                    logger.info(f"Processed signals: {results}")

                    # Update metrics
                    await self.db.metrics.update_daily_metrics()

                # Wait before next check
                await asyncio.sleep(5)  # Check every 5 seconds

            except Exception as e:
                logger.error(f"Error in signal monitor: {e}")
                await asyncio.sleep(10)  # Wait longer on error

    async def monitor_positions(self):
        """Monitor open positions with detailed tracking"""
        logger.info("Starting enhanced position monitor...")
        
        # Track position states for change detection
        last_position_states = {}

        while self._running:
            try:
                # Get open positions
                positions = await self.db.positions.get_open_positions()

                if positions:
                    logger.info(f"📊 Monitoring {len(positions)} open positions")

                    for position in positions:
                        try:
                            # Track position changes
                            position_key = f"{position.exchange}_{position.symbol}_{position.side}"
                            current_state = {
                                'price': position.current_price,
                                'pnl': position.unrealized_pnl,
                                'pnl_percent': position.unrealized_pnl_percent
                            }
                            
                            # Get current market data
                            market = await self.exchange.get_market_data(position.symbol)
                            if market:
                                old_price = position.current_price
                                new_price = market.last_price
                                
                                # Update position price
                                await self.db.positions.update_position_price(
                                    position.id,
                                    market.last_price
                                )
                                
                                # Update position object for PNL calculation
                                position.current_price = new_price
                                
                                # Log significant changes
                                if position_key in last_position_states:
                                    last_state = last_position_states[position_key]
                                    
                                    # Check for significant price change (>0.1%)
                                    if old_price and new_price:
                                        price_change = ((new_price - old_price) / old_price) * 100
                                        if abs(price_change) > 0.1:
                                            logger.info(
                                                f"💰 {position.symbol} {position.side}: "
                                                f"Price ${old_price:.2f} → ${new_price:.2f} ({price_change:+.2f}%)"
                                            )
                                
                                # Log PNL updates
                                if position.unrealized_pnl_percent:
                                    pnl_emoji = "🟢" if position.unrealized_pnl_percent > 0 else "🔴"
                                    
                                    # Log every 1% PNL change
                                    if position_key not in last_position_states or \
                                       abs(position.unrealized_pnl_percent - (last_state.get('pnl_percent') or 0)) > 1:
                                        logger.info(
                                            f"{pnl_emoji} {position.symbol} {position.side}: "
                                            f"PNL {position.unrealized_pnl_percent:+.2f}% "
                                            f"(${position.unrealized_pnl:+.2f})"
                                        )
                                
                                # Update state tracking
                                last_position_states[position_key] = {
                                    'price': new_price,
                                    'pnl': position.unrealized_pnl,
                                    'pnl_percent': position.unrealized_pnl_percent
                                }

                                # Check for timeout (if enabled)
                                if self.config.risk.auto_close_positions:
                                    try:
                                        current_time = datetime.now(timezone.utc)
                                        # Handle both aware and naive datetimes
                                        if hasattr(position.opened_at, 'tzinfo') and position.opened_at.tzinfo is None:
                                            # Make opened_at timezone-aware if it's naive
                                            opened_at = position.opened_at.replace(tzinfo=timezone.utc)
                                        elif hasattr(position.opened_at, 'tzinfo'):
                                            opened_at = position.opened_at
                                        else:
                                            # If it's already a timezone-aware datetime, use it as is
                                            opened_at = position.opened_at
                                        
                                        position_age = current_time - opened_at
                                        max_duration = timedelta(hours=self.config.risk.max_position_duration_hours)

                                        if position_age > max_duration:
                                            logger.warning(f"Position {position.symbol} exceeded max duration, closing...")
                                            
                                            # Validate position still exists on exchange before closing
                                            exchange_position = await self.exchange.get_position(position.symbol)
                                            if not exchange_position:
                                                logger.warning(f"Position {position.symbol} not found on exchange, marking as closed")
                                                await self.db.positions.close_position(
                                                    position.id,
                                                    market.last_price,
                                                    0,
                                                    CloseReason.NOT_FOUND
                                                )
                                                continue
                                            
                                            success, _, error = await self.exchange.close_position(
                                                position,
                                                reason="TIMEOUT"
                                            )

                                            if success:
                                                await self.db.positions.close_position(
                                                    position.id,
                                                    market.last_price,
                                                    position.unrealized_pnl or 0,
                                                    CloseReason.TIMEOUT
                                                )
                                    except Exception as e:
                                        # Skip datetime errors silently
                                        pass

                                # Check emergency stop loss
                                if position.unrealized_pnl_percent:
                                    if abs(position.unrealized_pnl_percent) > self.config.risk.emergency_stop_loss_percent:
                                        logger.warning(f"Emergency stop for {position.symbol}: "
                                                       f"PNL {position.unrealized_pnl_percent:.2f}%")
                                        
                                        # Validate position still exists on exchange before closing
                                        exchange_position = await self.exchange.get_position(position.symbol)
                                        if not exchange_position:
                                            logger.warning(f"Position {position.symbol} not found on exchange, marking as closed")
                                            await self.db.positions.close_position(
                                                position.id,
                                                market.last_price,
                                                position.unrealized_pnl or 0,
                                                CloseReason.NOT_FOUND
                                            )
                                            continue
                                        
                                        success, _, error = await self.exchange.close_position(
                                            position,
                                            reason="EMERGENCY"
                                        )

                                        if success:
                                            await self.db.positions.close_position(
                                                position.id,
                                                market.last_price,
                                                position.unrealized_pnl or 0,
                                                CloseReason.EMERGENCY
                                            )

                        except Exception as e:
                            logger.error(f"Error monitoring position {position.symbol}: {e}")

                # Wait before next check
                await asyncio.sleep(10)  # Check every 10 seconds

            except Exception as e:
                logger.error(f"Error in position monitor: {e}")
                await asyncio.sleep(30)  # Wait longer on error

    async def listen_for_predictions(self):
        """Listen for new predictions via PostgreSQL NOTIFY"""
        logger.info("Starting prediction listener...")

        async def handle_notification(connection, pid, channel, payload):
            """Handle incoming prediction notification"""
            try:
                logger.info(f"Received notification on {channel}: {payload}")

                # Parse notification payload
                data = json.loads(payload)
                prediction_id = data.get('prediction_id')

                if prediction_id:
                    # Fetch the full signal data
                    query = """
                        SELECT 
                            p.id as prediction_id,
                            p.signal_id,
                            p.prediction,
                            p.prediction_proba,
                            p.confidence_level,
                            p.signal_type,
                            p.created_at,
                            s.pair_symbol,
                            s.trading_pair_id,
                            tp.exchange_id,
                            CASE 
                                WHEN tp.exchange_id = 1 THEN 'binance'
                                WHEN tp.exchange_id = 2 THEN 'bybit'
                                ELSE 'unknown'
                            END as exchange_name
                        FROM smart_ml.predictions p
                        JOIN fas.scoring_history s ON s.id = p.signal_id
                        JOIN public.trading_pairs tp ON tp.id = s.trading_pair_id
                        WHERE p.id = $1
                        AND tp.contract_type_id = 1
                    """

                    row = await self.db.pool.fetchrow(query, prediction_id)

                    if row:
                        signal = TradingSignal(
                            prediction_id=row['prediction_id'],
                            signal_id=row['signal_id'],
                            prediction=row['prediction'],
                            prediction_proba=row['prediction_proba'],
                            confidence_level=row['confidence_level'],
                            signal_type=row['signal_type'],
                            created_at=row['created_at'],
                            pair_symbol=row['pair_symbol'],
                            trading_pair_id=row['trading_pair_id'],
                            exchange_id=row['exchange_id'],
                            exchange_name=row['exchange_name']
                        )

                        # Process signal immediately
                        logger.info(f"Processing real-time signal for {signal.pair_symbol} on {signal.exchange_name}")
                        await self.signal_processor.process_signal(signal)

            except Exception as e:
                logger.error(f"Error handling prediction notification: {e}")

        # Listen to the channel
        await self.db.listen_for_predictions(handle_notification)

        # Keep the listener alive
        while self._running:
            await asyncio.sleep(1)

    async def cleanup_task(self):
        """Periodic cleanup tasks"""
        logger.info("Starting cleanup task...")

        while self._running:
            try:
                # Clean up old duplicate protection entries
                await self.signal_processor.cleanup()

                # Update daily metrics
                await self.db.metrics.update_daily_metrics()

                # Log current stats
                metrics = await self.db.metrics.get_today_metrics()
                if metrics:
                    logger.info(f"Today's metrics: "
                                f"Signals: {metrics.get('executed_signals', 0)}, "
                                f"Positions: {metrics.get('opened_positions', 0)}, "
                                f"PNL: ${metrics.get('net_pnl', 0):.2f}")

                # Wait 5 minutes
                await asyncio.sleep(300)

            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(60)

    async def run(self):
        """Main run loop"""
        try:
            # Initialize system
            await self.initialize()

            self._running = True

            # Start all tasks
            tasks = [
                asyncio.create_task(self.monitor_signals()),
                asyncio.create_task(self.monitor_positions()),
                asyncio.create_task(self.listen_for_predictions()),
                asyncio.create_task(self.cleanup_task())
            ]
            self._tasks = tasks

            logger.info("=" * 60)
            logger.info("System is running. Press Ctrl+C to stop.")
            logger.info("=" * 60)

            # Wait for shutdown signal
            await self._shutdown_event.wait()

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Initiating graceful shutdown...")

        self._running = False

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close connections
        if self.exchange:
            await self.exchange.close()

        if self.db:
            await self.db.close()

        logger.info("Shutdown complete")


async def main():
    """Main entry point"""
    system = ATSSystem()
    await system.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"System error: {e}")
        sys.exit(1)