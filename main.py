#!/usr/bin/env python3
"""
ATS 2.0 - Automated Trading System
Main entry point for the trading system
"""
import asyncio
import signal
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional
import json

from core.config import SystemConfig
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

        # Log configuration
        logger.info(f"Configuration: {json.dumps(self.config.to_dict(), indent=2)}")

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

        # NOTE: Currently only Binance is fully implemented
        # Signals are routed based on exchange_id from trading_pairs table:
        # exchange_id=1 -> Binance (implemented)
        # exchange_id=2 -> Bybit (stub, signals are skipped)
        self.exchange = BinanceExchange(exchange_config)
        await self.exchange.initialize()
        logger.info(f"Exchange initialized ({'Testnet' if self.config.exchange.testnet else 'Production'})")

        # Check exchange connectivity
        balances = await self.exchange.get_account_balance()
        if balances:
            for asset, balance in balances.items():
                logger.info(f"Balance: {asset} = {balance.total}")

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
                # Get unprocessed signals
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
        """Monitor open positions"""
        logger.info("Starting position monitor...")

        while self._running:
            try:
                # Get open positions
                positions = await self.db.positions.get_open_positions()

                if positions:
                    logger.debug(f"Monitoring {len(positions)} open positions")

                    for position in positions:
                        try:
                            # Get current market data
                            market = await self.exchange.get_market_data(position.symbol)
                            if market:
                                # Update position price
                                await self.db.positions.update_position_price(
                                    position.id,
                                    market.last_price
                                )

                                # Check for timeout (if enabled)
                                if self.config.risk.auto_close_positions:
                                    position_age = datetime.utcnow() - position.opened_at
                                    max_duration = timedelta(hours=self.config.risk.max_position_duration_hours)

                                    if position_age > max_duration:
                                        logger.warning(f"Position {position.symbol} exceeded max duration, closing...")
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

                                # Check emergency stop loss
                                if position.unrealized_pnl_percent:
                                    if abs(position.unrealized_pnl_percent) > self.config.risk.emergency_stop_loss_percent:
                                        logger.warning(f"Emergency stop for {position.symbol}: "
                                                       f"PNL {position.unrealized_pnl_percent:.2f}%")
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