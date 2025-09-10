#!/usr/bin/env python3
"""
Protection Monitor - PRODUCTION READY VERSION
Monitors and protects all open positions with SL/TP/Trailing stops
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
import functools

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from exchanges.binance import BinanceExchange

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler('protection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProtectionMonitor:
    """
    Production-ready protection monitor for trading positions
    Handles stop-loss, take-profit, trailing stops, and position duration limits
    """

    def __init__(self):
        # Configuration
        self.stop_loss_type = os.getenv('STOP_LOSS_TYPE', 'fixed').lower()  # 'fixed' or 'trailing'
        self.sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.tp_percent = float(os.getenv('TAKE_PROFIT_PERCENT', '3'))
        self.trailing_activation = float(os.getenv('TRAILING_ACTIVATION_PERCENT', '3.5'))
        self.trailing_callback = float(os.getenv('TRAILING_CALLBACK_RATE', '0.5'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.max_position_duration_hours = int(os.getenv('MAX_POSITION_DURATION_HOURS', '0'))
        self.taker_fee_percent = float(os.getenv('TAKER_FEE_PERCENT', '0.06'))
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'

        # Exchange connections
        self.binance = None
        self.bybit_client = None

        # Statistics
        self.stats = {
            'checks': 0,
            'positions_protected': 0,
            'positions_closed': 0,
            'errors': 0,
            'start_time': datetime.now(timezone.utc)
        }

        # Rate limiting semaphores
        self.binance_semaphore = asyncio.Semaphore(10)  # 10 concurrent Binance requests
        self.bybit_semaphore = asyncio.Semaphore(5)  # 5 concurrent Bybit requests

        self._log_configuration()

    def _log_configuration(self):
        """Log current configuration"""
        logger.info("=" * 60)
        logger.info("Protection Monitor Configuration")
        logger.info("=" * 60)
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'PRODUCTION'}")
        logger.info(f"Stop Loss Type: {self.stop_loss_type.upper()}")
        logger.info(f"Stop Loss: {self.sl_percent}%")
        logger.info(f"Take Profit: {self.tp_percent}%")

        if self.stop_loss_type == 'trailing':
            logger.info(f"Trailing Activation: {self.trailing_activation}%")
            logger.info(f"Trailing Callback: {self.trailing_callback}%")

        if self.max_position_duration_hours > 0:
            logger.info(f"Max Position Duration: {self.max_position_duration_hours} hours")
            logger.info(f"Taker Fee: {self.taker_fee_percent}%")

        logger.info(f"Check Interval: {self.check_interval} seconds")
        logger.info("=" * 60)

    def _calculate_breakeven_price(self, entry_price: float, side: str) -> float:
        """Calculate breakeven price including fees"""
        fee_multiplier = self.taker_fee_percent / 100

        if side.upper() in ['LONG', 'BUY']:
            # For long: need price to rise to cover entry and exit fees
            return entry_price * (1 + 2 * fee_multiplier)
        else:
            # For short: need price to fall to cover entry and exit fees
            return entry_price * (1 - 2 * fee_multiplier)

    async def initialize(self):
        """Initialize exchange connections"""
        try:
            # Initialize Binance
            if os.getenv('BINANCE_API_KEY'):
                self.binance = BinanceExchange({
                    'api_key': os.getenv('BINANCE_API_KEY'),
                    'api_secret': os.getenv('BINANCE_API_SECRET'),
                    'testnet': self.testnet
                })
                await self.binance.initialize()
                balance = await self.binance.get_balance()
                logger.info(f"✅ Binance connected - Balance: ${balance:.2f}")
            else:
                logger.warning("⚠️ Binance API keys not configured")

            # Initialize Bybit
            if os.getenv('BYBIT_API_KEY'):
                self.bybit_client = HTTP(
                    testnet=self.testnet,
                    api_key=os.getenv('BYBIT_API_KEY'),
                    api_secret=os.getenv('BYBIT_API_SECRET')
                )
                # Test connection
                response = self.bybit_client.get_wallet_balance(accountType="UNIFIED")
                if response['retCode'] == 0:
                    logger.info("✅ Bybit connected")
                else:
                    raise Exception(f"Bybit connection failed: {response['retMsg']}")
            else:
                logger.warning("⚠️ Bybit API keys not configured")

            if not self.binance and not self.bybit_client:
                raise Exception("No exchanges configured!")

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    async def _handle_position_duration_limit(self, exchange: str, position: Dict) -> bool:
        """Handle positions that exceed duration limit"""
        symbol = position['symbol']
        side = position.get('side', 'LONG')
        entry_price = position.get('entry_price', 0)
        quantity = position.get('quantity', 0)
        pnl = position.get('pnl', 0)

        breakeven_price = self._calculate_breakeven_price(entry_price, side)

        logger.warning(f"⏰ {exchange} {symbol} exceeded max duration")
        logger.info(f"   PnL: ${pnl:.2f}, Breakeven: ${breakeven_price:.4f}")

        try:
            if exchange == 'Binance' and self.binance:
                # Check for existing breakeven order
                open_orders = await self.binance.get_open_orders(symbol)
                be_order_exists = any(
                    o.get('type') == 'LIMIT' and
                    abs(float(o.get('price', 0)) - breakeven_price) < 0.01
                    for o in open_orders
                )

                if be_order_exists:
                    logger.info(f"   Breakeven order already exists")
                    return False

                if pnl > 0:
                    logger.info(f"   Closing profitable position by market order")
                    await self.binance.close_position(symbol)
                    self.stats['positions_closed'] += 1
                    return True
                else:
                    logger.info(f"   Setting breakeven limit order")
                    await self.binance.cancel_all_open_orders(symbol)
                    await self.binance.create_limit_order(
                        symbol,
                        "SELL" if side == "LONG" else "BUY",
                        quantity,
                        breakeven_price,
                        reduce_only=True
                    )
                    return True

            elif exchange == 'Bybit' and self.bybit_client:
                # Similar logic for Bybit
                response = self.bybit_client.get_open_orders(
                    category="linear",
                    symbol=symbol
                )

                if response['retCode'] == 0:
                    orders = response.get('result', {}).get('list', [])
                    be_order_exists = any(
                        o.get('orderType') == 'Limit' and
                        abs(float(o.get('price', 0)) - breakeven_price) < 0.01
                        for o in orders
                    )

                    if be_order_exists:
                        logger.info(f"   Breakeven order already exists")
                        return False

                    if pnl > 0:
                        logger.info(f"   Closing profitable position by market order")
                        self.bybit_client.place_order(
                            category="linear",
                            symbol=symbol,
                            side="Sell" if side == "Buy" else "Buy",
                            orderType="Market",
                            qty=str(quantity),
                            reduceOnly=True
                        )
                        self.stats['positions_closed'] += 1
                        return True
                    else:
                        logger.info(f"   Setting breakeven limit order")
                        self.bybit_client.cancel_all_orders(
                            category="linear",
                            symbol=symbol
                        )
                        self.bybit_client.place_order(
                            category="linear",
                            symbol=symbol,
                            side="Sell" if side == "Buy" else "Buy",
                            orderType="Limit",
                            qty=str(quantity),
                            price=str(f"{breakeven_price:.5f}"),
                            reduceOnly=True
                        )
                        return True

        except Exception as e:
            logger.error(f"Failed to handle duration limit for {symbol}: {e}")
            self.stats['errors'] += 1

        return False

    async def _process_binance_position(self, pos: Dict):
        """Process single Binance position"""
        async with self.binance_semaphore:
            try:
                if pos.get('quantity', 0) <= 0:
                    return

                symbol = pos['symbol']
                side = pos['side']
                entry_price = pos['entry_price']
                quantity = pos['quantity']
                update_time = pos.get('updateTime', 0)

                # Check position duration
                if self.max_position_duration_hours > 0 and update_time > 0:
                    age_hours = (datetime.now(timezone.utc).timestamp() * 1000 - update_time) / 3600000
                    if age_hours > self.max_position_duration_hours:
                        await self._handle_position_duration_limit('Binance', pos)
                        return

                # Get current orders
                open_orders = await self.binance.get_open_orders(symbol)
                has_sl = any(o.get('type') == 'STOP_MARKET' for o in open_orders)
                has_tp = any(o.get('type') == 'TAKE_PROFIT_MARKET' for o in open_orders)
                has_ts = any(o.get('type') == 'TRAILING_STOP_MARKET' for o in open_orders)

                protection_needed = False

                if self.stop_loss_type == 'trailing':
                    # Trailing stop configuration
                    if not has_ts:
                        logger.warning(f"⚠️ Binance {symbol} missing TRAILING STOP")
                        ticker = await self.binance.get_ticker(symbol)
                        current_price = ticker.get('price', entry_price)

                        # Calculate activation price
                        if side == 'LONG':
                            activation_price = max(
                                entry_price * (1 + self.trailing_activation / 100),
                                current_price * 1.01
                            )
                        else:
                            activation_price = min(
                                entry_price * (1 - self.trailing_activation / 100),
                                current_price * 0.99
                            )

                        await self.binance.set_trailing_stop(
                            symbol,
                            activation_price,
                            self.trailing_callback
                        )
                        protection_needed = True

                    # Always have a stop loss as backup
                    if not has_sl:
                        logger.warning(f"⚠️ Binance {symbol} missing STOP LOSS (backup)")
                        sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'LONG' \
                            else entry_price * (1 + self.sl_percent / 100)
                        await self.binance.set_stop_loss(symbol, sl_price)
                        protection_needed = True

                else:  # Fixed SL/TP
                    if not has_sl:
                        logger.warning(f"⚠️ Binance {symbol} missing STOP LOSS")
                        sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'LONG' \
                            else entry_price * (1 + self.sl_percent / 100)
                        await self.binance.set_stop_loss(symbol, sl_price)
                        protection_needed = True

                    if not has_tp:
                        logger.warning(f"⚠️ Binance {symbol} missing TAKE PROFIT")
                        tp_price = entry_price * (1 + self.tp_percent / 100) if side == 'LONG' \
                            else entry_price * (1 - self.tp_percent / 100)
                        await self.binance.set_take_profit(symbol, tp_price)
                        protection_needed = True

                if protection_needed:
                    self.stats['positions_protected'] += 1
                    logger.info(f"✅ Protected Binance position: {symbol}")

            except Exception as e:
                logger.error(f"Error processing Binance position {pos.get('symbol', 'UNKNOWN')}: {e}")
                self.stats['errors'] += 1

    async def _process_bybit_position(self, pos: Dict):
        """Process single Bybit position asynchronously"""
        try:
            if float(pos.get('size', 0)) <= 0:
                return

            symbol = pos['symbol']
            side = pos['side']
            entry_price = float(pos['avgPrice'])
            size = pos['size']
            created_time = int(pos.get('createdTime', 0))

            # Check position duration
            if self.max_position_duration_hours > 0 and created_time > 0:
                age_hours = (datetime.now(timezone.utc).timestamp() * 1000 - created_time) / 3600000
                if age_hours > self.max_position_duration_hours:
                    pos_dict = {
                        'symbol': symbol,
                        'side': side.upper(),
                        'entry_price': entry_price,
                        'quantity': float(size),
                        'pnl': float(pos.get('unrealisedPnl', 0))
                    }
                    await self._handle_position_duration_limit('Bybit', pos_dict)
                    return

            # Check existing protection
            has_sl = pos.get('stopLoss') and str(pos.get('stopLoss')) not in ['', '0']
            has_tp = pos.get('takeProfit') and str(pos.get('takeProfit')) not in ['', '0']
            has_ts = pos.get('trailingStop') and str(pos.get('trailingStop')) not in ['', '0']

            protection_needed = False

            # Run Bybit operations in executor to avoid blocking
            loop = asyncio.get_event_loop()

            if self.stop_loss_type == 'trailing':
                if not has_ts:
                    logger.warning(f"⚠️ Bybit {symbol} missing TRAILING STOP")
                    await loop.run_in_executor(
                        None,
                        self._set_bybit_trailing_stop,
                        symbol, entry_price, side
                    )
                    protection_needed = True

                if not has_sl:
                    logger.warning(f"⚠️ Bybit {symbol} missing STOP LOSS (backup)")
                    await loop.run_in_executor(
                        None,
                        self._set_bybit_stop_loss,
                        symbol, entry_price, side
                    )
                    protection_needed = True
            else:
                if not has_sl:
                    logger.warning(f"⚠️ Bybit {symbol} missing STOP LOSS")
                    await loop.run_in_executor(
                        None,
                        self._set_bybit_stop_loss,
                        symbol, entry_price, side
                    )
                    protection_needed = True

                if not has_tp:
                    logger.warning(f"⚠️ Bybit {symbol} missing TAKE PROFIT")
                    await loop.run_in_executor(
                        None,
                        self._set_bybit_take_profit,
                        symbol, entry_price, side
                    )
                    protection_needed = True

            if protection_needed:
                self.stats['positions_protected'] += 1
                logger.info(f"✅ Protected Bybit position: {symbol}")

        except Exception as e:
            logger.error(f"Error processing Bybit position {pos.get('symbol', 'UNKNOWN')}: {e}")
            self.stats['errors'] += 1

    def _set_bybit_stop_loss(self, symbol: str, entry_price: float, side: str):
        """Set Bybit stop loss (synchronous)"""
        sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'Buy' \
            else entry_price * (1 + self.sl_percent / 100)

        logger.info(f"   Setting SL at {sl_price:.5f}")
        self.bybit_client.set_trading_stop(
            category="linear",
            symbol=symbol,
            stopLoss=str(f"{sl_price:.5f}"),
            positionIdx=0
        )

    def _set_bybit_take_profit(self, symbol: str, entry_price: float, side: str):
        """Set Bybit take profit (synchronous)"""
        tp_price = entry_price * (1 + self.tp_percent / 100) if side == 'Buy' \
            else entry_price * (1 - self.tp_percent / 100)

        logger.info(f"   Setting TP at {tp_price:.5f}")
        self.bybit_client.set_trading_stop(
            category="linear",
            symbol=symbol,
            takeProfit=str(f"{tp_price:.5f}"),
            positionIdx=0
        )

    def _set_bybit_trailing_stop(self, symbol: str, entry_price: float, side: str):
        """Set Bybit trailing stop (synchronous)"""
        trailing_stop_value = entry_price * (self.trailing_callback / 100)

        # Get current price
        ticker_resp = self.bybit_client.get_tickers(category="linear", symbol=symbol)
        current_price = entry_price
        if ticker_resp.get('retCode') == 0 and ticker_resp.get('result', {}).get('list'):
            current_price = float(ticker_resp['result']['list'][0]['lastPrice'])

        # Calculate activation price
        if side == 'Buy':
            activation_price = max(
                entry_price * (1 + self.trailing_activation / 100),
                current_price * 1.01
            )
        else:
            activation_price = min(
                entry_price * (1 - self.trailing_activation / 100),
                current_price * 0.99
            )

        logger.info(f"   Setting trailing stop: activation={activation_price:.5f}, callback={trailing_stop_value:.5f}")
        self.bybit_client.set_trading_stop(
            category="linear",
            symbol=symbol,
            trailingStop=str(f"{trailing_stop_value:.5f}"),
            activePrice=str(f"{activation_price:.5f}"),
            positionIdx=0
        )

    async def protect_binance_positions(self):
        """Protect all Binance positions"""
        if not self.binance:
            return

        try:
            positions = await self.binance.get_open_positions()
            if not positions:
                logger.debug("No open Binance positions")
                return

            logger.info(f"Found {len(positions)} Binance positions")

            # Process positions concurrently
            tasks = [self._process_binance_position(pos) for pos in positions]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count errors
            error_count = sum(1 for r in results if isinstance(r, Exception))
            if error_count > 0:
                logger.warning(f"Binance processing: {error_count} errors")

        except Exception as e:
            logger.error(f"Critical error in protect_binance_positions: {e}")
            self.stats['errors'] += 1

    async def protect_bybit_positions(self):
        """Protect all Bybit positions"""
        if not self.bybit_client:
            return

        try:
            response = self.bybit_client.get_positions(
                category="linear",
                settleCoin="USDT"
            )

            if response['retCode'] != 0:
                logger.error(f"Failed to get Bybit positions: {response['retMsg']}")
                return

            positions = response['result']['list']
            if not positions:
                logger.debug("No open Bybit positions")
                return

            logger.info(f"Found {len(positions)} Bybit positions")

            # Process positions concurrently with semaphore
            async with self.bybit_semaphore:
                tasks = [self._process_bybit_position(pos) for pos in positions]
                results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count errors
            error_count = sum(1 for r in results if isinstance(r, Exception))
            if error_count > 0:
                logger.warning(f"Bybit processing: {error_count} errors")

        except Exception as e:
            logger.error(f"Critical error in protect_bybit_positions: {e}")
            self.stats['errors'] += 1

    async def print_statistics(self):
        """Print performance statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        hours = uptime.total_seconds() / 3600

        logger.info("=" * 60)
        logger.info("Performance Statistics")
        logger.info("=" * 60)
        logger.info(f"Uptime: {hours:.2f} hours")
        logger.info(f"Checks performed: {self.stats['checks']}")
        logger.info(f"Positions protected: {self.stats['positions_protected']}")
        logger.info(f"Positions closed: {self.stats['positions_closed']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)

    async def run(self):
        """Main monitoring loop"""
        logger.info("🚀 Starting Protection Monitor")
        await self.initialize()

        try:
            while True:
                try:
                    self.stats['checks'] += 1
                    logger.info(f"=== Protection Check #{self.stats['checks']} ===")

                    # Run protection for both exchanges concurrently
                    await asyncio.gather(
                        self.protect_binance_positions(),
                        self.protect_bybit_positions(),
                        return_exceptions=True
                    )

                    # Print statistics every 10 checks
                    if self.stats['checks'] % 10 == 0:
                        await self.print_statistics()

                    await asyncio.sleep(self.check_interval)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    self.stats['errors'] += 1
                    await asyncio.sleep(5)  # Short delay before retry

        except KeyboardInterrupt:
            logger.info("⛔ Shutdown signal received")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up...")

        # Print final statistics
        await self.print_statistics()

        # Close connections
        if self.binance:
            await self.binance.close()
            logger.info("Binance connection closed")

        logger.info("✅ Cleanup complete")


async def main():
    """Entry point"""
    monitor = ProtectionMonitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())