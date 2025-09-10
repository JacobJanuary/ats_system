#!/usr/bin/env python3
"""
Protection Monitor - PRODUCTION READY v2.0
- Унифицирован коннектор Bybit (полностью асинхронный)
- Оптимизировано количество запросов к API
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv
import functools

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# <<< ИЗМЕНЕНИЕ: Импортируем наши кастомные асинхронные коннекторы >>>
from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

# <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

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
        self.stop_loss_type = os.getenv('STOP_LOSS_TYPE', 'fixed').lower()
        self.sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.tp_percent = float(os.getenv('TAKE_PROFIT_PERCENT', '3'))
        self.trailing_activation = float(os.getenv('TRAILING_ACTIVATION_PERCENT', '3.5'))
        self.trailing_callback = float(os.getenv('TRAILING_CALLBACK_RATE', '0.5'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.max_position_duration_hours = int(os.getenv('MAX_POSITION_DURATION_HOURS', '0'))
        self.taker_fee_percent = float(os.getenv('TAKER_FEE_PERCENT', '0.06'))
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'

        # <<< ИЗМЕНЕНИЕ: Унифицированные коннекторы >>>
        self.binance: Optional[BinanceExchange] = None
        self.bybit: Optional[BybitExchange] = None
        # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

        # Statistics
        self.stats = {
            'checks': 0,
            'positions_protected': 0,
            'positions_closed': 0,
            'errors': 0,
            'start_time': datetime.now(timezone.utc)
        }

        # <<< ИЗМЕНЕНИЕ: Убран семафор, т.к. оптимизация запросов снижает нагрузку >>>
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
            return entry_price * (1 + 2 * fee_multiplier)
        else:
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

            # <<< ИЗМЕНЕНИЕ: Инициализация асинхронного коннектора Bybit >>>
            if os.getenv('BYBIT_API_KEY'):
                self.bybit = BybitExchange({
                    'api_key': os.getenv('BYBIT_API_KEY'),
                    'api_secret': os.getenv('BYBIT_API_SECRET'),
                    'testnet': self.testnet
                })
                await self.bybit.initialize()
                balance = await self.bybit.get_balance()
                logger.info(f"✅ Bybit connected - Balance: ${balance:.2f}")
            else:
                logger.warning("⚠️ Bybit API keys not configured")
            # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

            if not self.binance and not self.bybit:
                raise Exception("No exchanges configured!")

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    # <<< ИЗМЕНЕНИЕ: Логика для Bybit теперь асинхронная и использует BybitExchange >>>
    async def _handle_position_duration_limit(self, exchange_name: str, position: Dict,
                                              open_orders: List[Dict]) -> bool:
        """Handle positions that exceed duration limit"""
        symbol = position['symbol']
        side = position.get('side', 'LONG')
        entry_price = position.get('entry_price', 0)
        quantity = position.get('quantity', 0)
        pnl = position.get('pnl', 0)
        exchange = self.binance if exchange_name == 'Binance' else self.bybit

        if not exchange: return False

        breakeven_price = self._calculate_breakeven_price(entry_price, side)
        logger.warning(f"⏰ {exchange_name} {symbol} exceeded max duration")
        logger.info(f"   PnL: ${pnl:.2f}, Breakeven: ${breakeven_price:.4f}")

        try:
            # Используем tickSize для корректного сравнения цен
            tick_size = float(exchange.symbol_info.get(symbol, {}).get('tick_size', 0.0001))

            be_order_exists = any(
                o.get('type' if exchange_name == 'Binance' else 'orderType') == 'LIMIT' and
                abs(float(o.get('price', 0)) - breakeven_price) < tick_size
                for o in open_orders
            )

            if be_order_exists:
                logger.info(f"   Breakeven order already exists")
                return False

            if pnl > 0:
                logger.info(f"   Closing profitable position by market order")
                await exchange.close_position(symbol)
                self.stats['positions_closed'] += 1
                return True
            else:
                logger.info(f"   Setting breakeven limit order")
                await exchange.cancel_all_orders(symbol)
                await exchange.create_limit_order(
                    symbol,
                    "SELL" if side in ["LONG", "BUY"] else "BUY",
                    quantity,
                    breakeven_price,
                    reduce_only=True
                )
                return True
        except Exception as e:
            logger.error(f"Failed to handle duration limit for {symbol}: {e}")
            self.stats['errors'] += 1
        return False

    async def _process_binance_position(self, pos: Dict, orders_by_symbol: Dict[str, List[Dict]]):
        """Process single Binance position"""
        try:
            symbol = pos['symbol']
            side = pos['side']
            entry_price = pos['entry_price']
            open_orders = orders_by_symbol.get(symbol, [])
            update_time = pos.get('updateTime', 0)

            if self.max_position_duration_hours > 0 and update_time > 0:
                age_hours = (datetime.now(timezone.utc).timestamp() * 1000 - update_time) / 3600000
                if age_hours > self.max_position_duration_hours:
                    await self._handle_position_duration_limit('Binance', pos, open_orders)
                    return

            has_sl = any(o.get('type') == 'STOP_MARKET' for o in open_orders)
            has_tp = any(o.get('type') == 'TAKE_PROFIT_MARKET' for o in open_orders)
            has_ts = any(o.get('type') == 'TRAILING_STOP_MARKET' for o in open_orders)
            protection_needed = False

            if self.stop_loss_type == 'trailing':
                if not has_ts:
                    logger.warning(f"⚠️ Binance {symbol} missing TRAILING STOP")
                    ticker = await self.binance.get_ticker(symbol)
                    current_price = ticker.get('price', entry_price)
                    activation_price = max(entry_price * (1 + self.trailing_activation / 100),
                                           current_price * 1.01) if side == 'LONG' else min(
                        entry_price * (1 - self.trailing_activation / 100), current_price * 0.99)
                    await self.binance.set_trailing_stop(symbol, activation_price, self.trailing_callback)
                    protection_needed = True
                if not has_sl:  # Backup SL
                    logger.warning(f"⚠️ Binance {symbol} missing STOP LOSS (backup)")
                    sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'LONG' else entry_price * (
                                1 + self.sl_percent / 100)
                    await self.binance.set_stop_loss(symbol, sl_price)
                    protection_needed = True
            else:  # Fixed SL/TP
                if not has_sl:
                    logger.warning(f"⚠️ Binance {symbol} missing STOP LOSS")
                    sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'LONG' else entry_price * (
                                1 + self.sl_percent / 100)
                    await self.binance.set_stop_loss(symbol, sl_price)
                    protection_needed = True
                if not has_tp:
                    logger.warning(f"⚠️ Binance {symbol} missing TAKE PROFIT")
                    tp_price = entry_price * (1 + self.tp_percent / 100) if side == 'LONG' else entry_price * (
                                1 - self.tp_percent / 100)
                    await self.binance.set_take_profit(symbol, tp_price)
                    protection_needed = True

            if protection_needed:
                self.stats['positions_protected'] += 1
                logger.info(f"✅ Protected Binance position: {symbol}")

        except Exception as e:
            logger.error(f"Error processing Binance position {pos.get('symbol', 'UNKNOWN')}: {e}")
            self.stats['errors'] += 1

    async def _process_bybit_position(self, pos: Dict, orders_by_symbol: Dict[str, List[Dict]]):
        """Process single Bybit position asynchronously"""
        try:
            symbol = pos['symbol']
            side = pos['side']  # 'BUY' or 'SELL'
            entry_price = pos['entry_price']
            open_orders = orders_by_symbol.get(symbol, [])
            created_time = pos.get('created_time', 0)

            # Bybit API returns SL/TP info with the position, which is very handy
            has_sl = pos.get('stopLoss') and str(pos.get('stopLoss')) not in ['', '0']
            has_tp = pos.get('takeProfit') and str(pos.get('takeProfit')) not in ['', '0']
            has_ts = pos.get('trailingStop') and str(pos.get('trailingStop')) not in ['', '0']

            if self.max_position_duration_hours > 0 and created_time > 0:
                age_hours = (datetime.now(timezone.utc).timestamp() * 1000 - created_time) / 3600000
                if age_hours > self.max_position_duration_hours:
                    await self._handle_position_duration_limit('Bybit', pos, open_orders)
                    return

            protection_needed = False

            if self.stop_loss_type == 'trailing':
                if not has_ts:
                    logger.warning(f"⚠️ Bybit {symbol} missing TRAILING STOP")
                    ticker = await self.bybit.get_ticker(symbol)
                    current_price = ticker.get('price', entry_price)
                    activation_price = max(entry_price * (1 + self.trailing_activation / 100),
                                           current_price * 1.01) if side == 'BUY' else min(
                        entry_price * (1 - self.trailing_activation / 100), current_price * 0.99)
                    await self.bybit.set_trailing_stop(symbol, activation_price, self.trailing_callback)
                    protection_needed = True
                if not has_sl:  # Backup SL
                    logger.warning(f"⚠️ Bybit {symbol} missing STOP LOSS (backup)")
                    sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'BUY' else entry_price * (
                                1 + self.sl_percent / 100)
                    await self.bybit.set_stop_loss(symbol, sl_price)
                    protection_needed = True
            else:  # Fixed SL/TP
                if not has_sl:
                    logger.warning(f"⚠️ Bybit {symbol} missing STOP LOSS")
                    sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'BUY' else entry_price * (
                                1 + self.sl_percent / 100)
                    await self.bybit.set_stop_loss(symbol, sl_price)
                    protection_needed = True
                if not has_tp:
                    logger.warning(f"⚠️ Bybit {symbol} missing TAKE PROFIT")
                    tp_price = entry_price * (1 + self.tp_percent / 100) if side == 'BUY' else entry_price * (
                                1 - self.tp_percent / 100)
                    await self.bybit.set_take_profit(symbol, tp_price)
                    protection_needed = True

            if protection_needed:
                self.stats['positions_protected'] += 1
                logger.info(f"✅ Protected Bybit position: {symbol}")

        except Exception as e:
            logger.error(f"Error processing Bybit position {pos.get('symbol', 'UNKNOWN')}: {e}")
            self.stats['errors'] += 1

    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

    # <<< ИЗМЕНЕНИЕ: Оптимизированная логика с единичными запросами >>>
    async def protect_binance_positions(self):
        """Protect all Binance positions"""
        if not self.binance: return
        try:
            positions = await self.binance.get_open_positions()
            if not positions:
                logger.debug("No open Binance positions")
                return
            logger.info(f"Found {len(positions)} Binance positions")

            # Оптимизация: получаем все ордера одним запросом
            all_open_orders = await self.binance.get_open_orders()
            orders_by_symbol = {}
            for order in all_open_orders:
                symbol = order['symbol']
                if symbol not in orders_by_symbol:
                    orders_by_symbol[symbol] = []
                orders_by_symbol[symbol].append(order)

            tasks = [self._process_binance_position(pos, orders_by_symbol) for pos in positions]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Critical error in protect_binance_positions: {e}")
            self.stats['errors'] += 1

    async def protect_bybit_positions(self):
        """Protect all Bybit positions"""
        if not self.bybit: return
        try:
            # Bybit API отдает SL/TP вместе с позицией, отдельный запрос ордеров не нужен
            positions = await self.bybit.get_open_positions()
            if not positions:
                logger.debug("No open Bybit positions")
                return
            logger.info(f"Found {len(positions)} Bybit positions")

            # В Bybit нет необходимости в отдельном запросе ордеров для проверки SL/TP
            # но он нужен для _handle_position_duration_limit
            all_open_orders = await self.bybit.get_open_orders()
            orders_by_symbol = {}
            for order in all_open_orders:
                symbol = order['symbol']
                if symbol not in orders_by_symbol:
                    orders_by_symbol[symbol] = []
                orders_by_symbol[symbol].append(order)

            tasks = [self._process_bybit_position(pos, orders_by_symbol) for pos in positions]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Critical error in protect_bybit_positions: {e}")
            self.stats['errors'] += 1

    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

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
        logger.info("🚀 Starting Protection Monitor v2.0")
        await self.initialize()
        try:
            while True:
                try:
                    self.stats['checks'] += 1
                    logger.info(f"=== Protection Check #{self.stats['checks']} ===")
                    await asyncio.gather(
                        self.protect_binance_positions(),
                        self.protect_bybit_positions()
                    )
                    if self.stats['checks'] % 10 == 0:
                        await self.print_statistics()
                    await asyncio.sleep(self.check_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    self.stats['errors'] += 1
                    await asyncio.sleep(5)
        except KeyboardInterrupt:
            logger.info("⛔ Shutdown signal received")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up...")
        await self.print_statistics()
        if self.binance:
            await self.binance.close()
            logger.info("Binance connection closed")
        # <<< ИЗМЕНЕНИЕ: Закрываем асинхронный Bybit >>>
        if self.bybit:
            await self.bybit.close()
            logger.info("Bybit connection closed")
        # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>
        logger.info("✅ Cleanup complete")


async def main():
    """Entry point"""
    monitor = ProtectionMonitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())