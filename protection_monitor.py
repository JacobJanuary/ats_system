#!/usr/bin/env python3
"""
Protection Monitor - PRODUCTION READY v2.5 (Stable Logic)
- Введена строгая иерархия: позиция либо обрабатывается по таймауту, либо защищается.
- Устранен конфликт, вызывавший постоянное создание/удаление ордеров.
- Логика стала полностью предсказуемой.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

load_dotenv()

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

        self.binance: Optional[BinanceExchange] = None
        self.bybit: Optional[BybitExchange] = None

        self.stats = {
            'checks': 0, 'positions_protected': 0, 'positions_closed': 0,
            'errors': 0, 'start_time': datetime.now(timezone.utc)
        }

        self.binance_semaphore = asyncio.Semaphore(10)
        self.bybit_semaphore = asyncio.Semaphore(5)

        self._log_configuration()

    def _log_configuration(self):
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
        fee_multiplier = self.taker_fee_percent / 100
        return entry_price * (1 + 2 * fee_multiplier) if side.upper() in ['LONG', 'BUY'] else entry_price * (
                    1 - 2 * fee_multiplier)

    async def initialize(self):
        try:
            if os.getenv('BINANCE_API_KEY'):
                self.binance = BinanceExchange(
                    {'api_key': os.getenv('BINANCE_API_KEY'), 'api_secret': os.getenv('BINANCE_API_SECRET'),
                     'testnet': self.testnet})
                await self.binance.initialize()
                logger.info(f"✅ Binance connected - Balance: ${await self.binance.get_balance():.2f}")

            if os.getenv('BYBIT_API_KEY'):
                self.bybit = BybitExchange(
                    {'api_key': os.getenv('BYBIT_API_KEY'), 'api_secret': os.getenv('BYBIT_API_SECRET'),
                     'testnet': self.testnet})
                await self.bybit.initialize()
                logger.info(f"✅ Bybit connected - Balance: ${await self.bybit.get_balance():.2f}")

            if not self.binance and not self.bybit:
                raise Exception("No exchanges configured!")
        except Exception as e:
            logger.error(f"Initialization failed: {e}", exc_info=True)
            raise

    # <<< ИЗМЕНЕНИЕ: Логика таймаута теперь атомарна >>>
    async def _handle_position_duration_limit(self, exchange: BinanceExchange | BybitExchange, position: Dict):
        symbol = position['symbol']
        side = position['side'].upper()
        pnl = position['pnl']

        logger.warning(f"⏰ {exchange.__class__.__name__} {symbol} exceeded max duration. Entering close-only mode.")

        try:
            # Шаг 1: Отменяем ВСЕ существующие ордера (включая SL/TS), чтобы избежать конфликтов
            logger.info(f"   Cancelling all existing orders for {symbol} to set a final closing order.")
            await exchange.cancel_all_open_orders(symbol)

            # Шаг 2: Устанавливаем единственный ордер на закрытие
            if pnl >= 0:
                logger.info(f"   Position is profitable/breakeven. Closing {symbol} by market order.")
                if await exchange.close_position(symbol):
                    self.stats['positions_closed'] += 1
            else:
                breakeven_price = self._calculate_breakeven_price(position['entry_price'], side)
                logger.info(
                    f"   Position is at a loss. Setting final breakeven limit order for {symbol} at ${breakeven_price:.4f}")
                await exchange.create_limit_order(
                    symbol, "SELL" if side in ["LONG", "BUY"] else "BUY",
                    position['quantity'], breakeven_price, reduce_only=True
                )
        except Exception as e:
            logger.error(f"Failed to handle duration limit for {symbol}: {e}", exc_info=True)
            self.stats['errors'] += 1

    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

    async def _process_single_position(self, exchange_name: str, pos: Dict, orders_by_symbol: Dict[str, List[Dict]]):
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        symbol = pos['symbol']

        try:
            # <<< ИЗМЕНЕНИЕ: Строгая иерархия - сначала таймаут, потом всё остальное >>>
            # 1. ПРИОРИТЕТ №1: Проверка на таймаут
            if self.max_position_duration_hours > 0:
                update_time = pos.get('updateTime', 0) if exchange_name == 'Binance' else pos.get('created_time', 0)
                if update_time > 0:
                    age_hours = (datetime.now(timezone.utc).timestamp() * 1000 - update_time) / 3600000
                    if age_hours > self.max_position_duration_hours:
                        # Если позиция просрочена, мы ТОЛЬКО управляем её закрытием и ничего больше.
                        await self._handle_position_duration_limit(exchange, pos)
                        return  # <--- ВАЖНО: Завершаем обработку этой позиции

            # 2. ПРИОРИТЕТ №2: Установка защиты (срабатывает, только если позиция не просрочена)
            protection_is_incomplete = self._is_protection_incomplete(exchange_name, pos,
                                                                      orders_by_symbol.get(symbol, []))
            if protection_is_incomplete:
                await self._apply_protection(exchange_name, exchange, pos)
            # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

        except Exception as e:
            logger.error(f"Critical error in _process_single_position for {symbol}: {e}", exc_info=True)
            self.stats['errors'] += 1

    def _is_protection_incomplete(self, exchange_name: str, pos: Dict, open_orders: List[Dict]) -> bool:
        """Проверяет, соответствует ли текущая защита конфигурации."""
        if exchange_name == 'Bybit':
            has_sl = pos.get('stopLoss') and str(pos.get('stopLoss')) not in ['', '0']
            has_tp = pos.get('takeProfit') and str(pos.get('takeProfit')) not in ['', '0']
            has_ts = pos.get('trailingStop') and str(pos.get('trailingStop')) not in ['', '0']
        else:  # Binance
            has_sl = any(o.get('type') == 'STOP_MARKET' for o in open_orders)
            has_tp = any(o.get('type') == 'TAKE_PROFIT_MARKET' for o in open_orders)
            has_ts = any(o.get('type') == 'TRAILING_STOP_MARKET' for o in open_orders)

        if self.stop_loss_type == 'trailing':
            return not has_ts or not has_sl
        else:  # fixed
            return not has_sl or not has_tp

    async def _apply_protection(self, exchange_name: str, exchange: BinanceExchange | BybitExchange, pos: Dict):
        symbol = pos['symbol']
        logger.warning(f"⚠️ {exchange_name} {symbol} protection is incomplete. Applying now.")

        if self.stop_loss_type == 'trailing':
            await exchange.cancel_all_open_orders(symbol)
            ts_success = await self._set_trailing_stop(exchange, pos)
            if ts_success:
                sl_success = await self._set_backup_sl(exchange, pos)
                if sl_success:
                    logger.info(f"✅ Fully protected {exchange_name} position with TS+SL: {symbol}")
                    self.stats['positions_protected'] += 1
        else:  # fixed
            await self._set_fixed_sl(exchange, pos)
            await self._set_fixed_tp(exchange, pos)

    async def _set_trailing_stop(self, exchange: BinanceExchange | BybitExchange, pos: Dict) -> bool:
        symbol, side, entry_price = pos['symbol'], pos['side'].upper(), pos['entry_price']
        ticker = await exchange.get_ticker(symbol)
        current_price = ticker.get('price', entry_price)
        activation_price = max(entry_price * (1 + self.trailing_activation / 100), current_price * 1.01) if side in [
            'LONG', 'BUY'] else min(entry_price * (1 - self.trailing_activation / 100), current_price * 0.99)
        return await exchange.set_trailing_stop(symbol, activation_price, self.trailing_callback)

    async def _set_backup_sl(self, exchange: BinanceExchange | BybitExchange, pos: Dict) -> bool:
        sl_price = pos['entry_price'] * (1 - self.sl_percent / 100) if pos['side'].upper() in ['LONG', 'BUY'] else pos[
                                                                                                                       'entry_price'] * (
                                                                                                                               1 + self.sl_percent / 100)
        return await exchange.set_stop_loss(pos['symbol'], sl_price)

    async def _set_fixed_sl(self, exchange: BinanceExchange | BybitExchange, pos: Dict):
        sl_price = pos['entry_price'] * (1 - self.sl_percent / 100) if pos['side'].upper() in ['LONG', 'BUY'] else pos[
                                                                                                                       'entry_price'] * (
                                                                                                                               1 + self.sl_percent / 100)
        if await exchange.set_stop_loss(pos['symbol'], sl_price):
            self.stats['positions_protected'] += 1

    async def _set_fixed_tp(self, exchange: BinanceExchange | BybitExchange, pos: Dict):
        tp_price = pos['entry_price'] * (1 + self.tp_percent / 100) if pos['side'].upper() in ['LONG', 'BUY'] else pos[
                                                                                                                       'entry_price'] * (
                                                                                                                               1 - self.tp_percent / 100)
        if await exchange.set_take_profit(pos['symbol'], tp_price):
            self.stats['positions_protected'] += 1

    async def run_with_semaphore(self, semaphore: asyncio.Semaphore, coro, *args, **kwargs):
        async with semaphore:
            return await coro(*args, **kwargs)

    async def protect_positions(self, exchange_name: str):
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        semaphore = self.binance_semaphore if exchange_name == 'Binance' else self.bybit_semaphore
        if not exchange: return

        try:
            positions = await exchange.get_open_positions()
            if not positions:
                logger.debug(f"No open {exchange_name} positions")
                return
            logger.info(f"Found {len(positions)} {exchange_name} positions to check")

            all_open_orders = await exchange.get_open_orders()
            orders_by_symbol = {pos['symbol']: [] for pos in positions}
            for order in all_open_orders:
                symbol = order['symbol']
                if symbol in orders_by_symbol:
                    orders_by_symbol[symbol].append(order)

            tasks = [
                self.run_with_semaphore(semaphore, self._process_single_position, exchange_name, pos, orders_by_symbol)
                for pos in positions]
            if tasks:
                await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Critical error in protect_{exchange_name.lower()}_positions: {e}", exc_info=True)
            self.stats['errors'] += 1

    async def run(self):
        logger.info(f"🚀 Starting Protection Monitor v2.5")
        await self.initialize()
        try:
            while True:
                try:
                    self.stats['checks'] += 1
                    logger.info(f"=== Protection Check #{self.stats['checks']} ===")
                    await asyncio.gather(
                        self.protect_positions('Binance'),
                        self.protect_positions('Bybit')
                    )
                    if self.stats['checks'] % 10 == 0:
                        await self.print_statistics()
                    await asyncio.sleep(self.check_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    self.stats['errors'] += 1
                    await asyncio.sleep(5)
        except KeyboardInterrupt:
            logger.info("⛔ Shutdown signal received")
        finally:
            await self.cleanup()

    async def print_statistics(self):
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

    async def cleanup(self):
        logger.info("🧹 Cleaning up...")
        await self.print_statistics()
        if self.binance:
            await self.binance.close()
            logger.info("Binance connection closed")
        if self.bybit:
            await self.bybit.close()
            logger.info("Bybit connection closed")
        logger.info("✅ Cleanup complete")


async def main():
    monitor = ProtectionMonitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())