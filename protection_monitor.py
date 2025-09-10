#!/usr/bin/env python3
# protection_monitor.py - ФИНАЛЬНАЯ ВЕРСИЯ V13

import asyncio
import logging
import os
import sys
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
import functools

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from exchanges.binance import BinanceExchange

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('protection.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)


class ProtectionMonitor:
    def __init__(self):
        # --- ОСНОВНЫЕ НАСТРОЙКИ ---
        self.use_trailing = os.getenv('USE_TRAILING_STOP', 'false').lower() == 'true'
        self.sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.tp_percent = float(os.getenv('TAKE_PROFIT_PERCENT', '3'))
        self.trailing_activation = float(os.getenv('TRAILING_ACTIVATION_PERCENT', '3.5'))
        self.trailing_callback = float(os.getenv('TRAILING_CALLBACK_RATE', '0.5'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.close_on_breached_sl = os.getenv('CLOSE_ON_BREACHED_SL', 'false').lower() == 'true'
        self.max_position_duration_hours = int(os.getenv('MAX_POSITION_DURATION_HOURS', '0'))
        self.taker_fee_percent = float(os.getenv('TAKER_FEE_PERCENT', '0.06'))
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'

        self.binance = None
        self.bybit_client = None
        self.stats = {'checks': 0, 'positions_protected': 0, 'start_time': datetime.now()}

        # --- СЕМАФОРЫ ДЛЯ КОНТРОЛЯ ЛИМИТОВ API ---
        self.binance_semaphore = asyncio.Semaphore(10)
        self.bybit_semaphore = threading.Semaphore(2)

        logger.info(f"Protection Monitor v13 FINAL initialized - Testnet: {self.testnet}")
        logger.info(f"API Rate Limiting Enabled: Binance (10/sec), Bybit (2/sec)")
        if self.max_position_duration_hours > 0:
            logger.info(
                f"🕒 Max Position Duration enabled: {self.max_position_duration_hours} hours (Fee: {self.taker_fee_percent}%)")

    def _calculate_breakeven_price(self, entry_price: float, side: str) -> float:
        fee_multiplier = 1 + (self.taker_fee_percent / 100)
        if side.upper() in ['LONG', 'BUY']:
            return entry_price * fee_multiplier / (1 - (self.taker_fee_percent / 100))
        else:
            return entry_price * (1 - (self.taker_fee_percent / 100)) / fee_multiplier

    async def initialize(self):
        try:
            if os.getenv('BINANCE_API_KEY'):
                self.binance = BinanceExchange(
                    {'api_key': os.getenv('BINANCE_API_KEY'), 'api_secret': os.getenv('BINANCE_API_SECRET'),
                     'testnet': self.testnet})
                await self.binance.initialize()
                logger.info("✅ Binance connected")
            if os.getenv('BYBIT_API_KEY'):
                self.bybit_client = HTTP(testnet=self.testnet, api_key=os.getenv('BYBIT_API_KEY'),
                                         api_secret=os.getenv('BYBIT_API_SECRET'))
                logger.info("✅ Bybit connected")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    # --- ЛОГИКА ДЛЯ BINANCE ---
    async def _process_single_binance_position(self, pos: dict):
        async with self.binance_semaphore:
            try:
                if pos.get('quantity', 0) <= 0: return

                symbol, side, entry_price, quantity, pnl, time_ms = pos['symbol'], pos['side'], pos['entry_price'], pos[
                    'quantity'], pos.get('pnl', 0.0), pos.get('updateTime', 0)

                if self.max_position_duration_hours > 0 and time_ms > 0:
                    age_hours = (datetime.now(timezone.utc).timestamp() * 1000 - time_ms) / 3600000
                    if age_hours > self.max_position_duration_hours:
                        logger.warning(f"🕒 Binance {symbol} has exceeded max duration ({age_hours:.2f}h).")
                        breakeven_price_float = self._calculate_breakeven_price(entry_price, side)
                        breakeven_price_formatted = self.binance.format_price(symbol, breakeven_price_float)

                        open_orders = await self.binance.get_open_orders(symbol)
                        be_order_exists = any(
                            o['type'] == 'LIMIT' and o.get('price') == breakeven_price_formatted for o in open_orders)

                        if be_order_exists:
                            logger.info(f"ℹ️ Breakeven limit order for {symbol} already exists. Skipping.")
                            return

                        if pnl > 0:
                            logger.info(f"💰 Position {symbol} is in profit. Closing by market order.")
                            await self.binance.close_position(symbol)
                        else:
                            logger.info(f"📉 Position {symbol} not in profit. Setting breakeven limit order.")
                            await self.binance.cancel_all_open_orders(symbol)
                            await self.binance.create_limit_order(symbol, "SELL" if side == "LONG" else "BUY", quantity,
                                                                  breakeven_price_float, reduce_only=True)
                        return

                open_orders = await self.binance.get_open_orders(symbol)
                has_sl = any(o['type'] == 'STOP_MARKET' for o in open_orders)
                has_tp = any(o['type'] == 'TAKE_PROFIT_MARKET' for o in open_orders)
                has_ts = any(o['type'] == 'TRAILING_STOP_MARKET' for o in open_orders)

                if self.use_trailing:
                    if not has_ts:
                        logger.warning(f"⚠️ Binance {symbol} missing TRAILING STOP")
                        ticker = await self.binance.get_ticker(symbol)
                        current_price = ticker.get('price', entry_price)
                        activation_price = max(entry_price * (1 + self.trailing_activation / 100),
                                               current_price * 1.01) if side == 'LONG' else min(
                            entry_price * (1 - self.trailing_activation / 100), current_price * 0.99)
                        await self.binance.set_trailing_stop(symbol, activation_price, self.trailing_callback)
                    if not has_sl:
                        logger.warning(f"⚠️ Binance {symbol} missing STOP LOSS")
                        sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'LONG' else entry_price * (
                                    1 + self.sl_percent / 100)
                        await self.binance.set_stop_loss(symbol, sl_price)
                else:
                    if not has_sl:
                        logger.warning(f"⚠️ Binance {symbol} missing STOP LOSS")
                        sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'LONG' else entry_price * (
                                    1 + self.sl_percent / 100)
                        await self.binance.set_stop_loss(symbol, sl_price)
                    if not has_tp:
                        logger.warning(f"⚠️ Binance {symbol} missing TAKE PROFIT")
                        tp_price = entry_price * (1 + self.tp_percent / 100) if side == 'LONG' else entry_price * (
                                    1 - self.tp_percent / 100)
                        await self.binance.set_take_profit(symbol, tp_price)

            except Exception as e:
                logger.error(f"Error processing single Binance position {pos.get('symbol', 'UNKNOWN')}: {e}")
                raise

    async def protect_binance_positions(self):
        if not self.binance: return
        try:
            positions = await self.binance.get_open_positions()
            if not positions:
                logger.info("No open Binance positions found.")
                return

            logger.info(f"Found {len(positions)} Binance positions. Processing concurrently...")
            tasks = [self._process_single_binance_position(pos) for pos in positions]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            success_count = sum(1 for r in results if not isinstance(r, Exception))
            error_count = len(results) - success_count
            if error_count > 0:
                logger.warning(f"Binance processing complete. Success: {success_count}, Failed: {error_count}.")
                for i, res in enumerate(results):
                    if isinstance(res, Exception): logger.error(
                        f"-> Failure for position {positions[i].get('symbol')}: {res}")
            else:
                logger.info(f"Binance processing complete. Success: {success_count}, Failed: {error_count}.")
        except Exception as e:
            logger.error(f"Critical error in protect_binance_positions: {e}")

    # --- ЛОГИКА ДЛЯ BYBIT ---
    def _process_single_bybit_position(self, pos: dict):
        with self.bybit_semaphore:  # Используем семафор в синхронном контексте
            try:
                if float(pos.get('size', 0)) <= 0: return

                symbol, side, entry_price, size, pnl, time_ms = pos['symbol'], pos['side'], float(pos['avgPrice']), pos[
                    'size'], float(pos.get('unrealisedPnl', 0.0)), int(pos.get('createdTime', 0))

                if self.max_position_duration_hours > 0 and time_ms > 0:
                    age_hours = (datetime.now(timezone.utc).timestamp() * 1000 - time_ms) / 3600000
                    if age_hours > self.max_position_duration_hours:
                        logger.warning(f"🕒 Bybit {symbol} has exceeded max duration ({age_hours:.2f}h).")

                        breakeven_price = self._calculate_breakeven_price(entry_price, side)
                        open_orders = self.bybit_client.get_open_orders(category="linear", symbol=symbol).get('result',
                                                                                                              {}).get(
                            'list', [])
                        be_order_exists = any(
                            o['orderType'] == 'Limit' and float(o.get('price', 0)) == float(f"{breakeven_price:.5f}")
                            for o in open_orders)

                        if be_order_exists:
                            logger.info(f"ℹ️ Breakeven limit order for {symbol} already exists. Skipping.")
                            return

                        if pnl > 0:
                            logger.info(f"💰 Position {symbol} is in profit. Closing by market order.")
                            self.bybit_client.place_order(category="linear", symbol=symbol,
                                                          side="Sell" if side == "Buy" else "Buy", orderType="Market",
                                                          qty=size, reduceOnly=True)
                        else:
                            logger.info(f"📉 Position {symbol} not in profit. Setting breakeven limit order.")
                            self.bybit_client.cancel_all_orders(category="linear", symbol=symbol)
                            self.bybit_client.place_order(category="linear", symbol=symbol,
                                                          side="Sell" if side == "Buy" else "Buy", orderType="Limit",
                                                          qty=size, price=str(f"{breakeven_price:.5f}"),
                                                          reduceOnly=True)
                        return

                has_sl = pos.get('stopLoss') and str(pos.get('stopLoss')) not in ['', '0']
                has_tp = pos.get('takeProfit') and str(pos.get('takeProfit')) not in ['', '0']
                has_ts = pos.get('trailingStop') and str(pos.get('trailingStop')) not in ['', '0']

                if self.use_trailing:
                    if not has_ts: self._set_bybit_trailing_stop(symbol, entry_price, side)
                    if not has_sl: self._set_bybit_stop_loss(symbol, entry_price, side)
                else:
                    if not has_sl: self._set_bybit_stop_loss(symbol, entry_price, side)
                    if not has_tp: self._set_bybit_take_profit(symbol, entry_price, side)
            except Exception as e:
                logger.error(f"Error processing single Bybit position {pos.get('symbol', 'UNKNOWN')}: {e}")
                raise

    def _set_bybit_stop_loss(self, symbol, entry_price, side):
        sl_price = entry_price * (1 - self.sl_percent / 100) if side == 'Buy' else entry_price * (
                    1 + self.sl_percent / 100)
        logger.warning(f"⚠️ Bybit {symbol} missing STOP LOSS. Setting at {sl_price:.5f}")
        self.bybit_client.set_trading_stop(category="linear", symbol=symbol, stopLoss=str(f"{sl_price:.5f}"),
                                           positionIdx=0)

    def _set_bybit_take_profit(self, symbol, entry_price, side):
        tp_price = entry_price * (1 + self.tp_percent / 100) if side == 'Buy' else entry_price * (
                    1 - self.tp_percent / 100)
        logger.warning(f"⚠️ Bybit {symbol} missing TAKE PROFIT. Setting at {tp_price:.5f}")
        self.bybit_client.set_trading_stop(category="linear", symbol=symbol, takeProfit=str(f"{tp_price:.5f}"),
                                           positionIdx=0)

    def _set_bybit_trailing_stop(self, symbol, entry_price, side):
        logger.warning(f"⚠️ Bybit {symbol} missing TRAILING STOP.")
        trailing_stop_value = entry_price * (self.trailing_callback / 100)
        ticker_resp = self.bybit_client.get_tickers(category="linear", symbol=symbol)
        current_price = float(ticker_resp['result']['list'][0]['lastPrice']) if ticker_resp.get(
            'retCode') == 0 and ticker_resp.get('result', {}).get('list') else entry_price
        activation_price = max(entry_price * (1 + self.trailing_activation / 100),
                               current_price * 1.01) if side == 'Buy' else min(
            entry_price * (1 - self.trailing_activation / 100), current_price * 0.99)
        self.bybit_client.set_trading_stop(category="linear", symbol=symbol,
                                           trailingStop=str(f"{trailing_stop_value:.5f}"),
                                           activePrice=str(f"{activation_price:.5f}"), positionIdx=0)

    async def protect_bybit_positions(self):
        if not self.bybit_client: return
        try:
            response = self.bybit_client.get_positions(category="linear", settleCoin="USDT")
            if response['retCode'] != 0: return
            positions = response['result']['list']
            if not positions:
                logger.info("No open Bybit positions found.")
                return

            logger.info(f"Found {len(positions)} Bybit positions. Processing concurrently in thread pool...")
            loop = asyncio.get_running_loop()
            tasks = []
            for pos in positions:
                # Запускаем каждую синхронную обработку в отдельном потоке
                task = loop.run_in_executor(None, functools.partial(self._process_single_bybit_position, pos))
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            error_count = len(results) - success_count
            if error_count > 0:
                logger.warning(f"Bybit processing complete. Success: {success_count}, Failed: {error_count}.")
                for i, res in enumerate(results):
                    if isinstance(res, Exception): logger.error(
                        f"-> Failure for Bybit position {positions[i].get('symbol')}: {res}")
            else:
                logger.info(f"Bybit processing complete. Success: {success_count}, Failed: {error_count}.")
        except Exception as e:
            logger.error(f"Critical error in protect_bybit_positions: {e}")

    async def run(self):
        logger.info("🚀 Starting Protection Monitor")
        await self.initialize()
        while True:
            try:
                self.stats['checks'] += 1
                logger.info(f"=== Check #{self.stats['checks']} ===")
                # Запускаем защиту для обеих бирж одновременно
                await asyncio.gather(
                    self.protect_binance_positions(),
                    self.protect_bybit_positions()
                )
                await asyncio.sleep(self.check_interval)
            except KeyboardInterrupt:
                logger.info("Stopping monitor...")
                break
        if self.binance: await self.binance.close()


async def main():
    monitor = ProtectionMonitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())