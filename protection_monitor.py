#!/usr/bin/env python3
"""
Protection Monitor - PRODUCTION READY v4.5 (FINAL, COMPLETE)
- FIXED the bug where age_hours was not being passed to PositionInfo, causing it to be 0.0.
- All other logic for aged positions and protection application remains correct.
"""

import asyncio
import asyncpg
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field
from enum import Enum
from dotenv import load_dotenv
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange
from utils.rate_limiter import RateLimiter

load_dotenv()

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO if os.getenv('DEBUG', 'false').lower() != 'true' else logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] %(message)s',
    handlers=[
        logging.FileHandler('logs/protection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PositionStatus(Enum):
    UNPROTECTED = "unprotected"
    PARTIALLY_PROTECTED = "partially_protected"
    FULLY_PROTECTED = "fully_protected"
    TRAILING_ACTIVE = "trailing_active"
    PENDING_CLOSE = "pending_close"


@dataclass
class PositionInfo:
    """Enhanced position information tracking"""
    symbol: str
    exchange: str
    side: str
    quantity: float
    entry_price: float
    current_price: float = 0.0
    pnl: float = 0.0
    pnl_percent: float = 0.0
    age_hours: float = 0.0
    has_sl: bool = False
    has_trailing: bool = False
    has_breakeven_order: bool = False
    sl_price: Optional[float] = None
    trailing_activation_price: Optional[float] = None
    status: PositionStatus = PositionStatus.UNPROTECTED
    db_position_id: Optional[int] = None


class ProtectionMonitor:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.sl_percent = float(os.getenv('STOP_LOSS_PERCENT', '2'))
        self.trailing_activation = float(os.getenv('TRAILING_ACTIVATION_PERCENT', '1'))
        self.trailing_callback = float(os.getenv('TRAILING_CALLBACK_RATE', '0.5'))
        self.max_position_duration_hours = int(os.getenv('MAX_POSITION_DURATION_HOURS', '24'))
        self.min_profit_for_breakeven = float(os.getenv('MIN_PROFIT_FOR_BREAKEVEN', '0.3'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        self.taker_fee_percent = float(os.getenv('TAKER_FEE_PERCENT', '0.06'))
        self.request_delay = 0.5 if self.testnet else 0.1
        self.between_positions_delay = 1.0 if self.testnet else 0.2
        self.binance: Optional[BinanceExchange] = None
        self.bybit: Optional[BybitExchange] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.tracked_positions: Dict[str, PositionInfo] = {}
        self._log_configuration()

    def _log_configuration(self):
        logger.info("=" * 80)
        logger.info("PROTECTION MONITOR CONFIGURATION v4.5 (FINAL)")
        logger.info("=" * 80)
        logger.info(f"Environment: {'TESTNET' if self.testnet else 'MAINNET'}")
        logger.info(f"Stop Loss: {self.sl_percent}%")
        logger.info(f"Trailing Activation: {self.trailing_activation}%")
        logger.info(f"Trailing Callback: {self.trailing_callback}%")
        logger.info(f"Max Position Duration: {self.max_position_duration_hours}h")
        logger.info(f"Check Interval: {self.check_interval}s")
        logger.info("=" * 80)

    async def initialize(self):
        logger.info("🚀 Initializing Protection Monitor...")
        # Database and exchange initialization logic remains the same
        await self._init_db()
        init_tasks = []
        if os.getenv('BINANCE_API_KEY'): init_tasks.append(self._init_exchange('Binance'))
        if os.getenv('BYBIT_API_KEY'): init_tasks.append(self._init_exchange('Bybit'))
        await asyncio.gather(*init_tasks)
        if not self.binance and not self.bybit: raise Exception("No exchanges available!")
        logger.info("✅ Protection Monitor initialized")

    async def _init_db(self):
        try:
            self.db_pool = await asyncpg.create_pool(**self.db_config)
            await self.db_pool.fetchval("SELECT 1")
            logger.info("✅ Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            self.db_pool = None

    async def _init_exchange(self, name: str):
        try:
            config = {
                'api_key': os.getenv(f'{name.upper()}_API_KEY'),
                'api_secret': os.getenv(f'{name.upper()}_API_SECRET'),
                'testnet': self.testnet
            }
            exchange_class = BinanceExchange if name == 'Binance' else BybitExchange
            exchange = exchange_class(config)
            await exchange.initialize()
            balance = await exchange.get_balance()
            logger.info(f"✅ {name} initialized. Balance: ${balance:.2f}")
            if name == 'Binance':
                self.binance = exchange
            else:
                self.bybit = exchange
        except Exception as e:
            logger.error(f"Failed to initialize {name}: {e}")

    def _calculate_position_age(self, position: Dict, exchange_name: str) -> float:
        ts_key = "updateTime" if exchange_name == "Binance" else "updatedTime"
        timestamp_ms = position.get(ts_key, 0)
        if not timestamp_ms: return 0.0
        age_seconds = datetime.now(timezone.utc).timestamp() - (int(timestamp_ms) / 1000)
        return age_seconds / 3600

    def _calculate_pnl_percent(self, entry_price: float, current_price: float, side: str) -> float:
        if entry_price <= 0: return 0.0
        if side.upper() in ['LONG', 'BUY']:
            return ((current_price - entry_price) / entry_price) * 100
        return ((entry_price - current_price) / entry_price) * 100

    async def _check_protection_status(self, exchange_name: str, position: Dict,
                                       open_orders: List[Dict]) -> PositionInfo:
        symbol = position.get('symbol')

        # --- FIX IS HERE: All data is now passed correctly to the constructor ---
        pos_info = PositionInfo(
            symbol=symbol,
            exchange=exchange_name,
            side=position.get('side', '').upper(),
            quantity=float(position.get('quantity', 0)),
            entry_price=float(position.get('entry_price', 0)),
            current_price=float(position.get('mark_price', position.get('entry_price', 0))),
            pnl=float(position.get('pnl', 0)),
            age_hours=self._calculate_position_age(position, exchange_name)
        )
        pos_info.pnl_percent = self._calculate_pnl_percent(pos_info.entry_price, pos_info.current_price, pos_info.side)

        symbol_orders = [o for o in open_orders if o.get('symbol') == symbol]

        if exchange_name == 'Bybit':
            pos_info.has_sl = bool(position.get('stopLoss') and float(position.get('stopLoss', 0)) > 0)
            pos_info.has_trailing = bool(position.get('trailingStop') and float(position.get('trailingStop', 0)) > 0)

        for order in symbol_orders:
            order_type = order.get('type', '').lower()
            if exchange_name == 'Binance':
                if order_type in ['stop_market', 'stop']:
                    pos_info.has_sl = True
                elif order_type == 'trailing_stop_market':
                    pos_info.has_trailing = True
            if order_type == 'limit' and order.get('reduceOnly', False):
                pos_info.has_breakeven_order = True

        if pos_info.has_breakeven_order:
            pos_info.status = PositionStatus.PENDING_CLOSE
        elif pos_info.has_sl and pos_info.has_trailing:
            pos_info.status = PositionStatus.FULLY_PROTECTED
        elif pos_info.has_sl or pos_info.has_trailing:
            pos_info.status = PositionStatus.PARTIALLY_PROTECTED
        else:
            pos_info.status = PositionStatus.UNPROTECTED

        if pos_info.has_trailing and pos_info.pnl_percent >= self.trailing_activation:
            pos_info.status = PositionStatus.TRAILING_ACTIVE

        return pos_info

    async def _apply_protection(self, exchange: Union[BinanceExchange, BybitExchange], pos_info: PositionInfo):
        symbol = pos_info.symbol
        logger.info(f"🛡️ Applying protection to {symbol} on {pos_info.exchange}")
        try:
            if not pos_info.has_sl:
                if pos_info.side in ['LONG', 'BUY']:
                    sl_price = pos_info.entry_price * (1 - self.sl_percent / 100)
                else:
                    sl_price = pos_info.entry_price * (1 + self.sl_percent / 100)
                await asyncio.sleep(self.request_delay)
                if await exchange.set_stop_loss(symbol, sl_price):
                    logger.info(f"✅ Stop Loss added for {symbol} at ${sl_price:.4f}")

            if not pos_info.has_trailing:
                if pos_info.side in ['LONG', 'BUY']:
                    activation_price = pos_info.entry_price * (1 + self.trailing_activation / 100)
                else:
                    activation_price = pos_info.entry_price * (1 - self.trailing_activation / 100)
                await asyncio.sleep(self.request_delay)
                if await exchange.set_trailing_stop(symbol, activation_price, self.trailing_callback):
                    logger.info(
                        f"✅ Trailing Stop added for {symbol}: activation=${activation_price:.4f}, callback={self.trailing_callback}%")
        except Exception as e:
            logger.error(f"Error applying protection to {symbol}: {e}", exc_info=True)

    async def _handle_aged_position(self, exchange: Union[BinanceExchange, BybitExchange], pos_info: PositionInfo):
        symbol = pos_info.symbol
        logger.warning(
            f"⏰ Position {symbol} is aged ({pos_info.age_hours:.1f}h) with inactive trail. Applying exit logic.")

        if pos_info.has_breakeven_order:
            logger.info(f"Breakeven limit order already exists for {symbol}. Monitoring.")
            return

        try:
            if pos_info.pnl_percent > self.min_profit_for_breakeven:
                logger.info(f"📈 Position {symbol} is in profit ({pos_info.pnl_percent:.2f}%), closing at market.")
                await exchange.cancel_all_open_orders(symbol)
                await asyncio.sleep(self.request_delay)
                if await exchange.close_position(symbol):
                    logger.info(f"✅ Position {symbol} closed at market due to age and profit.")
            else:
                logger.info(
                    f"📉 Position {symbol} is at a loss/small profit ({pos_info.pnl_percent:.2f}%), setting breakeven limit order.")
                fee_multiplier = 1 + (self.taker_fee_percent * 2 / 100)
                if pos_info.side in ['LONG', 'BUY']:
                    breakeven_price = pos_info.entry_price * fee_multiplier
                    side = 'SELL'
                else:
                    breakeven_price = pos_info.entry_price / fee_multiplier
                    side = 'BUY'
                logger.info(
                    f"Placing breakeven limit order for {symbol} at ${breakeven_price:.4f}. Stop Loss will remain active.")
                await exchange.create_limit_order(
                    symbol=symbol, side=side, quantity=pos_info.quantity,
                    price=breakeven_price, reduce_only=True
                )
        except Exception as e:
            logger.error(f"Error handling aged position {symbol}: {e}", exc_info=True)

    async def process_exchange_positions(self, exchange_name: str):
        exchange = self.binance if exchange_name == 'Binance' else self.bybit
        if not exchange: return

        try:
            positions = await exchange.get_open_positions()
            if not positions:
                logger.debug(f"No open positions on {exchange_name}")
                return

            logger.info(f"Found {len(positions)} open positions on {exchange_name}")
            all_orders = await exchange.get_open_orders() or []

            for position in positions:
                symbol = position.get('symbol')
                if not symbol: continue
                await asyncio.sleep(self.between_positions_delay)

                pos_info = await self._check_protection_status(exchange_name, position, all_orders)
                self.tracked_positions[f"{exchange_name}_{symbol}"] = pos_info

                logger.info(
                    f"📊 {symbol}: PnL={pos_info.pnl_percent:.2f}%, Age={pos_info.age_hours:.1f}h, Status={pos_info.status.value}")

                if pos_info.status == PositionStatus.TRAILING_ACTIVE:
                    logger.debug(f"Position {symbol} has an active trailing stop. Monitoring.")
                    continue
                if pos_info.status == PositionStatus.PENDING_CLOSE:
                    logger.info(f"Position {symbol} has a pending breakeven limit order. Monitoring.")
                    continue
                if self.max_position_duration_hours > 0 and pos_info.age_hours > self.max_position_duration_hours:
                    await self._handle_aged_position(exchange, pos_info)
                    continue
                if pos_info.status in [PositionStatus.UNPROTECTED, PositionStatus.PARTIALLY_PROTECTED]:
                    await self._apply_protection(exchange, pos_info)

        except Exception as e:
            logger.error(f"Error processing {exchange_name} positions: {e}", exc_info=True)

    async def run(self):
        logger.info(f"🚀 Starting Protection Monitor v4.5 - PRODUCTION READY")
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'MAINNET'}")
        await self.initialize()

        try:
            check_count = 0
            while True:
                check_count += 1
                logger.info(f"\n{'=' * 40}\nProtection Check #{check_count}\n{'=' * 40}")

                tasks = []
                if self.binance: tasks.append(self.process_exchange_positions('Binance'))
                if self.bybit: tasks.append(self.process_exchange_positions('Bybit'))
                if tasks: await asyncio.gather(*tasks)

                logger.info(f"Check complete. Positions tracked: {len(self.tracked_positions)}")
                await asyncio.sleep(self.check_interval)
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Shutdown requested")
        finally:
            logger.info("Cleaning up...")
            if self.db_pool: await self.db_pool.close()
            if self.binance: await self.binance.close()
            if self.bybit: await self.bybit.close()
            logger.info("✅ Cleanup complete")


async def main():
    monitor = ProtectionMonitor()
    try:
        await monitor.run()
    except asyncio.CancelledError:
        logger.info("Main task cancelled.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")