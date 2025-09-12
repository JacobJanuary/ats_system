#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы Bybit testnet
Создает ордера на 10 самых ликвидных пар и устанавливает SL
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import List, Dict, Optional
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.bybit import BybitExchange

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Список самых ликвидных пар на Bybit (обычно всегда работают)
LIQUID_SYMBOLS = [
    'BTCUSDT',  # Bitcoin - всегда работает
    'ETHUSDT',  # Ethereum - всегда работает
    'SOLUSDT',  # Solana - популярная
    'XRPUSDT',  # Ripple - стабильная
    'DOGEUSDT',  # Dogecoin - популярная
    'ADAUSDT',  # Cardano - стабильная
    'MATICUSDT',  # Polygon - популярная
    'LINKUSDT',  # Chainlink - стабильная
    'DOTUSDT',  # Polkadot - стабильная
    'UNIUSDT'  # Uniswap - популярная
]


class BybitTester:
    def __init__(self):
        self.exchange = None
        self.position_size_usd = float(os.getenv('TEST_POSITION_SIZE', '10'))  # Малый размер для теста
        self.leverage = int(os.getenv('TEST_LEVERAGE', '5'))  # Безопасное плечо
        self.sl_percent = float(os.getenv('TEST_SL_PERCENT', '2'))  # 2% SL
        self.test_results = []

    async def initialize(self):
        """Инициализация биржи"""
        try:
            self.exchange = BybitExchange({
                'api_key': os.getenv('BYBIT_API_KEY'),
                'api_secret': os.getenv('BYBIT_API_SECRET'),
                'testnet': True  # Всегда testnet для этого скрипта
            })

            await self.exchange.initialize()

            balance = await self.exchange.get_balance()
            logger.info(f"✅ Bybit testnet connected - Balance: ${balance:.2f}")

            if balance < 100:
                logger.warning(f"⚠️ Low balance: ${balance:.2f}. May not be enough for all tests.")

            return True

        except Exception as e:
            logger.error(f"Failed to initialize Bybit: {e}")
            return False

    async def check_symbol_info(self, symbol: str) -> Dict:
        """Проверка информации о символе"""
        result = {
            'symbol': symbol,
            'available': False,
            'status': None,
            'has_ticker': False,
            'price': 0,
            'min_qty': 0,
            'tick_size': 0,
            'error': None
        }

        try:
            # Проверяем наличие в загруженных инструментах
            if symbol in self.exchange.symbol_info:
                info = self.exchange.symbol_info[symbol]
                result['available'] = True
                result['status'] = info.get('status', 'Unknown')
                result['min_qty'] = info.get('minOrderQty', 0)
                result['tick_size'] = info.get('tickSize', 0)
            else:
                # Пытаемся загрузить
                await self.exchange._load_single_symbol_info(symbol)
                if symbol in self.exchange.symbol_info:
                    info = self.exchange.symbol_info[symbol]
                    result['available'] = True
                    result['status'] = info.get('status', 'Unknown')
                    result['min_qty'] = info.get('minOrderQty', 0)
                    result['tick_size'] = info.get('tickSize', 0)

            # Проверяем ticker
            ticker = await self.exchange.get_ticker(symbol)
            if ticker and ticker.get('price', 0) > 0:
                result['has_ticker'] = True
                result['price'] = ticker['price']

        except Exception as e:
            result['error'] = str(e)

        return result

    async def test_create_position(self, symbol: str) -> Dict:
        """Тестирование создания позиции с SL"""
        test_result = {
            'symbol': symbol,
            'success': False,
            'order_id': None,
            'price': 0,
            'quantity': 0,
            'sl_set': False,
            'sl_price': 0,
            'error': None,
            'steps': []
        }

        try:
            logger.info(f"\n{'=' * 50}")
            logger.info(f"🎯 Testing {symbol}")
            logger.info(f"{'=' * 50}")

            # Шаг 1: Проверка символа
            test_result['steps'].append("1. Checking symbol info...")
            symbol_info = await self.check_symbol_info(symbol)

            if not symbol_info['available']:
                test_result['error'] = f"Symbol not available"
                logger.error(f"❌ {symbol} not available")
                return test_result

            if not symbol_info['has_ticker']:
                test_result['error'] = f"No ticker data"
                logger.error(f"❌ {symbol} has no ticker")
                return test_result

            logger.info(f"✅ Symbol check passed: price=${symbol_info['price']:.4f}, status={symbol_info['status']}")
            test_result['steps'].append(f"   ✅ Price: ${symbol_info['price']:.4f}")

            # Шаг 2: Установка leverage
            test_result['steps'].append("2. Setting leverage...")
            leverage_set = await self.exchange.set_leverage(symbol, self.leverage)
            if leverage_set:
                logger.info(f"✅ Leverage set to {self.leverage}x")
                test_result['steps'].append(f"   ✅ Leverage: {self.leverage}x")
            else:
                logger.warning(f"⚠️ Could not set leverage (may be already set)")
                test_result['steps'].append(f"   ⚠️ Leverage may be already set")

            # Шаг 3: Расчет количества
            test_result['steps'].append("3. Calculating order size...")
            price = symbol_info['price']
            quantity = self.position_size_usd / price

            # Проверка минимального размера
            min_qty = symbol_info['min_qty']
            if quantity < min_qty:
                quantity = min_qty
                actual_size = quantity * price
                logger.info(f"📊 Adjusted to min quantity: {quantity} (${actual_size:.2f})")
            else:
                logger.info(f"📊 Quantity: {quantity:.6f} ${symbol}")

            test_result['steps'].append(f"   📊 Qty: {quantity:.6f}")

            # Шаг 4: Создание ордера
            test_result['steps'].append("4. Creating market order...")
            logger.info(f"📝 Creating BUY order: {quantity:.6f} {symbol} @ market")

            order = await self.exchange.create_market_order(symbol, 'BUY', quantity)

            if order and order.get('orderId'):
                test_result['success'] = True
                test_result['order_id'] = order['orderId']
                test_result['price'] = order.get('price', price)
                test_result['quantity'] = order.get('quantity', quantity)

                logger.info(f"✅ Order created: {order['orderId']}")
                logger.info(f"   Executed: {test_result['quantity']:.6f} @ ${test_result['price']:.4f}")
                test_result['steps'].append(f"   ✅ Order ID: {order['orderId']}")

                # Ждем пока позиция зарегистрируется
                await asyncio.sleep(3)

                # Шаг 5: Установка Stop Loss
                test_result['steps'].append("5. Setting Stop Loss...")
                sl_price = test_result['price'] * (1 - self.sl_percent / 100)
                logger.info(f"🛡️ Setting SL at ${sl_price:.4f} (-{self.sl_percent}%)")

                sl_success = await self.exchange.set_stop_loss(symbol, sl_price)

                if sl_success:
                    test_result['sl_set'] = True
                    test_result['sl_price'] = sl_price
                    logger.info(f"✅ Stop Loss set at ${sl_price:.4f}")
                    test_result['steps'].append(f"   ✅ SL: ${sl_price:.4f}")
                else:
                    logger.warning(f"⚠️ Failed to set Stop Loss")
                    test_result['steps'].append(f"   ❌ SL failed")

            else:
                test_result['error'] = "Order creation failed"
                logger.error(f"❌ Failed to create order")
                test_result['steps'].append(f"   ❌ Order failed")

        except Exception as e:
            test_result['error'] = str(e)
            logger.error(f"❌ Error testing {symbol}: {e}")
            test_result['steps'].append(f"   ❌ Error: {str(e)[:100]}")

        return test_result

    async def close_all_positions(self):
        """Закрытие всех открытых позиций после теста"""
        logger.info("\n🧹 Closing all test positions...")

        try:
            positions = await self.exchange.get_open_positions()

            if not positions:
                logger.info("No open positions to close")
                return

            for pos in positions:
                symbol = pos['symbol']
                logger.info(f"Closing {symbol}...")

                success = await self.exchange.close_position(symbol)
                if success:
                    logger.info(f"✅ Closed {symbol}")
                else:
                    logger.warning(f"⚠️ Failed to close {symbol}")

                await asyncio.sleep(0.5)  # Небольшая задержка между закрытиями

        except Exception as e:
            logger.error(f"Error closing positions: {e}")

    async def run_tests(self):
        """Запуск всех тестов"""
        logger.info("\n" + "=" * 60)
        logger.info("🚀 STARTING BYBIT TESTNET VALIDATION")
        logger.info("=" * 60)
        logger.info(f"Position Size: ${self.position_size_usd}")
        logger.info(f"Leverage: {self.leverage}x")
        logger.info(f"Stop Loss: {self.sl_percent}%")
        logger.info(f"Symbols to test: {len(LIQUID_SYMBOLS)}")
        logger.info("=" * 60)

        # Инициализация
        if not await self.initialize():
            logger.error("Failed to initialize exchange")
            return

        # Проверка баланса
        balance = await self.exchange.get_balance()
        required_balance = self.position_size_usd * len(LIQUID_SYMBOLS) * 1.5  # С запасом

        if balance < required_balance:
            logger.warning(f"⚠️ Balance ${balance:.2f} may be insufficient (need ~${required_balance:.2f})")

        # Тестирование каждого символа
        successful = 0
        failed = 0

        for i, symbol in enumerate(LIQUID_SYMBOLS, 1):
            logger.info(f"\n[{i}/{len(LIQUID_SYMBOLS)}] Testing {symbol}...")

            result = await self.test_create_position(symbol)
            self.test_results.append(result)

            if result['success']:
                successful += 1
                logger.info(f"✅ {symbol} test PASSED")
            else:
                failed += 1
                logger.error(f"❌ {symbol} test FAILED: {result['error']}")

            # Небольшая пауза между тестами
            await asyncio.sleep(2)

        # Итоговая статистика
        logger.info("\n" + "=" * 60)
        logger.info("📊 TEST RESULTS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total tests: {len(LIQUID_SYMBOLS)}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"Success rate: {(successful / len(LIQUID_SYMBOLS) * 100):.1f}%")

        # Детальные результаты
        logger.info("\n📋 DETAILED RESULTS:")
        for result in self.test_results:
            status = "✅" if result['success'] else "❌"
            sl_status = "✅" if result['sl_set'] else "❌"
            logger.info(f"{status} {result['symbol']}: Order={status}, SL={sl_status}")
            if result['error']:
                logger.info(f"   Error: {result['error']}")

        # Опционально: закрыть все позиции после теста
        logger.info("\n" + "=" * 60)
        close_positions = input("Close all test positions? (y/n): ").lower()
        if close_positions == 'y':
            await self.close_all_positions()

        # Закрытие соединения
        if self.exchange:
            await self.exchange.close()

        logger.info("\n✅ Test completed!")


async def main():
    tester = BybitTester()
    await tester.run_tests()


if __name__ == "__main__":
    # Проверка наличия API ключей
    if not os.getenv('BYBIT_API_KEY'):
        logger.error("BYBIT_API_KEY not found in environment")
        sys.exit(1)

    asyncio.run(main())