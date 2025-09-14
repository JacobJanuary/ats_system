#!/usr/bin/env python3
"""
Тестовый скрипт для проверки format_quantity в Binance
и правильности расчета Stop Loss при проскальзывании
"""

import asyncio
import os
import sys
from decimal import Decimal, ROUND_DOWN
from dotenv import load_dotenv

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange

load_dotenv()


class FormatQuantityTester:
    def __init__(self):
        self.exchange = None
        self.test_cases = [
            # (symbol, quantity, expected_result_range)
            ("BTCUSDT", 0.001, (0.001, 0.001)),
            ("ETHUSDT", 0.01, (0.01, 0.01)),
            ("AVAXUSDT", 0.674878, (0.67, 0.68)),  # Из вашего лога
            ("AAVEUSDT", 0.065212, (0.06, 0.07)),  # Из вашего лога - ПРОБЛЕМНЫЙ
            ("SOLUSDT", 0.1234, (0.1, 0.13)),
            ("DOTUSDT", 1.5678, (1.5, 1.6)),
            ("1000PEPEUSDT", 100.123, (100, 101)),
        ]

    async def initialize(self):
        """Инициализация подключения к Binance"""
        config = {
            'api_key': os.getenv('BINANCE_API_KEY'),
            'api_secret': os.getenv('BINANCE_API_SECRET'),
            'testnet': True
        }

        self.exchange = BinanceExchange(config)
        await self.exchange.initialize()
        print(f"✅ Binance {'testnet' if config['testnet'] else 'mainnet'} initialized")
        print(f"   Loaded {len(self.exchange.exchange_info)} symbols\n")

    def test_format_quantity_manual(self, symbol: str, quantity: float):
        """Ручная проверка форматирования для конкретного символа"""
        if symbol not in self.exchange.exchange_info:
            print(f"❌ Symbol {symbol} not found in exchange info")
            return None

        filters = self.exchange.exchange_info[symbol]['filters']
        lot_size_filter = next((f for f in filters if f['filterType'] == 'LOT_SIZE'), None)

        if not lot_size_filter:
            print(f"❌ LOT_SIZE filter not found for {symbol}")
            return None

        step_size = Decimal(lot_size_filter['stepSize'])
        min_qty = Decimal(lot_size_filter['minQty'])
        max_qty = Decimal(lot_size_filter['maxQty'])

        print(f"📊 {symbol} LOT_SIZE Filter:")
        print(f"   Step Size: {step_size}")
        print(f"   Min Qty: {min_qty}")
        print(f"   Max Qty: {max_qty}")

        # Текущая реализация из binance.py
        quantity_decimal = Decimal(str(quantity))
        quantized_qty = (quantity_decimal / step_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * step_size

        # Проблемная строка - определение decimals
        step_str = str(step_size).rstrip('0')
        decimals_old = len(step_str.split('.')[1]) if '.' in step_str else 0

        # Правильный способ
        decimals_new = abs(step_size.as_tuple().exponent)

        formatted_old = format(quantized_qty, f'.{decimals_old}f')
        formatted_new = f"{quantized_qty:.{decimals_new}f}"

        print(f"\n   Input quantity: {quantity}")
        print(f"   Quantized: {quantized_qty}")
        print(f"   Decimals (OLD method): {decimals_old}")
        print(f"   Decimals (NEW method): {decimals_new}")
        print(f"   Formatted (OLD): {formatted_old}")
        print(f"   Formatted (NEW): {formatted_new}")

        # Используем встроенный метод
        formatted_builtin = self.exchange.format_quantity(symbol, quantity)
        print(f"   Formatted (BUILTIN): {formatted_builtin}")

        return {
            'old': float(formatted_old),
            'new': float(formatted_new),
            'builtin': float(formatted_builtin)
        }

    async def run_tests(self):
        """Запуск всех тестов"""
        await self.initialize()

        print("=" * 60)
        print("TESTING format_quantity FUNCTION")
        print("=" * 60)

        failed_tests = []

        for symbol, quantity, expected_range in self.test_cases:
            print(f"\n🔍 Testing {symbol} with quantity {quantity}")
            print("-" * 40)

            result = self.test_format_quantity_manual(symbol, quantity)

            if result:
                # Проверяем результаты
                if result['builtin'] == 0:
                    print(f"   ⚠️  WARNING: Builtin method returned 0!")
                    failed_tests.append((symbol, quantity, result))
                elif result['builtin'] < expected_range[0] or result['builtin'] > expected_range[1]:
                    print(f"   ⚠️  WARNING: Result {result['builtin']} outside expected range {expected_range}")
                else:
                    print(f"   ✅ PASSED: {result['builtin']} in range {expected_range}")

            print()

        # Итоговый отчет
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        if failed_tests:
            print(f"❌ {len(failed_tests)} tests failed:")
            for symbol, qty, result in failed_tests:
                print(f"   {symbol}: {qty} -> {result['builtin']}")
            print("\n🔧 RECOMMENDATION: Update format_quantity to use:")
            print("   precision = abs(Decimal(step_size).as_tuple().exponent)")
            print("   return f\"{quantized_qty:.{precision}f}\"")
        else:
            print("✅ All tests passed!")

        await self.test_stop_loss_calculation()

    async def test_stop_loss_calculation(self):
        """Тест расчета Stop Loss при проскальзывании"""
        print("\n" + "=" * 60)
        print("TESTING STOP LOSS CALCULATION")
        print("=" * 60)

        test_scenarios = [
            {
                'name': 'LONG без проскальзывания',
                'side': 'BUY',
                'entry_price': 100.0,
                'current_price': 100.0,
                'sl_percent': 2.0,
                'expected_sl': 98.0
            },
            {
                'name': 'LONG с проскальзыванием вверх',
                'side': 'BUY',
                'entry_price': 100.0,
                'current_price': 95.0,  # Цена упала после входа
                'sl_percent': 2.0,
                'expected_sl': 93.15  # 95 * 0.98 (должен быть ниже текущей)
            },
            {
                'name': 'SHORT без проскальзывания',
                'side': 'SELL',
                'entry_price': 100.0,
                'current_price': 100.0,
                'sl_percent': 2.0,
                'expected_sl': 102.0
            },
            {
                'name': 'SHORT с проскальзыванием',
                'side': 'SELL',
                'entry_price': 100.0,
                'current_price': 105.0,  # Цена выросла после входа
                'sl_percent': 2.0,
                'expected_sl': 107.1  # 105 * 1.02 (должен быть выше текущей)
            },
            {
                'name': 'Реальный кейс из лога (RADUSDT)',
                'side': 'BUY',
                'entry_price': 0.244,  # Цена исполнения с проскальзыванием
                'current_price': 0.196,  # Реальная рыночная цена
                'sl_percent': 2.0,
                'expected_sl': 0.192  # 0.196 * 0.98
            }
        ]

        for scenario in test_scenarios:
            print(f"\n📍 {scenario['name']}:")
            print(f"   Entry: ${scenario['entry_price']:.4f}")
            print(f"   Current: ${scenario['current_price']:.4f}")
            print(f"   SL %: {scenario['sl_percent']}%")

            # Старый расчет (может быть неправильным)
            if scenario['side'] == 'BUY':
                sl_old = scenario['entry_price'] * (1 - scenario['sl_percent'] / 100)
            else:
                sl_old = scenario['entry_price'] * (1 + scenario['sl_percent'] / 100)

            # Новый расчет с проверкой
            if scenario['side'] == 'BUY':
                sl_new = scenario['entry_price'] * (1 - scenario['sl_percent'] / 100)
                if sl_new >= scenario['current_price']:
                    # Используем тот же процент от текущей цены
                    sl_new = scenario['current_price'] * (1 - scenario['sl_percent'] / 100)
                    print(f"   ⚠️  SL adjusted due to slippage")
            else:  # SELL/SHORT
                sl_new = scenario['entry_price'] * (1 + scenario['sl_percent'] / 100)
                if sl_new <= scenario['current_price']:
                    # Используем тот же процент от текущей цены
                    sl_new = scenario['current_price'] * (1 + scenario['sl_percent'] / 100)
                    print(f"   ⚠️  SL adjusted due to slippage")

            print(f"   SL (old logic): ${sl_old:.4f}")
            print(f"   SL (new logic): ${sl_new:.4f}")

            # Проверка корректности
            is_valid = True
            if scenario['side'] == 'BUY':
                if sl_new >= scenario['current_price']:
                    print(f"   ❌ ERROR: SL ${sl_new:.4f} >= current ${scenario['current_price']:.4f}")
                    is_valid = False
            else:
                if sl_new <= scenario['current_price']:
                    print(f"   ❌ ERROR: SL ${sl_new:.4f} <= current ${scenario['current_price']:.4f}")
                    is_valid = False

            if is_valid:
                print(f"   ✅ VALID: SL correctly positioned")


async def main():
    tester = FormatQuantityTester()
    await tester.run_tests()

    # Закрываем соединение
    if tester.exchange:
        await tester.exchange.close()


if __name__ == "__main__":
    asyncio.run(main())