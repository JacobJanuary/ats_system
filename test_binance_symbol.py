#!/usr/bin/env python3
"""
Тестовый скрипт для проверки доступности символов на Binance testnet/mainnet
"""

import asyncio
import aiohttp
import json
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

load_dotenv()


class BinanceSymbolChecker:
    def __init__(self, testnet: bool = True):
        self.testnet = testnet
        if testnet:
            self.base_url = "https://testnet.binancefuture.com"
        else:
            self.base_url = "https://fapi.binance.com"

        self.session = None
        self.all_symbols = set()
        self.active_symbols = set()
        self.symbol_info = {}

    async def initialize(self):
        """Инициализация и загрузка информации о символах"""
        self.session = aiohttp.ClientSession()

        # Загружаем exchange info
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        async with self.session.get(url) as response:
            if response.status == 200:
                data = await response.json()

                for symbol_info in data.get('symbols', []):
                    symbol = symbol_info['symbol']
                    self.all_symbols.add(symbol)

                    if (symbol_info.get('status') == 'TRADING' and
                            symbol_info.get('contractType') == 'PERPETUAL'):
                        self.active_symbols.add(symbol)
                        self.symbol_info[symbol] = {
                            'status': symbol_info.get('status'),
                            'contractType': symbol_info.get('contractType'),
                            'baseAsset': symbol_info.get('baseAsset'),
                            'quoteAsset': symbol_info.get('quoteAsset'),
                            'marginAsset': symbol_info.get('marginAsset'),
                            'pricePrecision': symbol_info.get('pricePrecision'),
                            'quantityPrecision': symbol_info.get('quantityPrecision'),
                        }

                print(f"✅ Loaded {len(self.active_symbols)} active perpetual symbols")
                print(f"   Total symbols in exchange: {len(self.all_symbols)}")

    async def check_symbol(self, symbol: str) -> Dict:
        """Проверка конкретного символа"""
        result = {
            'symbol': symbol,
            'exists': symbol in self.all_symbols,
            'is_active': symbol in self.active_symbols,
            'has_ticker': False,
            'ticker_data': None,
            'error': None,
            'suggestions': []
        }

        if not result['exists']:
            # Попробуем найти похожие символы
            base = symbol.replace('USDT', '').replace('BUSD', '')
            for s in self.active_symbols:
                if base in s or s.startswith(base[:3]) if len(base) >= 3 else False:
                    result['suggestions'].append(s)

            # Проверим особые случаи (1000PEPE, 1000FLOKI и т.д.)
            if not base.startswith('1000'):
                alt_symbol = f"1000{symbol}"
                if alt_symbol in self.active_symbols:
                    result['suggestions'].insert(0, alt_symbol)

            result['error'] = f"Symbol {symbol} not found on {'testnet' if self.testnet else 'mainnet'}"
            return result

        if not result['is_active']:
            info = self.symbol_info.get(symbol, {})
            result['error'] = f"Symbol exists but not active. Status: {info.get('status', 'UNKNOWN')}"
            return result

        # Проверяем ticker
        try:
            url = f"{self.base_url}/fapi/v1/ticker/bookTicker"
            params = {'symbol': symbol}

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    ticker = await response.json()
                    result['has_ticker'] = True
                    result['ticker_data'] = {
                        'bid': float(ticker.get('bidPrice', 0)),
                        'ask': float(ticker.get('askPrice', 0)),
                        'bidQty': float(ticker.get('bidQty', 0)),
                        'askQty': float(ticker.get('askQty', 0)),
                    }
                else:
                    error_text = await response.text()
                    result['error'] = f"Ticker request failed: {error_text}"
        except Exception as e:
            result['error'] = f"Exception getting ticker: {e}"

        return result

    async def check_multiple_symbols(self, symbols: List[str]):
        """Проверка нескольких символов"""
        print(f"\n{'=' * 60}")
        print(f"Checking symbols on Binance {'TESTNET' if self.testnet else 'MAINNET'}")
        print(f"{'=' * 60}\n")

        for symbol in symbols:
            result = await self.check_symbol(symbol)

            status_emoji = "✅" if result['is_active'] and result['has_ticker'] else "❌"
            print(f"{status_emoji} {symbol}:")

            if result['exists']:
                print(f"   Exists: YES")
                print(f"   Active: {'YES' if result['is_active'] else 'NO'}")

                if result['has_ticker']:
                    ticker = result['ticker_data']
                    print(f"   Ticker: Bid=${ticker['bid']:.4f}, Ask=${ticker['ask']:.4f}")
                    spread = (ticker['ask'] - ticker['bid']) / ticker['bid'] * 100 if ticker['bid'] > 0 else 0
                    print(f"   Spread: {spread:.3f}%")
                    print(f"   Liquidity: Bid Qty={ticker['bidQty']:.2f}, Ask Qty={ticker['askQty']:.2f}")
                else:
                    print(f"   Ticker: NOT AVAILABLE")
            else:
                print(f"   Exists: NO")

            if result['error']:
                print(f"   ⚠️  Error: {result['error']}")

            if result['suggestions']:
                print(f"   💡 Did you mean: {', '.join(result['suggestions'][:3])}")

            print()

    async def find_all_with_base(self, base_asset: str):
        """Найти все символы с определенной базовой валютой"""
        matches = []
        for symbol in self.active_symbols:
            info = self.symbol_info.get(symbol, {})
            if info.get('baseAsset', '').upper() == base_asset.upper():
                matches.append(symbol)

        if matches:
            print(f"\n📊 Found {len(matches)} active symbols with base asset {base_asset.upper()}:")
            for symbol in sorted(matches):
                print(f"   - {symbol}")
        else:
            print(f"\n❌ No active symbols found with base asset {base_asset.upper()}")

        return matches

    async def close(self):
        if self.session:
            await self.session.close()


async def main():
    # Проблемные символы из логов
    problem_symbols = [
        'SKATEUSDT',  # Из последнего лога
        'FUELUSDT',  # Из предыдущих логов
        'RFCUSDT',  # Из предыдущих логов
        'RADUSDT',  # Работал, но с проблемами
        'AVAXUSDT',  # Для сравнения - должен работать
        'BTCUSDT',  # Для сравнения - точно работает
    ]

    # Проверяем на testnet
    print("\n" + "=" * 60)
    print("TESTNET CHECK")
    print("=" * 60)

    checker = BinanceSymbolChecker(testnet=True)
    await checker.initialize()
    await checker.check_multiple_symbols(problem_symbols)

    # Ищем альтернативы для SKATE
    print("\nSearching for SKATE alternatives...")
    await checker.find_all_with_base('SKATE')

    # Проверяем особые случаи
    special_cases = ['1000PEPEUSDT', '1000FLOKIUSDT', '1000SHIBUSDT']
    print("\n" + "=" * 60)
    print("CHECKING SPECIAL CASES (1000x symbols)")
    print("=" * 60)
    await checker.check_multiple_symbols(special_cases)

    await checker.close()

    # Опционально: проверка на mainnet для сравнения
    check_mainnet = input("\nCheck on MAINNET too? (y/n): ").lower() == 'y'
    if check_mainnet:
        print("\n" + "=" * 60)
        print("MAINNET CHECK")
        print("=" * 60)

        mainnet_checker = BinanceSymbolChecker(testnet=False)
        await mainnet_checker.initialize()
        await mainnet_checker.check_multiple_symbols(problem_symbols[:3])  # Проверяем только проблемные
        await mainnet_checker.close()


if __name__ == "__main__":
    asyncio.run(main())