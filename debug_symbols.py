#!/usr/bin/env python3
"""
Debug script for testing problematic symbols on Binance
Helps diagnose issues with PUFFERUSDT, SKATEUSDT and other pairs
"""

import asyncio
import os
import sys
from dotenv import load_dotenv
import logging

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.binance import BinanceExchange

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_symbol(exchange: BinanceExchange, symbol: str):
    """Test all aspects of a symbol"""
    print(f"\n{'=' * 60}")
    print(f"Testing symbol: {symbol}")
    print('=' * 60)

    # 1. Check if symbol exists in exchange info
    if symbol in exchange.exchange_info:
        print(f"✅ Symbol found in exchange info")

        # Get symbol info
        symbol_info = exchange.exchange_info[symbol]
        print(f"   Status: {symbol_info.get('status')}")
        print(f"   Contract Type: {symbol_info.get('contractType')}")
        print(f"   Max Leverage: {exchange.symbol_leverage_limits.get(symbol, 'N/A')}")

        # Check filters
        filters = symbol_info.get('filters', [])
        for f in filters:
            if f['filterType'] == 'LOT_SIZE':
                print(f"   Min Qty: {f.get('minQty')}")
                print(f"   Max Qty: {f.get('maxQty')}")
                print(f"   Step Size: {f.get('stepSize')}")
            elif f['filterType'] == 'PRICE_FILTER':
                print(f"   Min Price: {f.get('minPrice')}")
                print(f"   Max Price: {f.get('maxPrice')}")
                print(f"   Tick Size: {f.get('tickSize')}")
            elif f['filterType'] == 'MIN_NOTIONAL':
                print(f"   Min Notional: {f.get('notional')}")
    else:
        print(f"❌ Symbol NOT found in exchange info")
        print(f"   Available similar symbols:")
        similar = [s for s in exchange.exchange_info.keys() if symbol[:4] in s]
        for s in similar[:5]:
            print(f"      - {s}")

    # 2. Test ticker methods
    print(f"\n📊 Testing ticker endpoints:")

    # Test bookTicker
    try:
        result = await exchange._make_request("GET", "/fapi/v1/ticker/bookTicker", {'symbol': symbol})
        if result:
            print(f"✅ bookTicker: bid={result.get('bidPrice')}, ask={result.get('askPrice')}")
        else:
            print(f"❌ bookTicker: No data")
    except Exception as e:
        print(f"❌ bookTicker error: {e}")

    # Test 24hr ticker
    try:
        result = await exchange._make_request("GET", "/fapi/v1/ticker/24hr", {'symbol': symbol})
        if result:
            print(f"✅ 24hr ticker: price={result.get('lastPrice')}, volume={result.get('volume')}")
        else:
            print(f"❌ 24hr ticker: No data")
    except Exception as e:
        print(f"❌ 24hr ticker error: {e}")

    # Test price ticker
    try:
        result = await exchange._make_request("GET", "/fapi/v1/ticker/price", {'symbol': symbol})
        if result:
            print(f"✅ Price ticker: {result.get('price')}")
        else:
            print(f"❌ Price ticker: No data")
    except Exception as e:
        print(f"❌ Price ticker error: {e}")

    # Test mark price
    try:
        result = await exchange._make_request("GET", "/fapi/v1/premiumIndex", {'symbol': symbol})
        if result:
            print(f"✅ Mark price: {result.get('markPrice')}")
        else:
            print(f"❌ Mark price: No data")
    except Exception as e:
        print(f"❌ Mark price error: {e}")

    # 3. Test get_ticker method
    print(f"\n🔧 Testing get_ticker method:")
    ticker = await exchange.get_ticker(symbol)
    if ticker:
        print(f"✅ get_ticker successful:")
        print(f"   Price: ${ticker.get('price', 0):.4f}")
        print(f"   Bid: ${ticker.get('bid', 0):.4f}")
        print(f"   Ask: ${ticker.get('ask', 0):.4f}")

        # Calculate spread
        bid = ticker.get('bid', 0)
        ask = ticker.get('ask', 0)
        if bid and ask:
            spread = ((ask - bid) / bid) * 100
            print(f"   Spread: {spread:.3f}%")
    else:
        print(f"❌ get_ticker failed - no data returned")

    # 4. Test leverage setting
    print(f"\n⚙️ Testing leverage:")
    for leverage in [125, 50, 20, 10, 5, 3, 1]:
        try:
            result = await exchange.set_leverage(symbol, leverage)
            if result:
                print(f"✅ Leverage {leverage}x set successfully")
                break
        except Exception as e:
            print(f"❌ Leverage {leverage}x failed: {e}")

    # 5. Test order creation (dry run - get minimum order size)
    print(f"\n📦 Order requirements:")
    if ticker and ticker.get('price'):
        price = ticker.get('price')

        # Calculate minimum order size
        min_notional = 10.0 if exchange.testnet else 5.0
        min_qty = min_notional / price

        print(f"   Current Price: ${price:.4f}")
        print(f"   Min Notional: ${min_notional}")
        print(f"   Min Quantity: {min_qty:.6f}")

        # Format quantity according to exchange rules
        if symbol in exchange.exchange_info:
            formatted_qty = exchange.format_quantity(symbol, min_qty)
            print(f"   Formatted Qty: {formatted_qty}")

            # Calculate actual order value
            order_value = float(formatted_qty) * price
            print(f"   Order Value: ${order_value:.2f}")


async def main():
    """Main test function"""
    print("🚀 Starting Symbol Debug Tool")
    print(f"Mode: {'TESTNET' if os.getenv('TESTNET', 'false').lower() == 'true' else 'MAINNET'}")

    # Initialize exchange
    exchange = BinanceExchange({
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': os.getenv('TESTNET', 'false').lower() == 'true'
    })

    await exchange.initialize()

    # Get balance
    balance = await exchange.get_balance()
    print(f"\n💰 Account Balance: ${balance:.2f}")

    # Test problematic symbols
    test_symbols = ['PUFFERUSDT', 'SKATEUSDT', 'BTCUSDT', 'ETHUSDT']

    # Add any symbols from command line
    if len(sys.argv) > 1:
        test_symbols = sys.argv[1:] + test_symbols

    # Remove duplicates
    test_symbols = list(dict.fromkeys(test_symbols))

    print(f"\n📋 Testing symbols: {test_symbols}")

    for symbol in test_symbols:
        await test_symbol(exchange, symbol)
        await asyncio.sleep(1)  # Rate limiting

    # List all available symbols containing specific strings
    print(f"\n📜 Available symbols on exchange:")

    search_terms = ['PUFFER', 'SKATE', 'NEW', 'LATEST']
    for term in search_terms:
        matching = [s for s in exchange.exchange_info.keys() if term in s.upper()]
        if matching:
            print(f"\nSymbols containing '{term}':")
            for s in matching[:10]:
                info = exchange.exchange_info[s]
                status = info.get('status')
                contract = info.get('contractType')
                print(f"   {s}: status={status}, type={contract}")

    # Summary
    print(f"\n{'=' * 60}")
    print("📊 Summary:")
    print(f"Total symbols loaded: {len(exchange.exchange_info)}")
    print(
        f"Active perpetuals: {sum(1 for s in exchange.exchange_info.values() if s.get('status') == 'TRADING' and s.get('contractType') == 'PERPETUAL')}")
    print('=' * 60)

    await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())