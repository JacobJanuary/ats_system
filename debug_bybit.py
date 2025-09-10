#!/usr/bin/env python3
"""
Debug script for testing Bybit order execution
Tests the complete order flow and identifies issues
"""

import asyncio
import os
import sys
from dotenv import load_dotenv
import logging
from decimal import Decimal

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.bybit import BybitExchange

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_symbol(exchange: BybitExchange, symbol: str):
    """Test all aspects of a symbol on Bybit"""
    print(f"\n{'=' * 60}")
    print(f"Testing symbol: {symbol}")
    print('=' * 60)

    # 1. Check if symbol exists in loaded info
    if symbol in exchange.symbol_info:
        print(f"✅ Symbol found in exchange info")
        info = exchange.symbol_info[symbol]
        print(f"   Min Qty: {info['min_qty']}")
        print(f"   Max Qty: {info['max_qty']}")
        print(f"   Qty Step: {info['qty_step']}")
        print(f"   Min Price: {info['min_price']}")
        print(f"   Max Price: {info['max_price']}")
        print(f"   Tick Size: {info['tick_size']}")
        print(f"   Max Leverage: {info['max_leverage']}")
    else:
        print(f"❌ Symbol NOT found in exchange info")
        # Try to get it directly
        result = await exchange._make_request(
            "GET",
            "/v5/market/instruments-info",
            {'category': 'linear', 'symbol': symbol}
        )
        if result and 'list' in result and result['list']:
            instrument = result['list'][0]
            print(f"   Found via API: status={instrument.get('status')}")

    # 2. Test ticker
    print(f"\n📊 Testing ticker:")
    ticker = await exchange.get_ticker(symbol)
    if ticker:
        print(f"✅ Ticker data retrieved:")
        print(f"   Price: ${ticker.get('price', 0):.4f}")
        print(f"   Bid: ${ticker.get('bid', 0):.4f}")
        print(f"   Ask: ${ticker.get('ask', 0):.4f}")

        if ticker.get('bid') and ticker.get('ask'):
            spread = ((ticker['ask'] - ticker['bid']) / ticker['bid']) * 100
            print(f"   Spread: {spread:.3f}%")
    else:
        print(f"❌ Failed to get ticker")

    # 3. Test leverage
    print(f"\n⚙️ Testing leverage:")
    leverage_options = [10, 5, 3, 2, 1]
    for lev in leverage_options:
        result = await exchange.set_leverage(symbol, lev)
        if result:
            print(f"✅ Leverage {lev}x set successfully")
            break
        else:
            print(f"❌ Failed to set leverage {lev}x")

    # 4. Calculate order parameters
    print(f"\n📦 Order parameters:")
    if ticker and ticker.get('price'):
        price = ticker['price']
        min_notional = 10.0  # Bybit minimum
        min_qty = min_notional / price

        print(f"   Current Price: ${price:.4f}")
        print(f"   Min Notional: ${min_notional}")
        print(f"   Calculated Min Qty: {min_qty:.6f}")

        # Format quantity
        formatted_qty = exchange.format_quantity(symbol, min_qty)
        print(f"   Formatted Qty: {formatted_qty}")

        # Calculate actual order value
        order_value = float(formatted_qty) * price
        print(f"   Order Value: ${order_value:.2f}")

        return {
            'symbol': symbol,
            'price': price,
            'quantity': float(formatted_qty),
            'order_value': order_value
        }

    return None


async def test_order_creation(exchange: BybitExchange, symbol: str, test_size_usd: float = 20):
    """Test actual order creation (BE CAREFUL - THIS CREATES REAL ORDERS!)"""
    print(f"\n{'=' * 60}")
    print(f"⚠️ TESTING ORDER CREATION for {symbol}")
    print(f"   Size: ${test_size_usd}")
    print('=' * 60)

    # Get ticker
    ticker = await exchange.get_ticker(symbol)
    if not ticker or not ticker.get('price'):
        print("❌ Cannot get price, aborting")
        return

    price = ticker['price']
    quantity = test_size_usd / price

    print(f"Order details:")
    print(f"   Price: ${price:.4f}")
    print(f"   Raw Quantity: {quantity:.6f}")
    print(f"   Formatted Qty: {exchange.format_quantity(symbol, quantity)}")

    # Confirm before creating order
    response = input("\n⚠️ Create test order? (yes/no): ")
    if response.lower() != 'yes':
        print("Order creation cancelled")
        return

    # Create order
    print("\n📤 Creating market order...")
    order = await exchange.create_market_order(symbol, 'BUY', quantity)

    if order:
        print(f"✅ Order created successfully!")
        print(f"   Order ID: {order['orderId']}")
        print(f"   Executed Qty: {order['quantity']}")
        print(f"   Avg Price: ${order['price']:.4f}")
        print(f"   Status: {order['status']}")
        print(f"   Value: ${order['quantity'] * order['price']:.2f}")

        # Wait a bit
        await asyncio.sleep(2)

        # Check position
        print(f"\n📊 Checking position:")
        positions = await exchange.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                print(f"✅ Position found:")
                print(f"   Side: {pos['side']}")
                print(f"   Quantity: {pos['quantity']}")
                print(f"   Entry Price: ${pos['entry_price']:.4f}")
                print(f"   Mark Price: ${pos['mark_price']:.4f}")
                print(f"   PnL: ${pos['pnl']:.2f} ({pos['pnl_percent']:.2f}%)")

                # Ask if should close
                response = input("\n⚠️ Close position? (yes/no): ")
                if response.lower() == 'yes':
                    result = await exchange.close_position(symbol)
                    if result:
                        print("✅ Position closed")
                    else:
                        print("❌ Failed to close position")
                break
    else:
        print(f"❌ Order creation failed!")


async def check_account_info(exchange: BybitExchange):
    """Check account configuration and status"""
    print(f"\n{'=' * 60}")
    print("Account Information")
    print('=' * 60)

    # Get wallet balance
    balance = await exchange.get_balance()
    print(f"💰 USDT Balance: ${balance:.2f}")

    # Get account info
    account_info = await exchange._make_request(
        "GET",
        "/v5/account/info",
        signed=True
    )

    if account_info:
        print(f"\n📋 Account Details:")
        print(f"   UID: {account_info.get('uid', 'N/A')}")
        print(f"   Account Type: {account_info.get('accountType', 'N/A')}")

        if 'unifiedMarginStatus' in account_info:
            status = account_info['unifiedMarginStatus']
            status_map = {
                0: "Classic Account",
                1: "Unified Margin (Spot/Linear)",
                2: "Unified Margin (Spot/Linear/Options)",
                3: "Unified Margin (Spot/Inverse)",
                4: "Unified Margin (All)"
            }
            print(f"   Margin Mode: {status_map.get(status, 'Unknown')}")

    # Check open positions
    positions = await exchange.get_open_positions()
    if positions:
        print(f"\n📈 Open Positions ({len(positions)}):")
        for pos in positions:
            print(f"   {pos['symbol']}: {pos['side']} {pos['quantity']} @ ${pos['entry_price']:.4f}")
            print(f"      PnL: ${pos['pnl']:.2f} ({pos['pnl_percent']:.2f}%)")
    else:
        print(f"\n📈 No open positions")

    # Check open orders
    orders = await exchange.get_open_orders()
    if orders:
        print(f"\n📋 Open Orders ({len(orders)}):")
        for order in orders[:5]:  # Show first 5
            print(f"   {order['symbol']}: {order['side']} {order['qty']} @ {order['price']}")
    else:
        print(f"\n📋 No open orders")


async def main():
    """Main test function"""
    print("🚀 Starting Bybit Debug Tool")
    print(f"Mode: {'TESTNET' if os.getenv('TESTNET', 'false').lower() == 'true' else 'MAINNET'}")

    # Initialize exchange
    exchange = BybitExchange({
        'api_key': os.getenv('BYBIT_API_KEY'),
        'api_secret': os.getenv('BYBIT_API_SECRET'),
        'testnet': os.getenv('TESTNET', 'false').lower() == 'true'
    })

    await exchange.initialize()

    # Check account info first
    await check_account_info(exchange)

    # Test symbols from the log
    test_symbols = ['FLRUSDT', 'HPOS10IUSDT', 'BEAMUSDT']

    # Add any symbols from command line
    if len(sys.argv) > 1:
        test_symbols = sys.argv[1:] + test_symbols

    # Remove duplicates
    test_symbols = list(dict.fromkeys(test_symbols))

    print(f"\n📋 Testing symbols: {test_symbols}")

    tested_symbols = []
    for symbol in test_symbols:
        result = await test_symbol(exchange, symbol)
        if result:
            tested_symbols.append(result)
        await asyncio.sleep(1)  # Rate limiting

    # Ask if want to test order creation
    if tested_symbols:
        print(f"\n{'=' * 60}")
        print("⚠️ ORDER CREATION TEST (CREATES REAL ORDERS!)")
        print('=' * 60)
        print("Available symbols for testing:")
        for i, sym_info in enumerate(tested_symbols, 1):
            print(f"   {i}. {sym_info['symbol']}: ${sym_info['price']:.4f} (min ${sym_info['order_value']:.2f})")

        response = input("\nEnter symbol number to test (or 'skip'): ")
        if response.lower() != 'skip':
            try:
                idx = int(response) - 1
                if 0 <= idx < len(tested_symbols):
                    await test_order_creation(exchange, tested_symbols[idx]['symbol'])
            except ValueError:
                print("Invalid input")

    # Summary
    print(f"\n{'=' * 60}")
    print("📊 Summary:")
    print(f"Position Mode: {exchange.position_mode}")
    print(f"Symbols Loaded: {len(exchange.symbol_info)}")
    print('=' * 60)

    await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())