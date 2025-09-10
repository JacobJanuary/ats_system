#!/usr/bin/env python3
"""
Check Bybit symbol specifications and calculate correct order quantities
"""

import asyncio
import os
import sys
from decimal import Decimal, ROUND_DOWN
from dotenv import load_dotenv
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from exchanges.bybit import BybitExchange

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def check_symbol_specs(exchange: BybitExchange, symbol: str, test_value_usd: float = 20):
    """Check symbol specifications and calculate correct quantity"""
    print(f"\n{'=' * 60}")
    print(f"Symbol: {symbol}")
    print('=' * 60)

    # Get symbol info
    if symbol not in exchange.symbol_info:
        # Try to load it
        result = await exchange._make_request(
            "GET",
            "/v5/market/instruments-info",
            {'category': 'linear', 'symbol': symbol}
        )

        if result and 'list' in result and result['list']:
            instrument = result['list'][0]
            print(f"Status: {instrument.get('status')}")

            if instrument.get('status') == 'Trading':
                lot_filter = instrument.get('lotSizeFilter', {})
                price_filter = instrument.get('priceFilter', {})
                leverage_filter = instrument.get('leverageFilter', {})

                print("\n📋 Lot Size Filter:")
                print(f"   Min Order Qty: {lot_filter.get('minOrderQty')}")
                print(f"   Max Order Qty: {lot_filter.get('maxOrderQty')}")
                print(f"   Qty Step: {lot_filter.get('qtyStep')}")

                print("\n💰 Price Filter:")
                print(f"   Min Price: {price_filter.get('minPrice')}")
                print(f"   Max Price: {price_filter.get('maxPrice')}")
                print(f"   Tick Size: {price_filter.get('tickSize')}")

                print("\n⚙️ Leverage Filter:")
                print(f"   Min Leverage: {leverage_filter.get('minLeverage')}")
                print(f"   Max Leverage: {leverage_filter.get('maxLeverage')}")

                # Get current price
                ticker = await exchange.get_ticker(symbol)
                if ticker and ticker.get('price'):
                    price = ticker['price']
                    print(f"\n💵 Current Price: ${price:.6f}")

                    # Calculate quantity
                    raw_qty = test_value_usd / price
                    print(f"\n📊 Order Calculation for ${test_value_usd}:")
                    print(f"   Raw quantity: {raw_qty:.8f}")

                    # Apply qty step
                    qty_step = Decimal(lot_filter.get('qtyStep', '1'))
                    min_qty = Decimal(lot_filter.get('minOrderQty', '1'))

                    qty_decimal = Decimal(str(raw_qty))

                    # Round down to qty_step
                    rounded_qty = (qty_decimal / qty_step).quantize(Decimal('1'), rounding=ROUND_DOWN) * qty_step

                    if rounded_qty < min_qty:
                        rounded_qty = min_qty
                        print(f"   ⚠️ Adjusted to minimum: {rounded_qty}")
                    else:
                        print(f"   Rounded to step: {rounded_qty}")

                    # Calculate decimal places needed
                    step_str = str(qty_step)
                    if '.' in step_str:
                        decimal_places = len(step_str.split('.')[1].rstrip('0'))
                    else:
                        decimal_places = 0

                    # Format final quantity
                    if decimal_places > 0:
                        final_qty = f"{float(rounded_qty):.{decimal_places}f}".rstrip('0').rstrip('.')
                    else:
                        final_qty = str(int(rounded_qty))

                    print(f"   ✅ Final quantity: {final_qty}")

                    # Calculate actual order value
                    order_value = float(final_qty) * price
                    print(f"   Order value: ${order_value:.2f}")

                    # Check if order value meets minimum
                    min_order_value = 10.0  # Bybit minimum
                    if order_value < min_order_value:
                        print(f"   ⚠️ Order value below minimum ${min_order_value}")

                        # Calculate minimum quantity needed
                        min_qty_needed = min_order_value / price
                        min_qty_decimal = Decimal(str(min_qty_needed))
                        min_qty_rounded = ((min_qty_decimal / qty_step).quantize(Decimal('1'),
                                                                                 rounding=ROUND_DOWN) + 1) * qty_step

                        if decimal_places > 0:
                            min_qty_final = f"{float(min_qty_rounded):.{decimal_places}f}".rstrip('0').rstrip('.')
                        else:
                            min_qty_final = str(int(min_qty_rounded))

                        min_value = float(min_qty_final) * price
                        print(f"   Minimum valid order: {min_qty_final} (${min_value:.2f})")

                    return {
                        'symbol': symbol,
                        'price': price,
                        'qty_step': float(qty_step),
                        'min_qty': float(min_qty),
                        'final_qty': final_qty,
                        'order_value': order_value
                    }
            else:
                print(f"❌ Symbol not trading: {instrument.get('status')}")
        else:
            print(f"❌ Symbol not found")
    else:
        # Symbol already loaded
        info = exchange.symbol_info[symbol]
        print("\n📋 Loaded Specifications:")
        print(f"   Min Qty: {info['min_qty']}")
        print(f"   Max Qty: {info['max_qty']}")
        print(f"   Qty Step: {info['qty_step']}")
        print(f"   Max Leverage: {info['max_leverage']}")

        # Get ticker and calculate
        ticker = await exchange.get_ticker(symbol)
        if ticker and ticker.get('price'):
            price = ticker['price']
            print(f"\n💵 Current Price: ${price:.6f}")

            raw_qty = test_value_usd / price
            print(f"\n📊 Order Calculation for ${test_value_usd}:")
            print(f"   Raw quantity: {raw_qty:.8f}")

            # Use the format_quantity method
            formatted_qty = exchange.format_quantity(symbol, raw_qty)
            print(f"   ✅ Formatted quantity: {formatted_qty}")

            order_value = float(formatted_qty) * price
            print(f"   Order value: ${order_value:.2f}")

            if order_value < 10:
                print(f"   ⚠️ Order value below $10 minimum!")

                # Calculate minimum
                min_needed = 10.0 / price
                min_formatted = exchange.format_quantity(symbol, min_needed * 1.1)  # Add 10% buffer
                min_value = float(min_formatted) * price
                print(f"   Minimum valid order: {min_formatted} (${min_value:.2f})")

            return {
                'symbol': symbol,
                'price': price,
                'qty_step': info['qty_step'],
                'min_qty': info['min_qty'],
                'final_qty': formatted_qty,
                'order_value': order_value
            }

    return None


async def test_order_with_correct_qty(exchange: BybitExchange, symbol: str, quantity: str):
    """Test order with pre-calculated quantity"""
    print(f"\n📤 Creating order: {quantity} {symbol}")

    order = await exchange.create_market_order(symbol, 'BUY', float(quantity))

    if order:
        print(f"✅ Order successful!")
        print(f"   Order ID: {order['orderId']}")
        print(f"   Executed: {order['quantity']} @ ${order['price']:.4f}")
        print(f"   Value: ${order['quantity'] * order['price']:.2f}")

        # Check position
        await asyncio.sleep(2)
        positions = await exchange.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                print(f"\n📈 Position created:")
                print(f"   Size: {pos['quantity']}")
                print(f"   Entry: ${pos['entry_price']:.4f}")

                # Ask to close
                response = input("\n⚠️ Close position? (yes/no): ")
                if response.lower() == 'yes':
                    await exchange.close_position(symbol)
                    print("✅ Position closed")
                break

        return True
    else:
        print("❌ Order failed!")
        return False


async def main():
    print("🚀 Bybit Symbol Specification Checker")
    print(f"Mode: {'TESTNET' if os.getenv('TESTNET', 'false').lower() == 'true' else 'MAINNET'}")

    # Initialize exchange
    exchange = BybitExchange({
        'api_key': os.getenv('BYBIT_API_KEY'),
        'api_secret': os.getenv('BYBIT_API_SECRET'),
        'testnet': os.getenv('TESTNET', 'false').lower() == 'true'
    })

    await exchange.initialize()

    # Get balance
    balance = await exchange.get_balance()
    print(f"\n💰 Balance: ${balance:.2f}")

    # Test symbols
    test_symbols = ['BTCUSDT', 'FLRUSDT', 'HPOS10IUSDT', 'BEAMUSDT']

    if len(sys.argv) > 1:
        test_symbols = sys.argv[1:]

    results = []
    for symbol in test_symbols:
        result = await check_symbol_specs(exchange, symbol)
        if result:
            results.append(result)

    # Summary
    if results:
        print(f"\n{'=' * 60}")
        print("📊 Summary - Correct Order Quantities:")
        print('=' * 60)
        for r in results:
            status = "✅" if r['order_value'] >= 10 else "⚠️"
            print(f"{status} {r['symbol']}: {r['final_qty']} (${r['order_value']:.2f}) @ ${r['price']:.4f}")

        # Offer to test
        print(f"\n{'=' * 60}")
        valid_symbols = [r for r in results if r['order_value'] >= 10]
        if valid_symbols:
            print("Test order creation? (select number or 'skip'):")
            for i, r in enumerate(valid_symbols, 1):
                print(f"   {i}. {r['symbol']}: {r['final_qty']} (${r['order_value']:.2f})")

            response = input("\nYour choice: ")
            if response.lower() != 'skip':
                try:
                    idx = int(response) - 1
                    if 0 <= idx < len(valid_symbols):
                        selected = valid_symbols[idx]
                        await test_order_with_correct_qty(
                            exchange,
                            selected['symbol'],
                            selected['final_qty']
                        )
                except ValueError:
                    print("Invalid input")

    await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())