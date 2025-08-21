"""
Test script for Bybit integration
"""
import asyncio
import os
from dotenv import load_dotenv
from exchanges.bybit import BybitExchange
from decimal import Decimal

load_dotenv('.env')


async def test_bybit():
    """Test Bybit connection and basic functions"""

    print("=" * 50)
    print("BYBIT INTEGRATION TEST")
    print("=" * 50)

    # Initialize
    config = {
        'api_key': os.getenv('BYBIT_API_KEY'),
        'api_secret': os.getenv('BYBIT_API_SECRET'),
        'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
    }

    exchange = BybitExchange(config)

    try:
        # 1. Test connection
        print("\\n1. Testing connection...")
        await exchange.initialize()
        print("✅ Connected to Bybit", "Testnet" if config['testnet'] else "Mainnet")

        # 2. Test balance
        print("\\n2. Getting account balance...")
        balance = await exchange.get_account_balance()
        print(f"✅ Balance: {balance.free} USDT (locked: {balance.locked})")

        # 3. Test symbol info
        print("\\n3. Testing symbol info...")
        symbol = "BTCUSDT"
        info = await exchange.get_symbol_info(symbol)
        if info:
            print(f"✅ {symbol} Info:")
            print(f"   Min quantity: {info.min_quantity}")
            print(f"   Step size: {info.step_size}")
            print(f"   Price precision: {info.price_precision}")
            print(f"   Max leverage: {info.max_leverage}")
            print(f"   Status: {info.status}")
        else:
            print(f"❌ Failed to get {symbol} info")

        # 4. Test market data
        print("\\n4. Testing market data...")
        market = await exchange.get_market_data(symbol)
        if market:
            print(f"✅ {symbol} Market:")
            print(f"   Last price: {market.last_price}")
            print(f"   24h volume: {market.volume_24h}")
        else:
            print(f"❌ Failed to get market data")

        # 5. Test positions
        print("\\n5. Getting open positions...")
        positions = await exchange.get_positions()
        if positions:
            for pos in positions:
                print(f"✅ Position: {pos.symbol} {pos.side} qty={pos.quantity}")
        else:
            print("   No open positions")

        # 6. Test order placement (optional)
        print("\\n6. Test order placement?")
        print("   ⚠️  This will open a real position on testnet")
        test_order = input("   Place test order? (y/n): ").lower()

        if test_order == 'y':
            test_symbol = input("   Symbol (default BTCUSDT): ") or "BTCUSDT"
            test_side = input("   Side (BUY/SELL, default BUY): ") or "BUY"
            test_qty = input("   Quantity in USDT (default 10): ") or "10"

            # Get symbol info for quantity calculation
            symbol_info = await exchange.get_symbol_info(test_symbol)
            market_data = await exchange.get_market_data(test_symbol)

            if symbol_info and market_data:
                # Calculate quantity
                usdt_amount = Decimal(test_qty)
                price = market_data.last_price
                quantity = usdt_amount / price
                quantity = symbol_info.round_quantity(quantity)

                print(f"\\n   Placing {test_side} order: {quantity} {test_symbol}")

                result = await exchange.place_market_order(
                    symbol=test_symbol,
                    side=test_side,
                    quantity=quantity
                )

                if result.get('status') != 'REJECTED':
                    print(f"   ✅ Order placed: {result}")

                    # Test trailing stop
                    if test_side == "BUY":
                        stop_side = "SELL"
                        activation = price * Decimal('1.01')  # +1%
                    else:
                        stop_side = "BUY"
                        activation = price * Decimal('0.99')  # -1%

                    print(f"\\n   Setting trailing stop...")
                    stop_result = await exchange.place_trailing_stop_order(
                        symbol=test_symbol,
                        side=stop_side,
                        callback_rate=Decimal('2'),  # 2%
                        activation_price=activation,
                        quantity=quantity
                    )

                    if stop_result.get('status') != 'REJECTED':
                        print(f"   ✅ Trailing stop set: {stop_result}")
                    else:
                        print(f"   ❌ Trailing stop failed: {stop_result}")
                else:
                    print(f"   ❌ Order failed: {result}")

        print("\\n" + "=" * 50)
        print("✅ BYBIT INTEGRATION TEST COMPLETED")
        print("=" * 50)

    except Exception as e:
        print(f"\\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

    finally:
        await exchange.close()


if __name__ == "__main__":
    asyncio.run(test_bybit())