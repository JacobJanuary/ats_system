#!/usr/bin/env python3
"""
Детальная проверка Trailing Stop через разные API endpoints
"""

import asyncio
import os
import json
from dotenv import load_dotenv
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from exchanges.binance import BinanceExchange

load_dotenv()


async def check_ts_detailed():
    """Проверка TS через разные методы API"""

    SYMBOL = "CARVUSDT"  # или любой другой символ

    config = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': os.getenv('TESTNET', 'false').lower() == 'true'
    }

    exchange = BinanceExchange(config)
    await exchange.initialize()

    try:
        print("=" * 80)
        print("DETAILED TRAILING STOP CHECK")
        print("=" * 80)

        # Метод 1: Через openOrders endpoint
        print("\n1. Checking via /fapi/v1/openOrders:")
        open_orders = await exchange._make_request(
            "GET",
            "/fapi/v1/openOrders",
            {'symbol': SYMBOL},
            signed=True
        )

        for order in open_orders:
            if 'TRAILING' in order.get('type', '').upper():
                print(f"\n  Found TS Order:")
                print(f"  Full response:")
                print(json.dumps(order, indent=2))

                # Извлекаем все возможные поля
                print(f"\n  Parsed values:")
                print(f"    activatePrice: {order.get('activatePrice')}")
                print(f"    stopPrice: {order.get('stopPrice')}")
                print(f"    callbackRate: {order.get('callbackRate')}")
                print(f"    priceRate: {order.get('priceRate')}")
                print(f"    trailingDelta: {order.get('trailingDelta')}")
                print(f"    priceProtect: {order.get('priceProtect')}")

        # Метод 2: Через queryOrder если знаем orderId
        print("\n2. Checking specific order (if orderId known):")
        if open_orders and 'TRAILING' in open_orders[0].get('type', '').upper():
            order_id = open_orders[0].get('orderId')

            specific_order = await exchange._make_request(
                "GET",
                "/fapi/v1/order",
                {'symbol': SYMBOL, 'orderId': order_id},
                signed=True
            )

            if specific_order:
                print(f"  Specific order query result:")
                print(json.dumps(specific_order, indent=2))

        # Метод 3: Проверка позиции на наличие trailing stop
        print("\n3. Checking position info:")
        position = await exchange._make_request(
            "GET",
            "/fapi/v2/positionRisk",
            {'symbol': SYMBOL},
            signed=True
        )

        if position:
            for pos in position:
                if float(pos.get('positionAmt', 0)) != 0:
                    print(f"  Position found:")
                    print(f"    Symbol: {pos.get('symbol')}")
                    print(f"    Position Amount: {pos.get('positionAmt')}")
                    print(f"    Entry Price: {pos.get('entryPrice')}")
                    print(f"    Mark Price: {pos.get('markPrice')}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        await exchange.close()


if __name__ == "__main__":
    asyncio.run(check_ts_detailed())