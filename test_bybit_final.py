#!/usr/bin/env python3
"""
Final test for Bybit - absolutely no orderId in GET requests
This script uses direct API calls to avoid any cached code issues
"""

import asyncio
import os
import time
import hmac
import hashlib
import json
import aiohttp
from decimal import Decimal, ROUND_DOWN
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BybitDirectClient:
    """Direct Bybit client - no complex logic, just raw API calls"""

    def __init__(self):
        self.api_key = os.getenv('BYBIT_API_KEY')
        self.api_secret = os.getenv('BYBIT_API_SECRET')
        self.testnet = os.getenv('TESTNET', 'false').lower() == 'true'

        if self.testnet:
            self.base_url = "https://api-testnet.bybit.com"
        else:
            self.base_url = "https://api.bybit.com"

        self.recv_window = 5000
        self.session = None

    def generate_signature(self, timestamp: str, params_str: str) -> str:
        message = f"{timestamp}{self.api_key}{self.recv_window}{params_str}"
        return hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def request(self, method: str, endpoint: str, params: dict = None):
        if not self.session:
            self.session = aiohttp.ClientSession()

        url = f"{self.base_url}{endpoint}"
        timestamp = str(int(time.time() * 1000))

        headers = {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': str(self.recv_window),
        }

        if method == "GET" and params:
            sorted_params = sorted(params.items())
            query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
            headers['X-BAPI-SIGN'] = self.generate_signature(timestamp, query_string)

            async with self.session.get(url, params=params, headers=headers) as response:
                return await response.json()

        elif method == "POST":
            body_string = json.dumps(params) if params else ""
            headers['X-BAPI-SIGN'] = self.generate_signature(timestamp, body_string)
            headers['Content-Type'] = 'application/json'

            async with self.session.post(url, json=params, headers=headers) as response:
                return await response.json()

    async def test_flrusdt(self):
        """Test FLRUSDT order creation and verification"""
        symbol = "FLRUSDT"

        print(f"\n{'=' * 60}")
        print(f"Testing {symbol} Order Flow")
        print('=' * 60)

        # Step 1: Get instrument info
        print("\n1️⃣ Getting instrument info...")
        info_result = await self.request(
            "GET",
            "/v5/market/instruments-info",
            {"category": "linear", "symbol": symbol}
        )

        if info_result['retCode'] == 0 and info_result['result']['list']:
            instrument = info_result['result']['list'][0]
            lot_filter = instrument['lotSizeFilter']

            min_qty = float(lot_filter['minOrderQty'])
            qty_step = float(lot_filter['qtyStep'])

            print(f"   Min Qty: {min_qty}")
            print(f"   Qty Step: {qty_step}")
        else:
            print("❌ Failed to get instrument info")
            return

        # Step 2: Get current price
        print("\n2️⃣ Getting current price...")
        ticker_result = await self.request(
            "GET",
            "/v5/market/tickers",
            {"category": "linear", "symbol": symbol}
        )

        if ticker_result['retCode'] == 0:
            price = float(ticker_result['result']['list'][0]['lastPrice'])
            print(f"   Price: ${price:.6f}")
        else:
            print("❌ Failed to get ticker")
            return

        # Step 3: Calculate correct quantity
        print("\n3️⃣ Calculating order quantity...")
        target_value = 20  # $20
        raw_qty = target_value / price

        # Round to qty_step
        qty_decimal = Decimal(str(raw_qty))
        step_decimal = Decimal(str(qty_step))
        rounded_qty = (qty_decimal / step_decimal).quantize(Decimal('1'), rounding=ROUND_DOWN) * step_decimal

        # Ensure minimum
        if rounded_qty < Decimal(str(min_qty)):
            rounded_qty = Decimal(str(min_qty))

        # Format quantity
        if qty_step == int(qty_step):
            final_qty = str(int(rounded_qty))
        else:
            final_qty = str(float(rounded_qty))

        order_value = float(final_qty) * price

        print(f"   Raw: {raw_qty:.2f}")
        print(f"   Formatted: {final_qty}")
        print(f"   Value: ${order_value:.2f}")

        if order_value < 10:
            print("⚠️ Order value below $10 minimum!")
            # Adjust quantity
            min_qty_for_value = 10 / price
            qty_decimal = Decimal(str(min_qty_for_value))
            rounded_qty = ((qty_decimal / step_decimal).quantize(Decimal('1'), rounding=ROUND_DOWN) + 1) * step_decimal
            final_qty = str(int(rounded_qty)) if qty_step == int(qty_step) else str(float(rounded_qty))
            order_value = float(final_qty) * price
            print(f"   Adjusted to: {final_qty} (${order_value:.2f})")

        # Step 4: Create order
        print(f"\n4️⃣ Creating market order for {final_qty} {symbol}...")

        order_params = {
            "category": "linear",
            "symbol": symbol,
            "side": "Buy",
            "orderType": "Market",
            "qty": final_qty,
            "positionIdx": 0,
            "timeInForce": "IOC"
        }

        order_result = await self.request("POST", "/v5/order/create", order_params)

        if order_result['retCode'] != 0:
            print(f"❌ Order creation failed: {order_result['retMsg']}")
            return

        order_id = order_result['result']['orderId']
        print(f"✅ Order created: {order_id}")

        # Step 5: Wait for execution
        print("\n⏳ Waiting for execution...")
        await asyncio.sleep(3)

        # Step 6: Get order status WITHOUT orderId!!!
        print("\n5️⃣ Checking order status (NO orderId in params!)...")

        # Method A: Get ALL recent orders for the symbol
        history_params = {
            "category": "linear",
            "symbol": symbol,
            "limit": "50"  # Get last 50 orders
        }

        history_result = await self.request("GET", "/v5/order/history", history_params)

        order_found = False
        if history_result['retCode'] == 0:
            for order in history_result['result']['list']:
                if order['orderId'] == order_id:
                    print(f"✅ Order found in history!")
                    print(f"   Status: {order['orderStatus']}")
                    print(f"   Executed: {order['cumExecQty']} @ {order.get('avgPrice', 'N/A')}")
                    order_found = True
                    break

        if not order_found:
            print("⚠️ Order not found in history, checking executions...")

            # Method B: Check executions
            exec_params = {
                "category": "linear",
                "symbol": symbol,
                "limit": "50"
            }

            exec_result = await self.request("GET", "/v5/execution/list", exec_params)

            if exec_result['retCode'] == 0:
                executions = [e for e in exec_result['result']['list'] if e['orderId'] == order_id]
                if executions:
                    total_qty = sum(float(e['execQty']) for e in executions)
                    avg_price = sum(float(e['execQty']) * float(e['execPrice']) for e in executions) / total_qty
                    print(f"✅ Found executions!")
                    print(f"   Total: {total_qty} @ ${avg_price:.6f}")
                    order_found = True

        # Step 7: Check position
        print("\n6️⃣ Checking position...")
        position_result = await self.request(
            "GET",
            "/v5/position/list",
            {"category": "linear", "symbol": symbol}
        )

        if position_result['retCode'] == 0 and position_result['result']['list']:
            pos = position_result['result']['list'][0]
            if float(pos['size']) > 0:
                print(f"✅ Position exists!")
                print(f"   Size: {pos['size']}")
                print(f"   Entry: ${pos['avgPrice']}")
                print(f"   PnL: ${pos['unrealisedPnl']}")

                # Ask to close
                response = input("\n⚠️ Close position? (yes/no): ")
                if response.lower() == 'yes':
                    close_params = {
                        "category": "linear",
                        "symbol": symbol,
                        "side": "Sell",
                        "orderType": "Market",
                        "qty": pos['size'],
                        "positionIdx": 0,
                        "reduceOnly": True
                    }
                    close_result = await self.request("POST", "/v5/order/create", close_params)
                    if close_result['retCode'] == 0:
                        print("✅ Close order created")
                        await asyncio.sleep(2)
                        print("Position should be closed now")
                    else:
                        print(f"❌ Failed to close: {close_result['retMsg']}")
            else:
                print("❌ No position found")
        else:
            print("❌ Failed to get position")

    async def close(self):
        if self.session:
            await self.session.close()


async def main():
    print("🚀 Final Bybit Test - Clean Implementation")
    print(f"Mode: {'TESTNET' if os.getenv('TESTNET', 'false').lower() == 'true' else 'MAINNET'}")
    print("\n⚠️ This script uses direct API calls without any orderId in GET requests")

    client = BybitDirectClient()

    try:
        await client.test_flrusdt()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())