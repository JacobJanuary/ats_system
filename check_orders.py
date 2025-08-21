import asyncio
from exchanges.binance import BinanceExchange
import os
from dotenv import load_dotenv

load_dotenv('.env')


async def check_orders():
    config = {
        'testnet': os.getenv('BINANCE_TESTNET', 'true').lower() == 'true',
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET')
    }

    exchange = BinanceExchange(config)
    await exchange.initialize()

    try:
        # Получаем все открытые ордера
        symbol = 'SOLUSDT'
        orders = await exchange.client.futures_get_open_orders(symbol=symbol)

        print(f"\\n=== Open Orders for {symbol} ===")
        for order in orders:
            print(f"Order ID: {order['orderId']}")
            print(f"Type: {order['type']}")
            print(f"Side: {order['side']}")
            print(f"Quantity: {order['origQty']}")
            if 'activatePrice' in order:
                print(f"Activation Price: {order['activatePrice']}")
            if 'priceRate' in order:
                print(f"Callback Rate: {order['priceRate']}%")
            print("-" * 30)

        # Проверяем позиции
        positions = await exchange.client.futures_position_information(symbol=symbol)
        for pos in positions:
            if float(pos['positionAmt']) != 0:
                print(f"\\n=== Position {symbol} ===")
                print(f"Amount: {pos['positionAmt']}")
                print(f"Entry Price: {pos['entryPrice']}")
                print(f"Mark Price: {pos['markPrice']}")
                print(f"Unrealized PnL: {pos['unRealizedProfit']}")
    finally:
        # Закрываем сессию
        await exchange.client.close_connection()

if __name__ == "__main__":
    asyncio.run(check_orders())