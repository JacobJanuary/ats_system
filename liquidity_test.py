import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

# Настройка путей для импорта ваших классов бирж
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange

# Настройка логирования для наглядности
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Загрузка переменных окружения (API ключи и т.д.)
load_dotenv()

# --- Конфигурация теста ---
TOP_N_PAIRS = 30
POSITION_SIZE_USD = 10.0
LEVERAGE = 10
SL_PERCENT = 5.0


# -------------------------

async def fetch_and_filter_binance(exchange: BinanceExchange):
    """Получает топ-N ликвидных пар с Binance."""
    logging.info("Fetching all 24h tickers from Binance...")
    all_tickers = await exchange._make_request("GET", "/fapi/v1/ticker/24hr")
    if not all_tickers:
        logging.error("Could not fetch tickers from Binance.")
        return []

    usdt_tickers = [t for t in all_tickers if t.get('symbol', '').endswith('USDT')]

    # Сортируем по объему в USDT (quoteVolume)
    sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)

    top_symbols = [t['symbol'] for t in sorted_tickers[:TOP_N_PAIRS]]
    logging.info(f"Top {len(top_symbols)} liquid symbols from Binance: {top_symbols}")
    return top_symbols


async def fetch_and_filter_bybit(exchange: BybitExchange):
    """Получает топ-N ликвидных пар с Bybit."""
    logging.info("Fetching all linear tickers from Bybit...")
    all_tickers_data = await exchange._make_request("GET", "/v5/market/tickers", {'category': 'linear'})
    if not all_tickers_data or 'list' not in all_tickers_data:
        logging.error("Could not fetch tickers from Bybit.")
        return []

    all_tickers = all_tickers_data['list']
    usdt_tickers = [t for t in all_tickers if t.get('symbol', '').endswith('USDT')]

    # Сортируем по объему в USDT (turnover24h)
    sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get('turnover24h', 0)), reverse=True)

    top_symbols = [t['symbol'] for t in sorted_tickers[:TOP_N_PAIRS]]
    logging.info(f"Top {len(top_symbols)} liquid symbols from Bybit: {top_symbols}")
    return top_symbols


async def test_symbol(exchange, symbol: str):
    """Проводит полный цикл теста для одного символа."""
    logging.info(f"--- Testing {symbol} ---")

    # 1. Установка плеча
    leverage_ok = await exchange.set_leverage(symbol, LEVERAGE)
    if not leverage_ok:
        logging.warning(f"[{symbol}] Failed to set leverage.")
        # Продолжаем, т.к. это не всегда критично

    # 2. Получение цены для расчета количества
    ticker = await exchange.get_ticker(symbol)
    if not ticker or 'price' not in ticker or ticker['price'] <= 0:
        logging.error(f"[{symbol}] Could not get a valid price. Skipping.")
        return False

    price = ticker['price']
    quantity = POSITION_SIZE_USD / price

    # 3. Создание ордера
    order_result = await exchange.create_market_order(symbol, 'BUY', quantity)
    if not order_result or order_result.get('status') != 'FILLED':
        logging.error(f"[{symbol}] FAILED to create market order.")
        return False

    logging.info(f"[{symbol}] Position opened successfully at price ${order_result['price']:.4f}")

    # 4. Установка стоп-лосса
    entry_price = order_result['price']
    sl_price = entry_price * (1 - SL_PERCENT / 100)
    sl_ok = await exchange.set_stop_loss(symbol, sl_price)

    if not sl_ok:
        logging.error(f"[{symbol}] FAILED to set Stop Loss!")
    else:
        logging.info(f"[{symbol}] Stop Loss set successfully at ${sl_price:.4f}")

    # <<< ИЗМЕНЕНИЕ: ШАГ ЗАКРЫТИЯ ПОЗИЦИИ ОТКЛЮЧЕН >>>
    # 5. Закрытие позиции для очистки
    logging.info(f"[{symbol}] Test finished. Position and SL order should now be active on the exchange.")
    # close_ok = await exchange.close_position(symbol)
    # if not close_ok:
    #     logging.warning(f"[{symbol}] Could not automatically close position. Please check your account.")
    # else:
    #     logging.info(f"[{symbol}] Position closed successfully.")
    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

    return sl_ok


async def main():
    """Главная функция для запуска тестов."""

    # --- Тест Binance ---
    logging.info("\n" + "=" * 50 + "\n" + "### STARTING BINANCE TEST ###" + "\n" + "=" * 50)
    binance = BinanceExchange({
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': os.getenv('TESTNET', 'false').lower() == 'true'
    })
    await binance.initialize()

    binance_symbols = await fetch_and_filter_binance(binance)
    binance_success_count = 0

    if binance_symbols:
        for symbol in binance_symbols:
            success = await test_symbol(binance, symbol)
            if success:
                binance_success_count += 1
            await asyncio.sleep(2)  # Пауза между символами, чтобы не попасть под rate limits

    await binance.close()

    # --- Тест Bybit ---
    logging.info("\n" + "=" * 50 + "\n" + "### STARTING BYBIT TEST ###" + "\n" + "=" * 50)
    bybit = BybitExchange({
        'api_key': os.getenv('BYBIT_API_KEY'),
        'api_secret': os.getenv('BYBIT_API_SECRET'),
        'testnet': os.getenv('TESTNET', 'false').lower() == 'true'
    })
    await bybit.initialize()

    bybit_symbols = await fetch_and_filter_bybit(bybit)
    bybit_success_count = 0

    if bybit_symbols:
        for symbol in bybit_symbols:
            success = await test_symbol(bybit, symbol)
            if success:
                bybit_success_count += 1
            await asyncio.sleep(2)

    await bybit.close()

    # --- Итоги ---
    logging.info("\n" + "=" * 50 + "\n" + "### TEST RESULTS ###" + "\n" + "=" * 50)
    logging.info(
        f"Binance SL Success Rate: {binance_success_count}/{len(binance_symbols)} ({((binance_success_count / len(binance_symbols)) * 100 if len(binance_symbols) > 0 else 0):.1f}%)")
    logging.info(
        f"Bybit SL Success Rate: {bybit_success_count}/{len(bybit_symbols)} ({((bybit_success_count / len(bybit_symbols)) * 100 if len(bybit_symbols) > 0 else 0):.1f}%)")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Test interrupted by user.")