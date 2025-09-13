import asyncio
import os
import logging
from dotenv import load_dotenv
from exchanges.binance import BinanceExchange  # Убедитесь, что файл binance.py находится в той же папке или доступен в sys.path

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Загрузка конфигурации ---
load_dotenv()
config = {
    'api_key': os.getenv('BINANCE_API_KEY'),
    'api_secret': os.getenv('BINANCE_API_SECRET'),
    'testnet': os.getenv('TESTNET', 'true').lower() == 'true'
}

# --- Список ликвидных пар для Binance ---
LIQUID_SYMBOLS_BINANCE = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "MATICUSDT",
    "DOTUSDT", "LTCUSDT", "BCHUSDT", "UNIUSDT", "TRXUSDT",
    "ATOMUSDT", "ETCUSDT", "FILUSDT", "NEARUSDT", "OPUSDT"
]


async def test_symbol_binance(exchange: BinanceExchange, symbol: str):
    """Тестирует полный цикл: открытие, защита, закрытие для одного символа."""
    logger.info(f"--- Тестирование символа: {symbol} ---")
    position_opened = False
    try:
        # 1. Установка плеча
        logger.info(f"Установка плеча 10x для {symbol}...")
        leverage_set = await exchange.set_leverage(symbol, 10)
        if not leverage_set:
            logger.warning(f"Не удалось установить плечо для {symbol}, но продолжаем.")

        # 2. Получение тикера и расчет количества
        ticker = await exchange.get_ticker(symbol)
        if not ticker or not ticker.get('price'):
            logger.error(f"Не удалось получить цену для {symbol}. Пропускаем.")
            return False

        price = ticker['price']
        position_size_usd = 100  # Используем $100 для теста, т.к. у BTC/ETH высокий minNotional
        quantity = position_size_usd / price
        logger.info(f"Текущая цена {symbol}: ${price:.4f}. Расчетное количество: {quantity:.6f}")

        # 3. Открытие рыночной LONG позиции
        logger.info(f"Открытие LONG позиции на {quantity:.6f} {symbol}...")
        order_result = await exchange.create_market_order(symbol, "BUY", quantity)
        if not order_result or order_result.get('status') != 'FILLED':
            logger.error(f"Не удалось открыть позицию для {symbol}. Ответ: {order_result}")
            return False

        position_opened = True
        entry_price = order_result['price']
        logger.info(f"✅ Позиция по {symbol} открыта. Цена входа: ${entry_price:.4f}")

        # 4. Ожидание регистрации позиции на бирже
        await asyncio.sleep(2)  # Пауза, чтобы биржа обработала позицию

        # 5. Установка Stop Loss (на 5% ниже цены входа)
        sl_price = entry_price * 0.95
        logger.info(f"Установка Stop Loss для {symbol} на цене ${sl_price:.4f}...")
        sl_set = await exchange.set_stop_loss(symbol, sl_price)
        if not sl_set:
            logger.error(f"❌ Не удалось установить Stop Loss для {symbol}")
        else:
            logger.info(f"✅ Stop Loss для {symbol} успешно установлен.")

        # 6. Установка Take Profit (на 10% выше цены входа)
        tp_price = entry_price * 1.10
        logger.info(f"Установка Take Profit для {symbol} на цене ${tp_price:.4f}...")
        tp_set = await exchange.set_take_profit(symbol, tp_price)
        if not tp_set:
            logger.error(f"❌ Не удалось установить Take Profit для {symbol}")
        else:
            logger.info(f"✅ Take Profit для {symbol} успешно установлен.")

        # 7. Проверка открытых ордеров
        open_orders = await exchange.get_open_orders(symbol)
        logger.info(f"Найдено {len(open_orders)} открытых ордеров для {symbol}: {open_orders}")

        return True

    except Exception as e:
        logger.error(f"Критическая ошибка при тестировании {symbol}: {e}", exc_info=True)
        return False
    finally:
        if position_opened:
            # 8. Закрытие позиции
            logger.info(f"Закрытие позиции по {symbol}...")
            await exchange.cancel_all_open_orders(symbol)  # Сначала отменяем SL/TP
            await asyncio.sleep(1)
            close_result = await exchange.close_position(symbol)
            if close_result:
                logger.info(f"✅ Позиция по {symbol} успешно закрыта.")
            else:
                logger.error(f"❌ Не удалось закрыть позицию по {symbol}.")
        logger.info(f"--- Тестирование {symbol} завершено ---\n")


async def main():
    logger.info(">>> Запуск тестового скрипта для Binance <<<")
    exchange = BinanceExchange(config)
    await exchange.initialize()

    successful_tests = 0
    for symbol in LIQUID_SYMBOLS_BINANCE:
        if await test_symbol_binance(exchange, symbol):
            successful_tests += 1
        await asyncio.sleep(2)  # Пауза между тестами символов

    logger.info(">>> Тестирование Binance завершено <<<")
    logger.info(f"Итог: {successful_tests} / {len(LIQUID_SYMBOLS_BINANCE)} тестов успешно пройдены.")
    await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())