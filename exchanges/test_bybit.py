import asyncio
import os
import logging
from dotenv import load_dotenv
from exchanges.bybit import BybitExchange, InvalidRequestError

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
    'api_key': os.getenv('BYBIT_API_KEY'),
    'api_secret': os.getenv('BYBIT_API_SECRET'),
    'testnet': os.getenv('TESTNET', 'true').lower() == 'true'
}

# --- Надежный список символов для тестнета Bybit ---
LIQUID_SYMBOLS_BYBIT_TESTNET = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT",
    "TRXUSDT", "BCHUSDT", "INJUSDT", "NEARUSDT", "OPUSDT", "ADAUSDT",
    "LINKUSDT", "DOTUSDT", "TIAUSDT"
]


# В файле test_bybit.py, заменяем ТОЛЬКО ЭТУ ФУНКЦИЮ

async def test_symbol_bybit(exchange: BybitExchange, symbol: str):
    """Финальная версия теста с адаптивной защитой и отказоустойчивым входом."""
    logger.info(f"--- Тестирование символа: {symbol} ---")
    position_opened = False
    try:
        await exchange.set_leverage(symbol, 10)

        ticker = await exchange.get_ticker(symbol)
        if not ticker or not ticker.get('price'):
            logger.error(f"Не удалось получить цену для {symbol}. Пропускаем.")
            return False

        position_size_usd = 15
        quantity = position_size_usd / ticker['price']
        logger.info(f"Текущая цена {symbol}: ${ticker['price']:.4f}. Расчетное количество: {quantity:.6f}")

        # ОТКАЗОУСТОЙЧИВЫЙ ВХОД В ПОЗИЦИЮ
        entry_price = 0
        order_result = None
        max_entry_attempts = 3
        for attempt in range(max_entry_attempts):
            logger.info(f"Попытка входа в позицию #{attempt + 1}...")
            order_result = await exchange.create_market_order(symbol, "BUY", quantity)

            # Новый, безопасный способ проверки
            if order_result and order_result.get('executed_qty', 0.0) > 0:
                break

            logger.warning(f"Попытка входа #{attempt + 1} не удалась. Результат: {order_result}. Повтор через 1 сек.")
            await asyncio.sleep(1)

        # Проверяем, удалось ли войти в позицию
        if order_result and order_result.get('executed_qty', 0.0) > 0:
            position_opened = True
            entry_price = order_result['price']
            logger.info(f"✅ Позиция по {symbol} открыта. Цена входа: ${entry_price:.4f}")
        else:
            logger.error(f"❌ Не удалось войти в позицию по {symbol} после {max_entry_attempts} попыток.")
            return False

        # АДАПТИВНАЯ УСТАНОВКА ЗАЩИТЫ
        await asyncio.sleep(1)
        protection_ticker = await exchange.get_ticker(symbol)
        if not protection_ticker or not protection_ticker.get('price'):
            logger.error(f"Не удалось получить цену для установки защиты {symbol}.")
            return False

        last_price = float(protection_ticker['price'])

        # --- Установка Stop Loss ---
        sl_price = entry_price * 0.95
        if sl_price < last_price:
            logger.info(f"Установка Stop Loss для {symbol} на цене ${sl_price:.4f}...")
            if await exchange.set_stop_loss(symbol, sl_price):
                logger.info(f"✅ Stop Loss успешно установлен.")
            else:
                logger.error(f"❌ Не удалось установить Stop Loss.")
        else:
            logger.warning(
                f"⚠️ Расчетный SL (${sl_price:.4f}) выше или равен текущей цене (${last_price:.4f}). SL не устанавливается.")

        # --- Установка Take Profit ---
        tp_price = entry_price * 1.10
        if tp_price > last_price:
            logger.info(f"Установка Take Profit для {symbol} на цене ${tp_price:.4f}...")
            if await exchange.set_take_profit(symbol, tp_price):
                logger.info(f"✅ Take Profit успешно установлен.")
            else:
                logger.error(f"❌ Не удалось установить Take Profit.")
        else:
            logger.warning(
                f"⚠️ Расчетный TP (${tp_price:.4f}) ниже или равен текущей цене (${last_price:.4f}). TP не устанавливается.")

        return True

    except Exception as e:
        logger.error(f"Критическая ошибка при тестировании {symbol}: {e}", exc_info=True)
        return False
    finally:
        if position_opened:
            logger.info(f"Закрытие позиции по {symbol}...")
            await exchange.cancel_position_trading_stops(symbol)
            await asyncio.sleep(0.5)
            if await exchange.close_position(symbol):
                logger.info(f"✅ Позиция по {symbol} успешно закрыта.")
            else:
                logger.error(f"❌ Не удалось закрыть позицию по {symbol}.")
        logger.info(f"--- Тестирование {symbol} завершено ---\n")


async def main():
    logger.info(">>> Запуск PRODUCTION-READY тестового скрипта для Bybit <<<")
    exchange = BybitExchange(config)
    await exchange.initialize()

    successful_tests = 0
    for symbol in LIQUID_SYMBOLS_BYBIT_TESTNET:
        if await test_symbol_bybit(exchange, symbol):
            successful_tests += 1
        await asyncio.sleep(1)

    logger.info(">>> Тестирование Bybit завершено <<<")
    logger.info(f"Итог: {successful_tests} / {len(LIQUID_SYMBOLS_BYBIT_TESTNET)} тестов успешно пройдены.")
    await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())