#!/usr/bin/env python3
"""
Тестовый скрипт для проверки РЕАЛЬНОГО выполнения ордеров на Bybit
Проверяет что ордера действительно создаются и позиции открываются
"""

import asyncio
import os
import sys
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.bybit import BybitExchange

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


async def test_real_order_execution():
    """
    Тест реального выполнения ордера с детальной проверкой
    """

    # Инициализация биржи
    exchange = BybitExchange({
        'api_key': os.getenv('BYBIT_API_KEY'),
        'api_secret': os.getenv('BYBIT_API_SECRET'),
        'testnet': True  # ВАЖНО: используем testnet для тестов
    })

    try:
        logger.info("=" * 60)
        logger.info("ТЕСТ РЕАЛЬНОГО ВЫПОЛНЕНИЯ ОРДЕРА НА BYBIT")
        logger.info("=" * 60)

        # Инициализация
        await exchange.initialize()
        logger.info("✅ Биржа инициализирована")

        # 1. Проверка начального баланса
        initial_balance = await exchange.get_balance()
        logger.info(f"💰 Начальный баланс: ${initial_balance:.2f} USDT")

        if initial_balance < 20:
            logger.error("❌ Недостаточный баланс для теста (нужно минимум $20)")
            return False

        # 2. Получение начальных позиций
        initial_positions = await exchange.get_open_positions()
        logger.info(f"📊 Начальные позиции: {len(initial_positions)}")
        for pos in initial_positions:
            logger.info(f"   - {pos['symbol']}: {pos['quantity']} @ ${pos['entry_price']}")

        # 3. Выбор символа для теста
        test_symbol = 'BTCUSDT'  # Самый ликвидный символ

        # 4. Проверка текущей цены и спреда
        ticker = await exchange.get_ticker(test_symbol)
        if not ticker:
            logger.error(f"❌ Не удалось получить тикер для {test_symbol}")
            return False

        current_price = ticker.get('price', 0)
        bid = ticker.get('bid', 0)
        ask = ticker.get('ask', 0)

        logger.info(f"📈 {test_symbol} - Цена: ${current_price:.2f}")
        logger.info(f"   Bid: ${bid:.2f}, Ask: ${ask:.2f}")

        if bid > 0 and ask > 0:
            spread_pct = ((ask - bid) / bid) * 100
            logger.info(f"   Спред: {spread_pct:.3f}%")

        # 5. Расчет размера позиции
        position_size_usd = 20  # Минимальный размер для теста
        quantity = position_size_usd / current_price

        logger.info(f"📝 Планируемый ордер:")
        logger.info(f"   Размер: ${position_size_usd:.2f}")
        logger.info(f"   Количество: {quantity:.6f} BTC")

        # 6. Установка кредитного плеча
        leverage = 5
        leverage_set = await exchange.set_leverage(test_symbol, leverage)
        if leverage_set:
            logger.info(f"⚙️ Кредитное плечо установлено: {leverage}x")
        else:
            logger.warning(f"⚠️ Не удалось установить кредитное плечо")

        # 7. СОЗДАНИЕ РЫНОЧНОГО ОРДЕРА
        logger.info("=" * 40)
        logger.info("🚀 СОЗДАНИЕ РЫНОЧНОГО ОРДЕРА...")
        logger.info("=" * 40)

        order_result = await exchange.create_market_order(test_symbol, 'BUY', quantity)

        if order_result:
            logger.info("✅ ОРДЕР ВЫПОЛНЕН!")
            logger.info(f"   Order ID: {order_result.get('orderId')}")
            logger.info(f"   Количество: {order_result.get('quantity'):.6f}")
            logger.info(f"   Цена: ${order_result.get('price'):.2f}")
            logger.info(f"   Статус: {order_result.get('status')}")
        else:
            logger.error("❌ ОРДЕР НЕ БЫЛ ВЫПОЛНЕН!")

            # Дополнительная диагностика
            logger.info("Проверяем последние ордера...")
            history_result = await exchange._make_request(
                "GET",
                "/v5/order/history",
                {
                    'category': 'linear',
                    'symbol': test_symbol,
                    'limit': 5
                },
                signed=True
            )

            if history_result and 'list' in history_result:
                logger.info(f"Последние {len(history_result['list'])} ордеров:")
                for order in history_result['list']:
                    logger.info(f"   - {order.get('orderId')[:8]}... "
                                f"Status: {order.get('orderStatus')} "
                                f"Qty: {order.get('cumExecQty')} "
                                f"Reason: {order.get('rejectReason')}")

        # 8. Ждем немного для обновления позиций
        await asyncio.sleep(2)

        # 9. Проверка новых позиций
        logger.info("=" * 40)
        logger.info("📊 ПРОВЕРКА ПОЗИЦИЙ ПОСЛЕ ОРДЕРА...")
        logger.info("=" * 40)

        new_positions = await exchange.get_open_positions()
        logger.info(f"Позиции после ордера: {len(new_positions)}")

        position_found = False
        for pos in new_positions:
            logger.info(f"   - {pos['symbol']}: {pos['quantity']} @ ${pos['entry_price']}")
            if pos['symbol'] == test_symbol:
                position_found = True

                # Проверяем, что позиция новая или увеличилась
                old_pos = next((p for p in initial_positions if p['symbol'] == test_symbol), None)
                if old_pos:
                    qty_diff = pos['quantity'] - old_pos['quantity']
                    if qty_diff > 0:
                        logger.info(f"✅ ПОЗИЦИЯ УВЕЛИЧЕНА на {qty_diff:.6f}")
                    else:
                        logger.warning(f"⚠️ Позиция не изменилась")
                else:
                    logger.info(f"✅ НОВАЯ ПОЗИЦИЯ СОЗДАНА!")

                # Показываем PnL
                if 'pnl' in pos:
                    logger.info(f"   PnL: ${pos['pnl']:.2f} ({pos.get('pnl_percent', 0):.2f}%)")

        if not position_found and order_result:
            logger.error("❌ ПОЗИЦИЯ НЕ НАЙДЕНА, хотя ордер был выполнен!")

        # 10. Проверка баланса после ордера
        final_balance = await exchange.get_balance()
        balance_diff = initial_balance - final_balance
        logger.info(f"💰 Финальный баланс: ${final_balance:.2f} USDT")
        logger.info(f"   Использовано: ${balance_diff:.2f}")

        # 11. Проверка исполнений (executions)
        logger.info("=" * 40)
        logger.info("📜 ПРОВЕРКА ИСПОЛНЕНИЙ...")
        logger.info("=" * 40)

        exec_result = await exchange._make_request(
            "GET",
            "/v5/execution/list",
            {
                'category': 'linear',
                'symbol': test_symbol,
                'limit': 5
            },
            signed=True
        )

        if exec_result and 'list' in exec_result:
            logger.info(f"Последние {len(exec_result['list'])} исполнений:")
            for exec in exec_result['list'][:3]:  # Показываем только 3 последних
                exec_time = datetime.fromtimestamp(
                    int(exec.get('execTime', 0)) / 1000,
                    tz=timezone.utc
                ).strftime('%H:%M:%S')
                logger.info(f"   - {exec_time}: "
                            f"{exec.get('side')} {exec.get('execQty')} @ ${exec.get('execPrice')} "
                            f"Fee: ${exec.get('execFee')}")

        # 12. ЗАКРЫТИЕ ТЕСТОВОЙ ПОЗИЦИИ (опционально)
        if position_found and order_result:
            logger.info("=" * 40)
            logger.info("🔄 ЗАКРЫТИЕ ТЕСТОВОЙ ПОЗИЦИИ...")
            logger.info("=" * 40)

            close_result = await exchange.close_position(test_symbol)
            if close_result:
                logger.info("✅ Позиция закрыта")
            else:
                logger.warning("⚠️ Не удалось закрыть позицию")

            # Финальная проверка
            await asyncio.sleep(2)
            final_positions = await exchange.get_open_positions()
            test_pos = next((p for p in final_positions if p['symbol'] == test_symbol), None)
            if not test_pos:
                logger.info("✅ Позиция успешно закрыта")
            else:
                logger.warning(f"⚠️ Позиция все еще открыта: {test_pos['quantity']}")

        # ИТОГОВЫЙ РЕЗУЛЬТАТ
        logger.info("=" * 60)
        if order_result and position_found:
            logger.info("✅ ТЕСТ УСПЕШНО ПРОЙДЕН!")
            logger.info("Ордер был создан, выполнен и позиция открыта")
            return True
        else:
            logger.error("❌ ТЕСТ ПРОВАЛЕН!")
            if not order_result:
                logger.error("Ордер не был выполнен")
            if not position_found:
                logger.error("Позиция не была создана")
            return False

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

    finally:
        if exchange:
            await exchange.close()
            logger.info("Соединение закрыто")


async def main():
    """Главная функция"""
    success = await test_real_order_execution()

    if success:
        logger.info("\n✅ Все тесты пройдены успешно!")
    else:
        logger.error("\n❌ Тесты провалены - требуется исправление кода")

    return success


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)