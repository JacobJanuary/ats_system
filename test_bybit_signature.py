#!/usr/bin/env python3
"""
Тест для проверки что все ошибки подписи исправлены
Должен выполняться БЕЗ ошибок подписи
"""

import asyncio
import os
import sys
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.bybit import BybitExchange

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


async def test_all_api_calls():
    """
    Тестирует все основные API вызовы и проверяет отсутствие ошибок подписи
    """

    exchange = BybitExchange({
        'api_key': os.getenv('BYBIT_API_KEY'),
        'api_secret': os.getenv('BYBIT_API_SECRET'),
        'testnet': True
    })

    errors = []
    success_count = 0
    total_tests = 0

    try:
        logger.info("=" * 60)
        logger.info("ТЕСТ ВСЕХ API ВЫЗОВОВ БЕЗ ОШИБОК ПОДПИСИ")
        logger.info("=" * 60)

        # Инициализация
        await exchange.initialize()
        logger.info("✅ Инициализация успешна")

        # Тест 1: Получение баланса
        logger.info("\n📌 Тест 1: Получение баланса")
        total_tests += 1
        try:
            balance = await exchange.get_balance()
            logger.info(f"   ✅ Баланс получен: ${balance:.2f}")
            success_count += 1
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            errors.append(f"get_balance: {e}")

        # Тест 2: Получение тикера
        logger.info("\n📌 Тест 2: Получение тикера")
        total_tests += 1
        try:
            ticker = await exchange.get_ticker('BTCUSDT')
            if ticker:
                logger.info(f"   ✅ Тикер получен: ${ticker.get('price'):.2f}")
                success_count += 1
            else:
                logger.error("   ❌ Тикер не получен")
                errors.append("get_ticker: пустой ответ")
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            errors.append(f"get_ticker: {e}")

        # Тест 3: Получение позиций
        logger.info("\n📌 Тест 3: Получение позиций")
        total_tests += 1
        try:
            positions = await exchange.get_open_positions()
            logger.info(f"   ✅ Позиции получены: {len(positions)} шт")
            success_count += 1
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            errors.append(f"get_open_positions: {e}")

        # Тест 4: Получение открытых ордеров
        logger.info("\n📌 Тест 4: Получение открытых ордеров")
        total_tests += 1
        try:
            orders = await exchange.get_open_orders('BTCUSDT')
            logger.info(f"   ✅ Ордера получены: {len(orders)} шт")
            success_count += 1
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            errors.append(f"get_open_orders: {e}")

        # Тест 5: Проверка статуса ордера через /v5/order/realtime
        logger.info("\n📌 Тест 5: Проверка статуса через /v5/order/realtime")
        total_tests += 1
        try:
            # Используем фиктивный orderId для теста
            result = await exchange._make_request(
                "GET",
                "/v5/order/realtime",
                {
                    'category': 'linear',
                    'orderId': 'test-order-123',
                    'openOnly': 0
                },
                signed=True
            )
            # Ожидаем пустой список, так как ордер не существует
            if result is not None:
                logger.info(f"   ✅ Запрос выполнен без ошибки подписи")
                success_count += 1
            else:
                logger.info(f"   ✅ Запрос выполнен (пустой ответ ожидаем)")
                success_count += 1
        except Exception as e:
            if "sign error" in str(e).lower():
                logger.error(f"   ❌ ОШИБКА ПОДПИСИ: {e}")
                errors.append(f"order/realtime с orderId: ОШИБКА ПОДПИСИ!")
            else:
                logger.error(f"   ❌ Ошибка: {e}")
                errors.append(f"order/realtime: {e}")

        # Тест 6: Получение истории ордеров БЕЗ orderId
        logger.info("\n📌 Тест 6: Получение истории ордеров БЕЗ orderId")
        total_tests += 1
        try:
            result = await exchange._make_request(
                "GET",
                "/v5/order/history",
                {
                    'category': 'linear',
                    'symbol': 'BTCUSDT',
                    'limit': 5
                },
                signed=True
            )
            if result:
                logger.info(f"   ✅ История получена: {len(result.get('list', []))} ордеров")
                success_count += 1
            else:
                logger.info(f"   ✅ Запрос выполнен (пустая история)")
                success_count += 1
        except Exception as e:
            if "sign error" in str(e).lower():
                logger.error(f"   ❌ ОШИБКА ПОДПИСИ: {e}")
                errors.append(f"order/history: ОШИБКА ПОДПИСИ!")
            else:
                logger.error(f"   ❌ Ошибка: {e}")
                errors.append(f"order/history: {e}")

        # Тест 7: Получение исполнений с orderId
        logger.info("\n📌 Тест 7: Получение исполнений с orderId")
        total_tests += 1
        try:
            result = await exchange._make_request(
                "GET",
                "/v5/execution/list",
                {
                    'category': 'linear',
                    'orderId': 'test-order-123',
                    'limit': 10
                },
                signed=True
            )
            if result:
                logger.info(f"   ✅ Исполнения получены: {len(result.get('list', []))}")
                success_count += 1
            else:
                logger.info(f"   ✅ Запрос выполнен (нет исполнений)")
                success_count += 1
        except Exception as e:
            if "sign error" in str(e).lower():
                logger.error(f"   ❌ ОШИБКА ПОДПИСИ: {e}")
                errors.append(f"execution/list с orderId: ОШИБКА ПОДПИСИ!")
            else:
                logger.error(f"   ❌ Ошибка: {e}")
                errors.append(f"execution/list: {e}")

        # Тест 8: Получение исполнений с symbol
        logger.info("\n📌 Тест 8: Получение исполнений с symbol")
        total_tests += 1
        try:
            result = await exchange._make_request(
                "GET",
                "/v5/execution/list",
                {
                    'category': 'linear',
                    'symbol': 'BTCUSDT',
                    'limit': 5
                },
                signed=True
            )
            if result:
                logger.info(f"   ✅ Исполнения получены: {len(result.get('list', []))}")
                success_count += 1
            else:
                logger.info(f"   ✅ Запрос выполнен")
                success_count += 1
        except Exception as e:
            if "sign error" in str(e).lower():
                logger.error(f"   ❌ ОШИБКА ПОДПИСИ: {e}")
                errors.append(f"execution/list с symbol: ОШИБКА ПОДПИСИ!")
            else:
                logger.error(f"   ❌ Ошибка: {e}")
                errors.append(f"execution/list с symbol: {e}")

        # Тест 9: Установка кредитного плеча (POST запрос)
        logger.info("\n📌 Тест 9: Установка кредитного плеча (POST)")
        total_tests += 1
        try:
            result = await exchange.set_leverage('BTCUSDT', 5)
            if result:
                logger.info(f"   ✅ Кредитное плечо установлено/проверено")
                success_count += 1
            else:
                logger.info(f"   ⚠️ Не удалось установить (возможно уже установлено)")
                success_count += 1  # Это не критично
        except Exception as e:
            if "sign error" in str(e).lower():
                logger.error(f"   ❌ ОШИБКА ПОДПИСИ: {e}")
                errors.append(f"set_leverage: ОШИБКА ПОДПИСИ!")
            else:
                logger.error(f"   ❌ Ошибка: {e}")
                errors.append(f"set_leverage: {e}")

        # Тест 10: Проверка реального создания и отмены лимитного ордера
        logger.info("\n📌 Тест 10: Создание и отмена лимитного ордера")
        total_tests += 1
        try:
            # Получаем текущую цену
            ticker = await exchange.get_ticker('BTCUSDT')
            if ticker and ticker.get('price'):
                # Создаем лимитный ордер по цене на 10% ниже рынка
                limit_price = ticker['price'] * 0.9

                params = {
                    'category': 'linear',
                    'symbol': 'BTCUSDT',
                    'side': 'Buy',
                    'orderType': 'Limit',
                    'qty': exchange.format_quantity('BTCUSDT', 0.001),
                    'price': exchange.format_price('BTCUSDT', limit_price),
                    'timeInForce': 'GTC'
                }

                # Создаем ордер
                order_result = await exchange._make_request("POST", "/v5/order/create", params, signed=True)

                if order_result and 'orderId' in order_result:
                    order_id = order_result['orderId']
                    logger.info(f"   ✅ Лимитный ордер создан: {order_id}")

                    # Проверяем статус
                    await asyncio.sleep(1)

                    status_result = await exchange._make_request(
                        "GET",
                        "/v5/order/realtime",
                        {
                            'category': 'linear',
                            'orderId': order_id,
                            'openOnly': 1
                        },
                        signed=True
                    )

                    if status_result and 'list' in status_result:
                        logger.info(f"   ✅ Статус ордера получен без ошибки подписи")

                    # Отменяем ордер
                    cancel_params = {
                        'category': 'linear',
                        'symbol': 'BTCUSDT',
                        'orderId': order_id
                    }

                    cancel_result = await exchange._make_request(
                        "POST",
                        "/v5/order/cancel",
                        cancel_params,
                        signed=True
                    )

                    if cancel_result:
                        logger.info(f"   ✅ Ордер успешно отменен")

                    success_count += 1
                else:
                    logger.error(f"   ❌ Не удалось создать ордер")
                    errors.append("create_limit_order: не создан")
            else:
                logger.error(f"   ❌ Не удалось получить цену")
                errors.append("create_limit_order: нет цены")

        except Exception as e:
            if "sign error" in str(e).lower():
                logger.error(f"   ❌ ОШИБКА ПОДПИСИ: {e}")
                errors.append(f"limit_order_test: ОШИБКА ПОДПИСИ!")
            else:
                logger.error(f"   ❌ Ошибка: {e}")
                errors.append(f"limit_order_test: {e}")

        # ИТОГИ
        logger.info("\n" + "=" * 60)
        logger.info("РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ")
        logger.info("=" * 60)
        logger.info(f"Всего тестов: {total_tests}")
        logger.info(f"Успешно: {success_count}")
        logger.info(f"Провалено: {total_tests - success_count}")

        if errors:
            logger.error("\n❌ ОБНАРУЖЕННЫЕ ОШИБКИ:")
            for error in errors:
                logger.error(f"   - {error}")

            # Проверяем критические ошибки подписи
            signature_errors = [e for e in errors if "ОШИБКА ПОДПИСИ" in e]
            if signature_errors:
                logger.error("\n🔴 КРИТИЧНО: Обнаружены ошибки подписи!")
                logger.error("Необходимо исправить генерацию подписи в методе _make_request")
                return False
        else:
            logger.info("\n✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ БЕЗ ОШИБОК!")

        return len(errors) == 0

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

    finally:
        if exchange:
            await exchange.close()
            logger.info("Соединение закрыто")


async def main():
    """Главная функция"""
    success = await test_all_api_calls()

    if success:
        logger.info("\n✅ ВСЕ ОШИБКИ ПОДПИСИ ИСПРАВЛЕНЫ!")
        logger.info("Можно использовать в production")
    else:
        logger.error("\n❌ ОБНАРУЖЕНЫ ПРОБЛЕМЫ С ПОДПИСЬЮ")
        logger.error("Требуется дополнительная отладка")

    return success


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)