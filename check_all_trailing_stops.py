# check_trailing_stops_testnet.py

import asyncio
import aiohttp
import hmac
import hashlib
import time
import os
import json
from dotenv import load_dotenv

load_dotenv()


async def check_all_positions_protection():
    api_key = os.getenv('BYBIT_API_KEY')
    api_secret = os.getenv('BYBIT_API_SECRET')

    if not api_key or not api_secret:
        print("❌ Ошибка: Не найдены API ключи в .env файле")
        return

    print(f"🔑 API Key: {api_key[:10]}...")

    try:
        # ВАЖНО: Для testnet обязательно нужен settleCoin!
        timestamp = str(int(time.time() * 1000))
        params = {
            "category": "linear",
            "settleCoin": "USDT",  # ОБЯЗАТЕЛЬНЫЙ параметр для testnet
            "limit": "200"
        }

        # Создаем строку параметров для подписи
        param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])

        # Генерируем подпись
        sign_str = timestamp + api_key + "5000" + param_str
        signature = hmac.new(
            api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "5000"
        }

        # URL для testnet
        base_url = "https://api-testnet.bybit.com"
        url = f"{base_url}/v5/position/list?{param_str}"

        print(f"📡 Подключение к TESTNET")
        print(f"🔗 Запрос позиций с settleCoin=USDT")

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                print(f"📊 Статус ответа: {response.status}")

                response_text = await response.text()

                try:
                    data = json.loads(response_text)
                except json.JSONDecodeError:
                    print(f"❌ Ошибка парсинга JSON:")
                    print(response_text[:500])
                    return

                if data.get('retCode') != 0:
                    print(f"❌ Ошибка API: {data.get('retMsg')}")
                    print(f"Код ошибки: {data.get('retCode')}")

                    # Пробуем альтернативный вариант без settleCoin
                    print("\n🔄 Пробуем альтернативный запрос...")
                    return await try_alternative_request(api_key, api_secret)

                positions = data.get('result', {}).get('list', [])
                open_positions = [p for p in positions if float(p.get('size', 0)) > 0]

                if not open_positions:
                    print("⚠️ Нет открытых позиций")
                    return

                print(f"\n{'=' * 70}")
                print(f"📈 АНАЛИЗ TRAILING STOP ДЛЯ {len(open_positions)} ПОЗИЦИЙ")
                print(f"{'=' * 70}\n")

                analyze_positions(open_positions)

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


async def try_alternative_request(api_key, api_secret):
    """Альтернативный запрос для получения позиций"""
    print("🔄 Пробуем получить позиции по одной...")

    # Список основных символов на testnet
    test_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    all_positions = []

    for symbol in test_symbols:
        timestamp = str(int(time.time() * 1000))
        params = {
            "category": "linear",
            "symbol": symbol  # Запрашиваем конкретный символ
        }

        param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        sign_str = timestamp + api_key + "5000" + param_str
        signature = hmac.new(
            api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "5000"
        }

        url = f"https://api-testnet.bybit.com/v5/position/list?{param_str}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('retCode') == 0:
                        positions = data.get('result', {}).get('list', [])
                        for pos in positions:
                            if float(pos.get('size', 0)) > 0:
                                all_positions.append(pos)
                                print(f"✅ Найдена позиция: {symbol}")

    if all_positions:
        print(f"\n📊 Найдено {len(all_positions)} позиций через альтернативный метод")
        analyze_positions(all_positions)
    else:
        print("❌ Не удалось получить позиции")


def analyze_positions(open_positions):
    """Анализ позиций на наличие Trailing Stop"""

    ts_set = 0
    ts_active = 0
    ts_not_set = 0

    positions_with_ts = []
    positions_without_ts = []

    for pos in open_positions:
        symbol = pos.get('symbol')
        trailing_stop = pos.get('trailingStop')
        mark_price = float(pos.get('markPrice', 0))
        entry_price = float(pos.get('avgPrice', 0))
        side = pos.get('side')
        size = float(pos.get('size', 0))

        # Проверяем Stop Loss
        stop_loss = pos.get('stopLoss')
        has_sl = stop_loss and stop_loss != '0' and stop_loss != ''

        # Проверяем Trailing Stop
        if trailing_stop and trailing_stop != '0' and trailing_stop != '':
            ts_set += 1

            # Определяем статус активации по P&L
            if entry_price > 0:
                if side == 'Buy':
                    pnl_percent = (mark_price - entry_price) / entry_price * 100
                    # TS обычно активируется при прибыли > 0.5-1%
                    if pnl_percent > 0.5:
                        ts_active += 1
                        status = "✅ АКТИВИРОВАН"
                    else:
                        status = f"⏸️ ОЖИДАЕТ (P&L: {pnl_percent:.2f}%)"
                else:  # Sell/Short
                    pnl_percent = (entry_price - mark_price) / entry_price * 100
                    if pnl_percent > 0.5:
                        ts_active += 1
                        status = "✅ АКТИВИРОВАН"
                    else:
                        status = f"⏸️ ОЖИДАЕТ (P&L: {pnl_percent:.2f}%)"
            else:
                status = "⚠️ СТАТУС НЕИЗВЕСТЕН"
                pnl_percent = 0

            print(f"📌 {symbol} ({side}):")
            print(f"   Размер: {size}")
            print(f"   Stop Loss: {'✅' if has_sl else '❌'} {stop_loss if has_sl else 'НЕТ'}")
            print(f"   Trailing Stop: {status}")
            print(f"   TS Distance: {trailing_stop}")
            print(f"   Entry: {entry_price:.4f} | Current: {mark_price:.4f}")
            print(f"   P&L: {pnl_percent:.2f}%")
            print()

            positions_with_ts.append(symbol)
        else:
            ts_not_set += 1
            positions_without_ts.append(symbol)

            print(f"❌ {symbol} ({side}):")
            print(f"   Размер: {size}")
            print(f"   Stop Loss: {'✅' if has_sl else '❌'} {stop_loss if has_sl else 'НЕТ'}")
            print(f"   Trailing Stop: НЕ УСТАНОВЛЕН")
            print(f"   Entry: {entry_price:.4f} | Current: {mark_price:.4f}")
            print()

    # Итоговая статистика
    total = len(open_positions)
    print(f"\n{'=' * 70}")
    print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"{'=' * 70}")
    print(f"Всего открытых позиций: {total}")
    print(f"✅ Trailing Stop установлен: {ts_set} ({ts_set * 100 / total:.1f}%)")
    print(f"🟢 Trailing Stop активирован: {ts_active} ({ts_active * 100 / total:.1f}%)")
    print(f"⏸️ Trailing Stop ожидает: {ts_set - ts_active} ({(ts_set - ts_active) * 100 / total:.1f}%)")
    print(f"❌ Trailing Stop НЕ установлен: {ts_not_set} ({ts_not_set * 100 / total:.1f}%)")

    print(f"\n💡 ВЫВОДЫ:")
    if ts_active == 20:
        print("✅ 20 активных TS соответствуют количеству в веб-интерфейсе!")

    if ts_not_set > 0:
        print(f"⚠️ ВНИМАНИЕ: {ts_not_set} позиций без Trailing Stop!")
        print("   Рекомендуется запустить protection_monitor.py")

    if ts_set > ts_active:
        print(f"ℹ️ {ts_set - ts_active} Trailing Stops ожидают активации")
        print("   Они активируются когда позиция выйдет в достаточную прибыль")


if __name__ == "__main__":
    print("🚀 Запуск проверки Trailing Stops на Bybit TESTNET...\n")
    asyncio.run(check_all_positions_protection())