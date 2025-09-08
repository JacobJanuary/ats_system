# diagnose_problematic_positions.py
# Детальный анализ позиций без Trailing Stop

import asyncio
import aiohttp
import hmac
import hashlib
import time
import json
import os
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class ProblematicPositionsDiagnostics:
    def __init__(self):
        self.api_key = os.getenv('BYBIT_API_KEY')
        self.api_secret = os.getenv('BYBIT_API_SECRET')
        self.testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
        self.base_url = "https://api-testnet.bybit.com" if self.testnet else "https://api.bybit.com"

    async def diagnose(self):
        """Детальная диагностика позиций без TS"""
        print("🔍 ДИАГНОСТИКА ПРОБЛЕМНЫХ ПОЗИЦИЙ БЕЗ TRAILING STOP")
        print("=" * 70)

        # Получаем все позиции
        positions = await self.get_all_positions()

        if not positions:
            print("❌ Нет открытых позиций")
            return

        # Находим позиции без TS
        positions_without_ts = []

        for pos in positions:
            trailing_stop = pos.get('trailingStop')
            if not trailing_stop or trailing_stop == '0' or trailing_stop == '':
                positions_without_ts.append(pos)

        print(f"\n📊 Найдено {len(positions_without_ts)} позиций без Trailing Stop")

        if not positions_without_ts:
            print("✅ Все позиции имеют Trailing Stop!")
            return

        # Детальный анализ каждой проблемной позиции
        for idx, pos in enumerate(positions_without_ts, 1):
            print(f"\n{'=' * 70}")
            print(f"ПОЗИЦИЯ #{idx}")
            print(f"{'=' * 70}")

            symbol = pos.get('symbol')
            side = pos.get('side')
            size = float(pos.get('size', 0))
            entry_price = float(pos.get('avgPrice', 0))
            mark_price = float(pos.get('markPrice', 0))
            created_time = pos.get('createdTime')

            # Расчет P&L
            if side == 'Buy':
                pnl_percent = ((mark_price - entry_price) / entry_price * 100) if entry_price > 0 else 0
            else:
                pnl_percent = ((entry_price - mark_price) / entry_price * 100) if entry_price > 0 else 0

            # Возраст позиции
            if created_time:
                created_dt = datetime.fromtimestamp(int(created_time) / 1000)
                age_hours = (datetime.now() - created_dt).total_seconds() / 3600
            else:
                age_hours = 0

            print(f"📌 Symbol: {symbol}")
            print(f"📊 Side: {side}")
            print(f"📏 Size: {size}")
            print(f"💵 Entry Price: ${entry_price:.4f}")
            print(f"💰 Current Price: ${mark_price:.4f}")
            print(f"📈 P&L: {pnl_percent:.2f}%")
            print(f"⏰ Position Age: {age_hours:.1f} hours")

            # Проверка других защит
            stop_loss = pos.get('stopLoss')
            take_profit = pos.get('takeProfit')

            print(f"\n🛡️ ТЕКУЩАЯ ЗАЩИТА:")
            print(f"   Stop Loss: {'✅ ' + str(stop_loss) if stop_loss and stop_loss != '0' else '❌ НЕТ'}")
            print(f"   Take Profit: {'✅ ' + str(take_profit) if take_profit and take_profit != '0' else '❌ НЕТ'}")
            print(f"   Trailing Stop: ❌ НЕТ")

            # Анализ возможных причин
            print(f"\n🔍 ВОЗМОЖНЫЕ ПРИЧИНЫ ОТСУТСТВИЯ TS:")

            # Причина 1: Слишком большой убыток
            if pnl_percent < -5:
                print(f"   ⚠️ Позиция в большом убытке ({pnl_percent:.2f}%)")
                print(f"      → TS может не устанавливаться из-за слишком близкой цены активации")

            # Причина 2: Минимальная цена для символа
            if mark_price < 1:
                print(f"   ⚠️ Низкая цена актива (${mark_price:.4f})")
                print(f"      → Возможны проблемы с tick size или минимальным шагом цены")

            # Причина 3: Особые символы
            if any(x in symbol for x in ['1000', 'SATS', 'SHIB', 'FLOKI']):
                print(f"   ⚠️ Особый символ с множителем")
                print(f"      → Требуется специальная обработка для расчета TS")

            # Причина 4: Старая позиция
            if age_hours > 24:
                print(f"   ⚠️ Старая позиция ({age_hours:.1f} часов)")
                print(f"      → Возможно, создана до внедрения автоматической защиты")

            # Попытка установить TS с разными параметрами
            print(f"\n🔧 ПОПЫТКА УСТАНОВКИ TRAILING STOP:")

            # Вариант 1: Стандартный
            ts_result1 = await self.try_set_trailing_stop(
                pos,
                distance_percent=1.5,
                activation_offset=0.5,
                description="Стандартный (1.5%, активация +0.5%)"
            )

            if not ts_result1['success']:
                # Вариант 2: Увеличенная дистанция
                ts_result2 = await self.try_set_trailing_stop(
                    pos,
                    distance_percent=3.0,
                    activation_offset=1.0,
                    description="Увеличенный (3%, активация +1%)"
                )

                if not ts_result2['success']:
                    # Вариант 3: Минимальный
                    ts_result3 = await self.try_set_trailing_stop(
                        pos,
                        distance_percent=0.5,
                        activation_offset=0.1,
                        description="Минимальный (0.5%, активация +0.1%)"
                    )

                    if not ts_result3['success']:
                        print(f"   ❌ НЕ УДАЕТСЯ УСТАНОВИТЬ TS НИ С КАКИМИ ПАРАМЕТРАМИ")
                        print(f"   Возможные причины:")
                        print(f"   - API ограничения для этого символа")
                        print(f"   - Недостаточная ликвидность")
                        print(f"   - Технические ограничения биржи")

            # Рекомендации
            print(f"\n💡 РЕКОМЕНДАЦИИ:")
            if pnl_percent < -5:
                print(f"   1. Рассмотрите закрытие позиции (убыток {pnl_percent:.2f}%)")
            print(f"   2. Установите хотя бы Stop Loss для защиты")
            print(f"   3. Мониторьте позицию вручную")

    async def get_all_positions(self):
        """Получение всех позиций"""
        timestamp = str(int(time.time() * 1000))
        params = {
            "category": "linear",
            "settleCoin": "USDT",
            "limit": "200"
        }

        param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        sign_str = timestamp + self.api_key + "5000" + param_str
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "5000"
        }

        url = f"{self.base_url}/v5/position/list?{param_str}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('retCode') == 0:
                        positions = data.get('result', {}).get('list', [])
                        return [p for p in positions if float(p.get('size', 0)) > 0]
        return []

    async def try_set_trailing_stop(self, position, distance_percent, activation_offset, description):
        """Попытка установить TS с заданными параметрами"""
        symbol = position.get('symbol')
        side = position.get('side')
        entry_price = float(position.get('avgPrice', 0))
        mark_price = float(position.get('markPrice', 0))

        print(f"\n   🔧 Попытка: {description}")

        if entry_price == 0:
            print(f"      ❌ Нет entry price")
            return {'success': False, 'error': 'No entry price'}

        # Расчет trailing distance
        trailing_distance = entry_price * (distance_percent / 100)

        # Специальная обработка для символов с множителем
        if '1000' in symbol:
            trailing_distance = trailing_distance * 1000

        # Расчет activation price
        if side == 'Buy':
            activation_price = max(mark_price, entry_price) * (1 + activation_offset / 100)
        else:
            activation_price = min(mark_price, entry_price) * (1 - activation_offset / 100)

        # Округление согласно tick size
        trailing_distance = self.round_to_tick_size(symbol, trailing_distance)
        activation_price = self.round_to_tick_size(symbol, activation_price)

        print(f"      Distance: {trailing_distance:.6f}")
        print(f"      Activation: {activation_price:.6f}")

        params = {
            "category": "linear",
            "symbol": symbol,
            "trailingStop": str(trailing_distance),
            "activePrice": str(activation_price),
            "positionIdx": 0
        }

        timestamp = str(int(time.time() * 1000))
        json_params = json.dumps(params, separators=(',', ':'))
        sign_str = timestamp + self.api_key + "5000" + json_params

        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "5000",
            "Content-Type": "application/json"
        }

        url = f"{self.base_url}/v5/position/trading-stop"

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=params) as response:
                result = await response.json()

                if result.get('retCode') == 0:
                    print(f"      ✅ УСПЕШНО УСТАНОВЛЕН!")
                    return {'success': True}
                else:
                    error_msg = result.get('retMsg', 'Unknown error')
                    error_code = result.get('retCode')
                    print(f"      ❌ Ошибка: {error_msg} (код: {error_code})")

                    # Детальный анализ ошибки
                    if 'price' in error_msg.lower():
                        print(f"         → Проблема с ценой (возможно, слишком близко к текущей)")
                    elif 'invalid' in error_msg.lower():
                        print(f"         → Недопустимые параметры для этого символа")
                    elif 'risk' in error_msg.lower():
                        print(f"         → Ограничения риск-менеджмента биржи")

                    return {'success': False, 'error': error_msg, 'code': error_code}

    def round_to_tick_size(self, symbol, price):
        """Округление цены согласно tick size символа"""
        # Упрощенная логика - нужно загрузить реальные tick sizes из API
        if price < 0.001:
            return round(price, 8)
        elif price < 0.01:
            return round(price, 6)
        elif price < 1:
            return round(price, 4)
        elif price < 100:
            return round(price, 2)
        else:
            return round(price, 1)


async def main():
    diagnostics = ProblematicPositionsDiagnostics()
    await diagnostics.diagnose()


if __name__ == "__main__":
    print("🚀 Запуск диагностики проблемных позиций...\n")
    asyncio.run(main())