# 📊 КОМПЛЕКСНЫЙ АУДИТ: Миграция с smart_ml.predictions на fas.scoring_history

## 🎯 РЕЗЮМЕ

Система до сих пор использует устаревшую таблицу `smart_ml.predictions` для получения торговых сигналов. Необходимо переключиться на новую таблицу `fas.scoring_history`, которая содержит более актуальные и качественные данные.

## 📌 ТЕКУЩЕЕ СОСТОЯНИЕ

### Старая архитектура (smart_ml.predictions)
- **Таблица**: `smart_ml.predictions`
- **Зависимость**: Требует JOIN с `fas.scoring_history` через `signal_id`
- **Проблемы**:
  - Двойная зависимость от двух таблиц
  - Устаревшая логика
  - Возможные несоответствия данных

### Новая архитектура (fas.scoring_history)
- **Таблица**: `fas.scoring_history`
- **Преимущества**:
  - Прямой источник сигналов
  - Содержит все необходимые scores
  - Актуальные данные
  - Более гибкая фильтрация

## ✅ ЧТО БЫЛО СДЕЛАНО

### 1. Созданы новые модели данных
- **Файл**: `database/models_v2.py`
- **Класс**: `TradingSignalV2`
- Поддержка новых полей:
  - `total_score`
  - `pattern_score`
  - `indicator_score`
  - `score_week`
  - `score_month`
  - `recommended_action`

### 2. Реализован новый репозиторий
- **Файл**: `database/signal_repository_v2.py`
- **Класс**: `SignalRepositoryV2`
- Методы:
  - `get_unprocessed_signals_v2()` - получение сигналов из fas.scoring_history
  - `get_unprocessed_signals_legacy()` - совместимость со старым форматом
  - `migrate_add_columns()` - миграция БД

### 3. Интеграция в систему
- **Файл**: `database/connection.py`
- Добавлен `signals_v2` репозиторий
- Флаг переключения: `USE_SCORING_HISTORY`

### 4. Миграция базы данных
Добавлены новые колонки в `ats.signals`:
- `total_score`
- `score_week`
- `score_month`
- `recommended_action`

## 🔍 ОБНАРУЖЕННЫЕ ПРОБЛЕМЫ

### 1. Отсутствие сигналов
**Причина**: Слишком высокие пороговые значения
- `MIN_SCORE_WEEK = 0.59` (59 из 100?)
- `MIN_SCORE_MONTH = 0.59`

**Решение**: Проверить реальные значения scores в БД

### 2. Различия в структуре данных
- Старая: использует `prediction`, `prediction_proba`
- Новая: использует scores и `recommended_action`

## 📋 ПЛАН МИГРАЦИИ

### Шаг 1: Анализ данных
```sql
-- Проверить диапазон scores
SELECT 
    MIN(score_week) as min_week,
    MAX(score_week) as max_week,
    AVG(score_week) as avg_week,
    MIN(score_month) as min_month,
    MAX(score_month) as max_month,
    AVG(score_month) as avg_month
FROM fas.scoring_history
WHERE created_at > NOW() - INTERVAL '1 day';
```

### Шаг 2: Настройка параметров
В файле `.env`:
```env
# Новые параметры для fas.scoring_history
USE_SCORING_HISTORY=true
MIN_SCORE_WEEK=59       # Настроить под реальные данные
MIN_SCORE_MONTH=59      # Настроить под реальные данные
SKIP_NO_TRADE=true      # Пропускать сигналы NO_TRADE
```

### Шаг 3: Обновление main.py
Изменить логику получения сигналов:
```python
# Старый код
signals = await self.db.signals.get_unprocessed_signals(limit)

# Новый код
if self.db.use_new_signal_source:
    signals = await self.db.signals_v2.get_unprocessed_signals_legacy(limit)
else:
    signals = await self.db.signals.get_unprocessed_signals(limit)
```

### Шаг 4: Тестирование
1. Запустить `test_new_signal_source.py`
2. Проверить получение сигналов
3. Провести тестовую торговлю на testnet

### Шаг 5: Постепенная миграция
1. Включить параллельную работу обоих источников
2. Сравнивать качество сигналов
3. Полностью переключиться после валидации

## 🚨 КРИТИЧЕСКИЕ ИЗМЕНЕНИЯ

### Файлы, требующие обновления:
1. ✅ `database/connection.py` - добавлен signals_v2
2. ⚠️ `main.py` - нужно обновить логику получения сигналов
3. ⚠️ `trading/signal_processor.py` - проверить совместимость

### Изменения в логике:
- Сигналы теперь идентифицируются по `signal_id` (fas.scoring_history.id)
- Вместо `prediction_id` используется `signal_id`
- Добавлена фильтрация по scores вместо prediction_proba

## 📊 СРАВНЕНИЕ ИСТОЧНИКОВ

| Параметр | smart_ml.predictions | fas.scoring_history |
|----------|---------------------|-------------------|
| Актуальность | Устаревшая | Актуальная |
| Зависимости | Требует JOIN | Самодостаточная |
| Фильтрация | По prediction_proba | По scores |
| Данные | Бинарные (BUY/SELL) | Детальные scores |
| Рекомендации | Нет | recommended_action |

## ✔️ РЕКОМЕНДАЦИИ

1. **СРОЧНО**: Проверить реальные значения scores в fas.scoring_history
2. **Настроить пороги**: MIN_SCORE_WEEK и MIN_SCORE_MONTH под реальные данные
3. **Тестировать**: На testnet перед production
4. **Мониторинг**: Следить за качеством новых сигналов
5. **Откат**: Сохранить возможность вернуться на старый источник

## 🎯 РЕЗУЛЬТАТ

После успешной миграции система будет:
- Использовать актуальный источник данных
- Иметь более гибкую фильтрацию сигналов
- Работать быстрее (меньше JOIN операций)
- Поддерживать новые типы анализа (patterns, combinations)

## 📝 КОМАНДЫ ДЛЯ ПРИМЕНЕНИЯ

```bash
# 1. Проверить текущую конфигурацию
python test_new_signal_source.py

# 2. Анализ структуры БД
python analyze_db_structure.py

# 3. Запустить с новым источником
USE_SCORING_HISTORY=true python main.py

# 4. Мониторинг
tail -f ats_system.log | grep "scoring_history"
```

**Система готова к миграции!** Требуется только настройка параметров и тестирование.