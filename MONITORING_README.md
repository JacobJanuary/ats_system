# ATS 2.0 WebSocket Monitoring System

## Описание

Production-ready система real-time мониторинга позиций через WebSocket API бирж Binance и Bybit.

## Архитектура

```
┌─────────────────┐     WebSocket      ┌──────────────┐
│    Binance      │◄──────────────────►│   WebSocket  │
│   WebSocket     │                    │    Daemon    │
└─────────────────┘                    │              │
                                       │              │
┌─────────────────┐     WebSocket      │              │
│     Bybit       │◄──────────────────►│              │
│   WebSocket     │                    └──────┬───────┘
└─────────────────┘                           │
                                              │ Updates
                                              ▼
                                       ┌──────────────┐
                                       │  PostgreSQL  │
                                       │              │
                                       │  positions   │
                                       │  balances    │
                                       └──────┬───────┘
                                              │
                                              │ Queries
                                              ▼
                                       ┌──────────────┐
                                       │     Web      │
                                       │  Interface   │
                                       │  (FastAPI)   │
                                       └──────────────┘
```

## Компоненты

### 1. WebSocket Daemon (`monitoring/websocket_daemon.py`)
- Подключение к WebSocket API Binance и Bybit
- Real-time получение обновлений позиций
- Синхронизация с PostgreSQL
- Автоматическое переподключение при сбоях
- Health check и мониторинг состояния

### 2. Web Interface (`monitoring/web_interface.py`)
- FastAPI backend с REST API
- WebSocket для real-time обновлений в браузере
- Современный HTML/Vue.js интерфейс
- Статистика и аналитика

### 3. Database Schema (`migrations/001_monitoring_tables.sql`)
- Таблицы для позиций, балансов, истории
- Автоматические триггеры и функции
- Оптимизированные индексы

## Установка

### 1. Требования
- Python 3.11+
- PostgreSQL 15+
- Docker и Docker Compose (опционально)

### 2. Установка зависимостей

```bash
pip install -r requirements.monitoring.txt
```

### 3. Настройка переменных окружения

Скопируйте `.env.example` в `.env` и заполните:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fox_crypto
DB_USER=your_user
DB_PASSWORD=your_password

# Binance
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret
BINANCE_TESTNET=false

# Bybit
BYBIT_API_KEY=your_key
BYBIT_API_SECRET=your_secret
BYBIT_TESTNET=true
```

### 4. Инициализация базы данных

```bash
# Создание базы данных
createdb fox_crypto

# Применение миграций
./monitoring_control.sh init
```

## Запуск

### Вариант 1: Локальный запуск

```bash
# Запуск всех компонентов
./monitoring_control.sh start

# Остановка
./monitoring_control.sh stop

# Перезапуск
./monitoring_control.sh restart

# Проверка статуса
./monitoring_control.sh status

# Просмотр логов
./monitoring_control.sh logs all
```

### Вариант 2: Docker Compose

```bash
# Запуск
./monitoring_control.sh docker-up

# Остановка
./monitoring_control.sh docker-down

# Логи
./monitoring_control.sh docker-logs
```

### Вариант 3: Systemd (Production)

```bash
# Копирование сервисов
sudo cp systemd/*.service /etc/systemd/system/

# Перезагрузка systemd
sudo systemctl daemon-reload

# Запуск сервисов
sudo systemctl enable ats-monitoring
sudo systemctl enable ats-web
sudo systemctl start ats-monitoring
sudo systemctl start ats-web

# Проверка статуса
sudo systemctl status ats-monitoring
sudo systemctl status ats-web
```

## Использование

### Web Interface

Откройте браузер: http://localhost:8000

Интерфейс показывает:
- Открытые позиции в реальном времени
- Статистику PnL
- Защиту позиций (SL/TP/Trailing)
- Балансы аккаунтов
- Историю торговли

### API Endpoints

```
GET /api/positions - Список позиций
GET /api/balances - Балансы аккаунтов
GET /api/stats - Общая статистика
GET /api/health - Состояние системы
GET /api/daily-pnl - История PnL по дням
GET /api/position-history/{id} - История изменений позиции
WS /ws - WebSocket для real-time обновлений
```

### Примеры запросов

```bash
# Получить открытые позиции
curl http://localhost:8000/api/positions?status=OPEN

# Получить позиции Binance
curl http://localhost:8000/api/positions?exchange=binance

# Получить статистику
curl http://localhost:8000/api/stats

# Проверить здоровье системы
curl http://localhost:8000/api/health
```

## Мониторинг

### Логи

```bash
# Daemon логи
tail -f logs/daemon.log

# Web interface логи
tail -f logs/web.log

# Все логи
./monitoring_control.sh logs all
```

### Метрики

Система автоматически отслеживает:
- Количество открытых позиций
- Unrealized/Realized PnL
- Процент защищенных позиций
- Win rate
- Состояние WebSocket соединений

### Health Check

```bash
curl http://localhost:8000/api/health
```

Возвращает статус всех компонентов:
- database - соединение с БД
- binance_ws - WebSocket Binance
- bybit_ws - WebSocket Bybit
- daemon - основной демон

## Безопасность

1. **API ключи**: Храните только в `.env` файле
2. **Доступ**: Ограничьте доступ к web интерфейсу через firewall
3. **SSL**: Используйте nginx с SSL для production
4. **Permissions**: Запускайте от отдельного пользователя

## Troubleshooting

### WebSocket не подключается

1. Проверьте API ключи
2. Проверьте сетевое соединение
3. Проверьте логи: `grep ERROR logs/daemon.log`

### Позиции не обновляются

1. Проверьте соединение с БД
2. Проверьте WebSocket статус в `/api/health`
3. Перезапустите daemon: `./monitoring_control.sh restart`

### Web интерфейс недоступен

1. Проверьте порт 8000
2. Проверьте логи web: `tail -f logs/web.log`
3. Проверьте firewall настройки

## Performance

Система оптимизирована для:
- До 1000 одновременных позиций
- Обновления каждые 1-2 секунды
- Минимальная задержка < 100ms
- PostgreSQL connection pooling
- Асинхронная обработка

## Обновление

```bash
# Остановить сервисы
./monitoring_control.sh stop

# Обновить код
git pull

# Установить новые зависимости
pip install -r requirements.monitoring.txt

# Применить миграции БД
./monitoring_control.sh init

# Запустить сервисы
./monitoring_control.sh start
```

## Поддержка

При возникновении проблем:
1. Проверьте логи
2. Проверьте `/api/health`
3. Убедитесь в правильности конфигурации
4. Перезапустите компоненты

## Лицензия

Proprietary - ATS 2.0 Trading System