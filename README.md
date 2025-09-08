# ATS 2.0 - Automated Trading System

## 🚀 Production-Ready Crypto Trading System

Automated trading system for Binance and Bybit with real-time signal processing, risk management, and comprehensive monitoring.

## ✨ Features

- **Multi-Exchange Support**: Binance and Bybit futures trading
- **Real-time Signal Processing**: WebSocket streaming for instant market data
- **Advanced Risk Management**: Position limits, stop-loss, trailing stops
- **Security First**: Encrypted API keys, sanitized logging, rate limiting
- **Monitoring & Metrics**: Prometheus metrics, health checks, web dashboard
- **Resilient Architecture**: Retry logic, circuit breakers, graceful shutdown

## 📋 Prerequisites

- Python 3.8+
- PostgreSQL 12+
- API keys for Binance/Bybit

## 🛠 Installation

1. **Clone repository**:
```bash
git clone <repository>
cd ats_system
```

2. **Create virtual environment**:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your settings
```

Required environment variables:
```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading_db
DB_USER=your_user
DB_PASSWORD=your_password

# Binance
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
BINANCE_TESTNET=true

# Bybit
BYBIT_API_KEY=your_api_key
BYBIT_API_SECRET=your_api_secret
BYBIT_TESTNET=true

# Security
ATS_MASTER_KEY=your_encryption_key  # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Trading
TRADING_MODE=paper  # live, paper, backtest
MAX_OPEN_POSITIONS=5
POSITION_SIZE_USD=100
USE_STOP_LOSS=true
STOP_LOSS_PERCENT=2.0
USE_TRAILING_STOP=true
TRAILING_CALLBACK_RATE=1.0
```

## 🚀 Quick Start

### 1. Start the trading system:
```bash
python main.py
```

### 2. Start the web dashboard (optional):
```bash
python -m uvicorn web.app:app --host 0.0.0.0 --port 8000
```

### 3. Access interfaces:
- Dashboard: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Metrics: http://localhost:8000/metrics
- Health: http://localhost:8000/health

## 📊 Architecture

```
ats_system/
├── core/           # Core utilities
│   ├── config.py   # Configuration management
│   ├── security.py # Security and encryption
│   └── retry.py    # Retry and circuit breaker
├── database/       # Database layer
│   ├── connection.py # Async PostgreSQL
│   └── models.py   # Data models
├── exchanges/      # Exchange integrations
│   ├── base.py     # Abstract base class
│   ├── binance.py  # Binance implementation
│   ├── bybit.py    # Bybit implementation
│   └── websocket_manager.py # WebSocket handling
├── trading/        # Trading logic
│   └── signal_processor.py # Signal processing
├── monitoring/     # Monitoring
│   └── metrics.py  # Prometheus metrics
├── web/           # Web interface
│   └── app.py     # FastAPI application
└── main.py        # Main entry point
```

## 🔒 Security Features

- **API Key Encryption**: All API keys encrypted at rest
- **Sanitized Logging**: Sensitive data automatically removed from logs
- **Rate Limiting**: Prevents API abuse and rate limit violations
- **IP Whitelisting**: Optional IP restrictions for production
- **Secure WebSocket**: Authenticated WebSocket connections

## 📈 Monitoring

### Prometheus Metrics
- Signal processing metrics
- Order execution metrics
- Position P&L tracking
- API latency monitoring
- System health metrics

### Health Checks
- `/health` - Comprehensive health status
- `/health/live` - Kubernetes liveness probe
- `/health/ready` - Kubernetes readiness probe

## 🐳 Docker Deployment

```bash
# Build image
docker build -t ats-system:2.0 .

# Run with docker-compose
docker-compose up -d
```

## 🧪 Testing

```bash
# Run unit tests
pytest tests/unit -v

# Run integration tests (requires test database)
pytest tests/integration -v

# Run with coverage
pytest --cov=. --cov-report=html
```

## 📝 API Documentation

### REST API Endpoints

- `GET /api/positions` - Get open positions
- `GET /api/stats` - System statistics
- `GET /api/config` - Configuration (sanitized)
- `POST /api/positions/{id}/close` - Close position
- `POST /api/emergency/stop` - Emergency stop

### WebSocket

Connect to `ws://localhost:8000/ws` for real-time updates.

## ⚠️ Production Checklist

- [ ] Set `TRADING_MODE=live` in production
- [ ] Use real API keys (not testnet)
- [ ] Configure proper database backups
- [ ] Set up monitoring (Prometheus + Grafana)
- [ ] Configure alerts for critical events
- [ ] Review and adjust risk parameters
- [ ] Set up SSL/TLS for web interface
- [ ] Configure firewall rules
- [ ] Set up log rotation
- [ ] Test emergency stop procedures

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Make changes with tests
4. Submit pull request

## 📄 License

[Your License]

## 🆘 Support

For issues and questions, please create an issue on GitHub.

## ⚡ Performance Tips

1. **Database Optimization**:
   - Add indexes on frequently queried columns
   - Use connection pooling (already configured)
   - Regular VACUUM and ANALYZE

2. **API Rate Limits**:
   - Binance: 1200 requests/min
   - Bybit: 120 requests/min
   - Use WebSocket for real-time data

3. **Memory Management**:
   - System uses ~200-500MB RAM
   - Monitor for memory leaks
   - Restart periodically if needed

## 🔧 Troubleshooting

### Connection Issues
```bash
# Test database connection
python -c "from database.connection import DatabaseManager; import asyncio; asyncio.run(DatabaseManager().health_check())"

# Test exchange connection
python -c "from exchanges.binance import BinanceExchange; import asyncio; asyncio.run(BinanceExchange({}).initialize())"
```

### Common Errors

1. **"Circuit breaker is OPEN"**
   - Too many failures, wait for recovery timeout
   - Check API keys and network connection

2. **"Rate limit exceeded"**
   - Reduce request frequency
   - Use WebSocket instead of polling

3. **"Position already exists"**
   - Duplicate signal received
   - Check signal cooldown settings

## 📊 System Requirements

- **Minimum**: 2 CPU cores, 2GB RAM, 10GB disk
- **Recommended**: 4 CPU cores, 4GB RAM, 20GB disk
- **Network**: Stable internet, <100ms latency to exchanges

---

**Version**: 2.0.0  
**Last Updated**: 2024  
**Status**: Production Ready