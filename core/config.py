"""
ATS 2.0 - Core Configuration Module
Centralized configuration management for the trading system
"""
import os
from dataclasses import dataclass, field
from typing import Optional, Literal, Dict, List
from decimal import Decimal
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = field(default_factory=lambda: os.getenv('DB_HOST', 'localhost'))
    port: int = field(default_factory=lambda: int(os.getenv('DB_PORT', 5432)))
    name: str = field(default_factory=lambda: os.getenv('DB_NAME', 'fox_crypto'))
    user: str = field(default_factory=lambda: os.getenv('DB_USER'))
    password: str = field(default_factory=lambda: os.getenv('DB_PASSWORD'))

    @property
    def connection_string(self) -> str:
        """PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def async_connection_string(self) -> str:
        """Async PostgreSQL connection string"""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

@dataclass
class ExchangeConfig:
    """Exchange-specific configuration"""
    name: Literal['binance', 'bybit'] = 'binance'
    api_key: str = field(default_factory=lambda: os.getenv('BINANCE_API_KEY', ''))
    api_secret: str = field(default_factory=lambda: os.getenv('BINANCE_API_SECRET', ''))
    testnet: bool = field(default_factory=lambda: os.getenv('BINANCE_TESTNET', 'true').lower() == 'true')

    # Rate limiting
    max_requests_per_minute: int = 1200
    max_orders_per_minute: int = 300

    # Connection settings
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0

@dataclass
class PositionConfig:
    """Position management configuration"""
    # Size and leverage
    size_usd: float = field(default_factory=lambda: float(os.getenv('POSITION_SIZE_USD', 10.0)))
    leverage: int = field(default_factory=lambda: int(os.getenv('LEVERAGE', 5)))

    # Position limits
    max_open_positions: int = field(default_factory=lambda: int(os.getenv('MAX_OPEN_POSITIONS', 500)))
    max_positions_per_symbol: int = 1  # Prevent duplicate positions

    # Entry filters
    max_spread_percent: float = field(default_factory=lambda: float(os.getenv('MAX_SPREAD_PERCENT', 2.0)))
    max_spread_percent_testnet: float = field(default_factory=lambda: float(os.getenv('MAX_SPREAD_PERCENT_TESTNET', 10.0)))
    min_volume_24h_usd: float = field(default_factory=lambda: float(os.getenv('MIN_VOLUME_24H_USD', 1_000_000)))
    min_volume_24h_usd_testnet: float = field(default_factory=lambda: float(os.getenv('MIN_VOLUME_24H_USD_TESTNET', 10_000)))

    # Blacklist
    skip_symbols: List[str] = field(default_factory=lambda: os.getenv('SKIP_SYMBOLS', 'NOTUSDT,BOMEUSDT').split(','))

@dataclass
class RiskConfig:
    """Risk management configuration"""
    # Stop Loss
    stop_loss_enabled: bool = field(default_factory=lambda: os.getenv('USE_STOP_LOSS', 'true').lower() == 'true')
    stop_loss_type: Literal['fixed', 'trailing'] = field(
        default_factory=lambda: 'trailing' if os.getenv('USE_TRAILING_STOP', 'false').lower() == 'true' else 'fixed'
    )
    stop_loss_percent: float = field(default_factory=lambda: float(os.getenv('STOP_LOSS_PERCENT', 2.0)))

    # Trailing Stop specific
    trailing_callback_rate: float = field(default_factory=lambda: float(os.getenv('TRAILING_STOP_CALLBACK_RATE', 2.0)))
    trailing_activation_percent: float = field(default_factory=lambda: float(os.getenv('ACTIVATION_PRICE_PERCENT', 3.0)))

    # Take Profit
    take_profit_enabled: bool = field(default_factory=lambda: os.getenv('USE_TAKE_PROFIT', 'true').lower() == 'true')
    take_profit_type: Literal['simple', 'partial'] = 'simple'
    take_profit_percent: float = field(default_factory=lambda: float(os.getenv('TAKE_PROFIT_PERCENT', 3.0)))

    # Partial TP configuration (can be extended)
    take_profit_levels: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "level1": {"percent": 2.0, "size": 33},  # Take 33% at +2%
        "level2": {"percent": 3.0, "size": 33},  # Take 33% at +3%
        "level3": {"percent": 5.0, "size": 34},  # Take 34% at +5%
    })

    # Daily limits
    max_daily_trades: int = field(default_factory=lambda: int(os.getenv('MAX_DAILY_TRADES', 200000)))
    max_daily_loss_usd: float = 1000.0  # Stop trading if daily loss exceeds this

    # Position timeouts
    auto_close_positions: bool = field(default_factory=lambda: os.getenv('AUTO_CLOSE_POSITIONS', 'false').lower() == 'true')
    max_position_duration_hours: int = field(default_factory=lambda: int(os.getenv('MAX_POSITION_DURATION_HOURS', 24)))

    # Emergency stop
    emergency_stop_loss_percent: float = 10.0  # Force close if loss exceeds 10%

@dataclass
class SignalConfig:
    """Signal filtering configuration"""
    # Confidence filters
    min_confidence_level: Optional[str] = field(
        default_factory=lambda: os.getenv('MIN_CONFIDENCE_LEVEL', '') or None
    )
    min_prediction_proba: float = field(
        default_factory=lambda: float(os.getenv('MIN_PREDICTION_PROBA', 0.0))
    )

    # Signal processing
    process_all_signals: bool = True  # Process both LONG and SHORT
    signal_timeout_seconds: int = 60  # Skip signals older than this

    # Duplicate prevention
    signal_cooldown_seconds: int = 30  # Minimum time between signals for same symbol

@dataclass
class MonitoringConfig:
    """Monitoring and alerting configuration"""
    # Telegram
    telegram_enabled: bool = field(
        default_factory=lambda: bool(os.getenv('TELEGRAM_BOT_TOKEN'))
    )
    telegram_bot_token: str = field(default_factory=lambda: os.getenv('TELEGRAM_BOT_TOKEN', ''))
    telegram_chat_id: str = field(default_factory=lambda: os.getenv('TELEGRAM_CHAT_ID', ''))

    # Alert thresholds
    alert_on_error: bool = True
    alert_on_position_open: bool = True
    alert_on_position_close: bool = True
    alert_on_daily_summary: bool = True

    # Performance monitoring
    log_level: str = field(default_factory=lambda: os.getenv('LOG_LEVEL', 'INFO'))
    log_to_file: bool = True
    log_file: str = 'ats_system.log'

    # Metrics
    metrics_update_interval_seconds: int = 60
    dashboard_port: int = 8080

@dataclass
class SystemConfig:
    """Main system configuration"""
    # Component configs
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    exchange: ExchangeConfig = field(default_factory=ExchangeConfig)
    position: PositionConfig = field(default_factory=PositionConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    signal: SignalConfig = field(default_factory=SignalConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)

    # System settings
    mode: Literal['live', 'paper', 'backtest'] = field(
        default_factory=lambda: os.getenv('TRADING_MODE', 'live').lower()
    )

    # Safety features
    dry_run: bool = field(default_factory=lambda: os.getenv('DRY_RUN', 'false').lower() == 'true')
    require_confirmation: bool = False  # Require manual confirmation for trades

    # Performance
    parallel_processing: bool = True
    max_workers: int = 4

    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []

        # Check required fields
        if not self.database.user:
            errors.append("Database user is required")
        if not self.database.password:
            errors.append("Database password is required")

        if self.mode == 'live':
            if not self.exchange.api_key:
                errors.append("Exchange API key is required for live trading")
            if not self.exchange.api_secret:
                errors.append("Exchange API secret is required for live trading")

        # Validate numeric ranges
        if self.position.leverage < 1 or self.position.leverage > 125:
            errors.append(f"Invalid leverage: {self.position.leverage} (must be 1-125)")

        if self.risk.stop_loss_percent <= 0 or self.risk.stop_loss_percent > 100:
            errors.append(f"Invalid stop loss percent: {self.risk.stop_loss_percent}")

        if self.risk.take_profit_percent <= 0 or self.risk.take_profit_percent > 1000:
            errors.append(f"Invalid take profit percent: {self.risk.take_profit_percent}")

        return errors

    def to_dict(self) -> Dict:
        """Convert config to dictionary for logging"""
        return {
            'mode': self.mode,
            'exchange': self.exchange.name,
            'testnet': self.exchange.testnet,
            'position_size': self.position.size_usd,
            'leverage': self.position.leverage,
            'max_positions': self.position.max_open_positions,
            'stop_loss': {
                'enabled': self.risk.stop_loss_enabled,
                'type': self.risk.stop_loss_type,
                'percent': self.risk.stop_loss_percent
            },
            'take_profit': {
                'enabled': self.risk.take_profit_enabled,
                'type': self.risk.take_profit_type,
                'percent': self.risk.take_profit_percent
            }
        }

    @classmethod
    def load(cls, config_file: Optional[str] = None) -> 'SystemConfig':
        """Load configuration from file or environment"""
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config_data = json.load(f)
                # TODO: Implement loading from JSON

        return cls()


# Singleton instance
config = SystemConfig()

# Validate on import
errors = config.validate()
if errors and config.mode == 'live':
    raise ValueError(f"Configuration errors: {'; '.join(errors)}")