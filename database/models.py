"""
ATS 2.0 - Database Models
Data models for signals, positions, orders and other entities
"""
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any, List
from enum import Enum
import json


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class SignalStatus(str, Enum):
    """Signal processing status"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    EXECUTED = "EXECUTED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


class PositionStatus(str, Enum):
    """Position lifecycle status"""
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    ERROR = "ERROR"


class OrderType(str, Enum):
    """Order types"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"


class OrderPurpose(str, Enum):
    """Purpose of the order"""
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    TRAILING_STOP = "TRAILING_STOP"


class OrderStatus(str, Enum):
    """Exchange order status"""
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class CloseReason(str, Enum):
    """Position close reasons"""
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    MANUAL = "MANUAL"
    SIGNAL = "SIGNAL"
    TIMEOUT = "TIMEOUT"
    EMERGENCY = "EMERGENCY"
    ERROR = "ERROR"


@dataclass
class TradingSignal:
    """Trading signal from ML predictions"""
    # From smart_ml.predictions
    prediction_id: int
    signal_id: int
    prediction: bool
    prediction_proba: Decimal
    confidence_level: Optional[str]
    signal_type: Optional[str]  # BUY/SELL
    created_at: datetime

    # From fas.scoring_history
    pair_symbol: str
    trading_pair_id: Optional[int] = None

    # From trading_pairs
    exchange_id: Optional[int] = None  # 1=Binance, 2=Bybit
    exchange_name: Optional[str] = None  # Derived from exchange_id

    # Processing
    id: Optional[int] = None  # ats.signals.id
    status: SignalStatus = SignalStatus.PENDING
    processed_at: Optional[datetime] = None
    skip_reason: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def side(self) -> str:
        """Convert signal_type to position side"""
        # SELL signal means SHORT position
        # BUY signal means LONG position
        if self.signal_type == "SELL":
            return "SHORT"
        elif self.signal_type == "BUY":
            return "LONG"
        return "UNKNOWN"

    @property
    def is_valid(self) -> bool:
        """Check if signal is valid for processing"""
        return (
            self.prediction and
            self.pair_symbol and
            self.signal_type in ["BUY", "SELL"]
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database/logging"""
        return {
            'prediction_id': self.prediction_id,
            'signal_id': self.signal_id,
            'pair_symbol': self.pair_symbol,
            'signal_type': self.signal_type,
            'side': self.side,
            'confidence_level': self.confidence_level,
            'prediction_proba': float(self.prediction_proba),
            'prediction': self.prediction,
            'exchange_id': self.exchange_id,
            'exchange_name': self.exchange_name,
            'status': self.status.value if isinstance(self.status, Enum) else self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'processed_at': self.processed_at.isoformat() if self.processed_at else None
        }


@dataclass
class Position:
    """Active trading position"""
    # Identity
    id: Optional[int] = None
    signal_id: Optional[int] = None
    prediction_id: int = 0

    # Position details
    exchange: str = "binance"
    symbol: str = ""
    side: str = ""  # LONG or SHORT

    # Entry
    entry_price: Decimal = Decimal(0)
    quantity: Decimal = Decimal(0)
    leverage: int = 1

    # Current state
    status: PositionStatus = PositionStatus.OPEN
    current_price: Optional[Decimal] = None

    # Risk management
    stop_loss_price: Optional[Decimal] = None
    stop_loss_type: Optional[str] = None
    stop_loss_order_id: Optional[str] = None
    take_profit_price: Optional[Decimal] = None
    take_profit_order_id: Optional[str] = None

    # Tracking for trailing stop
    max_price: Optional[Decimal] = None  # For LONG positions
    min_price: Optional[Decimal] = None  # For SHORT positions

    # Exit
    exit_price: Optional[Decimal] = None
    realized_pnl: Optional[Decimal] = None
    realized_pnl_percent: Optional[Decimal] = None
    commission_paid: Optional[Decimal] = None
    close_reason: Optional[CloseReason] = None

    # Timestamps
    opened_at: datetime = field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_open(self) -> bool:
        """Check if position is open"""
        return self.status == PositionStatus.OPEN

    @property
    def unrealized_pnl(self) -> Optional[Decimal]:
        """Calculate unrealized PNL"""
        if not self.current_price or not self.entry_price:
            return None

        if self.side == "LONG":
            pnl_percent = (self.current_price - self.entry_price) / self.entry_price
        else:  # SHORT
            pnl_percent = (self.entry_price - self.current_price) / self.entry_price

        return pnl_percent * self.quantity * self.entry_price

    @property
    def unrealized_pnl_percent(self) -> Optional[Decimal]:
        """Calculate unrealized PNL percentage"""
        if not self.current_price or not self.entry_price:
            return None

        if self.side == "LONG":
            return ((self.current_price - self.entry_price) / self.entry_price) * 100
        else:  # SHORT
            return ((self.entry_price - self.current_price) / self.entry_price) * 100

    def update_price(self, new_price: Decimal):
        """Update current price and track min/max"""
        self.current_price = new_price
        self.last_updated = datetime.utcnow()

        if self.side == "LONG":
            if self.max_price is None or new_price > self.max_price:
                self.max_price = new_price
        else:  # SHORT
            if self.min_price is None or new_price < self.min_price:
                self.min_price = new_price

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database/logging"""
        return {
            'id': self.id,
            'prediction_id': self.prediction_id,
            'symbol': self.symbol,
            'side': self.side,
            'entry_price': float(self.entry_price) if self.entry_price else None,
            'quantity': float(self.quantity) if self.quantity else None,
            'leverage': self.leverage,
            'status': self.status.value if isinstance(self.status, Enum) else self.status,
            'current_price': float(self.current_price) if self.current_price else None,
            'unrealized_pnl': float(self.unrealized_pnl) if self.unrealized_pnl else None,
            'unrealized_pnl_percent': float(self.unrealized_pnl_percent) if self.unrealized_pnl_percent else None,
            'stop_loss_price': float(self.stop_loss_price) if self.stop_loss_price else None,
            'take_profit_price': float(self.take_profit_price) if self.take_profit_price else None,
            'opened_at': self.opened_at.isoformat() if self.opened_at else None
        }


@dataclass
class Order:
    """Exchange order"""
    # Identity
    id: Optional[int] = None
    position_id: Optional[int] = None
    exchange: str = "binance"

    # Order identifiers
    exchange_order_id: Optional[str] = None
    client_order_id: Optional[str] = None

    # Order details
    order_type: OrderType = OrderType.MARKET
    order_purpose: OrderPurpose = OrderPurpose.ENTRY
    side: str = ""  # BUY or SELL

    # Pricing
    price: Optional[Decimal] = None
    quantity: Decimal = Decimal(0)
    executed_quantity: Optional[Decimal] = None
    avg_price: Optional[Decimal] = None

    # Status
    status: OrderStatus = OrderStatus.NEW
    time_in_force: str = "GTC"
    reduce_only: bool = False
    close_position: bool = False

    # Execution
    commission: Optional[Decimal] = None
    commission_asset: Optional[str] = None

    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    executed_at: Optional[datetime] = None
    updated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_filled(self) -> bool:
        """Check if order is filled"""
        return self.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]

    @property
    def fill_percent(self) -> Decimal:
        """Calculate fill percentage"""
        if not self.executed_quantity or not self.quantity:
            return Decimal(0)
        return (self.executed_quantity / self.quantity) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'position_id': self.position_id,
            'exchange_order_id': self.exchange_order_id,
            'order_type': self.order_type.value if isinstance(self.order_type, Enum) else self.order_type,
            'order_purpose': self.order_purpose.value if isinstance(self.order_purpose, Enum) else self.order_purpose,
            'side': self.side,
            'price': float(self.price) if self.price else None,
            'quantity': float(self.quantity) if self.quantity else None,
            'status': self.status.value if isinstance(self.status, Enum) else self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


@dataclass
class AuditLog:
    """Audit trail entry"""
    event_type: str
    event_category: str  # SIGNAL, POSITION, ORDER, RISK, SYSTEM
    event_data: Dict[str, Any]

    entity_id: Optional[int] = None
    entity_type: Optional[str] = None
    severity: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'event_type': self.event_type,
            'event_category': self.event_category,
            'entity_id': self.entity_id,
            'entity_type': self.entity_type,
            'event_data': self.event_data,
            'severity': self.severity,
            'created_at': self.created_at.isoformat()
        }


@dataclass
class PerformanceMetrics:
    """Daily performance metrics"""
    metric_date: datetime = field(default_factory=datetime.utcnow)
    exchange: str = "binance"

    # Signals
    total_signals: int = 0
    executed_signals: int = 0
    skipped_signals: int = 0
    error_signals: int = 0

    # Positions
    opened_positions: int = 0
    closed_positions: int = 0
    winning_positions: int = 0
    losing_positions: int = 0

    # Financial
    total_pnl: Decimal = Decimal(0)
    total_commission: Decimal = Decimal(0)
    net_pnl: Decimal = Decimal(0)

    # Statistics
    win_rate: Optional[Decimal] = None
    avg_win: Optional[Decimal] = None
    avg_loss: Optional[Decimal] = None
    profit_factor: Optional[Decimal] = None

    # Risk
    max_drawdown: Optional[Decimal] = None
    max_exposure: Optional[Decimal] = None
    sharpe_ratio: Optional[Decimal] = None

    def calculate_stats(self):
        """Calculate derived statistics"""
        if self.closed_positions > 0:
            self.win_rate = Decimal(self.winning_positions) / Decimal(self.closed_positions) * 100

        self.net_pnl = self.total_pnl - self.total_commission

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'metric_date': self.metric_date.isoformat(),
            'exchange': self.exchange,
            'total_signals': self.total_signals,
            'executed_signals': self.executed_signals,
            'opened_positions': self.opened_positions,
            'closed_positions': self.closed_positions,
            'win_rate': float(self.win_rate) if self.win_rate else None,
            'total_pnl': float(self.total_pnl),
            'net_pnl': float(self.net_pnl)
        }