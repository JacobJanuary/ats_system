"""
ATS 2.0 - Updated Database Models for fas.scoring_history
New signal structure using scoring_history instead of predictions
"""
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any, List
from enum import Enum
import json


class SignalStatus(str, Enum):
    """Signal processing status"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    EXECUTED = "EXECUTED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


@dataclass
class TradingSignalV2:
    """
    Trading signal from fas.scoring_history (NEW structure)
    No longer depends on smart_ml.predictions
    """
    # Primary identification
    signal_id: int  # fas.scoring_history.id
    
    # From fas.scoring_history
    pair_symbol: str
    trading_pair_id: int
    total_score: Decimal
    pattern_score: Decimal
    combination_score: Decimal
    indicator_score: Decimal
    score_week: Optional[Decimal] = None
    score_month: Optional[Decimal] = None
    recommended_action: Optional[str] = None  # BUY/SELL/NO_TRADE
    patterns_details: Optional[Dict] = None
    combinations_details: Optional[Dict] = None
    timestamp: datetime = None
    created_at: datetime = None
    
    # From trading_pairs join
    exchange_id: Optional[int] = None  # 1=Binance, 2=Bybit
    exchange_name: Optional[str] = None
    
    # Derived fields for compatibility
    signal_type: Optional[str] = None  # BUY/SELL derived from scores
    prediction_proba: Optional[Decimal] = None  # Normalized score
    confidence_level: Optional[str] = None  # HIGH/MEDIUM/LOW
    
    # Processing fields
    id: Optional[int] = None  # ats.signals.id
    status: SignalStatus = SignalStatus.PENDING
    processed_at: Optional[datetime] = None
    skip_reason: Optional[str] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        """Derive fields after initialization"""
        # Derive signal_type from recommended_action or scores
        if self.recommended_action and self.recommended_action != 'NO_TRADE':
            self.signal_type = self.recommended_action
        elif self.total_score is not None:
            # Use total_score to determine signal
            if self.total_score > 0:
                self.signal_type = "BUY"
            elif self.total_score < 0:
                self.signal_type = "SELL"
            else:
                self.signal_type = "NEUTRAL"
        
        # Calculate prediction probability (normalize score)
        if self.total_score is not None:
            # Convert score to probability (0-1 range)
            # Assuming scores range from -100 to +100
            normalized = (float(self.total_score) + 100) / 200
            self.prediction_proba = Decimal(str(max(0, min(1, normalized))))
        
        # Determine confidence level based on weekly/monthly scores
        if self.score_week and self.score_month:
            avg_score = (abs(self.score_week) + abs(self.score_month)) / 2
            if avg_score > 80:
                self.confidence_level = "HIGH"
            elif avg_score > 60:
                self.confidence_level = "MEDIUM"
            else:
                self.confidence_level = "LOW"
        elif self.total_score:
            # Fallback to total_score
            abs_score = abs(float(self.total_score))
            if abs_score > 50:
                self.confidence_level = "HIGH"
            elif abs_score > 30:
                self.confidence_level = "MEDIUM"
            else:
                self.confidence_level = "LOW"
    
    @property
    def side(self) -> str:
        """Convert signal_type to position side"""
        if self.signal_type == "SELL":
            return "SHORT"
        elif self.signal_type == "BUY":
            return "LONG"
        return "UNKNOWN"
    
    @property
    def is_valid(self) -> bool:
        """Check if signal is valid for processing"""
        return (
            self.signal_type in ["BUY", "SELL"] and
            self.pair_symbol and
            self.recommended_action != "NO_TRADE"
        )
    
    @property
    def should_trade(self) -> bool:
        """Determine if signal should be traded based on scores"""
        # Check minimum score thresholds
        min_weekly = 59  # From .env MIN_SCORE_WEEK
        min_monthly = 59  # From .env MIN_SCORE_MONTH
        
        if self.score_week and self.score_month:
            return (
                abs(float(self.score_week)) >= min_weekly or
                abs(float(self.score_month)) >= min_monthly
            )
        
        # Fallback to total_score
        return abs(float(self.total_score)) > 30
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database/logging"""
        return {
            'signal_id': self.signal_id,
            'pair_symbol': self.pair_symbol,
            'trading_pair_id': self.trading_pair_id,
            'signal_type': self.signal_type,
            'side': self.side,
            'total_score': float(self.total_score) if self.total_score else None,
            'pattern_score': float(self.pattern_score) if self.pattern_score else None,
            'indicator_score': float(self.indicator_score) if self.indicator_score else None,
            'score_week': float(self.score_week) if self.score_week else None,
            'score_month': float(self.score_month) if self.score_month else None,
            'confidence_level': self.confidence_level,
            'prediction_proba': float(self.prediction_proba) if self.prediction_proba else None,
            'recommended_action': self.recommended_action,
            'exchange_id': self.exchange_id,
            'exchange_name': self.exchange_name,
            'status': self.status.value if isinstance(self.status, Enum) else self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'processed_at': self.processed_at.isoformat() if self.processed_at else None
        }
    
    def to_legacy_format(self) -> 'TradingSignal':
        """Convert to legacy TradingSignal format for compatibility"""
        from database.models import TradingSignal
        
        return TradingSignal(
            prediction_id=self.signal_id,  # Use signal_id as prediction_id
            signal_id=self.signal_id,
            prediction=self.signal_type in ["BUY", "SELL"],
            prediction_proba=self.prediction_proba or Decimal('0.5'),
            confidence_level=self.confidence_level,
            signal_type=self.signal_type,
            created_at=self.created_at or datetime.utcnow(),
            pair_symbol=self.pair_symbol,
            trading_pair_id=self.trading_pair_id,
            exchange_id=self.exchange_id,
            exchange_name=self.exchange_name,
            status=self.status,
            processed_at=self.processed_at,
            skip_reason=self.skip_reason,
            error_message=self.error_message
        )