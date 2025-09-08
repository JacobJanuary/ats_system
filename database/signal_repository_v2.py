"""
Signal Repository V2 - Using fas.scoring_history instead of smart_ml.predictions
"""
import logging
from typing import List, Optional
from decimal import Decimal
from datetime import datetime
import os

from database.models_v2 import TradingSignalV2, SignalStatus
from database.models import TradingSignal  # Legacy format

logger = logging.getLogger(__name__)


class SignalRepositoryV2:
    """Repository for trading signals from fas.scoring_history"""
    
    def __init__(self, db):
        self.db = db
        # Load score thresholds from environment
        self.min_score_week = float(os.getenv('MIN_SCORE_WEEK', '59'))
        self.min_score_month = float(os.getenv('MIN_SCORE_MONTH', '59'))
        self.skip_no_trade = os.getenv('SKIP_NO_TRADE', 'true').lower() == 'true'
        logger.info(f"SignalRepositoryV2 initialized with MIN_SCORE_WEEK={self.min_score_week}, MIN_SCORE_MONTH={self.min_score_month}")
    
    async def get_unprocessed_signals_v2(self, limit: int = 10) -> List[TradingSignalV2]:
        """
        Get unprocessed signals directly from fas.scoring_history
        No longer depends on smart_ml.predictions
        """
        query = """
            SELECT 
                s.id as signal_id,
                s.pair_symbol,
                s.trading_pair_id,
                s.total_score,
                s.pattern_score,
                s.combination_score,
                s.indicator_score,
                s.score_week,
                s.score_month,
                s.recommended_action,
                s.patterns_details,
                s.combinations_details,
                s.timestamp,
                s.created_at,
                tp.exchange_id,
                CASE 
                    WHEN tp.exchange_id = 1 THEN 'binance'
                    WHEN tp.exchange_id = 2 THEN 'bybit'
                    ELSE 'unknown'
                END as exchange_name
            FROM fas.scoring_history s
            JOIN public.trading_pairs tp ON tp.id = s.trading_pair_id
            WHERE s.id NOT IN (
                SELECT signal_id FROM ats.signals WHERE signal_id IS NOT NULL
            )
            AND s.created_at > NOW() - INTERVAL '30 minutes'
            AND tp.contract_type_id = 1  -- Only futures
            AND s.is_active = true
            -- Filter by minimum scores
            AND (
                ABS(s.score_week) >= $2 OR 
                ABS(s.score_month) >= $3
            )
            -- Skip NO_TRADE signals if configured
            AND ($4 = false OR s.recommended_action != 'NO_TRADE')
            ORDER BY s.created_at DESC
            LIMIT $1
        """
        
        rows = await self.db.fetch(
            query, 
            limit,
            self.min_score_week,
            self.min_score_month,
            self.skip_no_trade
        )
        
        signals = []
        for row in rows:
            signal = TradingSignalV2(
                signal_id=row['signal_id'],
                pair_symbol=row['pair_symbol'],
                trading_pair_id=row['trading_pair_id'],
                total_score=Decimal(str(row['total_score'])) if row['total_score'] else Decimal('0'),
                pattern_score=Decimal(str(row['pattern_score'])) if row['pattern_score'] else Decimal('0'),
                combination_score=Decimal(str(row['combination_score'])) if row['combination_score'] else Decimal('0'),
                indicator_score=Decimal(str(row['indicator_score'])) if row['indicator_score'] else Decimal('0'),
                score_week=Decimal(str(row['score_week'])) if row['score_week'] else None,
                score_month=Decimal(str(row['score_month'])) if row['score_month'] else None,
                recommended_action=row['recommended_action'],
                patterns_details=row['patterns_details'],
                combinations_details=row['combinations_details'],
                timestamp=row['timestamp'],
                created_at=row['created_at'],
                exchange_id=row['exchange_id'],
                exchange_name=row['exchange_name']
            )
            
            # Only add valid signals
            if signal.is_valid and signal.should_trade:
                signals.append(signal)
                logger.info(f"Found new signal: {signal.pair_symbol} - {signal.signal_type} "
                          f"(week: {signal.score_week}, month: {signal.score_month})")
            else:
                logger.debug(f"Skipping invalid signal: {signal.pair_symbol} - {signal.recommended_action}")
        
        return signals
    
    async def get_unprocessed_signals_legacy(self, limit: int = 10) -> List[TradingSignal]:
        """
        Get signals in legacy format for backward compatibility
        Converts V2 signals to legacy format
        """
        v2_signals = await self.get_unprocessed_signals_v2(limit)
        legacy_signals = [signal.to_legacy_format() for signal in v2_signals]
        return legacy_signals
    
    async def create_signal(self, signal: TradingSignalV2) -> int:
        """Create signal record in ats.signals"""
        query = """
            INSERT INTO ats.signals (
                signal_id,  -- Now storing fas.scoring_history.id
                exchange,
                symbol,
                signal_type,
                confidence_level,
                prediction_proba,
                received_at,
                status,
                total_score,
                score_week,
                score_month,
                recommended_action
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (signal_id) DO NOTHING
            RETURNING id
        """
        
        signal_id = await self.db.fetchval(
            query,
            signal.signal_id,
            signal.exchange_name or 'binance',
            signal.pair_symbol,
            signal.signal_type,
            signal.confidence_level,
            float(signal.prediction_proba) if signal.prediction_proba else 0.5,
            signal.created_at or datetime.utcnow(),
            signal.status.value,
            float(signal.total_score) if signal.total_score else 0,
            float(signal.score_week) if signal.score_week else None,
            float(signal.score_month) if signal.score_month else None,
            signal.recommended_action
        )
        
        return signal_id
    
    async def update_signal_status(
        self,
        signal_id: int,  # fas.scoring_history.id
        status: SignalStatus,
        error_message: Optional[str] = None,
        skip_reason: Optional[str] = None
    ) -> None:
        """Update signal processing status"""
        query = """
            UPDATE ats.signals
            SET 
                status = $2,
                processed_at = NOW(),
                error_message = $3,
                skip_reason = $4
            WHERE signal_id = $1
        """
        
        await self.db.execute(
            query,
            signal_id,
            status.value,
            error_message,
            skip_reason
        )
    
    async def check_duplicate_signal(self, symbol: str, seconds: int = 30) -> bool:
        """Check if similar signal was recently processed"""
        query = """
            SELECT EXISTS(
                SELECT 1 FROM ats.signals
                WHERE symbol = $1
                AND status IN ('PROCESSING', 'EXECUTED')
                AND received_at > NOW() - INTERVAL '%s seconds'
            )
        """ % seconds
        
        return await self.db.fetchval(query, symbol)
    
    async def migrate_add_columns(self) -> bool:
        """Add new columns to ats.signals table if they don't exist"""
        try:
            # Check if columns exist
            check_query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'ats' 
                AND table_name = 'signals'
                AND column_name IN ('total_score', 'score_week', 'score_month', 'recommended_action')
            """
            
            existing_columns = await self.db.fetch(check_query)
            existing_names = [row['column_name'] for row in existing_columns]
            
            # Add missing columns
            if 'total_score' not in existing_names:
                await self.db.execute("""
                    ALTER TABLE ats.signals 
                    ADD COLUMN IF NOT EXISTS total_score NUMERIC
                """)
                logger.info("Added total_score column to ats.signals")
            
            if 'score_week' not in existing_names:
                await self.db.execute("""
                    ALTER TABLE ats.signals 
                    ADD COLUMN IF NOT EXISTS score_week NUMERIC
                """)
                logger.info("Added score_week column to ats.signals")
            
            if 'score_month' not in existing_names:
                await self.db.execute("""
                    ALTER TABLE ats.signals 
                    ADD COLUMN IF NOT EXISTS score_month NUMERIC
                """)
                logger.info("Added score_month column to ats.signals")
            
            if 'recommended_action' not in existing_names:
                await self.db.execute("""
                    ALTER TABLE ats.signals 
                    ADD COLUMN IF NOT EXISTS recommended_action VARCHAR(50)
                """)
                logger.info("Added recommended_action column to ats.signals")
            
            logger.info("Database migration completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False