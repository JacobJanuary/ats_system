"""
ATS 2.0 - Database Connection Manager
Async PostgreSQL connection pool and repository pattern
"""
import asyncio
import logging
import json
from typing import Optional, List, Dict, Any, AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from decimal import Decimal

import asyncpg
from asyncpg import Pool, Connection, Record

from core.config import SystemConfig
from database.models import (
    TradingSignal, Position, Order, AuditLog, PerformanceMetrics,
    SignalStatus, PositionStatus, OrderStatus, OrderPurpose,
    CloseReason
)

logger = logging.getLogger(__name__)


class DatabasePool:
    """Manages PostgreSQL connection pool"""

    def __init__(self, config: SystemConfig):
        self.config = config.database
        self.pool: Optional[Pool] = None
        self._listen_connection: Optional[Connection] = None
        self._callbacks = {}

    async def initialize(self) -> None:
        """Initialize connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.name,
                user=self.config.user,
                password=self.config.password,
                min_size=2,
                max_size=10,
                command_timeout=60,
                max_queries=50000,
                max_inactive_connection_lifetime=300
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    async def close(self) -> None:
        """Close all connections"""
        if self._listen_connection:
            await self._listen_connection.close()
        if self.pool:
            await self.pool.close()
        logger.info("Database connections closed")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Connection]:
        """Acquire connection from pool"""
        async with self.pool.acquire() as connection:
            yield connection

    async def execute(self, query: str, *args) -> str:
        """Execute a query"""
        async with self.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args) -> List[Record]:
        """Fetch multiple rows"""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> Optional[Record]:
        """Fetch single row"""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        """Fetch single value"""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def listen_to_channel(self, channel: str, callback) -> None:
        """Listen to PostgreSQL NOTIFY channel"""
        if not self._listen_connection:
            self._listen_connection = await asyncpg.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.name,
                user=self.config.user,
                password=self.config.password
            )

        await self._listen_connection.add_listener(channel, callback)
        self._callbacks[channel] = callback
        logger.info(f"Listening to channel: {channel}")

    async def stop_listening(self, channel: str) -> None:
        """Stop listening to channel"""
        if self._listen_connection and channel in self._callbacks:
            await self._listen_connection.remove_listener(channel, self._callbacks[channel])
            del self._callbacks[channel]
            logger.info(f"Stopped listening to channel: {channel}")


class SignalRepository:
    """Repository for trading signals"""

    def __init__(self, db: DatabasePool):
        self.db = db

    async def get_unprocessed_signals(self, limit: int = 10) -> List[TradingSignal]:
        """Get unprocessed signals from smart_ml.predictions"""
        query = """
            SELECT 
                p.id as prediction_id,
                p.signal_id,
                p.prediction,
                p.prediction_proba,
                p.confidence_level,
                p.signal_type,
                p.created_at,
                s.pair_symbol,
                s.trading_pair_id,
                tp.exchange_id,
                CASE 
                    WHEN tp.exchange_id = 1 THEN 'binance'
                    WHEN tp.exchange_id = 2 THEN 'bybit'
                    ELSE 'unknown'
                END as exchange_name
            FROM smart_ml.predictions p
            JOIN fas.scoring_history s ON s.id = p.signal_id
            JOIN public.trading_pairs tp ON tp.id = s.trading_pair_id
            WHERE p.id NOT IN (
                SELECT prediction_id FROM ats.signals
            )
            AND p.prediction = true
            AND p.created_at > NOW() - INTERVAL '5 minutes'
            AND tp.contract_type_id = 1  -- Only futures
            ORDER BY p.created_at DESC
            LIMIT $1
        """

        rows = await self.db.fetch(query, limit)
        signals = []

        for row in rows:
            signal = TradingSignal(
                prediction_id=row['prediction_id'],
                signal_id=row['signal_id'],
                prediction=row['prediction'],
                prediction_proba=row['prediction_proba'],
                confidence_level=row['confidence_level'],
                signal_type=row['signal_type'],
                created_at=row['created_at'],
                pair_symbol=row['pair_symbol'],
                trading_pair_id=row['trading_pair_id'],
                exchange_id=row['exchange_id'],
                exchange_name=row['exchange_name']
            )
            signals.append(signal)

        return signals

    async def create_signal(self, signal: TradingSignal) -> int:
        """Create signal record in ats.signals"""
        query = """
            INSERT INTO ats.signals (
                prediction_id, signal_id, exchange, symbol,
                signal_type, confidence_level, prediction_proba,
                received_at, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (prediction_id) DO NOTHING
            RETURNING id
        """

        # Determine exchange name for storage
        exchange_name = signal.exchange_name or 'binance'

        signal_id = await self.db.fetchval(
            query,
            signal.prediction_id,
            signal.signal_id,
            exchange_name,
            signal.pair_symbol,
            signal.signal_type,
            signal.confidence_level,
            signal.prediction_proba,
            signal.created_at,
            signal.status.value
        )

        return signal_id

    async def update_signal_status(
        self,
        prediction_id: int,
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
            WHERE prediction_id = $1
        """

        await self.db.execute(
            query,
            prediction_id,
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


class PositionRepository:
    """Repository for positions"""

    def __init__(self, db: DatabasePool):
        self.db = db

    async def create_position(self, position: Position) -> int:
        """Create new position"""
        query = """
            INSERT INTO ats.positions (
                signal_id, prediction_id, exchange, symbol, side,
                entry_price, quantity, leverage, status,
                stop_loss_price, stop_loss_type, stop_loss_order_id,
                take_profit_price, take_profit_order_id,
                opened_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, 
                $10, $11, $12, $13, $14, $15
            )
            RETURNING id
        """

        position_id = await self.db.fetchval(
            query,
            position.signal_id,
            position.prediction_id,
            position.exchange,
            position.symbol,
            position.side,
            position.entry_price,
            position.quantity,
            position.leverage,
            position.status.value,
            position.stop_loss_price,
            position.stop_loss_type,
            position.stop_loss_order_id,
            position.take_profit_price,
            position.take_profit_order_id,
            position.opened_at
        )

        position.id = position_id
        return position_id

    async def get_position(self, position_id: int) -> Optional[Position]:
        """Get position by ID"""
        query = """
            SELECT * FROM ats.positions WHERE id = $1
        """

        row = await self.db.fetchrow(query, position_id)
        if not row:
            return None

        return self._row_to_position(row)

    async def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        query = """
            SELECT * FROM ats.positions 
            WHERE status = 'OPEN'
            ORDER BY opened_at DESC
        """

        rows = await self.db.fetch(query)
        return [self._row_to_position(row) for row in rows]

    async def get_position_by_symbol(self, symbol: str) -> Optional[Position]:
        """Get open position for symbol"""
        query = """
            SELECT * FROM ats.positions 
            WHERE symbol = $1 AND status = 'OPEN'
            ORDER BY opened_at DESC
            LIMIT 1
        """

        row = await self.db.fetchrow(query, symbol)
        if not row:
            return None

        return self._row_to_position(row)

    async def update_position_price(self, position_id: int, price: Decimal) -> None:
        """Update current price and max/min"""
        # Get current position
        position = await self.get_position(position_id)
        if not position:
            return

        position.update_price(price)

        query = """
            UPDATE ats.positions
            SET 
                current_price = $2,
                max_price = $3,
                min_price = $4,
                last_updated = NOW()
            WHERE id = $1
        """

        await self.db.execute(
            query,
            position_id,
            price,
            position.max_price,
            position.min_price
        )

    async def close_position(
        self,
        position_id: int,
        exit_price: Decimal,
        realized_pnl: Decimal,
        close_reason: CloseReason,
        commission: Decimal = Decimal(0)
    ) -> None:
        """Close position"""
        query = """
            UPDATE ats.positions
            SET 
                status = 'CLOSED',
                exit_price = $2,
                realized_pnl = $3,
                realized_pnl_percent = $4,
                commission_paid = $5,
                close_reason = $6,
                closed_at = NOW()
            WHERE id = $1
        """

        # Calculate PNL percentage
        position = await self.get_position(position_id)
        if position:
            if position.side == "LONG":
                pnl_percent = ((exit_price - position.entry_price) / position.entry_price) * 100
            else:
                pnl_percent = ((position.entry_price - exit_price) / position.entry_price) * 100
        else:
            pnl_percent = Decimal(0)

        await self.db.execute(
            query,
            position_id,
            exit_price,
            realized_pnl,
            pnl_percent,
            commission,
            close_reason.value
        )

    async def count_open_positions(self) -> int:
        """Count open positions"""
        query = "SELECT COUNT(*) FROM ats.positions WHERE status = 'OPEN'"
        return await self.db.fetchval(query)

    async def check_position_exists(self, symbol: str) -> bool:
        """Check if open position exists for symbol"""
        query = """
            SELECT EXISTS(
                SELECT 1 FROM ats.positions 
                WHERE symbol = $1 AND status = 'OPEN'
            )
        """
        return await self.db.fetchval(query, symbol)

    def _row_to_position(self, row: Record) -> Position:
        """Convert database row to Position object"""
        position = Position(
            id=row['id'],
            signal_id=row['signal_id'],
            prediction_id=row['prediction_id'],
            exchange=row['exchange'],
            symbol=row['symbol'],
            side=row['side'],
            entry_price=row['entry_price'],
            quantity=row['quantity'],
            leverage=row['leverage'],
            status=PositionStatus(row['status']),
            current_price=row.get('current_price'),
            stop_loss_price=row.get('stop_loss_price'),
            stop_loss_type=row.get('stop_loss_type'),
            stop_loss_order_id=row.get('stop_loss_order_id'),
            take_profit_price=row.get('take_profit_price'),
            take_profit_order_id=row.get('take_profit_order_id'),
            max_price=row.get('max_price'),
            min_price=row.get('min_price'),
            exit_price=row.get('exit_price'),
            realized_pnl=row.get('realized_pnl'),
            realized_pnl_percent=row.get('realized_pnl_percent'),
            commission_paid=row.get('commission_paid'),
            opened_at=row['opened_at'],
            closed_at=row.get('closed_at'),
            last_updated=row.get('last_updated', row['opened_at'])
        )

        if row.get('close_reason'):
            position.close_reason = CloseReason(row['close_reason'])

        return position

    async def update_position(self, position: Position) -> bool:
        """Update position in database"""
        try:
            query = """
                UPDATE ats.positions 
                SET 
                    status = $2,
                    stop_loss_type = $3,
                    stop_loss_order_id = $4,
                    stop_loss_price = $5,
                    take_profit_order_id = $6,
                    take_profit_price = $7,
                    exit_price = $8,
                    closed_at = $9,
                    realized_pnl = $10,
                    last_updated = NOW()
                WHERE id = $1
            """

            await self.db.execute(
                query,
                position.id,
                position.status,
                position.stop_loss_type,
                position.stop_loss_order_id,
                position.stop_loss_price,
                position.take_profit_order_id,
                position.take_profit_price,
                position.exit_price,
                position.closed_at,
                position.realized_pnl
            )

            return True

        except Exception as e:
            logger.error(f"Error updating position: {e}")
            return False


class OrderRepository:
    """Repository for orders"""

    def __init__(self, db: DatabasePool):
        self.db = db

    async def create_order(self, order: Order) -> int:
        """Create new order"""
        query = """
            INSERT INTO ats.orders (
                position_id, exchange, exchange_order_id, client_order_id,
                order_type, order_purpose, side, price, quantity,
                status, time_in_force, reduce_only, close_position
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            RETURNING id
        """

        order_id = await self.db.fetchval(
            query,
            order.position_id,
            order.exchange,
            order.exchange_order_id,
            order.client_order_id,
            order.order_type.value,
            order.order_purpose.value,
            order.side,
            order.price,
            order.quantity,
            order.status.value,
            order.time_in_force,
            order.reduce_only,
            order.close_position
        )

        order.id = order_id
        return order_id

    async def update_order_status(
        self,
        order_id: int,
        status: OrderStatus,
        executed_quantity: Optional[Decimal] = None,
        avg_price: Optional[Decimal] = None,
        commission: Optional[Decimal] = None
    ) -> None:
        """Update order status"""
        query = """
            UPDATE ats.orders
            SET 
                status = $2,
                executed_quantity = COALESCE($3, executed_quantity),
                avg_price = COALESCE($4, avg_price),
                commission = COALESCE($5, commission),
                executed_at = CASE WHEN $2 = 'FILLED' THEN NOW() ELSE executed_at END,
                updated_at = NOW()
            WHERE id = $1
        """

        await self.db.execute(
            query,
            order_id,
            status.value,
            executed_quantity,
            avg_price,
            commission
        )

    async def get_position_orders(self, position_id: int) -> List[Order]:
        """Get all orders for position"""
        query = """
            SELECT * FROM ats.orders 
            WHERE position_id = $1
            ORDER BY created_at DESC
        """

        rows = await self.db.fetch(query, position_id)
        # TODO: Convert rows to Order objects
        return []


class AuditRepository:
    """Repository for audit logs"""

    def __init__(self, db: DatabasePool):
        self.db = db

    async def log(self, audit: AuditLog) -> None:
        """Create audit log entry"""
        query = """
            INSERT INTO ats.audit_log (
                event_type, event_category, entity_id, entity_type,
                event_data, severity
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """

        # Convert event_data dict to JSON string for PostgreSQL
        event_data_json = json.dumps(audit.event_data) if audit.event_data else '{}'

        await self.db.execute(
            query,
            audit.event_type,
            audit.event_category,
            audit.entity_id,
            audit.entity_type,
            event_data_json,
            audit.severity
        )

    async def log_event(
        self,
        event_type: str,
        event_category: str,
        event_data: Dict[str, Any],
        entity_id: Optional[int] = None,
        entity_type: Optional[str] = None,
        severity: str = "INFO"
    ) -> None:
        """Quick method to log event"""
        query = """
            INSERT INTO ats.audit_log (
                event_type, event_category, entity_id, entity_type,
                event_data, severity
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """

        # Convert event_data dict to JSON string for PostgreSQL
        event_data_json = json.dumps(event_data) if event_data else '{}'

        await self.db.execute(
            query,
            event_type,
            event_category,
            entity_id,
            entity_type,
            event_data_json,
            severity
        )


class MetricsRepository:
    """Repository for performance metrics"""

    def __init__(self, db: DatabasePool):
        self.db = db

    async def update_daily_metrics(self) -> None:
        """Update or create today's metrics"""
        query = """
            INSERT INTO ats.performance_metrics (metric_date, exchange)
            VALUES (CURRENT_DATE, 'binance')
            ON CONFLICT (metric_date) DO NOTHING;
            
            WITH daily_stats AS (
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'EXECUTED') as executed,
                    COUNT(*) FILTER (WHERE status = 'SKIPPED') as skipped,
                    COUNT(*) FILTER (WHERE status = 'ERROR') as error_count
                FROM ats.signals
                WHERE DATE(received_at) = CURRENT_DATE
            ),
            position_stats AS (
                SELECT 
                    COUNT(*) FILTER (WHERE DATE(opened_at) = CURRENT_DATE) as opened,
                    COUNT(*) FILTER (WHERE DATE(closed_at) = CURRENT_DATE) as closed,
                    COUNT(*) FILTER (WHERE DATE(closed_at) = CURRENT_DATE AND realized_pnl > 0) as winners,
                    COUNT(*) FILTER (WHERE DATE(closed_at) = CURRENT_DATE AND realized_pnl < 0) as losers,
                    COALESCE(SUM(realized_pnl) FILTER (WHERE DATE(closed_at) = CURRENT_DATE), 0) as total_pnl,
                    COALESCE(SUM(commission_paid) FILTER (WHERE DATE(closed_at) = CURRENT_DATE), 0) as total_commission
                FROM ats.positions
            )
            UPDATE ats.performance_metrics
            SET 
                executed_signals = daily_stats.executed,
                skipped_signals = daily_stats.skipped,
                error_signals = daily_stats.error_count,
                opened_positions = position_stats.opened,
                closed_positions = position_stats.closed,
                winning_positions = position_stats.winners,
                losing_positions = position_stats.losers,
                total_pnl = position_stats.total_pnl,
                total_commission = position_stats.total_commission,
                net_pnl = position_stats.total_pnl - position_stats.total_commission,
                win_rate = CASE 
                    WHEN position_stats.closed > 0 
                    THEN (position_stats.winners::DECIMAL / position_stats.closed) * 100
                    ELSE NULL
                END,
                updated_at = NOW()
            FROM daily_stats, position_stats
            WHERE metric_date = CURRENT_DATE;
        """

        await self.db.execute(query)

    async def get_today_metrics(self) -> Optional[Dict[str, Any]]:
        """Get today's metrics"""
        query = """
            SELECT * FROM ats.performance_metrics
            WHERE metric_date = CURRENT_DATE
        """

        row = await self.db.fetchrow(query)
        if not row:
            return None

        return dict(row)


class DatabaseManager:
    """Main database manager combining all repositories"""

    def __init__(self, config: SystemConfig):
        self.config = config
        self.pool = DatabasePool(config)

        # Initialize repositories
        self.signals = SignalRepository(self.pool)
        self.positions = PositionRepository(self.pool)
        self.orders = OrderRepository(self.pool)
        self.audit = AuditRepository(self.pool)
        self.metrics = MetricsRepository(self.pool)

    async def initialize(self) -> None:
        """Initialize database connection"""
        await self.pool.initialize()
        logger.info("Database manager initialized")

    async def close(self) -> None:
        """Close database connections"""
        await self.pool.close()

    async def listen_for_predictions(self, callback) -> None:
        """Listen for new predictions via NOTIFY"""
        await self.pool.listen_to_channel('smart_predictions', callback)

    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            result = await self.pool.fetchval("SELECT 1")
            return result == 1
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False