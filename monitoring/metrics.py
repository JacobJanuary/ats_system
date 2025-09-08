"""
ATS 2.0 - Metrics and Monitoring
Prometheus metrics and system monitoring
"""
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from decimal import Decimal
from functools import wraps

from prometheus_client import (
    Counter, Histogram, Gauge, Summary,
    generate_latest, CONTENT_TYPE_LATEST,
    CollectorRegistry
)

logger = logging.getLogger(__name__)

# Create custom registry
registry = CollectorRegistry()

# ============= SIGNALS METRICS =============
signals_received = Counter(
    'ats_signals_received_total',
    'Total number of signals received',
    ['exchange', 'symbol', 'signal_type'],
    registry=registry
)

signals_processed = Counter(
    'ats_signals_processed_total',
    'Total number of signals processed',
    ['exchange', 'symbol', 'status'],
    registry=registry
)

signal_processing_time = Histogram(
    'ats_signal_processing_seconds',
    'Signal processing time in seconds',
    ['exchange'],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=registry
)

# ============= ORDERS METRICS =============
orders_placed = Counter(
    'ats_orders_placed_total',
    'Total number of orders placed',
    ['exchange', 'side', 'order_type'],
    registry=registry
)

orders_failed = Counter(
    'ats_orders_failed_total',
    'Total number of failed orders',
    ['exchange', 'side', 'reason'],
    registry=registry
)

order_execution_time = Histogram(
    'ats_order_execution_seconds',
    'Order execution time in seconds',
    ['exchange', 'order_type'],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    registry=registry
)

# ============= POSITIONS METRICS =============
open_positions = Gauge(
    'ats_open_positions',
    'Number of open positions',
    ['exchange', 'symbol'],
    registry=registry
)

position_pnl = Gauge(
    'ats_position_pnl_usd',
    'Current PNL in USD',
    ['exchange', 'symbol', 'side'],
    registry=registry
)

position_duration = Summary(
    'ats_position_duration_seconds',
    'Position duration in seconds',
    ['exchange', 'side', 'close_reason'],
    registry=registry
)

total_pnl = Gauge(
    'ats_total_pnl_usd',
    'Total PNL across all positions',
    ['exchange'],
    registry=registry
)

# ============= API METRICS =============
api_requests = Counter(
    'ats_api_requests_total',
    'Total API requests',
    ['exchange', 'endpoint', 'method'],
    registry=registry
)

api_errors = Counter(
    'ats_api_errors_total',
    'Total API errors',
    ['exchange', 'endpoint', 'error_type'],
    registry=registry
)

api_latency = Histogram(
    'ats_api_latency_seconds',
    'API call latency',
    ['exchange', 'endpoint'],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
    registry=registry
)

rate_limit_hits = Counter(
    'ats_rate_limit_hits_total',
    'Number of rate limit hits',
    ['exchange'],
    registry=registry
)

# ============= SYSTEM METRICS =============
system_uptime = Gauge(
    'ats_system_uptime_seconds',
    'System uptime in seconds',
    registry=registry
)

database_connections = Gauge(
    'ats_database_connections',
    'Number of database connections',
    ['state'],  # active, idle, waiting
    registry=registry
)

websocket_connections = Gauge(
    'ats_websocket_connections',
    'Number of WebSocket connections',
    ['exchange', 'state'],  # connected, disconnected, reconnecting
    registry=registry
)

system_errors = Counter(
    'ats_system_errors_total',
    'Total system errors',
    ['component', 'severity'],
    registry=registry
)

# ============= RISK METRICS =============
risk_violations = Counter(
    'ats_risk_violations_total',
    'Risk management violations',
    ['type'],  # max_position, daily_loss, leverage, etc.
    registry=registry
)

daily_trades = Gauge(
    'ats_daily_trades',
    'Number of trades today',
    registry=registry
)

daily_loss = Gauge(
    'ats_daily_loss_usd',
    'Daily loss in USD',
    registry=registry
)

# ============= HELPER CLASSES =============
class MetricsCollector:
    """Collects and manages metrics"""
    
    def __init__(self):
        self.start_time = time.time()
    
    def track_signal(self, exchange: str, symbol: str, signal_type: str, status: str):
        """Track signal metrics"""
        signals_received.labels(exchange, symbol, signal_type).inc()
        if status in ['executed', 'skipped', 'error']:
            signals_processed.labels(exchange, symbol, status).inc()
    
    def track_order(
        self,
        exchange: str,
        side: str,
        order_type: str,
        success: bool,
        execution_time: float,
        error_reason: Optional[str] = None
    ):
        """Track order metrics"""
        if success:
            orders_placed.labels(exchange, side, order_type).inc()
        else:
            orders_failed.labels(exchange, side, error_reason or 'unknown').inc()
        
        order_execution_time.labels(exchange, order_type).observe(execution_time)
    
    def track_position(
        self,
        exchange: str,
        symbol: str,
        side: str,
        pnl: Decimal,
        is_open: bool = True
    ):
        """Track position metrics"""
        if is_open:
            open_positions.labels(exchange, symbol).inc()
            position_pnl.labels(exchange, symbol, side).set(float(pnl))
        else:
            open_positions.labels(exchange, symbol).dec()
    
    def track_api_call(
        self,
        exchange: str,
        endpoint: str,
        method: str,
        latency: float,
        success: bool,
        error_type: Optional[str] = None
    ):
        """Track API call metrics"""
        api_requests.labels(exchange, endpoint, method).inc()
        api_latency.labels(exchange, endpoint).observe(latency)
        
        if not success:
            api_errors.labels(exchange, endpoint, error_type or 'unknown').inc()
        
        if error_type == 'rate_limit':
            rate_limit_hits.labels(exchange).inc()
    
    def update_system_metrics(
        self,
        db_connections: Optional[Dict[str, int]] = None,
        ws_connections: Optional[Dict[str, Dict[str, int]]] = None
    ):
        """Update system metrics"""
        # Uptime
        system_uptime.set(time.time() - self.start_time)
        
        # Database connections
        if db_connections:
            for state, count in db_connections.items():
                database_connections.labels(state).set(count)
        
        # WebSocket connections
        if ws_connections:
            for exchange, states in ws_connections.items():
                for state, count in states.items():
                    websocket_connections.labels(exchange, state).set(count)
    
    def track_risk_violation(self, violation_type: str):
        """Track risk management violations"""
        risk_violations.labels(violation_type).inc()
    
    def update_daily_metrics(self, trades: int, loss: Decimal):
        """Update daily trading metrics"""
        daily_trades.set(trades)
        daily_loss.set(float(loss))
    
    def get_metrics(self) -> bytes:
        """Get metrics in Prometheus format"""
        return generate_latest(registry)


# ============= DECORATORS =============
def track_execution_time(metric: Histogram):
    """Decorator to track function execution time"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start
                # Extract labels from function arguments if needed
                labels = []
                if hasattr(args[0], 'exchange_name'):
                    labels.append(args[0].exchange_name)
                metric.labels(*labels).observe(duration) if labels else metric.observe(duration)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start
                metric.observe(duration)
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def track_api_call(exchange_name: str, endpoint: str):
    """Decorator to track API calls"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            success = False
            error_type = None
            
            try:
                result = await func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                error_type = type(e).__name__
                if 'rate' in str(e).lower():
                    error_type = 'rate_limit'
                raise
            finally:
                latency = time.time() - start
                metrics_collector.track_api_call(
                    exchange_name,
                    endpoint,
                    'GET',  # Could be extracted from function
                    latency,
                    success,
                    error_type
                )
        
        return wrapper
    return decorator


# Global metrics collector instance
metrics_collector = MetricsCollector()