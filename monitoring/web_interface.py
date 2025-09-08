"""
ATS 2.0 - Web Interface for Monitoring
FastAPI-based web interface for real-time position monitoring
"""

from fastapi import FastAPI, WebSocket, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
import asyncpg
import json
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv(Path(__file__).parent.parent / '.env')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="ATS 2.0 Position Monitor", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection pool
db_pool: Optional[asyncpg.Pool] = None


async def get_db_config():
    """Get database configuration from environment"""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'fox_crypto'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }


@app.on_event("startup")
async def startup():
    """Initialize database connection pool on startup"""
    global db_pool
    config = await get_db_config()
    db_pool = await asyncpg.create_pool(
        **config,
        min_size=5,
        max_size=10
    )
    logger.info("Database connection pool initialized")


@app.on_event("shutdown")
async def shutdown():
    """Close database connection pool on shutdown"""
    if db_pool:
        await db_pool.close()
    logger.info("Database connection pool closed")


def decimal_to_float(obj):
    """Convert Decimal to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


@app.get("/api/positions")
async def get_positions(
    exchange: Optional[str] = Query(None),
    status: Optional[str] = Query("OPEN"),
    symbol: Optional[str] = Query(None)
):
    """Get list of positions with filtering"""
    
    query = """
        SELECT 
            id, exchange, symbol, position_id, side,
            entry_price, current_price, quantity,
            unrealized_pnl, realized_pnl,
            has_stop_loss, stop_loss_price,
            has_take_profit, take_profit_price,
            has_trailing_stop, trailing_stop_distance,
            trailing_stop_activated,
            status, opened_at, last_update,
            leverage, margin
        FROM monitoring.positions
        WHERE 1=1
    """
    
    params = []
    param_count = 0
    
    if exchange:
        param_count += 1
        query += f" AND exchange = ${param_count}"
        params.append(exchange)
    
    if status:
        param_count += 1
        query += f" AND status = ${param_count}"
        params.append(status)
    
    if symbol:
        param_count += 1
        query += f" AND symbol ILIKE ${param_count}"
        params.append(f"%{symbol}%")
    
    query += " ORDER BY last_update DESC"
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    positions = []
    for row in rows:
        position = dict(row)
        
        # Convert Decimal to float
        for key in ['entry_price', 'current_price', 'quantity', 
                   'unrealized_pnl', 'realized_pnl', 'stop_loss_price', 
                   'take_profit_price', 'trailing_stop_distance', 'margin']:
            if position.get(key) is not None:
                position[key] = decimal_to_float(position[key])
        
        # Convert datetime to string
        for key in ['opened_at', 'last_update']:
            if position.get(key):
                position[key] = position[key].isoformat()
        
        # Calculate protection score
        protection_score = 0
        if position['has_stop_loss']:
            protection_score += 33
        if position['has_take_profit']:
            protection_score += 33
        if position['has_trailing_stop']:
            protection_score += 34
        position['protection_score'] = protection_score
        
        # Calculate price change percent
        if position['entry_price'] and position['current_price']:
            price_change = ((position['current_price'] - position['entry_price']) / position['entry_price']) * 100
            position['price_change_percent'] = round(price_change, 2)
        else:
            position['price_change_percent'] = 0
        
        positions.append(position)
    
    return {"positions": positions, "total": len(positions)}


@app.get("/api/balances")
async def get_balances(exchange: Optional[str] = Query(None)):
    """Get account balances"""
    
    query = """
        SELECT 
            exchange, asset, free, locked, total, in_usd, last_update
        FROM monitoring.balances
        WHERE 1=1
    """
    
    params = []
    if exchange:
        query += " AND exchange = $1"
        params.append(exchange)
    
    query += " ORDER BY exchange, total DESC"
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    balances = []
    for row in rows:
        balance = dict(row)
        for key in ['free', 'locked', 'total', 'in_usd']:
            if balance.get(key) is not None:
                balance[key] = decimal_to_float(balance[key])
        if balance.get('last_update'):
            balance['last_update'] = balance['last_update'].isoformat()
        balances.append(balance)
    
    # Group by exchange
    grouped = {}
    for balance in balances:
        exchange = balance['exchange']
        if exchange not in grouped:
            grouped[exchange] = {
                'balances': [],
                'total_usd': 0
            }
        grouped[exchange]['balances'].append(balance)
        if balance.get('in_usd'):
            grouped[exchange]['total_usd'] += balance['in_usd']
    
    return grouped


@app.get("/api/stats")
async def get_stats():
    """Get overall statistics"""
    
    query = """
        SELECT 
            COUNT(*) FILTER (WHERE status = 'OPEN') as open_positions,
            COUNT(*) FILTER (WHERE status = 'CLOSED' AND DATE(closed_at) = CURRENT_DATE) as closed_today,
            SUM(unrealized_pnl) FILTER (WHERE status = 'OPEN') as total_unrealized_pnl,
            SUM(realized_pnl) FILTER (WHERE status = 'CLOSED' AND DATE(closed_at) = CURRENT_DATE) as today_realized_pnl,
            SUM(realized_pnl) FILTER (WHERE status = 'CLOSED' AND closed_at >= CURRENT_DATE - INTERVAL '7 days') as week_realized_pnl,
            SUM(realized_pnl) FILTER (WHERE status = 'CLOSED' AND closed_at >= CURRENT_DATE - INTERVAL '30 days') as month_realized_pnl,
            COUNT(*) FILTER (WHERE status = 'OPEN' AND has_stop_loss) as protected_with_sl,
            COUNT(*) FILTER (WHERE status = 'OPEN' AND has_trailing_stop) as protected_with_trailing,
            COUNT(DISTINCT symbol) FILTER (WHERE status = 'OPEN') as unique_symbols,
            AVG(leverage) FILTER (WHERE status = 'OPEN') as avg_leverage
        FROM monitoring.positions
    """
    
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(query)
    
    stats = dict(row)
    for key in ['total_unrealized_pnl', 'today_realized_pnl', 'week_realized_pnl', 
                'month_realized_pnl', 'avg_leverage']:
        if stats.get(key) is not None:
            stats[key] = decimal_to_float(stats[key])
    
    # Get win rate for today
    win_rate_query = """
        SELECT 
            COUNT(*) FILTER (WHERE realized_pnl > 0) as wins,
            COUNT(*) FILTER (WHERE realized_pnl < 0) as losses,
            COUNT(*) as total
        FROM monitoring.positions
        WHERE status = 'CLOSED' AND DATE(closed_at) = CURRENT_DATE
    """
    
    async with db_pool.acquire() as conn:
        win_stats = await conn.fetchrow(win_rate_query)
    
    if win_stats['total'] > 0:
        stats['today_win_rate'] = round((win_stats['wins'] / win_stats['total']) * 100, 1)
    else:
        stats['today_win_rate'] = 0
    
    stats['today_wins'] = win_stats['wins']
    stats['today_losses'] = win_stats['losses']
    
    return stats


@app.get("/api/health")
async def get_health():
    """Get system health status"""
    
    query = """
        SELECT DISTINCT ON (component) 
            component, status, last_heartbeat, error_message
        FROM monitoring.system_health
        WHERE created_at > NOW() - INTERVAL '5 minutes'
        ORDER BY component, created_at DESC
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    health = {}
    for row in rows:
        health[row['component']] = {
            'status': row['status'],
            'last_heartbeat': row['last_heartbeat'].isoformat() if row['last_heartbeat'] else None,
            'error_message': row['error_message']
        }
    
    # Overall status
    all_healthy = all(h['status'] == 'healthy' for h in health.values())
    
    return {
        'overall_status': 'healthy' if all_healthy else 'degraded',
        'components': health,
        'timestamp': datetime.now().isoformat()
    }


@app.get("/api/daily-pnl")
async def get_daily_pnl(days: int = Query(30, ge=1, le=365)):
    """Get daily PnL history"""
    
    query = """
        SELECT 
            DATE(closed_at) as date,
            exchange,
            COUNT(*) as trades_count,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
            SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
            SUM(realized_pnl) as total_pnl,
            AVG(realized_pnl) as avg_pnl,
            MAX(realized_pnl) as best_trade,
            MIN(realized_pnl) as worst_trade
        FROM monitoring.positions
        WHERE status = 'CLOSED' 
        AND closed_at >= CURRENT_DATE - INTERVAL '%s days'
        AND closed_at IS NOT NULL
        GROUP BY DATE(closed_at), exchange
        ORDER BY date DESC
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, days)
    
    daily_pnl = []
    for row in rows:
        data = dict(row)
        data['date'] = data['date'].isoformat()
        for key in ['total_pnl', 'avg_pnl', 'best_trade', 'worst_trade']:
            if data.get(key) is not None:
                data[key] = decimal_to_float(data[key])
        
        # Calculate win rate
        total = data['winning_trades'] + data['losing_trades']
        data['win_rate'] = round((data['winning_trades'] / total * 100) if total > 0 else 0, 1)
        
        daily_pnl.append(data)
    
    return daily_pnl


@app.get("/api/position-history/{position_id}")
async def get_position_history(position_id: int):
    """Get history of a specific position"""
    
    query = """
        SELECT 
            event_type, event_data, created_at
        FROM monitoring.position_history
        WHERE position_id = $1
        ORDER BY created_at DESC
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, position_id)
    
    history = []
    for row in rows:
        history.append({
            'event_type': row['event_type'],
            'event_data': row['event_data'],
            'created_at': row['created_at'].isoformat()
        })
    
    return history


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates"""
    await websocket.accept()
    logger.info("WebSocket client connected")
    
    try:
        while True:
            # Get recent position updates
            query = """
                SELECT * FROM monitoring.positions
                WHERE last_update > NOW() - INTERVAL '5 seconds'
                AND status = 'OPEN'
                ORDER BY last_update DESC
            """
            
            async with db_pool.acquire() as conn:
                rows = await conn.fetch(query)
            
            if rows:
                updates = []
                for row in rows:
                    update = dict(row)
                    # Convert Decimal to float
                    for key in ['entry_price', 'current_price', 'quantity', 
                               'unrealized_pnl', 'realized_pnl', 'stop_loss_price',
                               'take_profit_price', 'trailing_stop_distance', 'margin']:
                        if update.get(key) is not None:
                            update[key] = decimal_to_float(update[key])
                    # Convert datetime
                    for key in ['opened_at', 'last_update']:
                        if update.get(key):
                            update[key] = update[key].isoformat()
                    updates.append(update)
                
                await websocket.send_json({
                    "type": "position_update",
                    "data": updates,
                    "timestamp": datetime.now().isoformat()
                })
            
            # Get stats update
            stats = await get_stats()
            await websocket.send_json({
                "type": "stats_update",
                "data": stats,
                "timestamp": datetime.now().isoformat()
            })
            
            await asyncio.sleep(2)  # Update every 2 seconds
            
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        logger.info("WebSocket client disconnected")


@app.get("/")
async def index():
    """Serve the main HTML interface"""
    html_path = Path(__file__).parent / "templates" / "monitor.html"
    
    if not html_path.exists():
        # Return simple HTML if template doesn't exist
        return HTMLResponse(content="""
        <!DOCTYPE html>
        <html>
        <head>
            <title>ATS 2.0 Monitor</title>
        </head>
        <body>
            <h1>ATS 2.0 Position Monitor</h1>
            <p>API is running. Available endpoints:</p>
            <ul>
                <li><a href="/api/positions">/api/positions</a> - Get positions</li>
                <li><a href="/api/balances">/api/balances</a> - Get balances</li>
                <li><a href="/api/stats">/api/stats</a> - Get statistics</li>
                <li><a href="/api/health">/api/health</a> - System health</li>
                <li><a href="/api/daily-pnl">/api/daily-pnl</a> - Daily PnL history</li>
                <li><a href="/docs">/docs</a> - API documentation</li>
            </ul>
        </body>
        </html>
        """)
    
    with open(html_path, 'r') as f:
        return HTMLResponse(content=f.read())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)