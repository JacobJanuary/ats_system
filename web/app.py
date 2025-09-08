"""
ATS 2.0 - Web API and Dashboard
FastAPI application for monitoring and control
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, Any, Optional
from typing import List
from decimal import Decimal

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from monitoring.metrics import metrics_collector, CONTENT_TYPE_LATEST
from database.connection import DatabaseManager
from core.config import SystemConfig
from core.security import LogSanitizer

# Initialize FastAPI app
app = FastAPI(
    title="ATS 2.0 - Trading System",
    description="Automated Trading System API",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global references (will be set by main application)
system_instance = None
db_manager: Optional[DatabaseManager] = None
config: Optional[SystemConfig] = None

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# ============= MODELS =============
class HealthStatus(BaseModel):
    status: str  # healthy, degraded, unhealthy
    timestamp: datetime
    uptime: float
    components: Dict[str, Dict[str, Any]]

class PositionInfo(BaseModel):
    id: int
    symbol: str
    exchange: str
    side: str
    quantity: float
    entry_price: float
    current_price: Optional[float]
    pnl: Optional[float]
    pnl_percent: Optional[float]
    stop_loss: Optional[float]
    take_profit: Optional[float]
    opened_at: datetime

class SystemStats(BaseModel):
    total_signals: int
    executed_signals: int
    open_positions: int
    total_pnl: float
    daily_trades: int
    daily_pnl: float
    win_rate: float
    
# ============= HEALTH ENDPOINTS =============
@app.get("/health", response_model=HealthStatus)
async def health_check():
    """System health check endpoint"""
    import time
    from datetime import timedelta
    
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "uptime": time.time() - metrics_collector.start_time,
        "components": {}
    }
    
    # Check database
    try:
        if db_manager:
            db_healthy = await db_manager.health_check()
            health["components"]["database"] = {
                "status": "healthy" if db_healthy else "unhealthy",
                "connected": db_healthy
            }
        else:
            health["components"]["database"] = {"status": "not_initialized"}
    except Exception as e:
        health["components"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health["status"] = "degraded"
    
    # Check exchanges
    if system_instance and hasattr(system_instance, 'exchange'):
        exchange = system_instance.exchange
        health["components"]["exchange"] = {
            "status": "healthy" if exchange and exchange._initialized else "unhealthy",
            "name": exchange.name if exchange else "none",
            "testnet": exchange.testnet if exchange else False
        }
    
    # Check WebSocket connections
    health["components"]["websocket"] = {
        "status": "healthy",
        "active_connections": len(manager.active_connections)
    }
    
    # Overall status
    if any(c.get("status") == "unhealthy" for c in health["components"].values()):
        health["status"] = "unhealthy"
    elif any(c.get("status") == "degraded" for c in health["components"].values()):
        health["status"] = "degraded"
    
    return health

@app.get("/health/live")
async def liveness_probe():
    """Kubernetes liveness probe"""
    return {"status": "alive"}

@app.get("/health/ready")
async def readiness_probe():
    """Kubernetes readiness probe"""
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not initialized")
    
    if not await db_manager.health_check():
        raise HTTPException(status_code=503, detail="Database not healthy")
    
    return {"status": "ready"}

# ============= METRICS ENDPOINT =============
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    metrics = metrics_collector.get_metrics()
    return Response(content=metrics, media_type=CONTENT_TYPE_LATEST)

# ============= API ENDPOINTS =============
@app.get("/api/positions", response_model=List[PositionInfo])
async def get_positions():
    """Get all open positions"""
    if not db_manager:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    positions = await db_manager.positions.get_open_positions()
    
    return [
        PositionInfo(
            id=pos.id,
            symbol=pos.symbol,
            exchange=pos.exchange,
            side=pos.side,
            quantity=float(pos.quantity),
            entry_price=float(pos.entry_price),
            current_price=float(pos.current_price) if pos.current_price else None,
            pnl=float(pos.unrealized_pnl) if pos.unrealized_pnl else None,
            pnl_percent=float(pos.unrealized_pnl_percent) if pos.unrealized_pnl_percent else None,
            stop_loss=float(pos.stop_loss_price) if pos.stop_loss_price else None,
            take_profit=float(pos.take_profit_price) if pos.take_profit_price else None,
            opened_at=pos.opened_at
        )
        for pos in positions
    ]

@app.get("/api/stats", response_model=SystemStats)
async def get_system_stats():
    """Get system statistics"""
    if not db_manager:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    # Get metrics from database
    today_metrics = await db_manager.metrics.get_today_metrics()
    
    stats = SystemStats(
        total_signals=today_metrics.get('total_signals', 0),
        executed_signals=today_metrics.get('executed_signals', 0),
        open_positions=await db_manager.positions.count_open_positions(),
        total_pnl=today_metrics.get('total_pnl', 0.0),
        daily_trades=today_metrics.get('daily_trades', 0),
        daily_pnl=today_metrics.get('daily_pnl', 0.0),
        win_rate=today_metrics.get('win_rate', 0.0)
    )
    
    return stats

@app.get("/api/config")
async def get_config():
    """Get system configuration (sanitized)"""
    if not config:
        raise HTTPException(status_code=503, detail="Configuration not loaded")
    
    # Sanitize sensitive data
    safe_config = LogSanitizer.sanitize_config(config)
    return safe_config

@app.post("/api/positions/{position_id}/close")
async def close_position(position_id: int, reason: str = "MANUAL"):
    """Manually close a position"""
    if not system_instance:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    # Get position
    position = await db_manager.positions.get_position(position_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    
    if not position.is_open:
        raise HTTPException(status_code=400, detail="Position already closed")
    
    # Close through exchange
    success, _, error = await system_instance.exchange.close_position(position, reason)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"Failed to close position: {error}")
    
    return {"status": "success", "message": f"Position {position_id} closed"}

@app.post("/api/emergency/stop")
async def emergency_stop():
    """Emergency stop - close all positions and halt trading"""
    if not system_instance:
        raise HTTPException(status_code=503, detail="System not initialized")
    
    # This should trigger emergency shutdown in main system
    # Implementation depends on system architecture
    
    return {"status": "emergency_stop_initiated"}

# ============= WEBSOCKET ENDPOINT =============
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates"""
    await manager.connect(websocket)
    
    try:
        while True:
            # Send periodic updates
            if db_manager:
                positions = await db_manager.positions.get_open_positions()
                stats = await get_system_stats()
                
                update = {
                    "type": "update",
                    "timestamp": datetime.utcnow().isoformat(),
                    "positions": [
                        {
                            "symbol": p.symbol,
                            "side": p.side,
                            "pnl": float(p.unrealized_pnl) if p.unrealized_pnl else 0
                        }
                        for p in positions
                    ],
                    "stats": stats.dict()
                }
                
                await websocket.send_json(update)
            
            await asyncio.sleep(1)  # Update every second
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ============= DASHBOARD =============
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Main dashboard page"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ATS 2.0 Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #1a1a1a; color: #fff; }
            .header { background: #2a2a2a; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
            .stat-card { background: #2a2a2a; padding: 15px; border-radius: 8px; }
            .stat-value { font-size: 24px; font-weight: bold; color: #4CAF50; }
            .positions { background: #2a2a2a; padding: 20px; border-radius: 8px; margin-top: 20px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 10px; text-align: left; border-bottom: 1px solid #444; }
            .profit { color: #4CAF50; }
            .loss { color: #f44336; }
            .neutral { color: #888; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>ATS 2.0 - Automated Trading System</h1>
            <div id="status">Connecting...</div>
        </div>
        
        <div class="stats-grid" id="stats">
            <div class="stat-card">
                <div>Open Positions</div>
                <div class="stat-value" id="open-positions">-</div>
            </div>
            <div class="stat-card">
                <div>Total PNL</div>
                <div class="stat-value" id="total-pnl">-</div>
            </div>
            <div class="stat-card">
                <div>Daily Trades</div>
                <div class="stat-value" id="daily-trades">-</div>
            </div>
            <div class="stat-card">
                <div>Win Rate</div>
                <div class="stat-value" id="win-rate">-</div>
            </div>
        </div>
        
        <div class="positions">
            <h2>Open Positions</h2>
            <table id="positions-table">
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Entry</th>
                        <th>Current</th>
                        <th>PNL</th>
                        <th>PNL %</th>
                    </tr>
                </thead>
                <tbody id="positions-body">
                </tbody>
            </table>
        </div>
        
        <script>
            const ws = new WebSocket('ws://localhost:8000/ws');
            
            ws.onopen = () => {
                document.getElementById('status').textContent = 'Connected';
                document.getElementById('status').style.color = '#4CAF50';
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };
            
            ws.onclose = () => {
                document.getElementById('status').textContent = 'Disconnected';
                document.getElementById('status').style.color = '#f44336';
            };
            
            function updateDashboard(data) {
                // Update stats
                if (data.stats) {
                    document.getElementById('open-positions').textContent = data.stats.open_positions;
                    document.getElementById('total-pnl').textContent = '$' + data.stats.total_pnl.toFixed(2);
                    document.getElementById('daily-trades').textContent = data.stats.daily_trades;
                    document.getElementById('win-rate').textContent = data.stats.win_rate.toFixed(1) + '%';
                }
                
                // Update positions table
                if (data.positions) {
                    const tbody = document.getElementById('positions-body');
                    tbody.innerHTML = '';
                    
                    data.positions.forEach(pos => {
                        const row = tbody.insertRow();
                        row.innerHTML = `
                            <td>${pos.symbol}</td>
                            <td>${pos.side}</td>
                            <td>$${pos.entry_price || '-'}</td>
                            <td>$${pos.current_price || '-'}</td>
                            <td class="${pos.pnl >= 0 ? 'profit' : 'loss'}">
                                $${pos.pnl ? pos.pnl.toFixed(2) : '-'}
                            </td>
                            <td class="${pos.pnl_percent >= 0 ? 'profit' : 'loss'}">
                                ${pos.pnl_percent ? pos.pnl_percent.toFixed(2) : '-'}%
                            </td>
                        `;
                    });
                }
            }
            
            // Load initial data
            fetch('/api/stats')
                .then(res => res.json())
                .then(stats => updateDashboard({stats}));
            
            fetch('/api/positions')
                .then(res => res.json())
                .then(positions => updateDashboard({positions}));
        </script>
    </body>
    </html>
    """
    return html_content

# ============= STARTUP/SHUTDOWN =============
@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    pass  # Initialization handled by main application

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    pass  # Cleanup handled by main application

def set_system_references(system, database, configuration):
    """Set global references to system components"""
    global system_instance, db_manager, config
    system_instance = system
    db_manager = database
    config = configuration

if __name__ == "__main__":
    # For development only
    uvicorn.run(app, host="0.0.0.0", port=8000)