#!/usr/bin/env python3
"""
ATS 2.0 - Real-time Web Monitor
Web-based dashboard for monitoring positions and system status
"""
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import logging

sys.path.insert(0, '/Users/evgeniyyanvarskiy/PycharmProjects/ats_system')

from aiohttp import web
import aiohttp_cors
from core.config import SystemConfig
from database.connection import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WebMonitor:
    """Real-time web monitoring dashboard"""
    
    def __init__(self):
        self.config = SystemConfig()
        self.db = None
        self.app = web.Application()
        self.setup_routes()
        
    def setup_routes(self):
        """Setup web routes"""
        # Setup CORS
        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })
        
        # Add routes
        self.app.router.add_get('/', self.index)
        self.app.router.add_get('/api/positions', self.get_positions)
        self.app.router.add_get('/api/metrics', self.get_metrics)
        self.app.router.add_get('/api/status', self.get_status)
        self.app.router.add_get('/ws', self.websocket_handler)
        
        # Setup CORS for all routes
        for route in list(self.app.router.routes()):
            cors.add(route)
    
    async def initialize(self):
        """Initialize database connection"""
        logger.info("Initializing Web Monitor...")
        self.db = DatabaseManager(self.config)
        await self.db.initialize()
        logger.info("Database connected")
    
    async def index(self, request):
        """Serve main HTML page"""
        html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATS 2.0 - Real-time Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            min-height: 100vh;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 {
            text-align: center;
            margin-bottom: 30px;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.2);
        }
        .stat-label {
            font-size: 0.9em;
            opacity: 0.8;
            margin-bottom: 5px;
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
        }
        .positions-table {
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.2);
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        th {
            font-weight: 600;
            opacity: 0.9;
        }
        .profit { color: #4ade80; }
        .loss { color: #f87171; }
        .status-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 10px;
        }
        .status-online { background: #4ade80; }
        .status-offline { background: #f87171; }
        .refresh-time {
            text-align: center;
            margin-top: 20px;
            opacity: 0.7;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 ATS 2.0 - Real-time Monitor</h1>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">System Status</div>
                <div class="stat-value">
                    <span class="status-indicator status-online"></span>
                    <span id="system-status">Online</span>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Positions</div>
                <div class="stat-value" id="total-positions">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Today's PNL</div>
                <div class="stat-value" id="total-pnl">$0.00</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Win Rate</div>
                <div class="stat-value" id="win-rate">0%</div>
            </div>
        </div>
        
        <div class="positions-table">
            <h2 style="margin-bottom: 20px;">Open Positions</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Quantity</th>
                        <th>Entry Price</th>
                        <th>Current Price</th>
                        <th>PNL</th>
                        <th>PNL %</th>
                        <th>Duration</th>
                    </tr>
                </thead>
                <tbody id="positions-tbody">
                    <tr><td colspan="8" style="text-align: center;">Loading...</td></tr>
                </tbody>
            </table>
        </div>
        
        <div class="refresh-time">
            Last updated: <span id="last-update">Never</span>
        </div>
    </div>
    
    <script>
        let ws = null;
        
        async function fetchData() {
            try {
                // Fetch positions
                const posResponse = await fetch('/api/positions');
                const positions = await posResponse.json();
                updatePositionsTable(positions);
                
                // Fetch metrics
                const metricsResponse = await fetch('/api/metrics');
                const metrics = await metricsResponse.json();
                updateMetrics(metrics);
                
                // Update last refresh time
                document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
            } catch (error) {
                console.error('Error fetching data:', error);
                document.getElementById('system-status').textContent = 'Error';
                document.querySelector('.status-indicator').className = 'status-indicator status-offline';
            }
        }
        
        function updatePositionsTable(positions) {
            const tbody = document.getElementById('positions-tbody');
            
            if (positions.length === 0) {
                tbody.innerHTML = '<tr><td colspan="8" style="text-align: center;">No open positions</td></tr>';
                return;
            }
            
            tbody.innerHTML = positions.map(pos => {
                const pnlClass = pos.unrealized_pnl >= 0 ? 'profit' : 'loss';
                const pnlSign = pos.unrealized_pnl >= 0 ? '+' : '';
                const duration = calculateDuration(pos.opened_at);
                
                return `
                    <tr>
                        <td>${pos.symbol}</td>
                        <td>${pos.side}</td>
                        <td>${pos.quantity.toFixed(4)}</td>
                        <td>$${pos.entry_price.toFixed(4)}</td>
                        <td>$${pos.current_price ? pos.current_price.toFixed(4) : 'N/A'}</td>
                        <td class="${pnlClass}">${pnlSign}$${pos.unrealized_pnl.toFixed(2)}</td>
                        <td class="${pnlClass}">${pnlSign}${pos.unrealized_pnl_percent.toFixed(2)}%</td>
                        <td>${duration}</td>
                    </tr>
                `;
            }).join('');
            
            document.getElementById('total-positions').textContent = positions.length;
        }
        
        function updateMetrics(metrics) {
            document.getElementById('total-pnl').textContent = 
                `$${metrics.net_pnl >= 0 ? '+' : ''}${metrics.net_pnl.toFixed(2)}`;
            document.getElementById('total-pnl').className = 
                metrics.net_pnl >= 0 ? 'stat-value profit' : 'stat-value loss';
            
            const winRate = metrics.total_trades > 0 
                ? (metrics.winning_trades / metrics.total_trades * 100).toFixed(1)
                : 0;
            document.getElementById('win-rate').textContent = `${winRate}%`;
        }
        
        function calculateDuration(openedAt) {
            const now = new Date();
            const opened = new Date(openedAt);
            const diff = now - opened;
            
            const hours = Math.floor(diff / (1000 * 60 * 60));
            const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
            
            if (hours > 24) {
                const days = Math.floor(hours / 24);
                return `${days}d ${hours % 24}h`;
            }
            return `${hours}h ${minutes}m`;
        }
        
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                console.log('WebSocket connected');
                document.getElementById('system-status').textContent = 'Online';
                document.querySelector('.status-indicator').className = 'status-indicator status-online';
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.type === 'positions') {
                    updatePositionsTable(data.positions);
                } else if (data.type === 'metrics') {
                    updateMetrics(data.metrics);
                }
                document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
            };
            
            ws.onclose = () => {
                console.log('WebSocket disconnected');
                document.getElementById('system-status').textContent = 'Offline';
                document.querySelector('.status-indicator').className = 'status-indicator status-offline';
                setTimeout(connectWebSocket, 5000);
            };
            
            ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };
        }
        
        // Initial load
        fetchData();
        
        // Connect WebSocket
        connectWebSocket();
        
        // Refresh every 5 seconds as fallback
        setInterval(fetchData, 5000);
    </script>
</body>
</html>
        """
        return web.Response(text=html, content_type='text/html')
    
    async def get_positions(self, request):
        """Get open positions"""
        positions = await self.db.positions.get_open_positions()
        
        result = []
        for pos in positions:
            result.append({
                'symbol': pos.symbol,
                'side': pos.side,
                'quantity': float(pos.quantity),
                'entry_price': float(pos.entry_price),
                'current_price': float(pos.current_price) if pos.current_price else None,
                'unrealized_pnl': float(pos.unrealized_pnl) if pos.unrealized_pnl else 0,
                'unrealized_pnl_percent': float(pos.unrealized_pnl_percent) if pos.unrealized_pnl_percent else 0,
                'opened_at': pos.opened_at.isoformat() if pos.opened_at else None
            })
        
        return web.json_response(result)
    
    async def get_metrics(self, request):
        """Get today's metrics"""
        metrics = await self.db.metrics.get_today_metrics()
        
        if not metrics:
            metrics = {
                'net_pnl': 0,
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0
            }
        else:
            # Convert any non-serializable types
            clean_metrics = {}
            for key, value in metrics.items():
                if hasattr(value, 'isoformat'):  # datetime/date objects
                    clean_metrics[key] = value.isoformat()
                elif isinstance(value, (int, float, str, bool, type(None))):
                    clean_metrics[key] = value
                else:
                    clean_metrics[key] = str(value)
            metrics = clean_metrics
        
        return web.json_response(metrics)
    
    async def get_status(self, request):
        """Get system status"""
        is_healthy = await self.db.health_check()
        
        return web.json_response({
            'status': 'online' if is_healthy else 'offline',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    
    async def websocket_handler(self, request):
        """WebSocket handler for real-time updates"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        try:
            while True:
                # Send positions update
                positions = await self.get_positions(request)
                await ws.send_json({
                    'type': 'positions',
                    'positions': json.loads(positions.text)
                })
                
                # Send metrics update
                try:
                    metrics = await self.get_metrics(request)
                    await ws.send_json({
                        'type': 'metrics',
                        'metrics': json.loads(metrics.text)
                    })
                except Exception:
                    # Send default metrics on error
                    await ws.send_json({
                        'type': 'metrics',
                        'metrics': {
                            'net_pnl': 0,
                            'total_trades': 0,
                            'winning_trades': 0,
                            'losing_trades': 0
                        }
                    })
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await ws.close()
        
        return ws
    
    async def start(self):
        """Start web server"""
        await self.initialize()
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        
        logger.info("Starting web monitor on http://0.0.0.0:8080")
        await site.start()
        
        # Keep running
        while True:
            await asyncio.sleep(3600)


async def main():
    """Main entry point"""
    monitor = WebMonitor()
    await monitor.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Web monitor stopped")