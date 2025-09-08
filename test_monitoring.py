#!/usr/bin/env python3
"""
Test script for ATS 2.0 Monitoring System
"""

import asyncio
import aiohttp
import json
import sys
import os
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Colors for output
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color


class MonitoringTester:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.session = None
        self.test_results = []
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def print_status(self, test_name, success, message=""):
        status = f"{GREEN}✓ PASS{NC}" if success else f"{RED}✗ FAIL{NC}"
        self.test_results.append((test_name, success))
        print(f"  {status} - {test_name}")
        if message and not success:
            print(f"      {YELLOW}{message}{NC}")
    
    async def test_api_health(self):
        """Test API health endpoint"""
        try:
            async with self.session.get(f"{self.base_url}/api/health") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.print_status("API Health Check", True)
                    
                    # Check components
                    components = data.get('components', {})
                    for component, status in components.items():
                        is_healthy = status.get('status') == 'healthy'
                        self.print_status(
                            f"  Component: {component}",
                            is_healthy,
                            f"Status: {status.get('status')}"
                        )
                else:
                    self.print_status("API Health Check", False, f"Status: {resp.status}")
        except Exception as e:
            self.print_status("API Health Check", False, str(e))
    
    async def test_positions_endpoint(self):
        """Test positions API endpoint"""
        try:
            async with self.session.get(f"{self.base_url}/api/positions") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.print_status("Positions Endpoint", True)
                    print(f"      Found {data.get('total', 0)} positions")
                    
                    # Test with filters
                    for exchange in ['binance', 'bybit']:
                        url = f"{self.base_url}/api/positions?exchange={exchange}"
                        async with self.session.get(url) as resp2:
                            if resp2.status == 200:
                                data2 = await resp2.json()
                                self.print_status(
                                    f"  Filter by {exchange}",
                                    True,
                                    f"Found {data2.get('total', 0)} positions"
                                )
                else:
                    self.print_status("Positions Endpoint", False, f"Status: {resp.status}")
        except Exception as e:
            self.print_status("Positions Endpoint", False, str(e))
    
    async def test_balances_endpoint(self):
        """Test balances API endpoint"""
        try:
            async with self.session.get(f"{self.base_url}/api/balances") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.print_status("Balances Endpoint", True)
                    
                    for exchange, balances in data.items():
                        total = balances.get('total_usd', 0)
                        count = len(balances.get('balances', []))
                        print(f"      {exchange}: {count} assets, ${total:.2f} total")
                else:
                    self.print_status("Balances Endpoint", False, f"Status: {resp.status}")
        except Exception as e:
            self.print_status("Balances Endpoint", False, str(e))
    
    async def test_stats_endpoint(self):
        """Test statistics API endpoint"""
        try:
            async with self.session.get(f"{self.base_url}/api/stats") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.print_status("Statistics Endpoint", True)
                    
                    # Display key stats
                    print(f"      Open positions: {data.get('open_positions', 0)}")
                    print(f"      Unrealized PnL: ${data.get('total_unrealized_pnl', 0):.2f}")
                    print(f"      Today's PnL: ${data.get('today_realized_pnl', 0):.2f}")
                    print(f"      Win rate: {data.get('today_win_rate', 0)}%")
                else:
                    self.print_status("Statistics Endpoint", False, f"Status: {resp.status}")
        except Exception as e:
            self.print_status("Statistics Endpoint", False, str(e))
    
    async def test_daily_pnl_endpoint(self):
        """Test daily PnL endpoint"""
        try:
            async with self.session.get(f"{self.base_url}/api/daily-pnl?days=7") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.print_status("Daily PnL Endpoint", True)
                    print(f"      Found {len(data)} days of data")
                else:
                    self.print_status("Daily PnL Endpoint", False, f"Status: {resp.status}")
        except Exception as e:
            self.print_status("Daily PnL Endpoint", False, str(e))
    
    async def test_websocket_connection(self):
        """Test WebSocket connection"""
        import websockets
        
        ws_url = self.base_url.replace('http:', 'ws:').replace('https:', 'wss:') + '/ws'
        
        try:
            async with websockets.connect(ws_url) as ws:
                # Wait for first message
                message = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(message)
                
                if data.get('type') in ['position_update', 'stats_update']:
                    self.print_status("WebSocket Connection", True)
                    print(f"      Received: {data.get('type')}")
                else:
                    self.print_status("WebSocket Connection", False, "Unexpected message type")
        except asyncio.TimeoutError:
            self.print_status("WebSocket Connection", False, "Timeout waiting for message")
        except Exception as e:
            self.print_status("WebSocket Connection", False, str(e))
    
    async def test_database_connection(self):
        """Test database connection"""
        import asyncpg
        from dotenv import load_dotenv
        
        load_dotenv()
        
        try:
            conn = await asyncpg.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME', 'fox_crypto'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            
            # Test monitoring schema
            result = await conn.fetchval(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'monitoring'"
            )
            
            await conn.close()
            
            if result > 0:
                self.print_status("Database Connection", True)
                print(f"      Found {result} monitoring tables")
            else:
                self.print_status("Database Connection", False, "No monitoring tables found")
        except Exception as e:
            self.print_status("Database Connection", False, str(e))
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "="*50)
        print(f"{BLUE}TEST SUMMARY{NC}")
        print("="*50)
        
        total = len(self.test_results)
        passed = sum(1 for _, success in self.test_results if success)
        failed = total - passed
        
        print(f"Total Tests: {total}")
        print(f"{GREEN}Passed: {passed}{NC}")
        print(f"{RED}Failed: {failed}{NC}")
        
        if failed == 0:
            print(f"\n{GREEN}✓ All tests passed!{NC}")
        else:
            print(f"\n{RED}✗ Some tests failed{NC}")
            print("\nFailed tests:")
            for name, success in self.test_results:
                if not success:
                    print(f"  - {name}")
        
        return failed == 0


async def main():
    print(f"{BLUE}ATS 2.0 Monitoring System Test{NC}")
    print("="*50)
    print(f"Testing at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target: http://localhost:8000")
    print("="*50 + "\n")
    
    async with MonitoringTester() as tester:
        print(f"{YELLOW}Running API Tests...{NC}")
        await tester.test_api_health()
        await tester.test_positions_endpoint()
        await tester.test_balances_endpoint()
        await tester.test_stats_endpoint()
        await tester.test_daily_pnl_endpoint()
        
        print(f"\n{YELLOW}Running Connection Tests...{NC}")
        await tester.test_websocket_connection()
        await tester.test_database_connection()
        
        # Print summary
        success = tester.print_summary()
        
        # Return exit code
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Tests interrupted by user{NC}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{RED}Test failed with error: {e}{NC}")
        sys.exit(1)