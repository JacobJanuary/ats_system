#!/usr/bin/env python3
"""
ATS 2.0 - System Monitor
Real-time monitoring of the trading system
"""
import asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from decimal import Decimal
from dotenv import load_dotenv
from typing import List, Dict, Any
import sys

# Load environment
load_dotenv()


# ANSI color codes for terminal
class Colors:
    RESET = '\033[0m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'


class SystemMonitor:
    """Monitors ATS system in real-time"""

    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.conn = None

    async def connect(self):
        """Connect to database"""
        self.conn = await asyncpg.connect(**self.db_config)

    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()

    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')

    async def get_signal_stats(self) -> Dict[str, Any]:
        """Get signal processing statistics"""
        query = """
            WITH recent_signals AS (
                SELECT * FROM ats.signals 
                WHERE received_at > NOW() - INTERVAL '24 hours'
            )
            SELECT 
                COUNT(*) as total_24h,
                COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '1 hour') as total_1h,
                COUNT(*) FILTER (WHERE status = 'PENDING') as pending,
                COUNT(*) FILTER (WHERE status = 'PROCESSING') as processing,
                COUNT(*) FILTER (WHERE status = 'EXECUTED') as executed,
                COUNT(*) FILTER (WHERE status = 'SKIPPED') as skipped,
                COUNT(*) FILTER (WHERE status = 'ERROR') as error,
                COUNT(DISTINCT symbol) as unique_symbols
            FROM recent_signals
        """

        return dict(await self.conn.fetchrow(query))

    async def get_position_stats(self) -> Dict[str, Any]:
        """Get position statistics"""
        query = """
            SELECT 
                COUNT(*) FILTER (WHERE status = 'OPEN') as open_positions,
                COUNT(*) FILTER (WHERE status = 'CLOSED' AND DATE(closed_at) = CURRENT_DATE) as closed_today,
                SUM(unrealized_pnl) FILTER (WHERE status = 'OPEN') as total_unrealized_pnl,
                SUM(realized_pnl) FILTER (WHERE status = 'CLOSED' AND DATE(closed_at) = CURRENT_DATE) as today_realized_pnl,
                COUNT(*) FILTER (WHERE status = 'CLOSED' AND realized_pnl > 0 AND DATE(closed_at) = CURRENT_DATE) as wins_today,
                COUNT(*) FILTER (WHERE status = 'CLOSED' AND realized_pnl < 0 AND DATE(closed_at) = CURRENT_DATE) as losses_today
            FROM ats.positions
        """

        return dict(await self.conn.fetchrow(query))

    async def get_recent_signals(self, limit: int = 5) -> List[Dict]:
        """Get recent signals"""
        query = """
            SELECT 
                s.prediction_id,
                s.symbol,
                s.exchange,
                s.signal_type,
                s.confidence_level,
                s.prediction_proba,
                s.status,
                s.skip_reason,
                s.received_at,
                s.processed_at
            FROM ats.signals s
            ORDER BY s.received_at DESC
            LIMIT $1
        """

        rows = await self.conn.fetch(query, limit)
        return [dict(row) for row in rows]

    async def get_open_positions(self) -> List[Dict]:
        """Get all open positions"""
        query = """
            SELECT 
                p.id,
                p.symbol,
                p.side,
                p.entry_price,
                p.current_price,
                p.quantity,
                p.leverage,
                p.unrealized_pnl,
                p.unrealized_pnl_percent,
                p.stop_loss_price,
                p.take_profit_price,
                p.opened_at,
                EXTRACT(EPOCH FROM (NOW() - p.opened_at))/3600 as hours_open
            FROM ats.positions p
            WHERE p.status = 'OPEN'
            ORDER BY p.opened_at DESC
        """

        rows = await self.conn.fetch(query)
        return [dict(row) for row in rows]

    async def get_recent_events(self, limit: int = 10) -> List[Dict]:
        """Get recent audit events"""
        query = """
            SELECT 
                event_type,
                event_category,
                severity,
                event_data->>'symbol' as symbol,
                event_data->>'error' as error,
                created_at
            FROM ats.audit_log
            WHERE created_at > NOW() - INTERVAL '1 hour'
            ORDER BY created_at DESC
            LIMIT $1
        """

        rows = await self.conn.fetch(query, limit)
        return [dict(row) for row in rows]

    def format_money(self, value: Any) -> str:
        """Format money value with color"""
        if value is None:
            return "$0.00"

        val = float(value)
        if val > 0:
            return f"{Colors.GREEN}+${val:.2f}{Colors.RESET}"
        elif val < 0:
            return f"{Colors.RED}-${abs(val):.2f}{Colors.RESET}"
        else:
            return "$0.00"

    def format_percent(self, value: Any) -> str:
        """Format percentage with color"""
        if value is None:
            return "0.00%"

        val = float(value)
        if val > 0:
            return f"{Colors.GREEN}+{val:.2f}%{Colors.RESET}"
        elif val < 0:
            return f"{Colors.RED}{val:.2f}%{Colors.RESET}"
        else:
            return "0.00%"

    def format_status(self, status: str) -> str:
        """Format status with color"""
        status_colors = {
            'PENDING': Colors.YELLOW,
            'PROCESSING': Colors.CYAN,
            'EXECUTED': Colors.GREEN,
            'SKIPPED': Colors.MAGENTA,
            'ERROR': Colors.RED,
            'OPEN': Colors.GREEN,
            'CLOSED': Colors.WHITE
        }
        color = status_colors.get(status, Colors.WHITE)
        return f"{color}{status}{Colors.RESET}"

    async def display_dashboard(self):
        """Display main dashboard"""

        # Get all stats
        signal_stats = await self.get_signal_stats()
        position_stats = await self.get_position_stats()
        recent_signals = await self.get_recent_signals(5)
        open_positions = await self.get_open_positions()

        # Clear screen
        self.clear_screen()

        # Header
        print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 80}")
        print(f"{'ATS 2.0 SYSTEM MONITOR':^80}")
        print(f"{'=' * 80}{Colors.RESET}")
        print(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Signal Statistics
        print(f"{Colors.BOLD}📊 SIGNAL STATISTICS (24H){Colors.RESET}")
        print(f"├─ Total Signals: {signal_stats['total_24h']} (Last Hour: {signal_stats['total_1h']})")
        print(f"├─ {self.format_status('PENDING')}: {signal_stats['pending']} | "
              f"{self.format_status('PROCESSING')}: {signal_stats['processing']} | "
              f"{self.format_status('EXECUTED')}: {signal_stats['executed']}")
        print(f"├─ {self.format_status('SKIPPED')}: {signal_stats['skipped']} | "
              f"{self.format_status('ERROR')}: {signal_stats['error']}")
        print(f"└─ Unique Symbols: {signal_stats['unique_symbols']}\n")

        # Position Statistics
        print(f"{Colors.BOLD}💰 POSITION STATISTICS{Colors.RESET}")
        print(f"├─ Open Positions: {position_stats['open_positions']}")
        print(f"├─ Closed Today: {position_stats['closed_today']} "
              f"(Wins: {Colors.GREEN}{position_stats['wins_today']}{Colors.RESET} / "
              f"Losses: {Colors.RED}{position_stats['losses_today']}{Colors.RESET})")
        print(f"├─ Unrealized PNL: {self.format_money(position_stats['total_unrealized_pnl'])}")
        print(f"└─ Today's PNL: {self.format_money(position_stats['today_realized_pnl'])}\n")

        # Recent Signals
        print(f"{Colors.BOLD}📡 RECENT SIGNALS{Colors.RESET}")
        if recent_signals:
            for sig in recent_signals[:3]:
                time_ago = (datetime.now(sig['received_at'].tzinfo or None) - sig['received_at']).total_seconds()
                time_str = f"{int(time_ago)}s ago" if time_ago < 60 else f"{int(time_ago / 60)}m ago"

                print(f"├─ {sig['symbol']:12} {sig['signal_type']:4} "
                      f"[{sig['confidence_level']:6}] "
                      f"P={sig['prediction_proba']:.3f} "
                      f"{self.format_status(sig['status']):20} "
                      f"({time_str})")

                if sig['skip_reason']:
                    print(f"│  └─ Reason: {sig['skip_reason']}")
        else:
            print("├─ No recent signals")
        print()

        # Open Positions
        print(f"{Colors.BOLD}📈 OPEN POSITIONS{Colors.RESET}")
        if open_positions:
            for pos in open_positions[:5]:
                pnl_str = self.format_percent(pos['unrealized_pnl_percent'])
                hours = pos['hours_open']
                time_str = f"{hours:.1f}h" if hours >= 1 else f"{int(hours * 60)}m"

                print(f"├─ {pos['symbol']:12} {pos['side']:5} "
                      f"Entry: ${pos['entry_price']:.4f} "
                      f"Current: ${pos['current_price'] or 0:.4f} "
                      f"PNL: {pnl_str:15} "
                      f"({time_str})")
        else:
            print("├─ No open positions")
        print()

        # Footer
        print(f"{Colors.CYAN}{'─' * 80}{Colors.RESET}")
        print("Press Ctrl+C to exit | Auto-refresh every 5 seconds")

    async def monitor_loop(self):
        """Main monitoring loop"""
        try:
            while True:
                await self.display_dashboard()
                await asyncio.sleep(5)  # Refresh every 5 seconds
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Monitor stopped by user{Colors.RESET}")

    async def watch_events(self):
        """Watch audit events in real-time"""
        self.clear_screen()
        print(f"{Colors.BOLD}{Colors.CYAN}ATS 2.0 - LIVE EVENT STREAM{Colors.RESET}\n")

        last_event_id = 0

        try:
            while True:
                # Get new events
                query = """
                    SELECT 
                        id,
                        event_type,
                        event_category,
                        severity,
                        event_data,
                        created_at
                    FROM ats.audit_log
                    WHERE id > $1
                    ORDER BY id
                    LIMIT 50
                """

                rows = await self.conn.fetch(query, last_event_id)

                for row in rows:
                    last_event_id = row['id']

                    # Format severity with color
                    severity_colors = {
                        'DEBUG': Colors.WHITE,
                        'INFO': Colors.CYAN,
                        'WARNING': Colors.YELLOW,
                        'ERROR': Colors.RED,
                        'CRITICAL': Colors.RED + Colors.BOLD
                    }

                    color = severity_colors.get(row['severity'], Colors.WHITE)

                    # Extract key info from event_data
                    data = row['event_data']
                    symbol = data.get('symbol', '')
                    error = data.get('error', '')

                    # Print event
                    timestamp = row['created_at'].strftime('%H:%M:%S')
                    print(f"[{timestamp}] {color}[{row['severity']:8}]{Colors.RESET} "
                          f"{row['event_category']:8} | {row['event_type']:20} "
                          f"{symbol:12} {error[:50]}")

                await asyncio.sleep(1)  # Check for new events every second

        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Event stream stopped{Colors.RESET}")


async def main():
    """Main function"""
    monitor = SystemMonitor()

    try:
        await monitor.connect()

        print(f"{Colors.BOLD}{Colors.CYAN}")
        print("╔══════════════════════════════════════╗")
        print("║     ATS 2.0 SYSTEM MONITOR          ║")
        print("╚══════════════════════════════════════╝")
        print(f"{Colors.RESET}")
        print("\nOptions:")
        print("1. Dashboard View (auto-refresh)")
        print("2. Live Event Stream")
        print("3. Position Details")
        print("4. Signal Analysis")
        print("0. Exit")

        choice = input("\nSelect option: ")

        if choice == "1":
            await monitor.monitor_loop()
        elif choice == "2":
            await monitor.watch_events()
        elif choice == "3":
            # Detailed position view
            positions = await monitor.get_open_positions()
            monitor.clear_screen()
            print(f"{Colors.BOLD}OPEN POSITIONS DETAIL{Colors.RESET}\n")

            for pos in positions:
                print(f"Symbol: {pos['symbol']}")
                print(f"  Side: {pos['side']}")
                print(f"  Entry: ${pos['entry_price']}")
                print(f"  Current: ${pos['current_price'] or 'N/A'}")
                print(f"  Quantity: {pos['quantity']}")
                print(f"  Leverage: {pos['leverage']}x")
                print(f"  PNL: {monitor.format_money(pos['unrealized_pnl'])}")
                print(f"  PNL%: {monitor.format_percent(pos['unrealized_pnl_percent'])}")
                print(f"  SL: ${pos['stop_loss_price'] or 'Not set'}")
                print(f"  TP: ${pos['take_profit_price'] or 'Not set'}")
                print(f"  Opened: {pos['opened_at']}")
                print("─" * 40)

            input("\nPress Enter to exit...")

        elif choice == "4":
            # Signal analysis
            stats = await monitor.get_signal_stats()
            monitor.clear_screen()
            print(f"{Colors.BOLD}SIGNAL ANALYSIS{Colors.RESET}\n")

            # Get signal breakdown by exchange
            exchange_query = """
                SELECT 
                    exchange,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'EXECUTED') as executed,
                    COUNT(*) FILTER (WHERE status = 'SKIPPED') as skipped
                FROM ats.signals
                WHERE received_at > NOW() - INTERVAL '24 hours'
                GROUP BY exchange
            """

            exchange_stats = await monitor.conn.fetch(exchange_query)

            print("By Exchange (24h):")
            for row in exchange_stats:
                print(f"  {row['exchange']:10} Total: {row['total']:3} "
                      f"Executed: {row['executed']:3} "
                      f"Skipped: {row['skipped']:3}")

            # Get top skip reasons
            skip_query = """
                SELECT 
                    skip_reason,
                    COUNT(*) as count
                FROM ats.signals
                WHERE status = 'SKIPPED'
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY skip_reason
                ORDER BY count DESC
                LIMIT 10
            """

            skip_reasons = await monitor.conn.fetch(skip_query)

            print("\nTop Skip Reasons:")
            for row in skip_reasons:
                print(f"  {row['count']:3}x {row['skip_reason']}")

            input("\nPress Enter to exit...")

    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.RESET}")
    finally:
        await monitor.close()


if __name__ == "__main__":
    asyncio.run(main())