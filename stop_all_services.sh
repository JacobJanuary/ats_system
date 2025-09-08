#!/bin/bash

# ATS 2.0 - Stop All Services Script

echo "=================================================="
echo "🛑 Stopping ATS 2.0 Trading System"
echo "=================================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to stop service
stop_service() {
    local service_name=$1
    local process_pattern=$2
    
    echo -e "${YELLOW}Stopping $service_name...${NC}"
    
    # Find and kill processes
    pids=$(pgrep -f "$process_pattern")
    
    if [ -n "$pids" ]; then
        kill $pids 2>/dev/null
        echo -e "  ✅ Stopped $service_name (PIDs: $pids)"
    else
        echo -e "  ℹ️  $service_name not running"
    fi
}

# Stop all services
stop_service "Main ATS" "main.py"
stop_service "Protection Monitor" "universal_protection_monitor.py"
stop_service "DB Sync Service" "db_sync_service.py"
stop_service "Health Monitor" "health_monitor.py"
stop_service "Web Interface" "web_interface.py"

echo ""
echo "⏳ Waiting for processes to terminate..."
sleep 3

# Check if any processes still running
remaining=$(pgrep -f "python3.*(main|monitor|sync|health|web_interface)" | wc -l)

if [ $remaining -gt 0 ]; then
    echo -e "${RED}Warning: $remaining processes still running${NC}"
    echo "Force killing remaining processes..."
    pkill -9 -f "python3.*(main|monitor|sync|health|web_interface)"
    sleep 1
fi

# Clean up PID files
if [ -d "pids" ]; then
    echo "🧹 Cleaning up PID files..."
    rm -f pids/*.pid
fi

echo ""
echo "=================================================="
echo "✅ All services stopped"
echo "=================================================="
echo ""