#!/bin/bash

# ATS 2.0 - Complete System Startup Script
# Starts all services with monitoring and protection

echo "=================================================="
echo "🚀 Starting ATS 2.0 Trading System"
echo "=================================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p logs
mkdir -p pids

# Function to check if process is running
check_process() {
    if pgrep -f "$1" > /dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to stop a service
stop_service() {
    echo -e "${YELLOW}Stopping $1...${NC}"
    pkill -f "$2" 2>/dev/null
    sleep 1
}

# Function to start a service
start_service() {
    local service_name=$1
    local command=$2
    local log_file=$3
    
    echo -e "${GREEN}Starting $service_name...${NC}"
    nohup $command > $log_file 2>&1 &
    local pid=$!
    echo $pid > "pids/${service_name}.pid"
    sleep 2
    
    if check_process "$command"; then
        echo -e "  ✅ $service_name started (PID: $pid)"
    else
        echo -e "  ${RED}❌ Failed to start $service_name${NC}"
        return 1
    fi
}

# Stop all existing services
echo "🛑 Stopping existing services..."
stop_service "Main ATS" "main.py"
stop_service "Protection Monitor" "universal_protection_monitor.py"
stop_service "DB Sync" "db_sync_service.py"
stop_service "Health Monitor" "health_monitor.py"
stop_service "Web Interface" "web_interface.py"
echo ""

# Wait for processes to fully stop
sleep 3

# Start core services
echo "🔧 Starting core services..."
echo ""

# 1. Database Sync Service
start_service "db_sync" "python3 db_sync_service.py" "logs/db_sync.log"

# 2. Universal Protection Monitor
start_service "protection_monitor" "python3 universal_protection_monitor.py" "logs/protection.log"

# 3. Main ATS System
start_service "main_ats" "python3 main.py" "logs/main.log"

# 4. Web Interface (on port 8000)
if [ -f "monitoring/web_interface.py" ]; then
    start_service "web_interface" "cd monitoring && python3 web_interface.py" "logs/web.log"
elif [ -f "web_interface.py" ]; then
    start_service "web_interface" "python3 web_interface.py" "logs/web.log"
fi

# 5. Health Monitor (watches all services)
start_service "health_monitor" "python3 health_monitor.py" "logs/health.log"

echo ""
echo "=================================================="
echo "📊 System Status Check"
echo "=================================================="

# Wait for services to stabilize
sleep 3

# Check running services
echo ""
echo "🔍 Active services:"
ps aux | grep -E "python3.*(main|monitor|sync|health|web_interface)" | grep -v grep | while read line; do
    echo "  - $line" | cut -c1-120
done

echo ""
echo "📝 Log files:"
ls -lh logs/*.log 2>/dev/null | while read line; do
    echo "  - $line"
done

echo ""
echo "=================================================="
echo "✅ ATS 2.0 System Started Successfully!"
echo "=================================================="
echo ""
echo "📌 Quick Commands:"
echo "  • View logs:        tail -f logs/*.log"
echo "  • Check status:     ps aux | grep python3"
echo "  • Stop all:         ./stop_all_services.sh"
echo "  • Web interface:    http://localhost:8000"
echo ""
echo "📊 Monitoring:"
echo "  • Protection status is checked every 30 seconds"
echo "  • Database sync runs every 5 minutes"
echo "  • Health monitor checks every 60 seconds"
echo ""
echo "⚠️  Note: Check logs/health.log for system health status"
echo ""