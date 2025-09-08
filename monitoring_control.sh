#!/bin/bash

# ATS 2.0 Monitoring Control Script
# Manage monitoring daemon and web interface

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ENV_FILE="${SCRIPT_DIR}/.env"
LOG_DIR="${SCRIPT_DIR}/logs"
PID_DIR="${SCRIPT_DIR}/pids"

# Create necessary directories
mkdir -p "$LOG_DIR" "$PID_DIR"

# Load environment variables
if [ -f "$ENV_FILE" ]; then
    # Load environment variables while handling special characters
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        if [[ ! "$key" =~ ^# ]] && [[ -n "$key" ]]; then
            # Remove surrounding quotes if present
            value="${value%\"}"
            value="${value#\"}"
            value="${value%\'}"
            value="${value#\'}"
            # Export the variable
            export "$key=$value"
        fi
    done < "$ENV_FILE"
else
    echo -e "${RED}Error: .env file not found!${NC}"
    echo "Please create .env file from .env.example"
    exit 1
fi

# Functions
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

check_postgres() {
    print_status "Checking PostgreSQL connection..."
    if PGPASSWORD="$DB_PASSWORD" psql -h "${DB_HOST:-localhost}" -U "$DB_USER" -d "${DB_NAME:-fox_crypto}" -c "SELECT 1" > /dev/null 2>&1; then
        print_status "PostgreSQL is accessible"
        return 0
    else
        print_error "Cannot connect to PostgreSQL"
        return 1
    fi
}

init_database() {
    print_status "Initializing database schema..."
    if check_postgres; then
        PGPASSWORD="$DB_PASSWORD" psql -h "${DB_HOST:-localhost}" -U "$DB_USER" -d "${DB_NAME:-fox_crypto}" -f "${SCRIPT_DIR}/migrations/001_monitoring_tables.sql"
        print_status "Database schema initialized"
    else
        print_error "Failed to initialize database"
        exit 1
    fi
}

start_daemon() {
    print_status "Starting monitoring daemon..."
    
    if [ -f "${PID_DIR}/daemon.pid" ]; then
        PID=$(cat "${PID_DIR}/daemon.pid")
        if ps -p $PID > /dev/null 2>&1; then
            print_warning "Monitoring daemon is already running (PID: $PID)"
            return
        fi
    fi
    
    nohup python3 "${SCRIPT_DIR}/monitoring/websocket_daemon.py" \
        > "${LOG_DIR}/daemon.log" 2>&1 &
    
    echo $! > "${PID_DIR}/daemon.pid"
    print_status "Monitoring daemon started (PID: $!)"
}

stop_daemon() {
    print_status "Stopping monitoring daemon..."
    
    if [ -f "${PID_DIR}/daemon.pid" ]; then
        PID=$(cat "${PID_DIR}/daemon.pid")
        if ps -p $PID > /dev/null 2>&1; then
            kill -TERM $PID
            sleep 2
            if ps -p $PID > /dev/null 2>&1; then
                kill -KILL $PID
            fi
            rm -f "${PID_DIR}/daemon.pid"
            print_status "Monitoring daemon stopped"
        else
            print_warning "Monitoring daemon is not running"
            rm -f "${PID_DIR}/daemon.pid"
        fi
    else
        print_warning "No PID file found for monitoring daemon"
    fi
}

start_web() {
    print_status "Starting web interface..."
    
    if [ -f "${PID_DIR}/web.pid" ]; then
        PID=$(cat "${PID_DIR}/web.pid")
        if ps -p $PID > /dev/null 2>&1; then
            print_warning "Web interface is already running (PID: $PID)"
            return
        fi
    fi
    
    nohup uvicorn monitoring.web_interface:app \
        --host 0.0.0.0 --port 8000 \
        > "${LOG_DIR}/web.log" 2>&1 &
    
    echo $! > "${PID_DIR}/web.pid"
    print_status "Web interface started (PID: $!)"
    print_status "Access the interface at: http://localhost:8000"
}

stop_web() {
    print_status "Stopping web interface..."
    
    if [ -f "${PID_DIR}/web.pid" ]; then
        PID=$(cat "${PID_DIR}/web.pid")
        if ps -p $PID > /dev/null 2>&1; then
            kill -TERM $PID
            sleep 2
            if ps -p $PID > /dev/null 2>&1; then
                kill -KILL $PID
            fi
            rm -f "${PID_DIR}/web.pid"
            print_status "Web interface stopped"
        else
            print_warning "Web interface is not running"
            rm -f "${PID_DIR}/web.pid"
        fi
    else
        print_warning "No PID file found for web interface"
    fi
}

status() {
    echo "=== ATS 2.0 Monitoring Status ==="
    
    # Check daemon
    if [ -f "${PID_DIR}/daemon.pid" ]; then
        PID=$(cat "${PID_DIR}/daemon.pid")
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "Monitoring Daemon: ${GREEN}Running${NC} (PID: $PID)"
        else
            echo -e "Monitoring Daemon: ${RED}Stopped${NC}"
        fi
    else
        echo -e "Monitoring Daemon: ${RED}Stopped${NC}"
    fi
    
    # Check web interface
    if [ -f "${PID_DIR}/web.pid" ]; then
        PID=$(cat "${PID_DIR}/web.pid")
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "Web Interface: ${GREEN}Running${NC} (PID: $PID)"
            echo "URL: http://localhost:8000"
        else
            echo -e "Web Interface: ${RED}Stopped${NC}"
        fi
    else
        echo -e "Web Interface: ${RED}Stopped${NC}"
    fi
    
    # Check PostgreSQL
    if check_postgres > /dev/null 2>&1; then
        echo -e "PostgreSQL: ${GREEN}Connected${NC}"
    else
        echo -e "PostgreSQL: ${RED}Disconnected${NC}"
    fi
}

logs() {
    case "$1" in
        daemon)
            tail -f "${LOG_DIR}/daemon.log"
            ;;
        web)
            tail -f "${LOG_DIR}/web.log"
            ;;
        all)
            tail -f "${LOG_DIR}/"*.log
            ;;
        *)
            echo "Usage: $0 logs {daemon|web|all}"
            ;;
    esac
}

docker_up() {
    print_status "Starting monitoring stack with Docker Compose..."
    docker-compose -f docker-compose.monitoring.yml up -d
    print_status "Monitoring stack started"
    echo "Access the interface at: http://localhost:8000"
}

docker_down() {
    print_status "Stopping monitoring stack..."
    docker-compose -f docker-compose.monitoring.yml down
    print_status "Monitoring stack stopped"
}

docker_logs() {
    docker-compose -f docker-compose.monitoring.yml logs -f
}

# Main script
case "$1" in
    start)
        start_daemon
        start_web
        ;;
    stop)
        stop_web
        stop_daemon
        ;;
    restart)
        stop_web
        stop_daemon
        sleep 2
        start_daemon
        start_web
        ;;
    status)
        status
        ;;
    init)
        init_database
        ;;
    logs)
        logs "$2"
        ;;
    docker-up)
        docker_up
        ;;
    docker-down)
        docker_down
        ;;
    docker-logs)
        docker_logs
        ;;
    *)
        echo "ATS 2.0 Monitoring Control Script"
        echo ""
        echo "Usage: $0 {start|stop|restart|status|init|logs|docker-up|docker-down|docker-logs}"
        echo ""
        echo "Commands:"
        echo "  start       - Start monitoring daemon and web interface"
        echo "  stop        - Stop monitoring daemon and web interface"
        echo "  restart     - Restart all components"
        echo "  status      - Show status of all components"
        echo "  init        - Initialize database schema"
        echo "  logs [type] - Show logs (daemon|web|all)"
        echo ""
        echo "Docker commands:"
        echo "  docker-up   - Start monitoring stack with Docker Compose"
        echo "  docker-down - Stop Docker Compose stack"
        echo "  docker-logs - Show Docker Compose logs"
        exit 1
        ;;
esac

exit 0