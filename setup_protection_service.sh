#!/bin/bash

# Setup script for Protection Monitor Service
# Run with: sudo bash setup_protection_service.sh

echo "=========================================="
echo "PROTECTION MONITOR SERVICE SETUP"
echo "=========================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
   echo "Please run as root (use sudo)"
   exit 1
fi

# Get current directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Update service file with correct paths
SERVICE_FILE="$SCRIPT_DIR/protection-monitor.service"
sed -i.bak "s|/Users/evgeniyyanvarskiy/PycharmProjects/ats_system|$SCRIPT_DIR|g" "$SERVICE_FILE"

# Copy service file to systemd
echo "Installing service file..."
cp "$SERVICE_FILE" /etc/systemd/system/

# Create log directory
echo "Creating log directory..."
mkdir -p /var/log
touch /var/log/protection-monitor.log
touch /var/log/protection-monitor-error.log

# Set permissions
chmod 644 /etc/systemd/system/protection-monitor.service
chmod 666 /var/log/protection-monitor*.log

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

# Enable service
echo "Enabling service..."
systemctl enable protection-monitor.service

# Start service
echo "Starting service..."
systemctl start protection-monitor.service

# Check status
echo ""
echo "Checking service status..."
systemctl status protection-monitor.service --no-pager

echo ""
echo "=========================================="
echo "SETUP COMPLETE!"
echo "=========================================="
echo ""
echo "Useful commands:"
echo "  Start:   sudo systemctl start protection-monitor"
echo "  Stop:    sudo systemctl stop protection-monitor"
echo "  Status:  sudo systemctl status protection-monitor"
echo "  Logs:    sudo journalctl -u protection-monitor -f"
echo "  Restart: sudo systemctl restart protection-monitor"
echo ""
echo "Log files:"
echo "  /var/log/protection-monitor.log"
echo "  /var/log/protection-monitor-error.log"