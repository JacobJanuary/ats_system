#!/usr/bin/env python3
"""
System Health Monitor
Monitors all services and automatically restarts them if they fail
"""
import asyncio
import psutil
import os
import subprocess
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SystemHealthMonitor:
    """Monitor system health and automatically restart failed services"""
    
    def __init__(self):
        self.check_interval = int(os.getenv('HEALTH_CHECK_INTERVAL', '60'))
        self.max_restarts = int(os.getenv('MAX_SERVICE_RESTARTS', '3'))
        self.enabled = os.getenv('ENABLE_HEALTH_MONITOR', 'true').lower() == 'true'
        
        self.services = {
            'main_ats': {
                'process_name': 'main.py',
                'restart_command': 'python3 main.py',
                'log_file': 'logs/main.log',
                'restart_count': 0,
                'last_restart': None,
                'enabled': True
            },
            'universal_monitor': {
                'process_name': 'universal_protection_monitor.py',
                'restart_command': 'python3 universal_protection_monitor.py',
                'log_file': 'logs/protection.log',
                'restart_count': 0,
                'last_restart': None,
                'enabled': True
            },
            'db_sync': {
                'process_name': 'db_sync_service.py',
                'restart_command': 'python3 db_sync_service.py',
                'log_file': 'logs/db_sync.log',
                'restart_count': 0,
                'last_restart': None,
                'enabled': True
            },
            'web_interface': {
                'process_name': 'monitoring/web_interface.py',
                'restart_command': 'python3 monitoring/web_interface.py',
                'log_file': 'logs/web.log',
                'restart_count': 0,
                'last_restart': None,
                'enabled': False  # Disabled by default, enable if needed
            }
        }
        
        self.system_thresholds = {
            'cpu_percent': 80,
            'memory_percent': 85,
            'disk_percent': 90
        }
        
        self.stats = {
            'checks_performed': 0,
            'services_restarted': 0,
            'last_check': None,
            'uptime_start': datetime.now()
        }
    
    async def start(self):
        """Start health monitoring"""
        if not self.enabled:
            logger.info("Health Monitor is disabled in configuration")
            return
        
        logger.info("🏥 System Health Monitor started")
        logger.info(f"Check interval: {self.check_interval} seconds")
        logger.info(f"Max restarts per service: {self.max_restarts}")
        
        # Create logs directory if not exists
        os.makedirs('logs', exist_ok=True)
        
        while True:
            try:
                await self.perform_health_check()
                self.stats['checks_performed'] += 1
                self.stats['last_check'] = datetime.now()
                
                # Log statistics every 10 checks
                if self.stats['checks_performed'] % 10 == 0:
                    await self.log_statistics()
                
                await asyncio.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("Health Monitor stopped by user")
                break
            except Exception as e:
                logger.error(f"Health check error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def perform_health_check(self):
        """Perform complete health check"""
        logger.debug("Performing health check...")
        
        # Check services
        await self.check_services()
        
        # Check system resources
        await self.check_system_resources()
        
        # Check database connectivity
        await self.check_database()
        
        # Check disk space for logs
        await self.check_log_rotation()
    
    async def check_services(self):
        """Check all services and restart if needed"""
        for service_name, config in self.services.items():
            if not config['enabled']:
                continue
            
            is_running = self.is_process_running(config['process_name'])
            
            if not is_running:
                logger.warning(f"⚠️ Service {service_name} is not running")
                
                # Check restart limits
                if config['restart_count'] >= self.max_restarts:
                    logger.error(f"❌ Service {service_name} exceeded restart limit")
                    continue
                
                # Check cooldown period
                if config['last_restart']:
                    time_since_restart = datetime.now() - config['last_restart']
                    if time_since_restart < timedelta(minutes=5):
                        logger.warning(f"Service {service_name} in cooldown period")
                        continue
                
                # Restart service
                await self.restart_service(service_name, config)
            else:
                # Reset restart count if service is stable for 30 minutes
                if config['last_restart']:
                    time_since_restart = datetime.now() - config['last_restart']
                    if time_since_restart > timedelta(minutes=30):
                        config['restart_count'] = 0
    
    def is_process_running(self, process_name):
        """Check if a process is running"""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if process_name in cmdline and 'health_monitor' not in cmdline:
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False
    
    async def restart_service(self, service_name, config):
        """Restart a service"""
        logger.info(f"🔄 Restarting {service_name}...")
        
        try:
            # Kill existing process if any
            for proc in psutil.process_iter(['pid', 'cmdline']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if config['process_name'] in cmdline:
                        logger.info(f"Killing old process {proc.pid}")
                        proc.kill()
                except:
                    pass
            
            # Wait a moment
            await asyncio.sleep(2)
            
            # Start new process
            with open(config['log_file'], 'a') as log_file:
                subprocess.Popen(
                    config['restart_command'],
                    shell=True,
                    stdout=log_file,
                    stderr=log_file,
                    start_new_session=True
                )
            
            config['restart_count'] += 1
            config['last_restart'] = datetime.now()
            self.stats['services_restarted'] += 1
            
            logger.info(f"✅ {service_name} restarted (attempt {config['restart_count']}/{self.max_restarts})")
            
        except Exception as e:
            logger.error(f"Failed to restart {service_name}: {e}")
    
    async def check_system_resources(self):
        """Check system resource usage"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > self.system_thresholds['cpu_percent']:
                logger.warning(f"⚠️ High CPU usage: {cpu_percent:.1f}%")
            
            # Memory usage
            memory = psutil.virtual_memory()
            if memory.percent > self.system_thresholds['memory_percent']:
                logger.warning(f"⚠️ High memory usage: {memory.percent:.1f}%")
                # Log top memory consumers
                self.log_top_processes()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            if disk.percent > self.system_thresholds['disk_percent']:
                logger.warning(f"⚠️ Low disk space: {disk.percent:.1f}% used")
            
        except Exception as e:
            logger.error(f"Error checking system resources: {e}")
    
    async def check_database(self):
        """Check database connectivity"""
        try:
            import asyncpg
            
            # Quick database connectivity check
            conn = await asyncpg.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                timeout=5
            )
            
            # Simple query
            result = await conn.fetchval("SELECT 1")
            await conn.close()
            
            if result != 1:
                logger.warning("⚠️ Database connectivity issue detected")
                
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
    
    async def check_log_rotation(self):
        """Check and rotate logs if they're too large"""
        max_log_size = 100 * 1024 * 1024  # 100 MB
        
        try:
            for service_name, config in self.services.items():
                log_file = config['log_file']
                
                if os.path.exists(log_file):
                    size = os.path.getsize(log_file)
                    
                    if size > max_log_size:
                        # Rotate log
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        archive_name = f"{log_file}.{timestamp}"
                        os.rename(log_file, archive_name)
                        logger.info(f"📁 Rotated log file: {log_file} -> {archive_name}")
                        
                        # Create new empty log file
                        open(log_file, 'a').close()
                        
        except Exception as e:
            logger.error(f"Error checking logs: {e}")
    
    def log_top_processes(self):
        """Log top memory-consuming processes"""
        try:
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'memory_percent']):
                try:
                    processes.append(proc.info)
                except:
                    pass
            
            # Sort by memory usage
            processes.sort(key=lambda x: x.get('memory_percent', 0), reverse=True)
            
            logger.info("Top memory consumers:")
            for proc in processes[:5]:
                logger.info(f"  - {proc['name']}: {proc['memory_percent']:.1f}%")
                
        except Exception as e:
            logger.error(f"Error logging processes: {e}")
    
    async def log_statistics(self):
        """Log monitoring statistics"""
        uptime = datetime.now() - self.stats['uptime_start']
        logger.info("📊 Health Monitor Statistics:")
        logger.info(f"  - Uptime: {uptime}")
        logger.info(f"  - Checks performed: {self.stats['checks_performed']}")
        logger.info(f"  - Services restarted: {self.stats['services_restarted']}")
        
        for service_name, config in self.services.items():
            if config['enabled']:
                status = "🟢 Running" if self.is_process_running(config['process_name']) else "🔴 Stopped"
                logger.info(f"  - {service_name}: {status} (restarts: {config['restart_count']})")


async def main():
    """Main function"""
    monitor = SystemHealthMonitor()
    await monitor.start()


if __name__ == "__main__":
    asyncio.run(main())