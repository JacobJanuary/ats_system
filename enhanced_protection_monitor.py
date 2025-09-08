#!/usr/bin/env python3
"""
Enhanced Protection Monitor with Mandatory Trailing Stops
Monitors and automatically adds protection to all positions
"""

import asyncio
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
import time

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('protection_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedProtectionMonitor:
    """
    Enhanced protection monitor that ensures ALL positions have trailing stops
    """
    
    def __init__(self):
        self.api_key = os.getenv('BYBIT_API_KEY')
        self.api_secret = os.getenv('BYBIT_API_SECRET')
        self.testnet = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
        
        # Initialize client
        self.client = HTTP(
            testnet=self.testnet,
            api_key=self.api_key,
            api_secret=self.api_secret
        )
        
        # Configuration
        self.check_interval = int(os.getenv('PROTECTION_CHECK_INTERVAL', 30))
        self.stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', 6.5))
        self.trailing_percent = float(os.getenv('TRAILING_CALLBACK_RATE', 0.5))
        self.activation_percent = float(os.getenv('TRAILING_ACTIVATION_PERCENT', 3.5))
        
        # Protection requirements
        self.require_stop_loss = os.getenv('USE_STOP_LOSS', 'true').lower() == 'true'
        self.require_trailing_stop = True  # ALWAYS require trailing stop
        
        self.running = False
        self.stats = {
            'checks': 0,
            'positions_protected': 0,
            'trailing_stops_added': 0,
            'stop_losses_added': 0,
            'failures': 0,
            'start_time': datetime.now()
        }
        
        logger.info("=" * 60)
        logger.info("🚀 ENHANCED PROTECTION MONITOR")
        logger.info("=" * 60)
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'MAINNET'}")
        logger.info(f"Check interval: {self.check_interval} seconds")
        logger.info(f"Stop Loss: {self.stop_loss_percent}%")
        logger.info(f"Trailing Stop: {self.trailing_percent}%")
        logger.info(f"Requirements: SL={self.require_stop_loss}, TS=MANDATORY")
        logger.info("=" * 60)
    
    async def start(self):
        """Start the protection monitor"""
        self.running = True
        
        while self.running:
            try:
                await self.check_and_protect_positions()
                await asyncio.sleep(self.check_interval)
            except KeyboardInterrupt:
                logger.info("Stopping monitor...")
                self.running = False
                self.print_stats()
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(10)
    
    async def check_and_protect_positions(self):
        """Check all positions and add protection where needed"""
        self.stats['checks'] += 1
        
        # Get all positions
        positions = self.get_all_positions()
        
        if not positions:
            return
        
        # Analyze protection status
        total_positions = 0
        missing_sl = []
        missing_ts = []
        fully_protected = []
        
        for pos in positions:
            if float(pos.get('size', 0)) > 0:
                total_positions += 1
                
                has_sl = pos.get('stopLoss') and float(pos.get('stopLoss', 0)) > 0
                has_ts = pos.get('trailingStop') and float(pos.get('trailingStop', 0)) > 0
                
                needs_protection = False
                
                if self.require_stop_loss and not has_sl:
                    missing_sl.append(pos)
                    needs_protection = True
                
                if not has_ts:  # ALWAYS require trailing stop
                    missing_ts.append(pos)
                    needs_protection = True
                
                if not needs_protection:
                    fully_protected.append(pos)
        
        # Log status
        logger.info(f"\n📊 Check #{self.stats['checks']} at {datetime.now().strftime('%H:%M:%S')}")
        logger.info(f"  Total positions: {total_positions}")
        logger.info(f"  Fully protected: {len(fully_protected)} ✅")
        
        if missing_sl:
            logger.warning(f"  Missing Stop Loss: {len(missing_sl)} ❌")
        
        if missing_ts:
            logger.warning(f"  Missing Trailing Stop: {len(missing_ts)} ❌")
        
        # Add protection to positions that need it
        if missing_sl or missing_ts:
            logger.info(f"\n🔧 Adding protection to {len(missing_sl) + len(missing_ts)} positions...")
            
            # First add stop losses (more critical)
            for pos in missing_sl:
                await self.add_stop_loss(pos)
                await asyncio.sleep(0.5)
            
            # Then add trailing stops
            for pos in missing_ts:
                await self.add_trailing_stop(pos)
                await asyncio.sleep(0.5)
        else:
            logger.info("  ✅ All positions fully protected!")
        
        # Log periodic summary
        if self.stats['checks'] % 10 == 0:
            self.print_stats()
    
    def get_all_positions(self):
        """Get all open positions"""
        try:
            all_positions = []
            cursor = ""
            
            while True:
                params = {
                    "category": "linear",
                    "settleCoin": "USDT",
                    "limit": 200
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                response = self.client.get_positions(**params)
                
                if response['retCode'] != 0:
                    logger.error(f"Failed to get positions: {response.get('retMsg')}")
                    break
                
                positions = response.get('result', {}).get('list', [])
                all_positions.extend(positions)
                
                cursor = response.get('result', {}).get('nextPageCursor', '')
                if not cursor:
                    break
            
            return all_positions
            
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    async def add_stop_loss(self, position):
        """Add stop loss to a position"""
        symbol = position.get('symbol')
        side = position.get('side')
        entry_price = float(position.get('avgPrice', 0))
        
        if entry_price == 0:
            logger.warning(f"Cannot add SL to {symbol}: no entry price")
            return
        
        logger.info(f"  Adding SL to {symbol}...")
        
        # Calculate stop loss price
        if side == 'Buy':
            stop_loss_price = entry_price * (1 - self.stop_loss_percent / 100)
        else:
            stop_loss_price = entry_price * (1 + self.stop_loss_percent / 100)
        
        # Get precision
        precision = self.get_price_precision(stop_loss_price)
        stop_loss_price = round(stop_loss_price, precision)
        
        try:
            response = self.client.set_trading_stop(
                category="linear",
                symbol=symbol,
                stopLoss=str(stop_loss_price),
                slTriggerBy="MarkPrice",
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                logger.info(f"    ✅ SL added at {stop_loss_price}")
                self.stats['stop_losses_added'] += 1
            else:
                logger.error(f"    ❌ Failed: {response.get('retMsg')}")
                self.stats['failures'] += 1
                
        except Exception as e:
            logger.error(f"    ❌ Exception: {e}")
            self.stats['failures'] += 1
    
    async def add_trailing_stop(self, position):
        """Add trailing stop to a position"""
        symbol = position.get('symbol')
        side = position.get('side')
        entry_price = float(position.get('avgPrice', 0))
        mark_price = float(position.get('markPrice', entry_price))
        
        if entry_price == 0:
            logger.warning(f"Cannot add TS to {symbol}: no entry price")
            return
        
        logger.info(f"  Adding TS to {symbol}...")
        
        # Calculate parameters
        precision = self.get_price_precision(entry_price)
        trailing_distance = round(entry_price * (self.trailing_percent / 100), precision)
        
        # Ensure minimum trailing distance
        if trailing_distance == 0:
            trailing_distance = 10 ** (-precision)
        
        # Smart activation price
        if side == 'Buy':
            if mark_price > entry_price * 1.001:
                # In profit - activate immediately
                activation_price = mark_price * 1.0001
            else:
                # At loss - activate at breakeven
                activation_price = entry_price * 1.001
        else:
            if mark_price < entry_price * 0.999:
                # In profit - activate immediately
                activation_price = mark_price * 0.9999
            else:
                # At loss - activate at breakeven
                activation_price = entry_price * 0.999
        
        activation_price = round(activation_price, precision)
        
        try:
            # First try with calculated parameters
            response = self.client.set_trading_stop(
                category="linear",
                symbol=symbol,
                trailingStop=str(trailing_distance),
                activePrice=str(activation_price),
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                logger.info(f"    ✅ TS added: distance={trailing_distance}, activation={activation_price}")
                self.stats['trailing_stops_added'] += 1
            else:
                error_msg = response.get('retMsg', '')
                
                # Try alternative parameters if first attempt fails
                if "price" in error_msg.lower():
                    logger.info(f"    Trying alternative parameters...")
                    
                    # Adjust activation price
                    if side == 'Buy':
                        activation_price = max(mark_price * 1.005, entry_price * 1.005)
                    else:
                        activation_price = min(mark_price * 0.995, entry_price * 0.995)
                    
                    activation_price = round(activation_price, precision)
                    
                    response = self.client.set_trading_stop(
                        category="linear",
                        symbol=symbol,
                        trailingStop=str(trailing_distance),
                        activePrice=str(activation_price),
                        positionIdx=0
                    )
                    
                    if response['retCode'] == 0:
                        logger.info(f"    ✅ TS added with adjusted parameters")
                        self.stats['trailing_stops_added'] += 1
                    else:
                        logger.error(f"    ❌ Failed: {response.get('retMsg')}")
                        self.stats['failures'] += 1
                else:
                    logger.error(f"    ❌ Failed: {error_msg}")
                    self.stats['failures'] += 1
                    
        except Exception as e:
            logger.error(f"    ❌ Exception: {e}")
            self.stats['failures'] += 1
    
    def get_price_precision(self, price):
        """Calculate appropriate decimal places based on price"""
        if price >= 1000:
            return 1
        elif price >= 100:
            return 2
        elif price >= 10:
            return 3
        elif price >= 1:
            return 4
        elif price >= 0.1:
            return 5
        elif price >= 0.01:
            return 6
        elif price >= 0.001:
            return 7
        else:
            return 8
    
    def print_stats(self):
        """Print monitor statistics"""
        runtime = (datetime.now() - self.stats['start_time']).total_seconds() / 60
        
        logger.info("\n" + "=" * 60)
        logger.info("📈 PROTECTION MONITOR STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Runtime: {runtime:.1f} minutes")
        logger.info(f"Total checks: {self.stats['checks']}")
        logger.info(f"Stop losses added: {self.stats['stop_losses_added']}")
        logger.info(f"Trailing stops added: {self.stats['trailing_stops_added']}")
        logger.info(f"Failed attempts: {self.stats['failures']}")
        logger.info("=" * 60)

async def main():
    """Main function to run the monitor"""
    monitor = EnhancedProtectionMonitor()
    
    # Initial check
    logger.info("\n🔍 Performing initial protection check...")
    await monitor.check_and_protect_positions()
    
    # Start continuous monitoring
    logger.info("\n🚀 Starting continuous monitoring...")
    try:
        await monitor.start()
    except KeyboardInterrupt:
        logger.info("\n👋 Monitor stopped by user")
        monitor.print_stats()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")