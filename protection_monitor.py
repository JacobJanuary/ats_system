#!/usr/bin/env python3
"""
Automatic Position Protection Monitor
Continuously monitors and protects positions
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
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PositionProtectionMonitor:
    """
    Automatic position protector that runs continuously
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
        self.check_interval = 30  # Check every 30 seconds
        self.stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', 6.5))
        self.trailing_percent = float(os.getenv('TRAILING_CALLBACK_RATE', 0.5))
        self.activation_percent = float(os.getenv('TRAILING_ACTIVATION_PERCENT', 3.5))
        
        self.running = False
        self.stats = {
            'checks': 0,
            'protected': 0,
            'failed': 0,
            'last_check': None
        }
    
    async def start(self):
        """Start the protection monitor"""
        self.running = True
        logger.info("🚀 Starting Position Protection Monitor")
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'MAINNET'}")
        logger.info(f"Check interval: {self.check_interval} seconds")
        logger.info(f"Stop Loss: {self.stop_loss_percent}%")
        logger.info(f"Trailing Stop: {self.trailing_percent}%")
        
        while self.running:
            try:
                await self.check_and_protect_positions()
                await asyncio.sleep(self.check_interval)
            except KeyboardInterrupt:
                logger.info("Stopping monitor...")
                self.running = False
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(10)
    
    async def check_and_protect_positions(self):
        """Check all positions and add protection where needed"""
        self.stats['checks'] += 1
        self.stats['last_check'] = datetime.now()
        
        # Get all positions
        positions = self.get_all_positions()
        
        if not positions:
            return
        
        unprotected = []
        missing_trailing = []
        
        for pos in positions:
            if float(pos.get('size', 0)) > 0:
                has_sl = pos.get('stopLoss') and float(pos.get('stopLoss', 0)) > 0
                has_ts = pos.get('trailingStop') and float(pos.get('trailingStop', 0)) > 0
                
                if not has_sl:
                    unprotected.append(pos)
                elif not has_ts:
                    missing_trailing.append(pos)
        
        # Log status
        total = len([p for p in positions if float(p.get('size', 0)) > 0])
        logger.info(f"📊 Check #{self.stats['checks']}: {total} positions, "
                   f"{len(unprotected)} unprotected, {len(missing_trailing)} missing TS")
        
        # Protect unprotected positions
        if unprotected:
            logger.warning(f"⚠️  Found {len(unprotected)} unprotected positions!")
            for pos in unprotected:
                await self.add_full_protection(pos)
        
        # Add trailing stops where missing (less critical)
        if missing_trailing and self.stats['checks'] % 10 == 0:  # Every 10th check
            logger.info(f"Adding trailing stops to {len(missing_trailing)} positions...")
            for pos in missing_trailing[:3]:  # Limit to 3 per cycle to avoid rate limits
                await self.add_trailing_stop(pos)
    
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
    
    async def add_full_protection(self, position):
        """Add both stop loss and trailing stop to a position"""
        symbol = position.get('symbol')
        side = position.get('side')
        entry_price = float(position.get('avgPrice', 0))
        mark_price = float(position.get('markPrice', 0))
        
        if entry_price == 0:
            logger.warning(f"Cannot protect {symbol}: no entry price")
            return
        
        logger.info(f"🛡️  Protecting {symbol} ({side})...")
        
        # Calculate stop loss
        if side == 'Buy':
            stop_loss_price = entry_price * (1 - self.stop_loss_percent / 100)
        else:
            stop_loss_price = entry_price * (1 + self.stop_loss_percent / 100)
        
        # Calculate precision
        precision = self.get_price_precision(stop_loss_price)
        stop_loss_price = round(stop_loss_price, precision)
        
        # Calculate trailing stop parameters
        trailing_distance = round(entry_price * (self.trailing_percent / 100), precision)
        
        if side == 'Buy':
            activation_price = max(mark_price, entry_price) * (1 + self.activation_percent / 100)
        else:
            activation_price = min(mark_price, entry_price) * (1 - self.activation_percent / 100)
        
        activation_price = round(activation_price, precision)
        
        try:
            # Set both protections at once
            response = self.client.set_trading_stop(
                category="linear",
                symbol=symbol,
                stopLoss=str(stop_loss_price),
                slTriggerBy="MarkPrice",
                trailingStop=str(trailing_distance),
                activePrice=str(activation_price),
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                logger.info(f"  ✅ Full protection set for {symbol}")
                self.stats['protected'] += 1
            else:
                # Try just stop loss if full protection fails
                response = self.client.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    stopLoss=str(stop_loss_price),
                    slTriggerBy="MarkPrice",
                    positionIdx=0
                )
                
                if response['retCode'] == 0:
                    logger.info(f"  ✅ Stop loss set for {symbol}")
                    self.stats['protected'] += 1
                else:
                    logger.error(f"  ❌ Failed to protect {symbol}: {response.get('retMsg')}")
                    self.stats['failed'] += 1
                    
        except Exception as e:
            logger.error(f"  ❌ Exception protecting {symbol}: {e}")
            self.stats['failed'] += 1
        
        # Small delay to avoid rate limits
        await asyncio.sleep(0.5)
    
    async def add_trailing_stop(self, position):
        """Add trailing stop to a position that already has stop loss"""
        symbol = position.get('symbol')
        side = position.get('side')
        entry_price = float(position.get('avgPrice', 0))
        mark_price = float(position.get('markPrice', 0))
        
        if entry_price == 0:
            return
        
        # Calculate parameters
        precision = self.get_price_precision(entry_price)
        trailing_distance = round(entry_price * (self.trailing_percent / 100), precision)
        
        # Only add trailing if price moved favorably
        if side == 'Buy':
            if mark_price > entry_price * 1.01:  # 1% profit
                activation_price = mark_price * 1.001
            else:
                return  # Don't add trailing yet
        else:
            if mark_price < entry_price * 0.99:  # 1% profit
                activation_price = mark_price * 0.999
            else:
                return  # Don't add trailing yet
        
        activation_price = round(activation_price, precision)
        
        try:
            response = self.client.set_trading_stop(
                category="linear",
                symbol=symbol,
                trailingStop=str(trailing_distance),
                activePrice=str(activation_price),
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                logger.info(f"  ✅ Trailing stop added to {symbol}")
            
        except:
            pass  # Trailing stop is optional
        
        await asyncio.sleep(0.5)
    
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
        print("\n" + "=" * 60)
        print("PROTECTION MONITOR STATISTICS")
        print("=" * 60)
        print(f"Total checks: {self.stats['checks']}")
        print(f"Positions protected: {self.stats['protected']}")
        print(f"Failed attempts: {self.stats['failed']}")
        if self.stats['last_check']:
            print(f"Last check: {self.stats['last_check'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

async def main():
    """Main function to run the monitor"""
    monitor = PositionProtectionMonitor()
    
    # Print initial status
    logger.info("Performing initial check...")
    await monitor.check_and_protect_positions()
    
    # Start continuous monitoring
    try:
        await monitor.start()
    except KeyboardInterrupt:
        logger.info("\nStopping monitor...")
        monitor.print_stats()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMonitor stopped by user")