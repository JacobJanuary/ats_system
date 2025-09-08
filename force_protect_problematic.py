#!/usr/bin/env python3
"""
Force protect problematic positions with alternative strategies
"""
import asyncio
import os
import logging
from decimal import Decimal
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ForceProtector:
    """Force protection for problematic positions with alternative parameters"""
    
    def __init__(self):
        self.api_key = os.getenv('BYBIT_API_KEY')
        self.api_secret = os.getenv('BYBIT_API_SECRET')
        self.testnet = os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
        
        # Initialize Bybit client
        self.client = HTTP(
            testnet=self.testnet,
            api_key=self.api_key,
            api_secret=self.api_secret
        )
        
        # Problematic symbols
        self.problematic_symbols = ['RUNEUSDT', 'SOLUSDT', 'BOMEUSDT']
        
    async def protect_problematic_positions(self):
        """Protect problematic positions with different strategies"""
        
        logger.info("🔧 Starting force protection for problematic positions")
        logger.info(f"Target symbols: {', '.join(self.problematic_symbols)}")
        
        for symbol in self.problematic_symbols:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {symbol}")
            logger.info(f"{'='*50}")
            
            # Get position info
            position = self.get_position_info(symbol)
            
            if not position:
                logger.warning(f"Position {symbol} not found")
                continue
            
            size = float(position.get('size', 0))
            if size == 0:
                logger.info(f"Position {symbol} is closed")
                continue
            
            # Try different protection strategies
            success = False
            
            # Strategy 1: Standard parameters
            logger.info("Trying Strategy 1: Standard parameters (SL=6.5%, TS=0.5%)")
            success = await self.try_protection_strategy(
                position,
                sl_percent=6.5,
                ts_percent=0.5
            )
            
            if not success:
                # Strategy 2: Increased distances
                logger.info("Trying Strategy 2: Increased distances (SL=8%, TS=1%)")
                success = await self.try_protection_strategy(
                    position,
                    sl_percent=8.0,
                    ts_percent=1.0
                )
            
            if not success:
                # Strategy 3: Minimal trailing
                logger.info("Trying Strategy 3: Minimal trailing (SL=10%, TS=0.2%)")
                success = await self.try_protection_strategy(
                    position,
                    sl_percent=10.0,
                    ts_percent=0.2
                )
            
            if not success:
                # Strategy 4: Only Stop Loss
                logger.info("Trying Strategy 4: Only Stop Loss (SL=7%)")
                success = await self.set_stop_loss_only(position, sl_percent=7.0)
                
                if success:
                    logger.info(f"✅ {symbol}: Set Stop Loss only (TS failed)")
            else:
                logger.info(f"✅ {symbol}: Successfully protected with SL+TS")
            
            if not success:
                logger.error(f"❌ {symbol}: All protection strategies failed")
            
            # Delay between positions
            await asyncio.sleep(1)
        
        logger.info("\n" + "="*50)
        logger.info("Force protection complete")
        logger.info("="*50)
    
    async def try_protection_strategy(self, position, sl_percent, ts_percent):
        """Try to set protection with given parameters"""
        
        symbol = position.get('symbol')
        side = position.get('side')
        entry_price = float(position.get('avgPrice', 0))
        mark_price = float(position.get('markPrice', 0))
        
        if entry_price == 0:
            logger.error(f"No entry price for {symbol}")
            return False
        
        # Calculate Stop Loss price
        if side == 'Buy':
            sl_price = entry_price * (1 - sl_percent / 100)
        else:
            sl_price = entry_price * (1 + sl_percent / 100)
        
        # Round values based on symbol
        sl_price = self.round_price(symbol, sl_price)
        
        # Set protection via position endpoint
        try:
            # Use set_trading_stop for both SL and TS
            params = {
                "category": "linear",
                "symbol": symbol,
                "stopLoss": str(sl_price),
                "slTriggerBy": "MarkPrice",
                "trailingStop": str(int(ts_percent * 100)),  # Bybit uses basis points (0.5% = 50)
                "activePrice": str(mark_price),
                "positionIdx": 0
            }
            
            response = self.client.set_trading_stop(**params)
            
            if response['retCode'] == 0:
                logger.info(f"  ✅ Protection set: SL={sl_price}, TS={ts_percent}%")
                return True
            else:
                logger.warning(f"  ❌ Failed: {response.get('retMsg', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"  ❌ Error: {e}")
            return False
    
    async def set_stop_loss_only(self, position, sl_percent):
        """Set only Stop Loss"""
        symbol = position.get('symbol')
        side = position.get('side')
        entry_price = float(position.get('avgPrice', 0))
        
        if side == 'Buy':
            sl_price = entry_price * (1 - sl_percent / 100)
        else:
            sl_price = entry_price * (1 + sl_percent / 100)
        
        sl_price = self.round_price(symbol, sl_price)
        
        try:
            params = {
                "category": "linear",
                "symbol": symbol,
                "stopLoss": str(sl_price),
                "slTriggerBy": "MarkPrice",
                "positionIdx": 0
            }
            
            response = self.client.set_trading_stop(**params)
            
            if response['retCode'] == 0:
                logger.info(f"  ✅ Stop Loss set: {sl_price}")
                return True
            else:
                logger.warning(f"  ❌ Failed: {response.get('retMsg', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"  ❌ Error: {e}")
            return False
    
    def round_price(self, symbol, price):
        """Round price based on symbol requirements"""
        # Special handling for different symbols
        if 'RUNE' in symbol:
            return round(price, 3)
        elif 'SOL' in symbol:
            return round(price, 2)
        elif 'BOME' in symbol:
            return round(price, 6)
        elif '1000000' in symbol:
            return round(price, 7)
        elif '10000' in symbol or '1000' in symbol:
            return round(price, 6)
        else:
            return round(price, 4)
    
    def get_position_info(self, symbol):
        """Get position information"""
        try:
            response = self.client.get_positions(
                category="linear",
                symbol=symbol
            )
            
            if response['retCode'] == 0 and response['result']['list']:
                return response['result']['list'][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting position {symbol}: {e}")
            return None


async def main():
    """Main function"""
    protector = ForceProtector()
    await protector.protect_problematic_positions()


if __name__ == "__main__":
    asyncio.run(main())