#!/usr/bin/env python3
"""
Emergency Trailing Stop Protection for ALL positions
Устанавливает Trailing Stop на ВСЕ позиции без него
"""

import os
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
import logging
from datetime import datetime

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EmergencyTrailingProtection:
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
        
        # Protection settings from .env
        self.trailing_percent = float(os.getenv('TRAILING_CALLBACK_RATE', 0.5))
        self.stop_loss_percent = float(os.getenv('STOP_LOSS_PERCENT', 6.5))
        
        logger.info("=" * 80)
        logger.info("🚨 EMERGENCY TRAILING STOP PROTECTION")
        logger.info("=" * 80)
        logger.info(f"Mode: {'TESTNET' if self.testnet else 'MAINNET'}")
        logger.info(f"Trailing Stop: {self.trailing_percent}%")
        logger.info(f"Stop Loss: {self.stop_loss_percent}%")
        logger.info("=" * 80)
    
    def protect_all_positions(self):
        """Main function to add trailing stops to all positions"""
        
        # Get all positions
        logger.info("\n📋 Fetching all positions...")
        all_positions = self.get_all_positions()
        
        if not all_positions:
            logger.warning("No open positions found")
            return
        
        # Filter positions without trailing stop
        positions_without_ts = []
        positions_with_ts = []
        
        for pos in all_positions:
            if float(pos.get('size', 0)) > 0:
                trailing_stop = pos.get('trailingStop', '')
                has_ts = trailing_stop and float(trailing_stop) > 0
                
                if has_ts:
                    positions_with_ts.append(pos)
                else:
                    positions_without_ts.append(pos)
        
        logger.info(f"\n📊 Current Status:")
        logger.info(f"  Total positions: {len(all_positions)}")
        logger.info(f"  With Trailing Stop: {len(positions_with_ts)} ✅")
        logger.info(f"  WITHOUT Trailing Stop: {len(positions_without_ts)} ❌")
        
        if not positions_without_ts:
            logger.info("\n✅ All positions already have trailing stops!")
            return
        
        # Add trailing stops
        logger.info(f"\n🔧 Adding trailing stops to {len(positions_without_ts)} positions...")
        
        success_count = 0
        failed_count = 0
        failed_symbols = []
        
        for i, pos in enumerate(positions_without_ts, 1):
            symbol = pos['symbol']
            side = pos['side']
            entry_price = float(pos['avgPrice'])
            mark_price = float(pos.get('markPrice', entry_price))
            size = pos['size']
            
            logger.info(f"\n[{i}/{len(positions_without_ts)}] Processing {symbol} ({side})")
            logger.info(f"  Size: {size}, Entry: {entry_price:.4f}, Mark: {mark_price:.4f}")
            
            # Calculate PnL percentage
            if side == 'Buy':
                pnl_percent = ((mark_price - entry_price) / entry_price) * 100
            else:
                pnl_percent = ((entry_price - mark_price) / entry_price) * 100
            
            logger.info(f"  Current PnL: {pnl_percent:.2f}%")
            
            # Try to set trailing stop
            success = self.set_trailing_stop_smart(pos)
            
            if success:
                logger.info(f"  ✅ Trailing Stop added successfully!")
                success_count += 1
            else:
                logger.error(f"  ❌ Failed to add Trailing Stop")
                failed_count += 1
                failed_symbols.append(symbol)
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("📊 PROTECTION RESULTS:")
        logger.info(f"  Success: {success_count} ✅")
        logger.info(f"  Failed: {failed_count} ❌")
        
        if failed_count > 0:
            logger.warning(f"\n⚠️  Failed positions:")
            for sym in failed_symbols:
                logger.warning(f"  - {sym}")
            logger.info("\nThese may need manual review or different parameters")
        
        if success_count > 0:
            logger.info(f"\n✅ Successfully added trailing stops to {success_count} positions!")
        
        logger.info("=" * 80)
    
    def get_all_positions(self):
        """Get all open positions"""
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
            
            try:
                response = self.client.get_positions(**params)
                
                if response['retCode'] != 0:
                    logger.error(f"Error getting positions: {response.get('retMsg')}")
                    break
                
                positions = response.get('result', {}).get('list', [])
                all_positions.extend(positions)
                
                cursor = response.get('result', {}).get('nextPageCursor', '')
                if not cursor:
                    break
                    
            except Exception as e:
                logger.error(f"Exception getting positions: {e}")
                break
        
        return all_positions
    
    def set_trailing_stop_smart(self, position):
        """Set trailing stop with smart parameters based on position status"""
        symbol = position['symbol']
        side = position['side']
        entry_price = float(position['avgPrice'])
        mark_price = float(position.get('markPrice', entry_price))
        
        # Calculate price precision
        precision = self.get_price_precision(entry_price)
        
        # Calculate trailing distance (0.5% of entry price)
        trailing_distance = entry_price * (self.trailing_percent / 100)
        trailing_distance = round(trailing_distance, precision)
        
        # Ensure minimum trailing distance
        if trailing_distance == 0:
            min_tick = 10 ** (-precision)
            trailing_distance = min_tick
        
        # Calculate activation price based on position status
        if side == 'Buy':
            # For long positions
            if mark_price > entry_price * 1.001:
                # Position is in profit - activate immediately
                activation_price = mark_price * 1.0001  # Just above current price
                logger.info(f"  Position in profit, immediate activation")
            else:
                # Position at loss or breakeven - activate at breakeven + 0.1%
                activation_price = entry_price * 1.001
                logger.info(f"  Position at loss/breakeven, activation at breakeven")
        else:
            # For short positions
            if mark_price < entry_price * 0.999:
                # Position is in profit - activate immediately
                activation_price = mark_price * 0.9999  # Just below current price
                logger.info(f"  Position in profit, immediate activation")
            else:
                # Position at loss or breakeven - activate at breakeven - 0.1%
                activation_price = entry_price * 0.999
                logger.info(f"  Position at loss/breakeven, activation at breakeven")
        
        activation_price = round(activation_price, precision)
        
        logger.info(f"  Trailing Distance: {trailing_distance}")
        logger.info(f"  Activation Price: {activation_price}")
        
        try:
            # First try with both SL and TS if no SL exists
            stop_loss = position.get('stopLoss', '')
            has_sl = stop_loss and float(stop_loss) > 0
            
            if not has_sl:
                # Add both SL and TS
                if side == 'Buy':
                    stop_loss_price = entry_price * (1 - self.stop_loss_percent / 100)
                else:
                    stop_loss_price = entry_price * (1 + self.stop_loss_percent / 100)
                
                stop_loss_price = round(stop_loss_price, precision)
                
                response = self.client.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    stopLoss=str(stop_loss_price),
                    slTriggerBy="MarkPrice",
                    trailingStop=str(trailing_distance),
                    activePrice=str(activation_price),
                    positionIdx=0
                )
            else:
                # Just add TS
                response = self.client.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    trailingStop=str(trailing_distance),
                    activePrice=str(activation_price),
                    positionIdx=0
                )
            
            if response['retCode'] == 0:
                return True
            else:
                error_msg = response.get('retMsg', 'Unknown error')
                logger.error(f"  API Error: {error_msg}")
                
                # Try alternative parameters if first attempt fails
                if "price" in error_msg.lower():
                    logger.info("  Trying alternative activation price...")
                    
                    # Try with activation further from current price
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
                        logger.info("  ✅ Success with alternative parameters")
                        return True
                
                return False
                
        except Exception as e:
            logger.error(f"  Exception: {str(e)}")
            return False
    
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

def main():
    """Main function"""
    protector = EmergencyTrailingProtection()
    protector.protect_all_positions()
    
    # Final check
    print("\n📋 Verifying protection status...")
    all_positions = protector.get_all_positions()
    
    protected_count = 0
    for pos in all_positions:
        if float(pos.get('size', 0)) > 0:
            has_sl = pos.get('stopLoss') and float(pos.get('stopLoss', 0)) > 0
            has_ts = pos.get('trailingStop') and float(pos.get('trailingStop', 0)) > 0
            if has_sl or has_ts:
                protected_count += 1
    
    total = len([p for p in all_positions if float(p.get('size', 0)) > 0])
    print(f"\nFinal Status: {protected_count}/{total} positions protected ({protected_count*100/total if total else 0:.1f}%)")

if __name__ == "__main__":
    main()