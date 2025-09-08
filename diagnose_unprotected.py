#!/usr/bin/env python3
"""
Detailed diagnostics for unprotected positions
"""

import os
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from datetime import datetime
from decimal import Decimal

load_dotenv()

class ProtectionDiagnostics:
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
        
    def analyze_all_positions(self):
        """Analyze all open positions for protection status"""
        
        print("\n" + "=" * 80)
        print(f"POSITION PROTECTION ANALYSIS - {'TESTNET' if self.testnet else 'MAINNET'}")
        print("=" * 80)
        
        # Get all positions
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
                print(f"Error getting positions: {response.get('retMsg')}")
                break
            
            positions = response.get('result', {}).get('list', [])
            all_positions.extend(positions)
            
            cursor = response.get('result', {}).get('nextPageCursor', '')
            if not cursor:
                break
        
        # Filter active positions
        open_positions = [p for p in all_positions if float(p.get('size', 0)) > 0]
        
        print(f"Total open positions: {len(open_positions)}")
        
        # Analyze protection
        protected_sl = []
        protected_tp = []
        protected_ts = []
        unprotected_sl = []
        unprotected_ts = []
        fully_protected = []
        partially_protected = []
        completely_unprotected = []
        
        for pos in open_positions:
            symbol = pos.get('symbol')
            size = float(pos.get('size', 0))
            entry_price = float(pos.get('avgPrice', 0))
            mark_price = float(pos.get('markPrice', 0))
            side = pos.get('side')
            
            # Check protection
            stop_loss = pos.get('stopLoss', '')
            take_profit = pos.get('takeProfit', '')
            trailing_stop = pos.get('trailingStop', '')
            
            has_sl = stop_loss and float(stop_loss) > 0
            has_tp = take_profit and float(take_profit) > 0
            has_ts = trailing_stop and float(trailing_stop) > 0
            
            protection_status = {
                'symbol': symbol,
                'side': side,
                'size': size,
                'entry_price': entry_price,
                'mark_price': mark_price,
                'has_sl': has_sl,
                'sl_price': stop_loss,
                'has_tp': has_tp,
                'tp_price': take_profit,
                'has_ts': has_ts,
                'ts_distance': trailing_stop,
                'created_time': pos.get('createdTime'),
                'updated_time': pos.get('updatedTime')
            }
            
            if has_sl:
                protected_sl.append(protection_status)
            else:
                unprotected_sl.append(protection_status)
            
            if has_tp:
                protected_tp.append(protection_status)
            
            if has_ts:
                protected_ts.append(protection_status)
            else:
                unprotected_ts.append(protection_status)
            
            # Classification
            if has_sl and has_ts:
                fully_protected.append(protection_status)
            elif has_sl or has_ts or has_tp:
                partially_protected.append(protection_status)
            else:
                completely_unprotected.append(protection_status)
        
        # Print statistics
        print(f"\n📊 PROTECTION STATISTICS:")
        print(f"  ✅ With Stop Loss: {len(protected_sl)}/{len(open_positions)} ({len(protected_sl)*100/len(open_positions) if open_positions else 0:.1f}%)")
        print(f"  ✅ With Take Profit: {len(protected_tp)}/{len(open_positions)} ({len(protected_tp)*100/len(open_positions) if open_positions else 0:.1f}%)")
        print(f"  ✅ With Trailing Stop: {len(protected_ts)}/{len(open_positions)} ({len(protected_ts)*100/len(open_positions) if open_positions else 0:.1f}%)")
        print(f"  ✅ Fully Protected (SL+TS): {len(fully_protected)}/{len(open_positions)} ({len(fully_protected)*100/len(open_positions) if open_positions else 0:.1f}%)")
        
        # Analyze problematic positions
        if unprotected_sl:
            print(f"\n❌ POSITIONS WITHOUT STOP LOSS ({len(unprotected_sl)}):")
            for pos in unprotected_sl:
                age_minutes = self._calculate_position_age(pos['created_time'])
                print(f"  - {pos['symbol']} ({pos['side']}): size={pos['size']}, age={age_minutes:.1f} min")
                print(f"    Entry: {pos['entry_price']}, Mark: {pos['mark_price']}")
        
        if unprotected_ts:
            print(f"\n⚠️  POSITIONS WITHOUT TRAILING STOP ({len(unprotected_ts)}):")
            for i, pos in enumerate(unprotected_ts[:10]):  # Show first 10
                age_minutes = self._calculate_position_age(pos['created_time'])
                pnl_percent = 0
                if pos['entry_price'] > 0:
                    if pos['side'] == 'Buy':
                        pnl_percent = (pos['mark_price'] - pos['entry_price']) / pos['entry_price'] * 100
                    else:
                        pnl_percent = (pos['entry_price'] - pos['mark_price']) / pos['entry_price'] * 100
                
                print(f"  {i+1}. {pos['symbol']} ({pos['side']})")
                print(f"     Size: {pos['size']}, Age: {age_minutes:.1f} min, PnL: {pnl_percent:.2f}%")
                print(f"     SL: {pos['sl_price'] if pos['has_sl'] else 'None'}")
            
            if len(unprotected_ts) > 10:
                print(f"  ... and {len(unprotected_ts) - 10} more")
        
        # Pattern analysis
        print(f"\n🔍 PATTERN ANALYSIS:")
        
        # Check by creation time
        recent_positions = [p for p in open_positions if self._calculate_position_age(p.get('createdTime')) < 60]
        old_positions = [p for p in open_positions if self._calculate_position_age(p.get('createdTime')) > 60]
        
        if recent_positions:
            recent_protected = sum(1 for p in recent_positions if self._has_full_protection(p))
            print(f"  Recent positions (<1h): {recent_protected}/{len(recent_positions)} fully protected")
        
        if old_positions:
            old_protected = sum(1 for p in old_positions if self._has_full_protection(p))
            print(f"  Old positions (>1h): {old_protected}/{len(old_positions)} fully protected")
        
        # Check if trailing stops are failing for specific reasons
        print(f"\n🔬 TRAILING STOP FAILURE ANALYSIS:")
        ts_failure_reasons = {}
        
        for pos in unprotected_ts[:5]:  # Analyze first 5
            print(f"\nTesting TS setup for {pos['symbol']}...")
            reason = self.test_trailing_stop_setup(pos)
            if reason != "Success":
                if reason not in ts_failure_reasons:
                    ts_failure_reasons[reason] = []
                ts_failure_reasons[reason].append(pos['symbol'])
        
        if ts_failure_reasons:
            print("\n📋 TS Failure Reasons Summary:")
            for reason, symbols in ts_failure_reasons.items():
                print(f"  - {reason}: {', '.join(symbols[:3])}")
        
        return {
            'total': len(open_positions),
            'protected_sl': len(protected_sl),
            'protected_ts': len(protected_ts),
            'unprotected_sl': unprotected_sl,
            'unprotected_ts': unprotected_ts,
            'completely_unprotected': completely_unprotected
        }
    
    def _calculate_position_age(self, created_time):
        """Calculate position age in minutes"""
        if not created_time:
            return 0
        try:
            created = datetime.fromtimestamp(int(created_time) / 1000)
            age = (datetime.now() - created).total_seconds() / 60
            return age
        except:
            return 0
    
    def _has_full_protection(self, position):
        """Check if position has full protection"""
        has_sl = position.get('stopLoss') and float(position.get('stopLoss', 0)) > 0
        has_ts = position.get('trailingStop') and float(position.get('trailingStop', 0)) > 0
        return has_sl and has_ts
    
    def test_trailing_stop_setup(self, position):
        """Test why trailing stop cannot be set"""
        symbol = position['symbol']
        entry_price = position['entry_price']
        mark_price = position['mark_price']
        side = position['side']
        
        if entry_price == 0:
            return "No entry price"
        
        # Calculate trailing parameters
        trailing_distance = entry_price * 0.005  # 0.5%
        
        # For Buy positions
        if side == 'Buy':
            # Activation should be above current price
            if mark_price > entry_price * 1.001:
                # Price already moved up, activate immediately
                activation_price = mark_price * 1.001
            else:
                # Price below entry, set activation above entry
                activation_price = entry_price * 1.005
        else:
            # For Sell positions
            if mark_price < entry_price * 0.999:
                # Price already moved down, activate immediately
                activation_price = mark_price * 0.999
            else:
                # Price above entry, set activation below entry
                activation_price = entry_price * 0.995
        
        # Try to set trailing stop
        try:
            response = self.client.set_trading_stop(
                category="linear",
                symbol=symbol,
                trailingStop=str(round(trailing_distance, 4)),
                activePrice=str(round(activation_price, 2)),
                positionIdx=0
            )
            
            if response['retCode'] == 0:
                print(f"  ✅ TS can be set for {symbol}")
                return "Success"
            else:
                error_msg = response.get('retMsg', 'Unknown error')
                print(f"  ❌ TS failed for {symbol}: {error_msg}")
                
                # Analyze error
                if "not modified" in error_msg:
                    return "Already has TS"
                elif "price" in error_msg.lower():
                    return "Price validation error"
                elif "10_pcnt" in error_msg:
                    return "Distance too large (>10%)"
                elif "min_price" in error_msg.lower():
                    return "Price too small"
                else:
                    return f"API Error: {response['retCode']}"
                    
        except Exception as e:
            return f"Exception: {str(e)}"

def main():
    diagnostics = ProtectionDiagnostics()
    result = diagnostics.analyze_all_positions()
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    if result['completely_unprotected']:
        print(f"⚠️  {len(result['completely_unprotected'])} positions have NO protection!")
        print("These need immediate attention:")
        for pos in result['completely_unprotected']:
            print(f"  - {pos['symbol']} ({pos['side']})")
    
    missing_ts = len(result['unprotected_ts'])
    if missing_ts > 0:
        print(f"\n⚠️  {missing_ts} positions lack trailing stops")
        print("This may be due to:")
        print("  1. Price hasn't moved enough for activation")
        print("  2. API limitations on testnet")
        print("  3. Incorrect activation price calculation")

if __name__ == "__main__":
    main()