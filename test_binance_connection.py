#!/usr/bin/env python3
"""Test Binance connection and signal processing"""

import asyncio
import os
from dotenv import load_dotenv
from exchanges.binance import BinanceExchange
from database.signal_repository_v2 import SignalRepositoryV2
from database.connection import DatabaseManager
from core.config import SystemConfig
from datetime import datetime, timezone

load_dotenv()

async def test():
    print("\n" + "="*60)
    print("TESTING BINANCE AND SIGNAL PROCESSING")
    print("="*60)
    
    # Test Binance connection
    print("\n1. Testing Binance connection...")
    config = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'testnet': True
    }
    
    try:
        exchange = BinanceExchange(config)
        await exchange.initialize()
        
        # Test market data
        market_data = await exchange.get_market_data('BTCUSDT')
        print(f"   ✅ BTC price: ${market_data.last_price if market_data else 'Error'}")
        
        # Test balance
        balance = await exchange.get_account_balance()
        if balance:
            print(f"   ✅ USDT balance: {balance.free}")
        else:
            print("   ⚠️ Could not get balance")
        
        await exchange.close()
    except Exception as e:
        print(f"   ❌ Binance error: {e}")
    
    # Test signal processing
    print("\n2. Testing signal repository...")
    try:
        system_config = SystemConfig()
        db = DatabaseManager(system_config)
        await db.initialize()
        
        signal_repo = SignalRepositoryV2(db.pool)
        
        # Get recent signals
        signals = await signal_repo.get_unprocessed_signals_v2()
        print(f"   Found {len(signals)} unprocessed signals")
        
        if signals:
            print(f"\n   Recent signals (MIN_SCORE_WEEK={os.getenv('MIN_SCORE_WEEK', 50)}, MIN_SCORE_MONTH={os.getenv('MIN_SCORE_MONTH', 50)}):")
            for signal in signals[:5]:
                print(f"   - {signal.symbol}: score_week={signal.score_week}, score_month={signal.score_month}")
        
        await db.close()
    except Exception as e:
        print(f"   ❌ Signal repository error: {e}")
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(test())