#!/usr/bin/env python3
"""
Check available signals in fas.scoring_history
"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

async def check_signals():
    """Check signals in database"""
    
    # Connect to database
    conn = await asyncpg.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT')),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    
    try:
        # Check total signals in last 10 minutes
        total_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE ABS(score_week) >= 50) as week_50,
                   COUNT(*) FILTER (WHERE ABS(score_month) >= 50) as month_50,
                   COUNT(*) FILTER (WHERE ABS(score_week) >= 50 OR ABS(score_month) >= 50) as either_50
            FROM fas.scoring_history
            WHERE created_at > NOW() - INTERVAL '10 minutes'
            AND is_active = true
        """
        
        result = await conn.fetchrow(total_query)
        print(f"\n📊 SIGNALS IN LAST 10 MINUTES:")
        print(f"Total signals: {result['total']}")
        print(f"Score Week >= 50: {result['week_50']}")
        print(f"Score Month >= 50: {result['month_50']}")
        print(f"Either >= 50: {result['either_50']}")
        
        # Check recent signals
        signals_query = """
            SELECT 
                s.id,
                s.pair_symbol,
                s.score_week,
                s.score_month,
                s.recommended_action,
                s.created_at,
                tp.contract_type_id,
                CASE 
                    WHEN tp.exchange_id = 1 THEN 'binance'
                    WHEN tp.exchange_id = 2 THEN 'bybit'
                    ELSE 'unknown'
                END as exchange
            FROM fas.scoring_history s
            JOIN public.trading_pairs tp ON tp.id = s.trading_pair_id
            WHERE s.created_at > NOW() - INTERVAL '5 minutes'
            AND s.is_active = true
            AND tp.contract_type_id = 1  -- Futures only
            AND (ABS(s.score_week) >= 50 OR ABS(s.score_month) >= 50)
            ORDER BY s.created_at DESC
            LIMIT 10
        """
        
        signals = await conn.fetch(signals_query)
        
        print(f"\n🔍 RECENT SIGNALS (Futures, Score >= 50):")
        print("-" * 80)
        for signal in signals:
            action = "BUY" if signal['recommended_action'] == 'BUY' else "SELL"
            print(f"ID: {signal['id']} | {signal['pair_symbol']:15} | {action:4} | "
                  f"Week: {float(signal['score_week']):6.2f} | Month: {float(signal['score_month']):6.2f} | "
                  f"{signal['exchange']:7} | {signal['created_at'].strftime('%H:%M:%S')}")
        
        # Check which signals are already processed
        processed_query = """
            SELECT COUNT(*) as processed
            FROM fas.scoring_history s
            WHERE s.id IN (
                SELECT signal_id FROM ats.signals WHERE signal_id IS NOT NULL
            )
            AND s.created_at > NOW() - INTERVAL '10 minutes'
        """
        
        processed = await conn.fetchrow(processed_query)
        print(f"\n✅ Already processed: {processed['processed']} signals")
        
        # Check unprocessed signals
        unprocessed_query = """
            SELECT 
                s.id,
                s.pair_symbol,
                s.score_week,
                s.score_month,
                s.recommended_action
            FROM fas.scoring_history s
            JOIN public.trading_pairs tp ON tp.id = s.trading_pair_id
            WHERE s.id NOT IN (
                SELECT signal_id FROM ats.signals WHERE signal_id IS NOT NULL
            )
            AND s.created_at > NOW() - INTERVAL '5 minutes'
            AND tp.contract_type_id = 1
            AND s.is_active = true
            AND (ABS(s.score_week) >= 50 OR ABS(s.score_month) >= 50)
            AND s.recommended_action != 'NO_TRADE'
            ORDER BY s.created_at DESC
            LIMIT 5
        """
        
        unprocessed = await conn.fetch(unprocessed_query)
        
        print(f"\n⏳ UNPROCESSED SIGNALS (ready for trading):")
        print("-" * 80)
        for signal in unprocessed:
            action = "BUY" if signal['recommended_action'] == 'BUY' else "SELL"
            print(f"ID: {signal['id']} | {signal['pair_symbol']:15} | {action:4} | "
                  f"Week: {float(signal['score_week']):6.2f} | Month: {float(signal['score_month']):6.2f}")
        
        if not unprocessed:
            print("No unprocessed signals found!")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_signals())