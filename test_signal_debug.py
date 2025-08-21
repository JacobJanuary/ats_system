#!/usr/bin/env python3
"""Debug test signal creation"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()


async def test_single_signal():
    conn = await asyncpg.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    try:
        # Test parameters
        symbol = 'APTUSDT'
        exchange_id = 1

        # 1. Check/create trading pair
        print(f"1. Checking trading pair for {symbol}...")

        check_query = """
            SELECT id FROM public.trading_pairs 
            WHERE pair_symbol = $1::varchar 
            AND exchange_id = $2::integer 
            AND contract_type_id = 1
            LIMIT 1
        """

        pair_id = await conn.fetchval(check_query, symbol, exchange_id)
        print(f"   Existing pair_id: {pair_id}")

        if not pair_id:
            print(f"   Creating new trading pair...")
            create_query = """
                INSERT INTO public.trading_pairs (
                    token_id, exchange_id, pair_symbol, 
                    contract_type_id, is_active, created_at
                ) VALUES (
                    1, $1::integer, $2::varchar, 1, true, NOW()
                )
                RETURNING id
            """
            pair_id = await conn.fetchval(create_query, exchange_id, symbol)
            print(f"   Created pair_id: {pair_id}")

        # 2. Create scoring history
        print(f"2. Creating scoring history...")
        scoring_query = """
            INSERT INTO fas.scoring_history (
                timestamp, trading_pair_id, pair_symbol,
                total_score, pattern_score, combination_score,
                indicator_score, created_at, is_active
            ) VALUES (
                NOW(), $1::integer, $2::varchar, 
                75.5, 80.0, 70.0, 85.0, 
                NOW(), true
            )
            RETURNING id
        """

        signal_id = await conn.fetchval(scoring_query, pair_id, symbol)
        print(f"   Created signal_id: {signal_id}")

        # 3. Create prediction
        print(f"3. Creating prediction...")
        prediction_query = """
            INSERT INTO smart_ml.predictions (
                signal_id, model_name, market_regime,
                signal_type, prediction_proba, prediction,
                confidence_level, created_at
            ) VALUES (
                $1::bigint, 'test_model', 'normal', 
                'BUY', 0.9, true, 
                'HIGH', NOW()
            )
            RETURNING id
        """

        prediction_id = await conn.fetchval(prediction_query, signal_id)
        print(f"   Created prediction_id: {prediction_id}")

        print("\n✅ Test signal created successfully!")
        print(f"   Symbol: {symbol}")
        print(f"   Prediction ID: {prediction_id}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(test_single_signal())