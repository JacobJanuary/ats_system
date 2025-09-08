#!/usr/bin/env python3
"""
Analyze database structure and compare old vs new signal sources
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


async def analyze_database_structure():
    """Analyze the structure of both old and new tables"""
    
    # Database connection parameters
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    conn = None
    try:
        conn = await asyncpg.connect(**db_config)
        
        logger.info("=" * 80)
        logger.info("DATABASE STRUCTURE ANALYSIS")
        logger.info("=" * 80)
        
        # 1. Analyze smart_ml.predictions structure (OLD)
        logger.info("\n1. OLD TABLE: smart_ml.predictions")
        logger.info("-" * 40)
        
        predictions_columns = await conn.fetch("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'smart_ml' 
            AND table_name = 'predictions'
            ORDER BY ordinal_position
        """)
        
        if predictions_columns:
            logger.info("Columns:")
            for col in predictions_columns:
                logger.info(f"  - {col['column_name']}: {col['data_type']} "
                          f"(nullable: {col['is_nullable']})")
        else:
            logger.warning("Table smart_ml.predictions not found!")
        
        # Sample data from predictions
        sample_predictions = await conn.fetch("""
            SELECT * FROM smart_ml.predictions 
            ORDER BY created_at DESC 
            LIMIT 2
        """)
        
        if sample_predictions:
            logger.info("\nSample data:")
            for row in sample_predictions:
                logger.info(f"  ID: {row['id']}, Signal: {row['signal_id']}, "
                          f"Prediction: {row['prediction']}, "
                          f"Created: {row['created_at']}")
        
        # 2. Analyze fas.scoring_history structure (NEW)
        logger.info("\n2. NEW TABLE: fas.scoring_history")
        logger.info("-" * 40)
        
        scoring_columns = await conn.fetch("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'fas' 
            AND table_name = 'scoring_history'
            ORDER BY ordinal_position
        """)
        
        if scoring_columns:
            logger.info("Columns:")
            for col in scoring_columns:
                logger.info(f"  - {col['column_name']}: {col['data_type']} "
                          f"(nullable: {col['is_nullable']})")
        else:
            logger.warning("Table fas.scoring_history not found!")
        
        # Sample data from scoring_history
        sample_scoring = await conn.fetch("""
            SELECT * FROM fas.scoring_history 
            ORDER BY created_at DESC 
            LIMIT 2
        """)
        
        if sample_scoring:
            logger.info("\nSample data:")
            for row in sample_scoring:
                logger.info(f"  ID: {row.get('id')}, Symbol: {row.get('pair_symbol')}, "
                          f"Created: {row.get('created_at')}")
                # Print all columns for analysis
                for key, value in row.items():
                    if value is not None:
                        logger.info(f"    {key}: {value}")
        
        # 3. Check if we can get signals directly from fas.scoring_history
        logger.info("\n3. SIGNAL EXTRACTION TEST")
        logger.info("-" * 40)
        
        # Try to get signals directly from scoring_history
        test_query = """
            SELECT 
                s.id,
                s.pair_symbol,
                s.trading_pair_id,
                s.created_at,
                s.score,
                s.score_weekly,
                s.score_monthly,
                s.model_name,
                s.model_version,
                tp.exchange_id,
                CASE 
                    WHEN tp.exchange_id = 1 THEN 'binance'
                    WHEN tp.exchange_id = 2 THEN 'bybit'
                    ELSE 'unknown'
                END as exchange_name,
                -- Determine signal type based on score
                CASE 
                    WHEN s.score > 0.5 THEN 'BUY'
                    WHEN s.score <= 0.5 THEN 'SELL'
                    ELSE 'UNKNOWN'
                END as signal_type
            FROM fas.scoring_history s
            JOIN public.trading_pairs tp ON tp.id = s.trading_pair_id
            WHERE s.created_at > NOW() - INTERVAL '1 hour'
            AND tp.contract_type_id = 1  -- Only futures
            ORDER BY s.created_at DESC
            LIMIT 5
        """
        
        test_signals = await conn.fetch(test_query)
        
        if test_signals:
            logger.info("✅ Can extract signals from fas.scoring_history!")
            logger.info("\nSample signals:")
            for signal in test_signals:
                logger.info(f"  - {signal['pair_symbol']} on {signal['exchange_name']}: "
                          f"{signal['signal_type']} (score: {signal['score']:.4f})")
        else:
            logger.warning("No recent signals found in fas.scoring_history")
        
        # 4. Check relationships
        logger.info("\n4. RELATIONSHIP ANALYSIS")
        logger.info("-" * 40)
        
        # Check if predictions table references scoring_history
        relationship_check = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_predictions,
                COUNT(DISTINCT p.signal_id) as unique_signals,
                COUNT(DISTINCT s.id) as scoring_records
            FROM smart_ml.predictions p
            LEFT JOIN fas.scoring_history s ON s.id = p.signal_id
            WHERE p.created_at > NOW() - INTERVAL '1 day'
        """)
        
        if relationship_check:
            logger.info(f"Predictions in last 24h: {relationship_check['total_predictions']}")
            logger.info(f"Unique signal_ids: {relationship_check['unique_signals']}")
            logger.info(f"Matching scoring records: {relationship_check['scoring_records']}")
        
        # 5. Propose new structure
        logger.info("\n5. PROPOSED NEW SIGNAL QUERY")
        logger.info("-" * 40)
        
        new_query = """
        -- NEW: Direct from fas.scoring_history
        SELECT 
            s.id as signal_id,
            s.pair_symbol,
            s.trading_pair_id,
            s.score,
            s.score_weekly,
            s.score_monthly,
            s.model_name,
            s.model_version,
            s.created_at,
            tp.exchange_id,
            CASE 
                WHEN tp.exchange_id = 1 THEN 'binance'
                WHEN tp.exchange_id = 2 THEN 'bybit'
                ELSE 'unknown'
            END as exchange_name,
            CASE 
                WHEN s.score > 0.5 THEN 'BUY'
                ELSE 'SELL'
            END as signal_type,
            s.score as prediction_proba,
            CASE 
                WHEN s.score > 0.8 THEN 'HIGH'
                WHEN s.score > 0.6 THEN 'MEDIUM'
                ELSE 'LOW'
            END as confidence_level
        FROM fas.scoring_history s
        JOIN public.trading_pairs tp ON tp.id = s.trading_pair_id
        WHERE s.id NOT IN (
            SELECT signal_id FROM ats.signals WHERE signal_id IS NOT NULL
        )
        AND s.created_at > NOW() - INTERVAL '5 minutes'
        AND tp.contract_type_id = 1
        ORDER BY s.created_at DESC
        """
        
        logger.info("Proposed query structure:")
        logger.info(new_query)
        
    except Exception as e:
        logger.error(f"Error analyzing database: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            await conn.close()


if __name__ == "__main__":
    asyncio.run(analyze_database_structure())