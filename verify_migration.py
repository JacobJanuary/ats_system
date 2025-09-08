#!/usr/bin/env python3
"""
Verify that migration to fas.scoring_history is complete and working
"""

import asyncio
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

from core.config import SystemConfig
from database.connection import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


async def verify_migration():
    """Comprehensive migration verification"""
    
    config = SystemConfig()
    db = DatabaseManager(config)
    
    try:
        await db.initialize()
        
        logger.info("=" * 80)
        logger.info("MIGRATION VERIFICATION REPORT")
        logger.info("=" * 80)
        logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        # 1. Check configuration
        logger.info("\n1. CONFIGURATION CHECK")
        logger.info("-" * 40)
        
        use_new_source = os.getenv('USE_SCORING_HISTORY', 'false').lower() == 'true'
        logger.info(f"USE_SCORING_HISTORY: {use_new_source}")
        
        if use_new_source:
            logger.info("✅ System is configured to use NEW source (fas.scoring_history)")
        else:
            logger.warning("⚠️ System is still using OLD source (smart_ml.predictions)")
            logger.warning("   Set USE_SCORING_HISTORY=true in .env to switch")
        
        # 2. Database structure check
        logger.info("\n2. DATABASE STRUCTURE CHECK")
        logger.info("-" * 40)
        
        # Check if new columns exist in ats.signals
        check_columns = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'ats' 
            AND table_name = 'signals'
            AND column_name IN ('total_score', 'score_week', 'score_month', 'recommended_action')
        """
        
        columns = await db.pool.fetch(check_columns)
        column_names = [col['column_name'] for col in columns]
        
        required_columns = ['total_score', 'score_week', 'score_month', 'recommended_action']
        missing_columns = [col for col in required_columns if col not in column_names]
        
        if missing_columns:
            logger.warning(f"⚠️ Missing columns: {missing_columns}")
            logger.info("   Running migration...")
            await db.signals_v2.migrate_add_columns()
        else:
            logger.info("✅ All required columns present in ats.signals")
        
        # 3. Check signal availability
        logger.info("\n3. SIGNAL AVAILABILITY")
        logger.info("-" * 40)
        
        # Check old source
        old_count_query = """
            SELECT COUNT(*) as count
            FROM smart_ml.predictions p
            JOIN fas.scoring_history s ON s.id = p.signal_id
            WHERE p.created_at > NOW() - INTERVAL '1 hour'
        """
        old_count = await db.pool.fetchval(old_count_query)
        logger.info(f"Old source (smart_ml.predictions): {old_count} signals in last hour")
        
        # Check new source
        new_count_query = """
            SELECT COUNT(*) as count
            FROM fas.scoring_history
            WHERE created_at > NOW() - INTERVAL '1 hour'
            AND is_active = true
        """
        new_count = await db.pool.fetchval(new_count_query)
        logger.info(f"New source (fas.scoring_history): {new_count} signals in last hour")
        
        # 4. Test signal fetching
        logger.info("\n4. SIGNAL FETCHING TEST")
        logger.info("-" * 40)
        
        if use_new_source:
            # Test new source
            signals = await db.signals_v2.get_unprocessed_signals_legacy(limit=5)
            logger.info(f"Fetched {len(signals)} signals from NEW source")
            
            if signals:
                sample = signals[0]
                logger.info(f"Sample signal:")
                logger.info(f"  Symbol: {sample.pair_symbol}")
                logger.info(f"  Type: {sample.signal_type}")
                logger.info(f"  Exchange: {sample.exchange_name}")
                logger.info(f"  Confidence: {sample.confidence_level}")
        else:
            # Test old source
            signals = await db.signals.get_unprocessed_signals(limit=5)
            logger.info(f"Fetched {len(signals)} signals from OLD source")
        
        # 5. Check score thresholds
        logger.info("\n5. SCORE THRESHOLDS CHECK")
        logger.info("-" * 40)
        
        min_week = float(os.getenv('MIN_SCORE_WEEK', '59'))
        min_month = float(os.getenv('MIN_SCORE_MONTH', '59'))
        
        logger.info(f"MIN_SCORE_WEEK: {min_week}")
        logger.info(f"MIN_SCORE_MONTH: {min_month}")
        
        # Check actual score ranges
        score_range_query = """
            SELECT 
                MIN(ABS(score_week)) as min_week,
                MAX(ABS(score_week)) as max_week,
                AVG(ABS(score_week)) as avg_week,
                MIN(ABS(score_month)) as min_month,
                MAX(ABS(score_month)) as max_month,
                AVG(ABS(score_month)) as avg_month,
                COUNT(*) as total
            FROM fas.scoring_history
            WHERE created_at > NOW() - INTERVAL '24 hours'
            AND is_active = true
        """
        
        scores = await db.pool.fetchrow(score_range_query)
        if scores and scores['total'] > 0:
            logger.info(f"Score ranges (last 24h):")
            logger.info(f"  Weekly:  {scores['min_week']:.2f} - {scores['max_week']:.2f} (avg: {scores['avg_week']:.2f})")
            logger.info(f"  Monthly: {scores['min_month']:.2f} - {scores['max_month']:.2f} (avg: {scores['avg_month']:.2f})")
            
            # Check if thresholds are reasonable
            if min_week > scores['max_week']:
                logger.warning(f"⚠️ MIN_SCORE_WEEK ({min_week}) is higher than max score ({scores['max_week']:.2f})")
                logger.warning("   Consider lowering MIN_SCORE_WEEK")
            
            if min_month > scores['max_month']:
                logger.warning(f"⚠️ MIN_SCORE_MONTH ({min_month}) is higher than max score ({scores['max_month']:.2f})")
                logger.warning("   Consider lowering MIN_SCORE_MONTH")
        
        # 6. System status
        logger.info("\n6. SYSTEM STATUS")
        logger.info("-" * 40)
        
        # Check if main.py is using new source
        if use_new_source and db.use_new_signal_source:
            logger.info("✅ System is READY to use fas.scoring_history")
            logger.info("✅ DatabaseManager is configured for new source")
            logger.info("✅ main.py will use new signal source")
        else:
            logger.warning("⚠️ System is NOT fully migrated")
            if not use_new_source:
                logger.warning("   - Set USE_SCORING_HISTORY=true in .env")
            if not db.use_new_signal_source:
                logger.warning("   - DatabaseManager not detecting configuration")
        
        # 7. Recommendations
        logger.info("\n7. RECOMMENDATIONS")
        logger.info("-" * 40)
        
        if use_new_source and signals:
            logger.info("✅ Migration is COMPLETE and WORKING!")
            logger.info("   - Signals are being fetched from fas.scoring_history")
            logger.info("   - System is configured correctly")
            logger.info("   - Ready for production use")
        elif use_new_source and not signals:
            logger.warning("⚠️ Migration is configured but NO SIGNALS found")
            logger.warning("   Possible issues:")
            logger.warning("   1. Score thresholds too high")
            logger.warning("   2. No recent data in fas.scoring_history")
            logger.warning("   3. All signals already processed")
            logger.warning("\n   Try:")
            logger.warning("   - Lower MIN_SCORE_WEEK and MIN_SCORE_MONTH")
            logger.warning("   - Check if fas.scoring_history has recent data")
        else:
            logger.warning("⚠️ System still using OLD source")
            logger.warning("   To complete migration:")
            logger.warning("   1. Set USE_SCORING_HISTORY=true in .env")
            logger.warning("   2. Restart the application")
            logger.warning("   3. Monitor logs for any issues")
        
        logger.info("\n" + "=" * 80)
        logger.info("VERIFICATION COMPLETE")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(verify_migration())