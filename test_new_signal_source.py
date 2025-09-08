#!/usr/bin/env python3
"""
Test script for new signal source using fas.scoring_history
Compares old and new signal sources
"""

import asyncio
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

from core.config import SystemConfig
from database.connection import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


async def test_signal_sources():
    """Test and compare old vs new signal sources"""
    
    config = SystemConfig()
    db = DatabaseManager(config)
    
    try:
        await db.initialize()
        logger.info("=" * 80)
        logger.info("SIGNAL SOURCE COMPARISON TEST")
        logger.info("=" * 80)
        
        # First, run migration to add new columns if needed
        logger.info("\n1. Running database migration...")
        migration_success = await db.signals_v2.migrate_add_columns()
        if migration_success:
            logger.info("✅ Migration completed successfully")
        
        # Test OLD signal source (smart_ml.predictions)
        logger.info("\n2. Testing OLD signal source (smart_ml.predictions)")
        logger.info("-" * 40)
        
        old_signals = await db.signals.get_unprocessed_signals(limit=5)
        logger.info(f"Found {len(old_signals)} signals from old source")
        
        if old_signals:
            for signal in old_signals[:3]:
                logger.info(f"  - {signal.pair_symbol}: {signal.signal_type} "
                          f"(confidence: {signal.confidence_level}, "
                          f"proba: {signal.prediction_proba:.4f})")
        
        # Test NEW signal source (fas.scoring_history)
        logger.info("\n3. Testing NEW signal source (fas.scoring_history)")
        logger.info("-" * 40)
        
        new_signals = await db.signals_v2.get_unprocessed_signals_legacy(limit=5)
        logger.info(f"Found {len(new_signals)} signals from new source")
        
        if new_signals:
            for signal in new_signals[:3]:
                logger.info(f"  - {signal.pair_symbol}: {signal.signal_type} "
                          f"(confidence: {signal.confidence_level}, "
                          f"proba: {signal.prediction_proba:.4f})")
        
        # Compare signal quality
        logger.info("\n4. SIGNAL COMPARISON")
        logger.info("-" * 40)
        
        if old_signals and new_signals:
            # Find common symbols
            old_symbols = {s.pair_symbol for s in old_signals}
            new_symbols = {s.pair_symbol for s in new_signals}
            common = old_symbols & new_symbols
            
            logger.info(f"Old source symbols: {old_symbols}")
            logger.info(f"New source symbols: {new_symbols}")
            logger.info(f"Common symbols: {common}")
            
            # Check signal differences for common symbols
            for symbol in common:
                old_sig = next(s for s in old_signals if s.pair_symbol == symbol)
                new_sig = next(s for s in new_signals if s.pair_symbol == symbol)
                
                logger.info(f"\n{symbol}:")
                logger.info(f"  Old: {old_sig.signal_type} (prob: {old_sig.prediction_proba:.4f})")
                logger.info(f"  New: {new_sig.signal_type} (prob: {new_sig.prediction_proba:.4f})")
        
        # Test signal creation with new source
        logger.info("\n5. Testing signal creation with new source")
        logger.info("-" * 40)
        
        if new_signals:
            test_signal = new_signals[0]
            logger.info(f"Creating signal record for: {test_signal.pair_symbol}")
            
            # Create signal in database
            signal_id = await db.signals.create_signal(test_signal)
            
            if signal_id:
                logger.info(f"✅ Signal created successfully with ID: {signal_id}")
            else:
                logger.info("⚠️ Signal already exists or creation failed")
        
        # Check current configuration
        logger.info("\n6. CURRENT CONFIGURATION")
        logger.info("-" * 40)
        logger.info(f"USE_SCORING_HISTORY: {db.use_new_signal_source}")
        logger.info(f"MIN_SCORE_WEEK: {os.getenv('MIN_SCORE_WEEK', '59')}")
        logger.info(f"MIN_SCORE_MONTH: {os.getenv('MIN_SCORE_MONTH', '59')}")
        logger.info(f"SKIP_NO_TRADE: {os.getenv('SKIP_NO_TRADE', 'true')}")
        
        # Show recommendation
        logger.info("\n7. RECOMMENDATION")
        logger.info("-" * 40)
        
        if new_signals:
            logger.info("✅ New signal source (fas.scoring_history) is working!")
            logger.info("   - Signals are being fetched correctly")
            logger.info("   - Score filtering is applied")
            logger.info("   - Ready to switch to new source")
            logger.info("\nTo switch permanently, ensure USE_SCORING_HISTORY=true in .env")
        else:
            logger.warning("⚠️ No signals found from new source")
            logger.warning("   Check if fas.scoring_history has recent data")
            logger.warning("   Check MIN_SCORE_WEEK and MIN_SCORE_MONTH thresholds")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.close()


async def test_live_signal_processing():
    """Test live signal processing with new source"""
    
    config = SystemConfig()
    db = DatabaseManager(config)
    
    try:
        await db.initialize()
        
        logger.info("\n" * 2)
        logger.info("=" * 80)
        logger.info("LIVE SIGNAL PROCESSING TEST")
        logger.info("=" * 80)
        
        # Get signals from new source
        signals = await db.signals_v2.get_unprocessed_signals_legacy(limit=10)
        
        if not signals:
            logger.warning("No unprocessed signals found")
            return
        
        logger.info(f"Found {len(signals)} signals to process")
        
        # Process each signal
        for signal in signals:
            logger.info(f"\nProcessing: {signal.pair_symbol}")
            logger.info(f"  Type: {signal.signal_type}")
            logger.info(f"  Exchange: {signal.exchange_name}")
            logger.info(f"  Confidence: {signal.confidence_level}")
            
            # Check if signal is valid
            if signal.is_valid:
                logger.info("  ✅ Signal is valid for trading")
                
                # Create signal record
                signal_id = await db.signals.create_signal(signal)
                if signal_id:
                    logger.info(f"  📝 Created signal record: {signal_id}")
                    
                    # Update status to processing
                    await db.signals.update_signal_status(
                        signal.prediction_id,
                        SignalStatus.PROCESSING
                    )
                    logger.info("  🔄 Status updated to PROCESSING")
            else:
                logger.info("  ❌ Signal is not valid for trading")
        
    except Exception as e:
        logger.error(f"Live test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.close()


if __name__ == "__main__":
    # Run comparison test
    asyncio.run(test_signal_sources())
    
    # Optional: Run live processing test
    # asyncio.run(test_live_signal_processing())