#!/usr/bin/env python3
"""
ATS 2.0 - Test Signal Emulator
Generates test signals for system testing
"""
import asyncio
import asyncpg
import os
import random
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Test signals data
TEST_SIGNALS = [
    # Format: (symbol, signal_type, prediction_proba, confidence_level)
    ('ETHUSDT', 'SELL', 0.9533, 'LOW'),
    ('1000BONKUSDT', 'BUY', 0.8847, 'LOW'),
    ('1000SHIBUSDT', 'SELL', 0.8133, 'LOW'),
    ('BTCUSDT', 'BUY', 0.8297, 'HIGH'),  # Added high confidence
    ('SOLUSDT', 'SELL', 0.8248, 'MEDIUM'),  # Added medium confidence
    ('FUNUSDT', 'SELL', 0.7839, 'LOW'),
    ('ADAUSDT', 'SELL', 0.7972, 'LOW'),
    ('BNBUSDT', 'BUY', 0.8805, 'HIGH'),  # Added high confidence
    ('1000FLOKIUSDT', 'SELL', 0.8314, 'LOW'),
    ('DOGEUSDT', 'BUY', 0.9754, 'HIGH'),  # High confidence
    ('XRPUSDT', 'SELL', 0.7856, 'LOW'),
    ('AVAXUSDT', 'BUY', 0.8088, 'MEDIUM'),
    ('ONDOUSDT', 'SELL', 0.7904, 'LOW'),
    ('MATICUSDT', 'BUY', 0.8785, 'MEDIUM'),
    ('DOTUSDT', 'SELL', 0.7933, 'LOW'),
    ('LINKUSDT', 'BUY', 0.7820, 'LOW'),
    ('UNIUSDT', 'SELL', 0.8051, 'LOW'),
    ('WLDUSDT', 'SELL', 0.8331, 'LOW'),
    ('TONUSDT', 'SELL', 0.8423, 'LOW'),
    ('NEARUSDT', 'BUY', 0.8301, 'MEDIUM'),
    ('XMRUSDT', 'SELL', 0.8223, 'LOW'),
    ('KASUSDT', 'SELL', 0.8235, 'LOW'),
    ('ATOMUSDT', 'BUY', 0.9682, 'HIGH'),
    ('ARBUSDT', 'BUY', 0.8772, 'MEDIUM'),
    ('OPUSDT', 'SELL', 0.7915, 'LOW'),
    ('APTUSDT', 'SELL', 0.8143, 'LOW'),
    ('SUIUSDT', 'BUY', 0.8873, 'HIGH'),
    ('SEIUSDT', 'SELL', 0.8170, 'LOW'),
    ('INJUSDT', 'BUY', 0.9763, 'HIGH'),
    ('TIAUSDT', 'SELL', 0.8128, 'LOW'),
]


class SignalEmulator:
    """Emulates trading signals for testing"""

    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.conn = None

    async def connect(self):
        """Connect to database"""
        self.conn = await asyncpg.connect(**self.db_config)
        logger.info("Connected to database")

    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()
            logger.info("Database connection closed")

    async def get_or_create_trading_pair(self, symbol: str, exchange_id: int = 1):
        """Get trading pair ID or create if not exists"""
        try:
            # Check if pair exists
            query = """
                SELECT id FROM public.trading_pairs 
                WHERE pair_symbol = $1::varchar 
                AND exchange_id = $2::integer 
                AND contract_type_id = 1
                LIMIT 1
            """

            pair_id = await self.conn.fetchval(query, str(symbol), int(exchange_id))

            if not pair_id:
                # Create trading pair
                insert_query = """
                    INSERT INTO public.trading_pairs (
                        token_id, exchange_id, pair_symbol, 
                        contract_type_id, is_active, created_at
                    ) VALUES (
                        1, $1::integer, $2::varchar, 1, true, NOW()
                    )
                    RETURNING id
                """
                pair_id = await self.conn.fetchval(insert_query, int(exchange_id), str(symbol))
                logger.info(f"Created trading pair: {symbol} (ID: {pair_id})")
            else:
                logger.debug(f"Found existing trading pair: {symbol} (ID: {pair_id})")

            if not pair_id:
                raise ValueError(f"Failed to create or find trading pair for {symbol}")

            return int(pair_id)

        except Exception as e:
            logger.error(f"Error in get_or_create_trading_pair for {symbol}: {e}")
            raise

    async def create_test_signal(
            self,
            symbol: str,
            signal_type: str,
            prediction_proba: float,
            confidence_level: str,
            exchange_id: int = 1,
            delay_seconds: int = 0
    ):
        """Create a test signal in the database"""

        # Wait if delay specified
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

        # Get or create trading pair
        trading_pair_id = await self.get_or_create_trading_pair(symbol, exchange_id)

        # Insert into fas.scoring_history
        scoring_query = """
            INSERT INTO fas.scoring_history (
                timestamp, trading_pair_id, pair_symbol,
                total_score, pattern_score, combination_score,
                indicator_score, created_at, is_active
            ) VALUES (
                NOW(), $1, $2, $3, $4, $5, $6, NOW(), true
            )
            RETURNING id
        """

        # Generate random scores and convert to float for PostgreSQL
        total_score = float(random.uniform(60, 95))
        pattern_score = float(random.uniform(50, 90))
        combination_score = float(random.uniform(55, 85))
        indicator_score = float(random.uniform(60, 90))

        signal_id = await self.conn.fetchval(
            scoring_query,
            trading_pair_id,
            symbol,
            total_score,
            pattern_score,
            combination_score,
            indicator_score
        )

        signal_id = await self.conn.fetchval(
            scoring_query,
            trading_pair_id,
            symbol,
            total_score,
            pattern_score,
            combination_score,
            indicator_score
        )

        # Insert into smart_ml.predictions
        prediction_query = """
            INSERT INTO smart_ml.predictions (
                signal_id, model_name, market_regime,
                signal_type, prediction_proba, prediction,
                confidence_level, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, NOW()
            )
            RETURNING id
        """

        prediction_id = await self.conn.fetchval(
            prediction_query,
            signal_id,
            'test_model',
            'normal',
            signal_type,
            float(prediction_proba),  # Convert to float for PostgreSQL
            True,  # prediction is always True for test
            confidence_level
        )

        # Trigger NOTIFY
        notify_query = """
            SELECT pg_notify('smart_predictions', 
                json_build_object('prediction_id', $1)::text
            )
        """
        await self.conn.execute(notify_query, prediction_id)

        logger.info(f"✅ Created signal: {symbol} {signal_type} "
                    f"(prediction_id: {prediction_id}, confidence: {confidence_level}, "
                    f"proba: {prediction_proba:.4f})")

        return prediction_id

    async def create_batch_signals(self, count: int = 30, interval_seconds: float = 2.0):
        """Create multiple test signals with intervals"""

        logger.info(f"Starting to create {count} test signals...")

        # Shuffle signals for variety
        signals = TEST_SIGNALS.copy()
        random.shuffle(signals)

        # If we need more than available, repeat
        while len(signals) < count:
            signals.extend(TEST_SIGNALS)

        signals = signals[:count]

        created = []
        for i, (symbol, signal_type, proba, confidence) in enumerate(signals):
            try:
                # For testing, use only Binance (exchange_id = 1)
                exchange_id = 1  # Always Binance for testing

                prediction_id = await self.create_test_signal(
                    symbol=symbol,
                    signal_type=signal_type,
                    prediction_proba=proba,
                    confidence_level=confidence,
                    exchange_id=exchange_id
                )

                if prediction_id:
                    created.append({
                        'prediction_id': prediction_id,
                        'symbol': symbol,
                        'type': signal_type,
                        'exchange': 'Binance'
                    })

                # Wait between signals
                if i < count - 1:
                    logger.info(f"Waiting {interval_seconds}s before next signal...")
                    await asyncio.sleep(interval_seconds)

            except Exception as e:
                logger.error(f"Error creating signal for {symbol}: {e}")
                # Continue with next signal instead of stopping

        return created

    async def create_instant_batch(self, count: int = 10):
        """Create multiple signals instantly (stress test)"""

        logger.info(f"Creating {count} instant signals for stress testing...")

        signals = TEST_SIGNALS[:count]
        tasks = []

        for symbol, signal_type, proba, confidence in signals:
            task = self.create_test_signal(
                symbol=symbol,
                signal_type=signal_type,
                prediction_proba=proba,
                confidence_level=confidence,
                exchange_id=1  # All Binance for stress test
            )
            tasks.append(task)

        # Wait for all tasks, catching exceptions
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                symbol = signals[i][0]
                logger.error(f"Failed to create signal for {symbol}: {result}")
            else:
                success_count += 1

        logger.info(f"Created {success_count}/{count} signals instantly")

        return results

    async def cleanup_test_data(self):
        """Clean up test data from database"""

        logger.warning("Cleaning up test data...")

        try:
            # First delete from ats schema
            delete_ats = """
                DELETE FROM ats.signals 
                WHERE prediction_id IN (
                    SELECT id FROM smart_ml.predictions 
                    WHERE model_name = 'test_model'
                )
            """
            await self.conn.execute(delete_ats)

            # Delete test predictions
            delete_predictions = """
                DELETE FROM smart_ml.predictions 
                WHERE model_name = 'test_model'
                RETURNING id
            """

            deleted = await self.conn.fetch(delete_predictions)
            logger.info(f"Deleted {len(deleted)} test predictions")

            # Delete test scoring history
            delete_scoring = """
                DELETE FROM fas.scoring_history 
                WHERE id IN (
                    SELECT signal_id FROM smart_ml.predictions 
                    WHERE model_name = 'test_model'
                )
            """
            await self.conn.execute(delete_scoring)

            logger.info("Test data cleaned up")

        except Exception as e:
            logger.error(f"Error cleaning up: {e}")


async def main():
    """Main test function"""

    emulator = SignalEmulator()

    try:
        await emulator.connect()

        print("\n" + "=" * 60)
        print("ATS 2.0 - TEST SIGNAL EMULATOR")
        print("=" * 60)
        print("\nOptions:")
        print("1. Create 30 signals with 2s interval (normal test)")
        print("2. Create 10 instant signals (stress test)")
        print("3. Create 5 mixed exchange signals (multi-exchange test)")
        print("4. Create custom test")
        print("5. Cleanup test data")
        print("0. Exit")

        choice = input("\nSelect option: ")

        if choice == "1":
            # Normal test - 30 signals with intervals
            created = await emulator.create_batch_signals(count=30, interval_seconds=2)
            print(f"\n✅ Created {len(created)} test signals")
            print("\nFirst 5 signals:")
            for sig in created[:5]:
                print(f"  - {sig['symbol']} {sig['type']} on {sig['exchange']} (ID: {sig['prediction_id']})")

        elif choice == "2":
            # Stress test - instant signals
            results = await emulator.create_instant_batch(count=10)
            print(f"\n✅ Stress test completed")

        elif choice == "3":
            # Multi-exchange test
            signals = [
                ('BTCUSDT', 'BUY', 0.85, 'HIGH', 1),  # Binance
                ('ETHUSDT', 'SELL', 0.82, 'MEDIUM', 1),  # Binance
                ('XRPUSDT', 'BUY', 0.88, 'HIGH', 2),  # Bybit
                ('DOGEUSDT', 'SELL', 0.79, 'LOW', 2),  # Bybit
                ('SOLUSDT', 'BUY', 0.91, 'HIGH', 1),  # Binance
            ]

            for symbol, sig_type, proba, conf, exchange in signals:
                await emulator.create_test_signal(
                    symbol, sig_type, proba, conf, exchange
                )
                await asyncio.sleep(1)

            print(f"\n✅ Created 5 mixed exchange signals")

        elif choice == "4":
            # Custom test
            symbol = input("Symbol (e.g., BTCUSDT): ").upper()
            sig_type = input("Type (BUY/SELL): ").upper()
            proba = float(input("Probability (0.0-1.0): "))
            confidence = input("Confidence (LOW/MEDIUM/HIGH): ").upper()
            exchange = int(input("Exchange (1=Binance, 2=Bybit): "))

            pred_id = await emulator.create_test_signal(
                symbol, sig_type, proba, confidence, exchange
            )
            print(f"\n✅ Created signal with prediction_id: {pred_id}")

        elif choice == "5":
            # Cleanup
            confirm = input("Are you sure you want to delete test data? (yes/no): ")
            if confirm.lower() == 'yes':
                await emulator.cleanup_test_data()

        print("\n" + "=" * 60)
        print("Test completed! Check ATS logs for processing results.")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        await emulator.close()


if __name__ == "__main__":
    asyncio.run(main())