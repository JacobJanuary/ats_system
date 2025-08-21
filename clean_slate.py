#!/usr/bin/env python3
"""Clean all test data and positions for fresh start"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()


async def clean_slate():
    conn = await asyncpg.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    try:
        print("=" * 60)
        print("ATS 2.0 - COMPLETE CLEANUP")
        print("=" * 60)

        # 1. Close all open positions
        print("\n1. Closing all open positions...")
        result = await conn.execute("""
            UPDATE ats.positions 
            SET status = 'CLOSED',
                close_reason = 'MANUAL',
                closed_at = NOW()
            WHERE status = 'OPEN'
        """)
        print(f"   ✓ Closed positions: {result}")

        # 2. Clear all orders
        print("\n2. Clearing all orders...")
        result = await conn.execute("DELETE FROM ats.orders")
        print(f"   ✓ Deleted orders: {result}")

        # 3. Clear all positions
        print("\n3. Clearing all positions...")
        result = await conn.execute("DELETE FROM ats.positions")
        print(f"   ✓ Deleted positions: {result}")

        # 4. Clear all signals
        print("\n4. Clearing all signals...")
        result = await conn.execute("DELETE FROM ats.signals")
        print(f"   ✓ Deleted signals: {result}")

        # 5. Clear audit log
        print("\n5. Clearing audit log...")
        result = await conn.execute("DELETE FROM ats.audit_log")
        print(f"   ✓ Deleted audit entries: {result}")

        # 6. Clear performance metrics
        print("\n6. Clearing performance metrics...")
        result = await conn.execute("DELETE FROM ats.performance_metrics")
        print(f"   ✓ Deleted metrics: {result}")

        # 7. Clear test predictions from smart_ml
        print("\n7. Clearing test predictions...")
        result = await conn.execute("""
            DELETE FROM smart_ml.predictions 
            WHERE model_name = 'test_model'
        """)
        print(f"   ✓ Deleted test predictions: {result}")

        # 8. Clear test scoring history
        print("\n8. Clearing test scoring history...")
        result = await conn.execute("""
            DELETE FROM fas.scoring_history 
            WHERE id IN (
                SELECT signal_id FROM smart_ml.predictions 
                WHERE model_name = 'test_model'
            )
        """)
        print(f"   ✓ Deleted test scoring: {result}")

        # 9. Reset sequences
        print("\n9. Resetting sequences...")
        await conn.execute("ALTER SEQUENCE ats.signals_id_seq RESTART WITH 1")
        await conn.execute("ALTER SEQUENCE ats.positions_id_seq RESTART WITH 1")
        await conn.execute("ALTER SEQUENCE ats.orders_id_seq RESTART WITH 1")
        await conn.execute("ALTER SEQUENCE ats.audit_log_id_seq RESTART WITH 1")
        print("   ✓ Sequences reset")

        # 10. Show current state
        print("\n10. Verifying clean state...")

        # Check counts
        signals_count = await conn.fetchval("SELECT COUNT(*) FROM ats.signals")
        positions_count = await conn.fetchval("SELECT COUNT(*) FROM ats.positions")
        orders_count = await conn.fetchval("SELECT COUNT(*) FROM ats.orders")

        print(f"\n✅ CLEANUP COMPLETE!")
        print(f"   Signals: {signals_count}")
        print(f"   Positions: {positions_count}")
        print(f"   Orders: {orders_count}")
        print("\n" + "=" * 60)
        print("System is ready for fresh start!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error during cleanup: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()


if __name__ == "__main__":
    confirm = input("⚠️  This will DELETE ALL DATA in ATS schema. Continue? (yes/no): ")
    if confirm.lower() == 'yes':
        asyncio.run(clean_slate())
    else:
        print("Cleanup cancelled.")