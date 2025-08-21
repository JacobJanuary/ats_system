#!/usr/bin/env python3
"""Quick test for fixed JSON handling"""
import asyncio
import asyncpg
import os
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


async def test_json_fix():
    # Connect to DB
    conn = await asyncpg.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    try:
        # Test audit log with JSON
        import json
        test_data = {
            'symbol': 'BTCUSDT',
            'price': float(Decimal('50000.50')),
            'timestamp': datetime.now().isoformat(),
            'nested': {'key': 'value'}
        }

        query = """
            INSERT INTO ats.audit_log (
                event_type, event_category, event_data, severity
            ) VALUES ($1, $2, $3::jsonb, $4)
            RETURNING id
        """

        audit_id = await conn.fetchval(
            query,
            'TEST_EVENT',
            'TEST',
            json.dumps(test_data),
            'INFO'
        )

        print(f"✅ Test audit log created with ID: {audit_id}")

        # Clean up
        await conn.execute("DELETE FROM ats.audit_log WHERE id = $1", audit_id)
        print("✅ Test data cleaned up")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(test_json_fix())