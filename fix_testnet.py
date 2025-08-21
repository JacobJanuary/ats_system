#!/usr/bin/env python3
"""Fix testnet issues"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()


async def fix_testnet():
    conn = await asyncpg.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    try:
        # 1. Close invalid positions
        print("Closing positions with zero values...")
        await conn.execute("""
            UPDATE ats.positions 
            SET status = 'CLOSED',
                close_reason = 'ERROR',
                closed_at = NOW()
            WHERE (quantity = 0 OR entry_price = 0)
            AND status = 'OPEN'
        """)

        # 2. Clear failed signals
        print("Clearing error signals...")
        await conn.execute("""
            UPDATE ats.signals 
            SET status = 'ERROR'
            WHERE status = 'EXECUTED' 
            AND prediction_id IN (
                SELECT prediction_id FROM ats.positions 
                WHERE quantity = 0 OR entry_price = 0
            )
        """)

        # 3. Show stats
        open_count = await conn.fetchval(
            "SELECT COUNT(*) FROM ats.positions WHERE status = 'OPEN'"
        )
        print(f"Open positions remaining: {open_count}")

        print("✅ Cleanup complete!")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(fix_testnet())