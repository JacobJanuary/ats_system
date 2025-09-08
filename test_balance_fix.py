#!/usr/bin/env python3
"""Test and fix balance display issues"""

import asyncio
import asyncpg
import aiohttp
import hmac
import hashlib
import time
import json
from decimal import Decimal
import os
from pathlib import Path
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

# Setup logging
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load config
import yaml
config_path = Path(__file__).parent / 'config' / 'monitoring.yaml'
with open(config_path) as f:
    config = yaml.safe_load(f)

async def test_database_connection():
    """Test database connection and check current balances"""
    logger.info("=" * 50)
    logger.info("TESTING DATABASE CONNECTION")
    logger.info("=" * 50)
    
    try:
        pool = await asyncpg.create_pool(
            host=config['db']['host'],
            port=config['db']['port'],
            database=config['db']['database'],
            user=config['db']['user'],
            password=config['db']['password']
        )
        
        async with pool.acquire() as conn:
            # Check if table exists
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'monitoring' 
                    AND table_name = 'balances'
                )
            """)
            logger.info(f"Table monitoring.balances exists: {exists}")
            
            # Get current balances
            rows = await conn.fetch("""
                SELECT exchange, asset, free, locked, 
                       (COALESCE(free, 0) + COALESCE(locked, 0)) as total,
                       last_update
                FROM monitoring.balances
                ORDER BY exchange, asset
            """)
            
            logger.info(f"Found {len(rows)} balance records in database")
            for row in rows:
                logger.info(f"  {row['exchange']}: {row['asset']} = {row['total']} (free: {row['free']}, locked: {row['locked']})")
        
        await pool.close()
        return True
        
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return False

async def test_binance_api():
    """Test Binance API directly"""
    logger.info("=" * 50)
    logger.info("TESTING BINANCE API")
    logger.info("=" * 50)
    
    api_key = config['binance']['api_key']
    api_secret = config['binance']['api_secret']
    
    if config['binance']['testnet']:
        base_url = "https://testnet.binancefuture.com"
    else:
        base_url = "https://fapi.binance.com"
    
    url = f"{base_url}/fapi/v2/account"
    
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    
    signature = hmac.new(
        api_secret.encode(),
        query_string.encode(),
        hashlib.sha256
    ).hexdigest()
    
    full_url = f"{url}?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(full_url, headers=headers) as response:
            logger.info(f"Binance Response Status: {response.status}")
            
            if response.status == 200:
                data = await response.json()
                
                logger.info(f"Total Wallet Balance: {data.get('totalWalletBalance')}")
                logger.info(f"Available Balance: {data.get('availableBalance')}")
                
                assets = data.get('assets', [])
                non_zero = [a for a in assets if float(a.get('walletBalance', 0)) > 0]
                
                logger.info(f"Non-zero assets: {len(non_zero)}")
                for asset in non_zero:
                    logger.info(f"  {asset['asset']}: {asset['walletBalance']} (available: {asset['availableBalance']})")
                
                return non_zero
            else:
                error = await response.text()
                logger.error(f"Error: {error}")
                return []

async def test_bybit_api():
    """Test Bybit API directly"""
    logger.info("=" * 50)
    logger.info("TESTING BYBIT API")
    logger.info("=" * 50)
    
    api_key = config['bybit']['api_key']
    api_secret = config['bybit']['api_secret']
    
    if config['bybit']['testnet']:
        base_url = "https://api-testnet.bybit.com"
    else:
        base_url = "https://api.bybit.com"
    
    timestamp = str(int(time.time() * 1000))
    
    # Try UNIFIED account
    param_str = "accountType=UNIFIED"
    sign_str = f"{timestamp}{api_key}5000{param_str}"
    signature = hmac.new(
        api_secret.encode(),
        sign_str.encode(),
        hashlib.sha256
    ).hexdigest()
    
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": "5000"
    }
    
    url = f"{base_url}/v5/account/wallet-balance?{param_str}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            logger.info(f"Bybit Response Status: {response.status}")
            
            if response.status == 200:
                data = await response.json()
                
                if data.get('retCode') == 0:
                    accounts = data.get('result', {}).get('list', [])
                    
                    all_coins = []
                    for account in accounts:
                        logger.info(f"Account Type: {account.get('accountType')}")
                        logger.info(f"Total Equity: {account.get('totalEquity')}")
                        
                        coins = account.get('coin', [])
                        for coin in coins:
                            wallet = float(coin.get('walletBalance', 0))
                            if wallet > 0:
                                all_coins.append(coin)
                                logger.info(f"  {coin['coin']}: {wallet} (USD: {coin.get('usdValue')})")
                    
                    return all_coins
                else:
                    logger.error(f"API Error: {data.get('retMsg')}")
            else:
                error = await response.text()
                logger.error(f"Error: {error}")
    
    return []

async def save_balances_to_db(exchange: str, balances: list):
    """Save balances to database"""
    logger.info(f"Saving {len(balances)} {exchange} balances to database...")
    
    pool = await asyncpg.create_pool(
        host=config['db']['host'],
        port=config['db']['port'],
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password']
    )
    
    saved_count = 0
    
    try:
        for balance in balances:
            if exchange == 'binance':
                asset = balance.get('asset')
                wallet_balance = Decimal(str(balance.get('walletBalance', '0')))
                available = Decimal(str(balance.get('availableBalance', '0')))
                locked = wallet_balance - available
            else:  # bybit
                asset = balance.get('coin')
                wallet_balance = Decimal(str(balance.get('walletBalance', '0')))
                
                # Handle empty availableToWithdraw
                available_str = balance.get('availableToWithdraw', '')
                if available_str == '' or available_str is None:
                    # Use equity if available
                    equity = balance.get('equity', wallet_balance)
                    available = Decimal(str(equity))
                else:
                    available = Decimal(str(available_str))
                
                locked = wallet_balance - available if wallet_balance > available else Decimal('0')
            
            if wallet_balance > 0:
                query = """
                    INSERT INTO monitoring.balances (exchange, asset, free, locked)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (exchange, asset)
                    DO UPDATE SET
                        free = $3,
                        locked = $4,
                        last_update = NOW()
                    RETURNING *
                """
                
                result = await pool.fetchrow(query, exchange, asset, available, locked)
                saved_count += 1
                logger.info(f"  Saved: {asset} = {wallet_balance} (free: {available}, locked: {locked})")
    
    except Exception as e:
        logger.error(f"Error saving to database: {e}")
    
    finally:
        await pool.close()
    
    logger.info(f"Saved {saved_count} balances to database")
    return saved_count

async def verify_web_api():
    """Test the web API endpoint"""
    logger.info("=" * 50)
    logger.info("TESTING WEB API")
    logger.info("=" * 50)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8000/api/balances") as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info("Web API Response:")
                    logger.info(json.dumps(data, indent=2))
                else:
                    logger.error(f"Web API Error: {response.status}")
    except Exception as e:
        logger.error(f"Cannot connect to web API: {e}")

async def main():
    """Main test function"""
    logger.info("Starting balance diagnostics...")
    
    # Test database
    if not await test_database_connection():
        logger.error("Database connection failed!")
        return
    
    # Test Binance API
    binance_balances = await test_binance_api()
    if binance_balances:
        await save_balances_to_db('binance', binance_balances)
    
    # Test Bybit API
    bybit_balances = await test_bybit_api()
    if bybit_balances:
        await save_balances_to_db('bybit', bybit_balances)
    
    # Verify what's in database now
    logger.info("\n" + "=" * 50)
    logger.info("FINAL DATABASE STATE")
    logger.info("=" * 50)
    
    pool = await asyncpg.create_pool(
        host=config['db']['host'],
        port=config['db']['port'],
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password']
    )
    
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT exchange, asset, free, locked, 
                   (COALESCE(free, 0) + COALESCE(locked, 0)) as total
            FROM monitoring.balances
            WHERE (COALESCE(free, 0) + COALESCE(locked, 0)) > 0
            ORDER BY exchange, asset
        """)
        
        for row in rows:
            logger.info(f"{row['exchange']}: {row['asset']} = {row['total']} (free: {row['free']}, locked: {row['locked']})")
    
    await pool.close()
    
    # Test web API
    await verify_web_api()
    
    logger.info("\nDiagnostics complete!")

if __name__ == "__main__":
    asyncio.run(main())