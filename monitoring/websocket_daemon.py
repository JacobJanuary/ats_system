"""
ATS 2.0 - WebSocket Monitoring Daemon
Real-time position and balance monitoring via WebSocket
"""

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Any, List
import asyncpg
import websockets
from websockets.exceptions import ConnectionClosed
import hmac
import hashlib
import time
from dataclasses import dataclass, asdict
import signal
import sys
import aiohttp
from enum import Enum

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ExchangeType(Enum):
    BINANCE = "binance"
    BYBIT = "bybit"


@dataclass
class Position:
    """Position data structure"""
    exchange: str
    symbol: str
    position_id: str
    side: str
    entry_price: Decimal
    current_price: Decimal
    quantity: Decimal
    margin: Optional[Decimal] = None
    leverage: Optional[int] = None
    unrealized_pnl: Optional[Decimal] = None
    realized_pnl: Optional[Decimal] = None
    has_stop_loss: bool = False
    stop_loss_price: Optional[Decimal] = None
    has_take_profit: bool = False
    take_profit_price: Optional[Decimal] = None
    has_trailing_stop: bool = False
    trailing_stop_distance: Optional[Decimal] = None
    trailing_stop_activated: bool = False
    status: str = 'OPEN'
    opened_at: Optional[datetime] = None
    raw_data: Optional[Dict] = None


@dataclass
class Balance:
    """Balance data structure"""
    exchange: str
    asset: str
    free: Decimal
    locked: Decimal
    in_usd: Optional[Decimal] = None


class BinanceWebSocketClient:
    """Binance WebSocket client for real-time monitoring"""
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.base_url = "https://fapi.binance.com" if not testnet else "https://testnet.binancefuture.com"
        self.base_ws_url = "wss://fstream.binance.com/ws" if not testnet else "wss://stream.binancefuture.com/ws"
        self.listen_key = None
        self.ws = None
        self.running = False
        self.session = None
        
    async def get_listen_key(self) -> str:
        """Get listen key for user data stream"""
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        url = f"{self.base_url}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}
        
        async with self.session.post(url, headers=headers) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"Failed to get listen key: {text}")
            data = await response.json()
            return data['listenKey']
    
    async def keep_alive_listen_key(self):
        """Keep listen key active"""
        while self.running:
            try:
                if not self.listen_key:
                    await asyncio.sleep(60)
                    continue
                    
                url = f"{self.base_url}/fapi/v1/listenKey"
                headers = {"X-MBX-APIKEY": self.api_key}
                
                async with self.session.put(url, headers=headers) as response:
                    if response.status == 200:
                        logger.debug("Binance listen key refreshed")
                    else:
                        logger.warning(f"Failed to refresh listen key: {response.status}")
                        # Get new listen key
                        self.listen_key = await self.get_listen_key()
                        
                await asyncio.sleep(1800)  # Refresh every 30 minutes
            except Exception as e:
                logger.error(f"Error refreshing listen key: {e}")
                await asyncio.sleep(60)
    
    async def connect(self, db_pool: asyncpg.Pool):
        """Connect to WebSocket and start monitoring"""
        self.running = True
        
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        # Get listen key
        try:
            self.listen_key = await self.get_listen_key()
            logger.info(f"Got Binance listen key: {self.listen_key[:8]}...")
        except Exception as e:
            logger.error(f"Failed to get listen key: {e}")
            return
        
        # Start listen key keeper
        asyncio.create_task(self.keep_alive_listen_key())
        
        ws_url = f"{self.base_ws_url}/{self.listen_key}"
        
        while self.running:
            try:
                async with websockets.connect(ws_url) as ws:
                    self.ws = ws
                    logger.info("Connected to Binance WebSocket")
                    
                    # Sync initial positions
                    await self.sync_initial_positions(db_pool)
                    
                    # Sync initial balances
                    await self.sync_account_balance(db_pool)
                    
                    # Listen for updates
                    async for message in ws:
                        await self.process_message(message, db_pool)
                        
            except ConnectionClosed:
                logger.warning("Binance WebSocket connection closed, reconnecting...")
                await asyncio.sleep(5)
                # Get new listen key
                try:
                    self.listen_key = await self.get_listen_key()
                    ws_url = f"{self.base_ws_url}/{self.listen_key}"
                except Exception as e:
                    logger.error(f"Failed to get new listen key: {e}")
                    await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Binance WebSocket error: {e}")
                await asyncio.sleep(10)
    
    async def sync_initial_positions(self, db_pool: asyncpg.Pool):
        """Sync initial positions via REST API"""
        try:
            url = f"{self.base_url}/fapi/v2/positionRisk"
            headers = {"X-MBX-APIKEY": self.api_key}
            
            timestamp = int(time.time() * 1000)
            query_string = f"timestamp={timestamp}"
            signature = hmac.new(
                self.api_secret.encode(),
                query_string.encode(),
                hashlib.sha256
            ).hexdigest()
            
            full_url = f"{url}?{query_string}&signature={signature}"
            
            async with self.session.get(full_url, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"Failed to get positions: {text}")
                    return
                    
                positions = await response.json()
                
                # Clear existing open positions for this exchange
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE monitoring.positions SET status = 'CLOSED', closed_at = NOW() "
                        "WHERE exchange = 'binance' AND status = 'OPEN'"
                    )
                
                # Insert current positions
                for pos_data in positions:
                    if float(pos_data.get('positionAmt', 0)) != 0:
                        position = self.parse_position(pos_data)
                        await self.update_position_in_db(position, db_pool)
                        
                logger.info(f"Synced {len([p for p in positions if float(p.get('positionAmt', 0)) != 0])} Binance positions")
                
        except Exception as e:
            logger.error(f"Error syncing initial positions: {e}")
    
    async def sync_account_balance(self, db_pool: asyncpg.Pool):
        """Sync Binance Futures account balance via REST API"""
        try:
            timestamp = int(time.time() * 1000)
            params = f"timestamp={timestamp}"
            
            # Generate signature
            signature = hmac.new(
                self.api_secret.encode(),
                params.encode(),
                hashlib.sha256
            ).hexdigest()
            
            url = f"{self.base_url}/fapi/v2/account?{params}&signature={signature}"
            headers = {"X-MBX-APIKEY": self.api_key}
            
            async with self.session.get(url, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"Failed to get Binance account: {text}")
                    return
                
                data = await response.json()
                
                # Process balances
                for asset_data in data.get('assets', []):
                    try:
                        wallet_balance = Decimal(str(asset_data.get('walletBalance', '0')))
                        available_balance = Decimal(str(asset_data.get('availableBalance', '0')))
                        
                        # Skip zero balances
                        if wallet_balance == 0:
                            continue
                        
                        locked_balance = wallet_balance - available_balance
                        
                        balance = Balance(
                            exchange="binance",
                            asset=asset_data.get('asset'),
                            free=available_balance,
                            locked=locked_balance
                        )
                        
                        await self.update_balance_in_db(balance, db_pool)
                        
                        logger.debug(f"Binance balance: {asset_data.get('asset')} = {wallet_balance}")
                    except Exception as e:
                        logger.error(f"Error parsing Binance balance: {e}")
                
                logger.info(f"Synced Binance account balances")
                
        except Exception as e:
            logger.error(f"Error syncing Binance account: {e}")
    
    def parse_position(self, data: Dict) -> Position:
        """Parse position data from Binance"""
        pos_amt = float(data.get('positionAmt', 0))
        return Position(
            exchange="binance",
            symbol=data.get('symbol'),
            position_id=f"binance_{data.get('symbol')}_{data.get('positionSide', 'BOTH')}",
            side='LONG' if pos_amt > 0 else 'SHORT',
            entry_price=Decimal(str(data.get('entryPrice', 0))),
            current_price=Decimal(str(data.get('markPrice', 0))),
            quantity=abs(Decimal(str(pos_amt))),
            margin=Decimal(str(data.get('isolatedWallet', 0))) if data.get('marginType') == 'isolated' else None,
            leverage=int(data.get('leverage', 1)),
            unrealized_pnl=Decimal(str(data.get('unRealizedProfit', 0))),
            opened_at=datetime.now(),
            raw_data=data
        )
    
    async def process_message(self, message: str, db_pool: asyncpg.Pool):
        """Process WebSocket message"""
        try:
            data = json.loads(message)
            event_type = data.get('e')
            
            if event_type == 'ACCOUNT_UPDATE':
                await self.handle_account_update(data, db_pool)
            elif event_type == 'ORDER_TRADE_UPDATE':
                await self.handle_order_update(data, db_pool)
                
        except Exception as e:
            logger.error(f"Error processing Binance message: {e}")
    
    async def handle_account_update(self, data: Dict, db_pool: asyncpg.Pool):
        """Handle account update event"""
        # Update balances
        for balance in data.get('a', {}).get('B', []):
            await self.update_balance_in_db(
                Balance(
                    exchange="binance",
                    asset=balance.get('a'),
                    free=Decimal(str(balance.get('wb', 0))),
                    locked=Decimal(str(balance.get('cw', 0))) - Decimal(str(balance.get('wb', 0)))
                ),
                db_pool
            )
        
        # Update positions
        for position in data.get('a', {}).get('P', []):
            pos = self.parse_position(position)
            await self.update_position_in_db(pos, db_pool)
    
    async def handle_order_update(self, data: Dict, db_pool: asyncpg.Pool):
        """Handle order update event"""
        order = data.get('o', {})
        order_type = order.get('ot')
        
        # Track stop loss and take profit orders
        if order_type in ['STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET']:
            symbol = order.get('s')
            side = order.get('ps')
            position_id = f"binance_{symbol}_{side}"
            
            async with db_pool.acquire() as conn:
                if order_type in ['STOP', 'STOP_MARKET']:
                    await conn.execute(
                        "UPDATE monitoring.positions SET has_stop_loss = true, stop_loss_price = $1 "
                        "WHERE position_id = $2 AND status = 'OPEN'",
                        Decimal(str(order.get('sp', 0))), position_id
                    )
                elif order_type in ['TAKE_PROFIT', 'TAKE_PROFIT_MARKET']:
                    await conn.execute(
                        "UPDATE monitoring.positions SET has_take_profit = true, take_profit_price = $1 "
                        "WHERE position_id = $2 AND status = 'OPEN'",
                        Decimal(str(order.get('sp', 0))), position_id
                    )
                elif order_type == 'TRAILING_STOP_MARKET':
                    await conn.execute(
                        "UPDATE monitoring.positions SET has_trailing_stop = true, trailing_stop_distance = $1 "
                        "WHERE position_id = $2 AND status = 'OPEN'",
                        Decimal(str(order.get('cr', 0))), position_id
                    )
    
    async def update_position_in_db(self, position: Position, db_pool: asyncpg.Pool):
        """Update position in database"""
        query = """
            INSERT INTO monitoring.positions (
                exchange, symbol, position_id, side, entry_price,
                current_price, quantity, margin, leverage,
                unrealized_pnl, realized_pnl, status, opened_at, raw_data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (exchange, position_id) 
            DO UPDATE SET
                current_price = $6,
                quantity = $7,
                unrealized_pnl = $10,
                realized_pnl = $11,
                status = $12,
                raw_data = $14
        """
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                query,
                position.exchange, position.symbol, position.position_id,
                position.side, position.entry_price, position.current_price,
                position.quantity, position.margin, position.leverage,
                position.unrealized_pnl, position.realized_pnl,
                position.status, position.opened_at,
                json.dumps(position.raw_data) if position.raw_data else None
            )
    
    async def update_balance_in_db(self, balance: Balance, db_pool: asyncpg.Pool):
        """Update balance in database"""
        query = """
            INSERT INTO monitoring.balances (exchange, asset, free, locked)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (exchange, asset)
            DO UPDATE SET
                free = $3,
                locked = $4
        """
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                query,
                balance.exchange,
                balance.asset,
                balance.free,
                balance.locked
            )
    
    async def disconnect(self):
        """Disconnect WebSocket and cleanup"""
        self.running = False
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()


class BybitWebSocketClient:
    """Bybit WebSocket client for real-time monitoring"""
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.base_url = "https://api.bybit.com" if not testnet else "https://api-testnet.bybit.com"
        self.base_ws_url = "wss://stream.bybit.com/v5/private" if not testnet else "wss://stream-testnet.bybit.com/v5/private"
        self.ws = None
        self.running = False
        self.session = None
        
    def generate_signature(self) -> tuple:
        """Generate signature for authentication"""
        expires = int((time.time() + 10) * 1000)
        signature = hmac.new(
            self.api_secret.encode(),
            f"GET/realtime{expires}".encode(),
            hashlib.sha256
        ).hexdigest()
        return expires, signature
    
    async def connect(self, db_pool: asyncpg.Pool):
        """Connect to WebSocket and start monitoring"""
        self.running = True
        
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        while self.running:
            try:
                async with websockets.connect(self.base_ws_url) as ws:
                    self.ws = ws
                    logger.info("Connected to Bybit WebSocket")
                    
                    # Authenticate
                    await self.authenticate()
                    
                    # Subscribe to channels
                    await self.subscribe_to_channels()
                    
                    # Sync initial positions
                    await self.sync_initial_positions(db_pool)
                    
                    # Sync initial balances
                    await self.sync_initial_balances(db_pool)
                    
                    # Process messages
                    async for message in ws:
                        await self.process_message(message, db_pool)
                        
            except ConnectionClosed:
                logger.warning("Bybit WebSocket connection closed, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Bybit WebSocket error: {e}")
                await asyncio.sleep(10)
    
    async def authenticate(self):
        """Authenticate WebSocket connection"""
        expires, signature = self.generate_signature()
        
        auth_msg = {
            "op": "auth",
            "args": [self.api_key, expires, signature]
        }
        
        await self.ws.send(json.dumps(auth_msg))
        
        # Wait for confirmation
        response = await self.ws.recv()
        data = json.loads(response)
        
        if data.get('success'):
            logger.info("Bybit authentication successful")
        else:
            raise Exception(f"Bybit authentication failed: {data}")
    
    async def subscribe_to_channels(self):
        """Subscribe to required channels"""
        channels = [
            "position",
            "wallet",
            "order"
        ]
        
        for channel in channels:
            subscribe_msg = {
                "op": "subscribe",
                "args": [channel]
            }
            await self.ws.send(json.dumps(subscribe_msg))
            await asyncio.sleep(0.1)
        
        logger.info(f"Subscribed to Bybit channels: {channels}")
    
    async def sync_initial_positions(self, db_pool: asyncpg.Pool):
        """Sync initial positions via REST API with pagination"""
        try:
            all_positions = []
            url = f"{self.base_url}/v5/position/list"
            cursor = ""
            page = 1
            
            # Clear existing open positions for this exchange
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE monitoring.positions SET status = 'CLOSED', closed_at = NOW() "
                    "WHERE exchange = 'bybit' AND status = 'OPEN'"
                )
            
            while True:
                timestamp = int(time.time() * 1000)
                
                # Build params with pagination - maintain specific order for Bybit
                if cursor:
                    param_str = f"category=linear&settleCoin=USDT&limit=200&cursor={cursor}"
                else:
                    param_str = "category=linear&settleCoin=USDT&limit=200"
                
                # Create signature for Bybit V5
                # Bybit V5 signature format: timestamp + api_key + recv_window + query_string
                sign_str = f"{timestamp}{self.api_key}5000{param_str}"
                signature = hmac.new(
                    self.api_secret.encode(),
                    sign_str.encode(),
                    hashlib.sha256
                ).hexdigest()
                
                headers = {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-SIGN": signature,
                    "X-BAPI-TIMESTAMP": str(timestamp),
                    "X-BAPI-RECV-WINDOW": "5000"
                }
                
                # Build full URL with parameters
                full_url = f"{url}?{param_str}"
                
                async with self.session.get(full_url, headers=headers) as response:
                    if response.status != 200:
                        text = await response.text()
                        logger.error(f"Failed to get Bybit positions: {text}")
                        break
                        
                    data = await response.json()
                    
                    if data.get('retCode') != 0:
                        logger.error(f"Bybit API error: {data.get('retMsg')}")
                        break
                    
                    # Get positions from this page
                    positions = data.get('result', {}).get('list', [])
                    logger.info(f"Fetched {len(positions)} positions on page {page}")
                    
                    # Filter and add open positions
                    for pos_data in positions:
                        if float(pos_data.get('size', 0)) > 0:
                            all_positions.append(pos_data)
                    
                    # Check for next page
                    next_cursor = data.get('result', {}).get('nextPageCursor', '')
                    if not next_cursor:
                        break
                    
                    cursor = next_cursor
                    page += 1
                    
                    # Small delay between requests
                    await asyncio.sleep(0.1)
            
            # Process all positions
            logger.info(f"Total positions found: {len(all_positions)}")
            
            for pos_data in all_positions:
                try:
                    position = self.parse_position(pos_data)
                    await self.update_position_in_db(position, db_pool)
                except Exception as e:
                    logger.error(f"Error parsing position {pos_data.get('symbol')}: {e}")
                    logger.debug(f"Position data: {json.dumps(pos_data, indent=2)}")
            
            logger.info(f"Successfully synced {len(all_positions)} Bybit positions")
            
            # Also sync initial balances
            await self.sync_initial_balances(db_pool)
                
        except Exception as e:
            logger.error(f"Error syncing initial Bybit positions: {e}")
    
    async def sync_wallet_balances(self, db_pool: asyncpg.Pool):
        """Sync wallet balances via REST API - proper naming for periodic sync"""
        await self.sync_initial_balances(db_pool)
    
    async def sync_initial_balances(self, db_pool: asyncpg.Pool):
        """Sync initial wallet balances via REST API"""
        try:
            url = f"{self.base_url}/v5/account/wallet-balance"
            
            timestamp = int(time.time() * 1000)
            param_str = "accountType=UNIFIED"
            sign_str = f"{timestamp}{self.api_key}5000{param_str}"
            signature = hmac.new(
                self.api_secret.encode(),
                sign_str.encode(),
                hashlib.sha256
            ).hexdigest()
            
            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-RECV-WINDOW": "5000"
            }
            
            # Build full URL with parameters
            full_url = f"{url}?{param_str}"
            
            async with self.session.get(full_url, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"Failed to get Bybit wallet balance: {text}")
                    return
                    
                data = await response.json()
                
                if data.get('retCode') != 0:
                    logger.error(f"Bybit wallet API error: {data.get('retMsg')}")
                    # Try CONTRACT account type if UNIFIED fails
                    await self.sync_contract_balance(db_pool)
                    return
                
                # Process wallet data
                total_usd_value = Decimal('0')
                for account in data.get('result', {}).get('list', []):
                    for coin_data in account.get('coin', []):
                        try:
                            # Get wallet balance and available balance
                            wallet_balance_str = str(coin_data.get('walletBalance', '0'))
                            available_str = coin_data.get('availableToWithdraw', '')
                            locked_str = str(coin_data.get('locked', '0'))
                            usd_value_str = str(coin_data.get('usdValue', '0'))
                            equity_str = str(coin_data.get('equity', '0'))
                            
                            # Handle empty strings
                            wallet_balance = Decimal(wallet_balance_str) if wallet_balance_str and wallet_balance_str != '' else Decimal('0')
                            
                            # For UNIFIED accounts, use equity as the total balance (includes unrealized PnL)
                            # availableToWithdraw might be empty for UNIFIED accounts
                            if available_str == '' or available_str is None:
                                # Use equity as total balance (includes unrealized PnL)
                                total_balance = Decimal(equity_str) if equity_str and equity_str != '' else wallet_balance
                                # Available is the total minus any locked amount
                                locked_balance = Decimal(locked_str) if locked_str and locked_str != '' else Decimal('0')
                                available_balance = total_balance - locked_balance
                            else:
                                # Standard account - use provided values
                                available_balance = Decimal(available_str)
                                locked_balance = Decimal(locked_str) if locked_str and locked_str != '' else Decimal('0')
                                total_balance = available_balance + locked_balance
                            
                            usd_value = Decimal(usd_value_str) if usd_value_str and usd_value_str != '' else Decimal('0')
                            
                            # Skip zero balances
                            if total_balance == 0:
                                continue
                            
                            balance = Balance(
                                exchange="bybit",
                                asset=coin_data.get('coin'),
                                free=available_balance,
                                locked=locked_balance
                            )
                            await self.update_balance_in_db(balance, db_pool)
                            total_usd_value += usd_value
                            
                            logger.debug(f"Bybit balance: {coin_data.get('coin')} = {wallet_balance} (available: {available_balance}, USD: {usd_value})")
                        except Exception as e:
                            logger.error(f"Error parsing balance for {coin_data.get('coin', 'unknown')}: {e}")
                
                logger.info(f"Synced Bybit wallet balances, total USD value: ${total_usd_value:.2f}")
                
        except Exception as e:
            logger.error(f"Error syncing Bybit balances: {e}")
    
    async def sync_contract_balance(self, db_pool: asyncpg.Pool):
        """Try CONTRACT account type if UNIFIED doesn't work"""
        try:
            url = f"{self.base_url}/v5/account/wallet-balance"
            
            timestamp = int(time.time() * 1000)
            param_str = "accountType=CONTRACT"
            sign_str = f"{timestamp}{self.api_key}5000{param_str}"
            signature = hmac.new(
                self.api_secret.encode(),
                sign_str.encode(),
                hashlib.sha256
            ).hexdigest()
            
            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-RECV-WINDOW": "5000"
            }
            
            full_url = f"{url}?{param_str}"
            
            async with self.session.get(full_url, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"Failed to get CONTRACT balance: {text}")
                    return
                    
                data = await response.json()
                
                if data.get('retCode') != 0:
                    logger.error(f"CONTRACT balance API error: {data.get('retMsg')}")
                    return
                
                # Process CONTRACT wallet data
                for account in data.get('result', {}).get('list', []):
                    for coin_data in account.get('coin', []):
                        try:
                            wallet_balance = Decimal(str(coin_data.get('walletBalance', '0')))
                            available_balance = Decimal(str(coin_data.get('availableBalance', '0')))
                            
                            if wallet_balance == 0:
                                continue
                            
                            locked_balance = wallet_balance - available_balance
                            
                            balance = Balance(
                                exchange="bybit",
                                asset=coin_data.get('coin'),
                                free=available_balance,
                                locked=locked_balance
                            )
                            await self.update_balance_in_db(balance, db_pool)
                            
                            logger.debug(f"CONTRACT balance: {coin_data.get('coin')} = {wallet_balance}")
                        except Exception as e:
                            logger.error(f"Error parsing CONTRACT balance: {e}")
                
                logger.info("Synced Bybit CONTRACT balances")
                
        except Exception as e:
            logger.error(f"Error syncing Bybit balances: {e}")
    
    def parse_position(self, data: Dict) -> Position:
        """Parse position data from Bybit V5 API"""
        try:
            symbol = data.get('symbol', 'UNKNOWN')
            
            # Helper function to safely convert to Decimal
            def to_decimal(value, default=0):
                if value is None or value == '' or value == '0':
                    return Decimal(str(default))
                try:
                    return Decimal(str(value))
                except:
                    logger.warning(f"Cannot convert to Decimal: {value}")
                    return Decimal(str(default))
            
            # Helper to safely convert to int
            def to_int(value, default=1):
                if value is None or value == '':
                    return default
                try:
                    return int(float(str(value)))
                except:
                    return default
            
            # Get prices - Bybit V5 specific field names
            avg_price = to_decimal(data.get('avgPrice', 0))
            mark_price = to_decimal(data.get('markPrice', 0))
            
            # If avgPrice is 0, try to use other fields
            if avg_price == 0:
                # Try busPrice (business price) which is sometimes used
                avg_price = to_decimal(data.get('busPrice', 0))
                if avg_price == 0:
                    # Use markPrice as fallback
                    avg_price = mark_price
            
            # Get side and position index - improved logic
            raw_side = data.get('side', '')
            position_idx = int(data.get('positionIdx', 0))
            size = to_decimal(data.get('size', 0))
            
            # Determine side with multiple fallbacks
            side = 'NONE'
            
            # Primary: Use the 'side' field
            if raw_side in ['Buy', 'BUY']:
                side = 'LONG'
            elif raw_side in ['Sell', 'SELL']:
                side = 'SHORT'
            # Secondary: Use positionIdx (1=Buy/Long, 2=Sell/Short)
            elif position_idx == 1:
                side = 'LONG'
            elif position_idx == 2:
                side = 'SHORT'
            # Tertiary: For hedge mode, check position value
            elif size != 0:
                # Check positionValue field which includes direction
                position_value = to_decimal(data.get('positionValue', 0))
                if position_value > 0:
                    side = 'LONG'
                elif position_value < 0:
                    side = 'SHORT'
                else:
                    # Last resort - assume LONG for positive size
                    side = 'LONG' if size > 0 else 'SHORT'
                    logger.warning(f"Could not determine side for {symbol}, using size-based guess: {side}")
            
            # Log determination for debugging
            if side == 'NONE':
                logger.error(f"Failed to determine side for {symbol}: raw_side={raw_side}, positionIdx={position_idx}, size={size}")
                logger.debug(f"Full position data: {json.dumps(data, indent=2)}")
            
            # Get stop loss and take profit
            stop_loss = data.get('stopLoss', '')
            take_profit = data.get('takeProfit', '')
            trailing_stop = data.get('trailingStop', '')
            
            # Parse created time
            created_time = data.get('createdTime', '')
            if created_time and str(created_time) != '0':
                opened_at = datetime.fromtimestamp(int(created_time) / 1000)
            else:
                opened_at = datetime.now()
            
            position = Position(
                exchange="bybit",
                symbol=symbol,
                position_id=f"bybit_{symbol}_{side}_{position_idx}",
                side=side,
                entry_price=avg_price if avg_price > 0 else mark_price,
                current_price=mark_price,
                quantity=abs(to_decimal(data.get('size', 0))),  # Always positive
                margin=to_decimal(data.get('positionIM', 0)),
                leverage=to_int(data.get('leverage', 1)),
                unrealized_pnl=to_decimal(data.get('unrealisedPnl', 0)),
                realized_pnl=to_decimal(data.get('cumRealisedPnl', 0)),
                has_stop_loss=bool(stop_loss and str(stop_loss) != '0' and str(stop_loss) != ''),
                stop_loss_price=to_decimal(stop_loss) if stop_loss and str(stop_loss) != '0' and str(stop_loss) != '' else None,
                has_take_profit=bool(take_profit and str(take_profit) != '0' and str(take_profit) != ''),
                take_profit_price=to_decimal(take_profit) if take_profit and str(take_profit) != '0' and str(take_profit) != '' else None,
                has_trailing_stop=bool(trailing_stop and str(trailing_stop) != '0' and str(trailing_stop) != ''),
                trailing_stop_distance=to_decimal(trailing_stop) if trailing_stop and str(trailing_stop) != '0' and str(trailing_stop) != '' else None,
                opened_at=opened_at,
                status='OPEN',
                raw_data=data
            )
            
            # Log for debugging
            if avg_price == 0 or mark_price == 0:
                logger.warning(f"Position {symbol} has zero prices: entry={avg_price}, mark={mark_price}")
                logger.debug(f"Raw data: {json.dumps(data, indent=2)}")
            
            return position
            
        except Exception as e:
            logger.error(f"Error parsing Bybit position: {e}")
            logger.error(f"Position data: {json.dumps(data, indent=2)}")
            raise
    
    async def process_message(self, message: str, db_pool: asyncpg.Pool):
        """Process WebSocket message"""
        try:
            data = json.loads(message)
            topic = data.get('topic')
            
            if topic == 'position':
                await self.handle_position_update(data, db_pool)
            elif topic == 'wallet':
                await self.handle_wallet_update(data, db_pool)
            elif topic == 'order':
                await self.handle_order_update(data, db_pool)
                
        except Exception as e:
            logger.error(f"Error processing Bybit message: {e}")
    
    async def handle_position_update(self, data: Dict, db_pool: asyncpg.Pool):
        """Handle position update"""
        for pos_data in data.get('data', []):
            position = self.parse_position(pos_data)
            await self.update_position_in_db(position, db_pool)
    
    async def handle_wallet_update(self, data: Dict, db_pool: asyncpg.Pool):
        """Handle wallet update"""
        for wallet_data in data.get('data', []):
            for coin in wallet_data.get('coin', []):
                try:
                    # Safe decimal conversion
                    free_val = coin.get('availableToWithdraw', 0)
                    locked_val = coin.get('locked', 0)
                    
                    # Convert to string and handle null/empty values
                    free_str = str(free_val) if free_val not in [None, '', 'null'] else '0'
                    locked_str = str(locked_val) if locked_val not in [None, '', 'null'] else '0'
                    
                    balance = Balance(
                        exchange="bybit",
                        asset=coin.get('coin'),
                        free=Decimal(free_str),
                        locked=Decimal(locked_str)
                    )
                    await self.update_balance_in_db(balance, db_pool)
                except Exception as e:
                    logger.error(f"Error parsing wallet data for {coin.get('coin', 'unknown')}: {e}")
    
    async def handle_order_update(self, data: Dict, db_pool: asyncpg.Pool):
        """Handle order update"""
        for order_data in data.get('data', []):
            # Update position protection if order is SL/TP
            if order_data.get('stopOrderType'):
                symbol = order_data.get('symbol')
                side = order_data.get('side')
                position_id = f"bybit_{symbol}_{side}"
                
                async with db_pool.acquire() as conn:
                    if order_data.get('stopOrderType') == 'StopLoss':
                        await conn.execute(
                            "UPDATE monitoring.positions SET has_stop_loss = true, stop_loss_price = $1 "
                            "WHERE position_id = $2 AND status = 'OPEN'",
                            Decimal(str(order_data.get('triggerPrice', 0))), position_id
                        )
                    elif order_data.get('stopOrderType') == 'TakeProfit':
                        await conn.execute(
                            "UPDATE monitoring.positions SET has_take_profit = true, take_profit_price = $1 "
                            "WHERE position_id = $2 AND status = 'OPEN'",
                            Decimal(str(order_data.get('triggerPrice', 0))), position_id
                        )
    
    async def update_position_in_db(self, position: Position, db_pool: asyncpg.Pool):
        """Update position in database"""
        query = """
            INSERT INTO monitoring.positions (
                exchange, symbol, position_id, side, entry_price,
                current_price, quantity, margin, leverage,
                unrealized_pnl, realized_pnl, has_stop_loss, stop_loss_price,
                has_take_profit, take_profit_price, has_trailing_stop,
                trailing_stop_distance, status, opened_at, raw_data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
            ON CONFLICT (exchange, position_id) 
            DO UPDATE SET
                current_price = $6,
                quantity = $7,
                unrealized_pnl = $10,
                realized_pnl = $11,
                has_stop_loss = $12,
                stop_loss_price = $13,
                has_take_profit = $14,
                take_profit_price = $15,
                has_trailing_stop = $16,
                trailing_stop_distance = $17,
                status = $18,
                raw_data = $20
        """
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                query,
                position.exchange, position.symbol, position.position_id,
                position.side, position.entry_price, position.current_price,
                position.quantity, position.margin, position.leverage,
                position.unrealized_pnl, position.realized_pnl,
                position.has_stop_loss, position.stop_loss_price,
                position.has_take_profit, position.take_profit_price,
                position.has_trailing_stop, position.trailing_stop_distance,
                position.status, position.opened_at,
                json.dumps(position.raw_data) if position.raw_data else None
            )
    
    async def update_balance_in_db(self, balance: Balance, db_pool: asyncpg.Pool):
        """Update balance in database"""
        query = """
            INSERT INTO monitoring.balances (exchange, asset, free, locked)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (exchange, asset)
            DO UPDATE SET
                free = $3,
                locked = $4
        """
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                query,
                balance.exchange,
                balance.asset,
                balance.free,
                balance.locked
            )
    
    async def disconnect(self):
        """Disconnect WebSocket and cleanup"""
        self.running = False
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()


class MonitoringDaemon:
    """Main monitoring daemon coordinator"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.db_pool = None
        self.binance_client = None
        self.bybit_client = None
        self.running = False
        
    async def init_db(self):
        """Initialize database connection pool"""
        self.db_pool = await asyncpg.create_pool(
            host=self.config['db']['host'],
            port=self.config['db']['port'],
            database=self.config['db']['database'],
            user=self.config['db']['user'],
            password=self.config['db']['password'],
            min_size=10,
            max_size=20
        )
        
        # Create schema if not exists
        async with self.db_pool.acquire() as conn:
            await conn.execute('CREATE SCHEMA IF NOT EXISTS monitoring')
            logger.info("Database connection pool initialized")
    
    async def update_system_health(self, component: str, status: str, error_message: str = None):
        """Update system health status"""
        if not self.db_pool:
            return
            
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO monitoring.system_health (component, status, last_heartbeat, error_message)
                    VALUES ($1, $2, NOW(), $3)
                    """,
                    component, status, error_message
                )
        except Exception as e:
            logger.error(f"Failed to update system health: {e}")
    
    async def start(self):
        """Start the monitoring daemon"""
        self.running = True
        
        # Initialize database
        await self.init_db()
        await self.update_system_health('daemon', 'healthy')
        
        # Initialize exchange clients
        tasks = []
        
        if self.config.get('binance', {}).get('enabled'):
            self.binance_client = BinanceWebSocketClient(
                api_key=self.config['binance']['api_key'],
                api_secret=self.config['binance']['api_secret'],
                testnet=self.config['binance'].get('testnet', False)
            )
            tasks.append(asyncio.create_task(
                self.binance_client.connect(self.db_pool)
            ))
            logger.info("Starting Binance WebSocket client")
        
        if self.config.get('bybit', {}).get('enabled'):
            self.bybit_client = BybitWebSocketClient(
                api_key=self.config['bybit']['api_key'],
                api_secret=self.config['bybit']['api_secret'],
                testnet=self.config['bybit'].get('testnet', False)
            )
            tasks.append(asyncio.create_task(
                self.bybit_client.connect(self.db_pool)
            ))
            logger.info("Starting Bybit WebSocket client")
        
        # Start periodic tasks
        tasks.append(asyncio.create_task(self.cleanup_old_data()))
        tasks.append(asyncio.create_task(self.health_check()))
        tasks.append(asyncio.create_task(self.periodic_sync()))
        
        # Wait for all tasks
        await asyncio.gather(*tasks)
    
    async def cleanup_old_data(self):
        """Clean up old closed positions"""
        while self.running:
            try:
                query = """
                    DELETE FROM monitoring.positions 
                    WHERE status = 'CLOSED' 
                    AND closed_at < NOW() - INTERVAL '7 days'
                """
                async with self.db_pool.acquire() as conn:
                    result = await conn.execute(query)
                    # Extract row count from result
                    count = int(result.split()[-1]) if result else 0
                    if count > 0:
                        logger.info(f"Cleaned up {count} old positions")
                
                # Clean up old health records
                query = """
                    DELETE FROM monitoring.system_health 
                    WHERE created_at < NOW() - INTERVAL '1 day'
                """
                async with self.db_pool.acquire() as conn:
                    await conn.execute(query)
                
                await asyncio.sleep(3600)  # Run every hour
            except Exception as e:
                logger.error(f"Error in cleanup: {e}")
                await asyncio.sleep(3600)
    
    async def health_check(self):
        """Check system health"""
        while self.running:
            try:
                # Check database connection
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval('SELECT 1')
                await self.update_system_health('database', 'healthy')
                
                # Check WebSocket connections
                if self.binance_client:
                    status = "healthy" if self.binance_client.ws else "offline"
                    await self.update_system_health('binance_ws', status)
                
                if self.bybit_client:
                    status = "healthy" if self.bybit_client.ws else "offline"
                    await self.update_system_health('bybit_ws', status)
                
                # Log overall status
                binance_status = "connected" if self.binance_client and self.binance_client.ws else "disconnected"
                bybit_status = "connected" if self.bybit_client and self.bybit_client.ws else "disconnected"
                
                logger.info(f"Health check - DB: OK, Binance: {binance_status}, Bybit: {bybit_status}")
                
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                await self.update_system_health('daemon', 'error', str(e))
                await asyncio.sleep(30)
    
    async def periodic_sync(self):
        """Periodically sync positions from REST API to catch any missed updates"""
        await asyncio.sleep(60)  # Wait 1 minute before first sync
        
        while self.running:
            try:
                logger.info("Starting periodic position sync")
                
                # Sync Binance positions
                if self.binance_client and self.binance_client.ws:
                    try:
                        await self.binance_client.sync_initial_positions(self.db_pool)
                        logger.info("Periodic sync: Binance positions updated")
                    except Exception as e:
                        logger.error(f"Periodic sync failed for Binance: {e}")
                
                # Sync Bybit positions
                if self.bybit_client and self.bybit_client.ws:
                    try:
                        await self.bybit_client.sync_initial_positions(self.db_pool)
                        logger.info("Periodic sync: Bybit positions updated")
                    except Exception as e:
                        logger.error(f"Periodic sync failed for Bybit: {e}")
                
                # Sync balances
                if self.binance_client and self.binance_client.ws:
                    try:
                        await self.binance_client.sync_account_balance(self.db_pool)
                        logger.info("Periodic sync: Binance balances updated")
                    except Exception as e:
                        logger.error(f"Periodic sync failed for Binance balances: {e}")
                
                if self.bybit_client and self.bybit_client.ws:
                    try:
                        await self.bybit_client.sync_wallet_balances(self.db_pool)
                        logger.info("Periodic sync: Bybit balances updated")
                    except Exception as e:
                        logger.error(f"Periodic sync failed for Bybit balances: {e}")
                
                # Wait 5 minutes before next sync
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Error in periodic sync: {e}")
                await asyncio.sleep(300)
    
    async def stop(self):
        """Stop the monitoring daemon"""
        self.running = False
        
        if self.binance_client:
            await self.binance_client.disconnect()
        
        if self.bybit_client:
            await self.bybit_client.disconnect()
        
        if self.db_pool:
            await self.db_pool.close()
        
        logger.info("Monitoring daemon stopped")


async def main():
    """Main entry point"""
    import yaml
    import os
    from pathlib import Path
    
    # Load configuration
    config_path = Path(__file__).parent.parent / 'config' / 'monitoring.yaml'
    
    # Check if config exists, if not create from environment
    if not config_path.exists():
        config = {
            'db': {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 5432)),
                'database': os.getenv('DB_NAME', 'fox_crypto'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD')
            },
            'binance': {
                'enabled': bool(os.getenv('BINANCE_API_KEY')),
                'api_key': os.getenv('BINANCE_API_KEY', ''),
                'api_secret': os.getenv('BINANCE_API_SECRET', ''),
                'testnet': os.getenv('BINANCE_TESTNET', 'false').lower() == 'true'
            },
            'bybit': {
                'enabled': bool(os.getenv('BYBIT_API_KEY')),
                'api_key': os.getenv('BYBIT_API_KEY', ''),
                'api_secret': os.getenv('BYBIT_API_SECRET', ''),
                'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true'
            }
        }
    else:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    
    daemon = MonitoringDaemon(config)
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        asyncio.create_task(daemon.stop())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await daemon.start()
    except Exception as e:
        logger.error(f"Daemon crashed: {e}")
        await daemon.stop()


if __name__ == "__main__":
    asyncio.run(main())