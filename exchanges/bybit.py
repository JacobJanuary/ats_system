#!/usr/bin/env python3
"""
Bybit Exchange Implementation
"""

import asyncio
import logging
from typing import Dict, Optional, List
from decimal import Decimal
import hmac
import hashlib
import time
import aiohttp
import json
from .base import BaseExchange

logger = logging.getLogger(__name__)


class BybitExchange(BaseExchange):
    """Bybit futures exchange implementation"""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        if self.testnet:
            self.base_url = "https://api-testnet.bybit.com"
            self.ws_url = "wss://stream-testnet.bybit.com"
        else:
            self.base_url = "https://api.bybit.com"
            self.ws_url = "wss://stream.bybit.com"
        
        self.session = None
        self.recv_window = 5000
        
    async def initialize(self):
        """Initialize Bybit connection"""
        self.session = aiohttp.ClientSession()
        
        # Test connectivity and authentication
        server_time = await self._make_request("GET", "/v5/market/time")
        if server_time:
            logger.info(f"Bybit {'testnet' if self.testnet else 'mainnet'} initialized")
        else:
            raise Exception("Failed to connect to Bybit")
    
    def _generate_signature(self, timestamp: str, params: str) -> str:
        """Generate signature for Bybit API"""
        param_str = f"{timestamp}{self.api_key}{self.recv_window}{params}"
        return hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    async def _make_request(self, method: str, endpoint: str, params: Dict = None, signed: bool = False):
        """Make HTTP request to Bybit API"""
        if not self.session:
            return None
            
        url = self.base_url + endpoint
        timestamp = str(int(time.time() * 1000))
        
        headers = {}
        
        if signed and self.api_key and self.api_secret:
            headers = {
                'X-BAPI-API-KEY': self.api_key,
                'X-BAPI-TIMESTAMP': timestamp,
                'X-BAPI-RECV-WINDOW': str(self.recv_window),
            }
            
            if method == "GET" and params:
                query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
                headers['X-BAPI-SIGN'] = self._generate_signature(timestamp, query_string)
            elif method == "POST":
                body_string = json.dumps(params) if params else ""
                headers['X-BAPI-SIGN'] = self._generate_signature(timestamp, body_string)
                headers['Content-Type'] = 'application/json'
        
        try:
            if method == "GET":
                async with self.session.get(url, params=params, headers=headers) as response:
                    return await self._handle_response(response)
            elif method == "POST":
                async with self.session.post(url, json=params, headers=headers) as response:
                    return await self._handle_response(response)
        except Exception as e:
            logger.error(f"Bybit request error: {e}")
            return None
    
    async def _handle_response(self, response):
        """Handle API response"""
        try:
            data = await response.json()
            if response.status == 200:
                if data.get('retCode') == 0:
                    return data.get('result', data)
                else:
                    logger.error(f"Bybit API error: {data.get('retMsg')}")
                    return None
            else:
                logger.error(f"Bybit HTTP error {response.status}: {data}")
                return None
        except Exception as e:
            logger.error(f"Error parsing Bybit response: {e}")
            return None
    
    async def get_balance(self) -> float:
        """Get USDT balance"""
        wallet = await self._make_request("GET", "/v5/account/wallet-balance", 
                                         {'accountType': 'UNIFIED'}, signed=True)
        if not wallet:
            # Try CONTRACT account if UNIFIED fails
            wallet = await self._make_request("GET", "/v5/account/wallet-balance", 
                                             {'accountType': 'CONTRACT'}, signed=True)
        
        if wallet and 'list' in wallet:
            for account in wallet['list']:
                for coin in account.get('coin', []):
                    if coin['coin'] == 'USDT':
                        # Try different balance fields in order of preference
                        # For futures trading, walletBalance is the total balance
                        # availableToWithdraw might be empty on testnet
                        for balance_field in ['walletBalance', 'equity', 'availableToWithdraw']:
                            balance_str = coin.get(balance_field, 0)
                            
                            # Skip empty strings and None
                            if balance_str == '' or balance_str is None:
                                continue
                            
                            try:
                                balance = float(balance_str)
                                if balance > 0:
                                    logger.debug(f"Using {balance_field} for USDT balance: {balance}")
                                    return balance
                            except (ValueError, TypeError):
                                continue
                        
                        # If all fields failed, return 0
                        logger.warning("Could not extract USDT balance from any field")
                        return 0.0
        return 0.0
    
    async def get_ticker(self, symbol: str) -> Dict:
        """Get ticker info"""
        ticker = await self._make_request("GET", "/v5/market/tickers", 
                                         {'category': 'linear', 'symbol': symbol})
        if not ticker or 'list' not in ticker or not ticker['list']:
            return {}
        
        ticker_data = ticker['list'][0]
        
        # Handle empty string prices
        def safe_float(value, default=0):
            if value == '' or value is None:
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default
        
        return {
            'symbol': ticker_data.get('symbol'),
            'bid': safe_float(ticker_data.get('bid1Price', 0)),
            'ask': safe_float(ticker_data.get('ask1Price', 0)),
            'price': safe_float(ticker_data.get('lastPrice', 0))
        }

    async def set_leverage(self, symbol: str, leverage: int):
        """Set leverage for symbol with proper error handling"""
        params = {
            'category': 'linear',
            'symbol': symbol,
            'buyLeverage': str(leverage),
            'sellLeverage': str(leverage)
        }

        try:
            result = await self._make_request("POST", "/v5/position/set-leverage", params, signed=True)
            if result:
                # Only log if actually changed
                logger.debug(f"Leverage set to {leverage}x for {symbol}")
                return True
        except Exception as e:
            error_msg = str(e).lower()
            # Silently ignore if leverage already set
            if "leverage not modified" in error_msg or "leverage is not modified" in error_msg:
                logger.debug(f"Leverage already set for {symbol}")
                return True
            else:
                # Re-raise other errors
                raise

        return True
    
    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """Create market order"""
        # Get symbol info for quantity precision
        instruments = await self._make_request("GET", "/v5/market/instruments-info",
                                              {'category': 'linear', 'symbol': symbol})
        
        qty_step = 0.001  # Default
        if instruments and 'list' in instruments and instruments['list']:
            qty_step = float(instruments['list'][0].get('lotSizeFilter', {}).get('qtyStep', 0.001))
        
        # Round quantity to step
        quantity = round(quantity / qty_step) * qty_step
        
        params = {
            'category': 'linear',
            'symbol': symbol,
            'side': 'Buy' if side.upper() == 'BUY' else 'Sell',
            'orderType': 'Market',
            'qty': str(quantity),
            'timeInForce': 'GTC',  # Changed from IOC to GTC for better fill on testnet
            'positionIdx': 0  # One-way mode
        }
        
        order = await self._make_request("POST", "/v5/order/create", params, signed=True)
        if order:
            logger.info(f"Market order created: {side} {quantity} {symbol}")
            return {
                'orderId': order.get('orderId'),
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'price': 0,  # Market order, price will be in execution
                'status': 'FILLED'
            }
        return None
    
    async def get_open_positions(self) -> List[Dict]:
        """Get open positions"""
        positions = await self._make_request("GET", "/v5/position/list",
                                           {'category': 'linear', 'settleCoin': 'USDT'}, 
                                           signed=True)
        if not positions or 'list' not in positions:
            return []
        
        open_positions = []
        for pos in positions['list']:
            if float(pos.get('size', 0)) > 0:
                open_positions.append({
                    'symbol': pos['symbol'],
                    'side': pos['side'].upper(),
                    'quantity': float(pos['size']),
                    'entry_price': float(pos.get('avgPrice', 0)),
                    'mark_price': float(pos.get('markPrice', 0)),
                    'pnl': float(pos.get('unrealisedPnl', 0)),
                    'pnl_percent': (float(pos.get('unrealisedPnl', 0)) / float(pos.get('positionValue', 1)) * 100) if float(pos.get('positionValue', 0)) > 0 else 0
                })
        
        return open_positions
    
    async def close_position(self, symbol: str) -> bool:
        """Close position"""
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                side = 'Sell' if pos['side'] == 'LONG' else 'Buy'
                order = await self.create_market_order(symbol, side, pos['quantity'])
                return order is not None
        return False
    
    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        """Set stop loss"""
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'stopLoss': str(round(stop_price, 2)),
                    'slTriggerBy': 'MarkPrice',
                    'positionIdx': 0
                }
                result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)
                return result is not None
        return False
    
    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Set take profit"""
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'takeProfit': str(round(take_profit_price, 2)),
                    'tpTriggerBy': 'MarkPrice',
                    'positionIdx': 0
                }
                result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)
                return result is not None
        return False
    
    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """Set trailing stop"""
        positions = await self.get_open_positions()
        for pos in positions:
            if pos['symbol'] == symbol:
                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'trailingStop': str(callback_rate * 100),  # Bybit uses basis points
                    'activePrice': str(round(activation_price, 2)),
                    'positionIdx': 0
                }
                result = await self._make_request("POST", "/v5/position/trading-stop", params, signed=True)
                return result is not None
        return False
    
    async def close(self):
        """Close connection"""
        if self.session:
            await self.session.close()