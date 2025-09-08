"""
ATS 2.0 - WebSocket Manager
Real-time data streaming for exchanges
"""
import asyncio
import json
import logging
from typing import Dict, Any, Optional, Callable, Set, List
from datetime import datetime
import websockets
from websockets.client import WebSocketClientProtocol
from enum import Enum

from core.retry import retry, ExponentialBackoff
from core.security import safe_log

logger = logging.getLogger(__name__)


class StreamType(Enum):
    """WebSocket stream types"""
    TRADES = "trades"
    ORDERBOOK = "orderbook"
    TICKER = "ticker"
    POSITIONS = "positions"
    ORDERS = "orders"
    BALANCE = "balance"


class WebSocketManager:
    """Manages WebSocket connections for real-time data"""
    
    def __init__(self, exchange_name: str, url: str, api_key: Optional[str] = None):
        """
        Initialize WebSocket manager
        
        Args:
            exchange_name: Name of exchange
            url: WebSocket URL
            api_key: Optional API key for authenticated streams
        """
        self.exchange_name = exchange_name
        self.url = url
        self.api_key = api_key
        
        self.ws: Optional[WebSocketClientProtocol] = None
        self.subscriptions: Dict[str, Set[str]] = {}
        self.callbacks: Dict[str, Callable] = {}
        self.running = False
        self.reconnect_delay = ExponentialBackoff(initial=1.0, maximum=60.0)
        
        # Heartbeat
        self.heartbeat_interval = 30
        self.last_heartbeat = datetime.now()
        self.heartbeat_task = None
    
    async def connect(self) -> bool:
        """Establish WebSocket connection"""
        try:
            logger.info(f"Connecting to {self.exchange_name} WebSocket...")
            self.ws = await websockets.connect(
                self.url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )
            
            # Authenticate if needed
            if self.api_key:
                await self._authenticate()
            
            self.running = True
            self.reconnect_delay.reset()
            
            # Start heartbeat
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            logger.info(f"Connected to {self.exchange_name} WebSocket")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.exchange_name} WebSocket: {e}")
            return False
    
    async def disconnect(self):
        """Close WebSocket connection"""
        self.running = False
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        if self.ws:
            await self.ws.close()
            self.ws = None
        
        logger.info(f"Disconnected from {self.exchange_name} WebSocket")
    
    async def subscribe(
        self,
        stream_type: StreamType,
        symbols: List[str],
        callback: Callable
    ) -> bool:
        """
        Subscribe to data stream
        
        Args:
            stream_type: Type of stream
            symbols: List of symbols to subscribe
            callback: Callback function for data
        """
        if not self.ws:
            logger.error("WebSocket not connected")
            return False
        
        try:
            # Store subscription info
            stream_key = stream_type.value
            if stream_key not in self.subscriptions:
                self.subscriptions[stream_key] = set()
            self.subscriptions[stream_key].update(symbols)
            
            # Store callback
            for symbol in symbols:
                callback_key = f"{stream_key}:{symbol}"
                self.callbacks[callback_key] = callback
            
            # Send subscription message (format depends on exchange)
            if self.exchange_name == "binance":
                await self._subscribe_binance(stream_type, symbols)
            elif self.exchange_name == "bybit":
                await self._subscribe_bybit(stream_type, symbols)
            
            logger.info(f"Subscribed to {stream_type.value} for {symbols}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")
            return False
    
    async def _subscribe_binance(self, stream_type: StreamType, symbols: List[str]):
        """Binance-specific subscription"""
        streams = []
        
        for symbol in symbols:
            symbol_lower = symbol.lower()
            if stream_type == StreamType.TRADES:
                streams.append(f"{symbol_lower}@trade")
            elif stream_type == StreamType.ORDERBOOK:
                streams.append(f"{symbol_lower}@depth20")
            elif stream_type == StreamType.TICKER:
                streams.append(f"{symbol_lower}@ticker")
        
        if streams:
            message = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": 1
            }
            await self.ws.send(json.dumps(message))
    
    async def _subscribe_bybit(self, stream_type: StreamType, symbols: List[str]):
        """Bybit-specific subscription"""
        topics = []
        
        for symbol in symbols:
            if stream_type == StreamType.TRADES:
                topics.append(f"publicTrade.{symbol}")
            elif stream_type == StreamType.ORDERBOOK:
                topics.append(f"orderbook.50.{symbol}")
            elif stream_type == StreamType.TICKER:
                topics.append(f"tickers.{symbol}")
            elif stream_type == StreamType.POSITIONS:
                topics.append(f"position")
            elif stream_type == StreamType.ORDERS:
                topics.append(f"order")
        
        if topics:
            message = {
                "op": "subscribe",
                "args": topics
            }
            await self.ws.send(json.dumps(message))
    
    async def unsubscribe(self, stream_type: StreamType, symbols: List[str]) -> bool:
        """Unsubscribe from data stream"""
        if not self.ws:
            return False
        
        try:
            stream_key = stream_type.value
            if stream_key in self.subscriptions:
                self.subscriptions[stream_key] -= set(symbols)
            
            # Remove callbacks
            for symbol in symbols:
                callback_key = f"{stream_key}:{symbol}"
                self.callbacks.pop(callback_key, None)
            
            # Send unsubscribe message
            if self.exchange_name == "binance":
                await self._unsubscribe_binance(stream_type, symbols)
            elif self.exchange_name == "bybit":
                await self._unsubscribe_bybit(stream_type, symbols)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}")
            return False
    
    async def _unsubscribe_binance(self, stream_type: StreamType, symbols: List[str]):
        """Binance-specific unsubscription"""
        streams = []
        
        for symbol in symbols:
            symbol_lower = symbol.lower()
            if stream_type == StreamType.TRADES:
                streams.append(f"{symbol_lower}@trade")
            elif stream_type == StreamType.ORDERBOOK:
                streams.append(f"{symbol_lower}@depth20")
            elif stream_type == StreamType.TICKER:
                streams.append(f"{symbol_lower}@ticker")
        
        if streams:
            message = {
                "method": "UNSUBSCRIBE",
                "params": streams,
                "id": 2
            }
            await self.ws.send(json.dumps(message))
    
    async def _unsubscribe_bybit(self, stream_type: StreamType, symbols: List[str]):
        """Bybit-specific unsubscription"""
        topics = []
        
        for symbol in symbols:
            if stream_type == StreamType.TRADES:
                topics.append(f"publicTrade.{symbol}")
            elif stream_type == StreamType.ORDERBOOK:
                topics.append(f"orderbook.50.{symbol}")
            elif stream_type == StreamType.TICKER:
                topics.append(f"tickers.{symbol}")
        
        if topics:
            message = {
                "op": "unsubscribe",
                "args": topics
            }
            await self.ws.send(json.dumps(message))
    
    async def listen(self):
        """Main listening loop"""
        while self.running:
            try:
                if not self.ws:
                    # Try to reconnect
                    delay = self.reconnect_delay.next_delay()
                    logger.info(f"Reconnecting in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    
                    if await self.connect():
                        # Resubscribe to all streams
                        await self._resubscribe_all()
                    continue
                
                # Receive message
                message = await asyncio.wait_for(self.ws.recv(), timeout=60)
                
                # Process message
                await self._process_message(message)
                
            except asyncio.TimeoutError:
                logger.warning("WebSocket receive timeout")
                continue
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed")
                self.ws = None
                
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(1)
    
    async def _process_message(self, message: str):
        """Process incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Handle different message types based on exchange
            if self.exchange_name == "binance":
                await self._process_binance_message(data)
            elif self.exchange_name == "bybit":
                await self._process_bybit_message(data)
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")
    
    async def _process_binance_message(self, data: Dict[str, Any]):
        """Process Binance WebSocket message"""
        # Handle different event types
        if 'e' in data:  # Event type
            event_type = data['e']
            symbol = data.get('s', '')
            
            if event_type == 'trade':
                callback_key = f"trades:{symbol}"
            elif event_type == 'depthUpdate':
                callback_key = f"orderbook:{symbol}"
            elif event_type == '24hrTicker':
                callback_key = f"ticker:{symbol}"
            else:
                return
            
            # Call registered callback
            if callback_key in self.callbacks:
                await self.callbacks[callback_key](data)
    
    async def _process_bybit_message(self, data: Dict[str, Any]):
        """Process Bybit WebSocket message"""
        # Handle different message types
        if 'topic' in data:
            topic = data['topic']
            
            # Parse topic to get stream type and symbol
            if topic.startswith('publicTrade.'):
                symbol = topic.replace('publicTrade.', '')
                callback_key = f"trades:{symbol}"
            elif topic.startswith('orderbook.'):
                parts = topic.split('.')
                symbol = parts[-1]
                callback_key = f"orderbook:{symbol}"
            elif topic.startswith('tickers.'):
                symbol = topic.replace('tickers.', '')
                callback_key = f"ticker:{symbol}"
            elif topic == 'position':
                callback_key = "positions:all"
            elif topic == 'order':
                callback_key = "orders:all"
            else:
                return
            
            # Call registered callback
            if callback_key in self.callbacks:
                await self.callbacks[callback_key](data.get('data', data))
    
    async def _authenticate(self):
        """Authenticate WebSocket connection"""
        # Implementation depends on exchange
        if self.exchange_name == "binance":
            # Binance uses listen key for user data streams
            pass
        elif self.exchange_name == "bybit":
            # Bybit authentication
            import time
            import hmac
            import hashlib
            
            expires = int((time.time() + 10) * 1000)
            signature = hmac.new(
                self.api_key.encode('utf-8'),
                f'GET/realtime{expires}'.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            auth_message = {
                "op": "auth",
                "args": [self.api_key, expires, signature]
            }
            await self.ws.send(json.dumps(auth_message))
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeat to keep connection alive"""
        while self.running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                if self.ws:
                    # Send ping based on exchange
                    if self.exchange_name == "binance":
                        await self.ws.send(json.dumps({"method": "ping"}))
                    elif self.exchange_name == "bybit":
                        await self.ws.send(json.dumps({"op": "ping"}))
                    
                    self.last_heartbeat = datetime.now()
                    
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    async def _resubscribe_all(self):
        """Resubscribe to all streams after reconnection"""
        for stream_type_str, symbols in self.subscriptions.items():
            if symbols:
                stream_type = StreamType(stream_type_str)
                
                # Get callbacks for these subscriptions
                for symbol in symbols:
                    callback_key = f"{stream_type_str}:{symbol}"
                    if callback_key in self.callbacks:
                        await self.subscribe(
                            stream_type,
                            [symbol],
                            self.callbacks[callback_key]
                        )