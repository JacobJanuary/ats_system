#!/usr/bin/env python3
"""
Base Exchange Class
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any
import logging

logger = logging.getLogger(__name__)


class BaseExchange(ABC):
    """Abstract base class for exchange implementations"""
    
    def __init__(self, config: Dict):
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')
        self.testnet = config.get('testnet', False)
        self.client = None
        self.name = self.__class__.__name__.replace('Exchange', '')
        
    @abstractmethod
    async def initialize(self):
        """Initialize exchange connection"""
        pass
    
    @abstractmethod
    async def get_balance(self) -> float:
        """Get account balance in USDT"""
        pass
    
    @abstractmethod
    async def get_ticker(self, symbol: str) -> Dict:
        """Get ticker info for symbol"""
        pass
    
    @abstractmethod
    async def create_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """Create market order"""
        pass
    
    @abstractmethod
    async def set_leverage(self, symbol: str, leverage: int):
        """Set leverage for symbol"""
        pass
    
    @abstractmethod
    async def get_open_positions(self) -> List[Dict]:
        """Get all open positions"""
        pass
    
    @abstractmethod
    async def close_position(self, symbol: str) -> bool:
        """Close position for symbol"""
        pass
    
    @abstractmethod
    async def set_stop_loss(self, symbol: str, stop_price: float) -> bool:
        """Set stop loss for position"""
        pass
    
    @abstractmethod
    async def set_take_profit(self, symbol: str, take_profit_price: float) -> bool:
        """Set take profit for position"""
        pass
    
    @abstractmethod
    async def set_trailing_stop(self, symbol: str, activation_price: float, callback_rate: float) -> bool:
        """Set trailing stop for position"""
        pass
    
    @abstractmethod
    async def close(self):
        """Close exchange connection"""
        pass