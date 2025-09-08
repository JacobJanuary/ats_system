"""
ATS 2.0 - Security Module
Handles encryption, sanitization and security features
"""
import os
import re
import json
from typing import Any, Dict, Optional, Union
from cryptography.fernet import Fernet
import base64
import logging

logger = logging.getLogger(__name__)


class SecretsManager:
    """Manages encryption and decryption of sensitive data"""
    
    def __init__(self, master_key: Optional[str] = None):
        """
        Initialize with master key from environment or generate new
        
        Args:
            master_key: Base64 encoded master key
        """
        if master_key:
            self.cipher = Fernet(master_key.encode() if isinstance(master_key, str) else master_key)
        else:
            # Try to load from environment
            env_key = os.getenv('ATS_MASTER_KEY')
            if env_key:
                self.cipher = Fernet(env_key.encode())
            else:
                # Generate new key (should be saved securely)
                key = Fernet.generate_key()
                self.cipher = Fernet(key)
                logger.warning(f"Generated new master key: {key.decode()}")
                logger.warning("Save this key securely in ATS_MASTER_KEY environment variable!")
    
    def encrypt(self, data: str) -> str:
        """Encrypt sensitive data"""
        return self.cipher.encrypt(data.encode()).decode()
    
    def decrypt(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        return self.cipher.decrypt(encrypted_data.encode()).decode()
    
    def encrypt_dict(self, data: Dict[str, Any], keys_to_encrypt: list) -> Dict[str, Any]:
        """Encrypt specific keys in dictionary"""
        encrypted = data.copy()
        for key in keys_to_encrypt:
            if key in encrypted and encrypted[key]:
                encrypted[key] = self.encrypt(str(encrypted[key]))
        return encrypted
    
    def decrypt_dict(self, data: Dict[str, Any], keys_to_decrypt: list) -> Dict[str, Any]:
        """Decrypt specific keys in dictionary"""
        decrypted = data.copy()
        for key in keys_to_decrypt:
            if key in decrypted and decrypted[key]:
                try:
                    decrypted[key] = self.decrypt(str(decrypted[key]))
                except Exception as e:
                    logger.error(f"Failed to decrypt {key}: {e}")
        return decrypted


class LogSanitizer:
    """Sanitizes logs to prevent leaking sensitive information"""
    
    # Patterns to detect sensitive data
    SENSITIVE_PATTERNS = [
        (r'([aA][pP][iI][-_]?[kK][eE][yY])["\']?\s*[:=]\s*["\']?([a-zA-Z0-9\-_]+)', r'\1=***HIDDEN***'),
        (r'([aA][pP][iI][-_]?[sS][eE][cC][rR][eE][tT])["\']?\s*[:=]\s*["\']?([a-zA-Z0-9\-_]+)', r'\1=***HIDDEN***'),
        (r'([pP][aA][sS][sS][wW][oO][rR][dD])["\']?\s*[:=]\s*["\']?([^\s"\']+)', r'\1=***HIDDEN***'),
        (r'([tT][oO][kK][eE][nN])["\']?\s*[:=]\s*["\']?([a-zA-Z0-9\-_]+)', r'\1=***HIDDEN***'),
        (r'([sS][eE][cC][rR][eE][tT])["\']?\s*[:=]\s*["\']?([^\s"\']+)', r'\1=***HIDDEN***'),
        # Binance API keys pattern
        (r'[a-zA-Z0-9]{64}', lambda m: '***API_KEY***' if len(m.group()) == 64 else m.group()),
        # Common private key patterns
        (r'-----BEGIN[A-Z ]+PRIVATE KEY-----[\s\S]+?-----END[A-Z ]+PRIVATE KEY-----', '***PRIVATE_KEY***'),
    ]
    
    SENSITIVE_KEYS = {
        'api_key', 'api_secret', 'apikey', 'apisecret', 'secret_key', 'secret',
        'password', 'pwd', 'pass', 'token', 'access_token', 'refresh_token',
        'private_key', 'priv_key', 'credential', 'auth', 'authorization'
    }
    
    @classmethod
    def sanitize_string(cls, text: str) -> str:
        """Sanitize sensitive data in string"""
        if not text:
            return text
            
        sanitized = text
        for pattern, replacement in cls.SENSITIVE_PATTERNS:
            if callable(replacement):
                sanitized = re.sub(pattern, replacement, sanitized)
            else:
                sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
        
        return sanitized
    
    @classmethod
    def sanitize_dict(cls, data: Union[dict, list, Any], deep: bool = True) -> Union[dict, list, Any]:
        """
        Sanitize sensitive data in dictionary or list
        
        Args:
            data: Data to sanitize
            deep: Whether to recursively sanitize nested structures
        """
        if isinstance(data, dict):
            sanitized = {}
            for key, value in data.items():
                # Check if key is sensitive
                if any(sensitive in key.lower() for sensitive in cls.SENSITIVE_KEYS):
                    sanitized[key] = '***HIDDEN***'
                elif deep and isinstance(value, (dict, list)):
                    sanitized[key] = cls.sanitize_dict(value, deep=True)
                elif isinstance(value, str):
                    sanitized[key] = cls.sanitize_string(value)
                else:
                    sanitized[key] = value
            return sanitized
            
        elif isinstance(data, list):
            return [cls.sanitize_dict(item, deep=deep) if isinstance(item, (dict, list)) 
                   else cls.sanitize_string(item) if isinstance(item, str) 
                   else item for item in data]
        
        elif isinstance(data, str):
            return cls.sanitize_string(data)
        
        return data
    
    @classmethod
    def sanitize_config(cls, config: Any) -> dict:
        """
        Sanitize configuration object for safe logging
        
        Args:
            config: Configuration object (dict, dataclass, or object with to_dict())
        """
        # Convert to dict if needed
        if hasattr(config, 'to_dict'):
            config_dict = config.to_dict()
        elif hasattr(config, '__dict__'):
            config_dict = config.__dict__
        elif isinstance(config, dict):
            config_dict = config
        else:
            return {'type': type(config).__name__, 'value': '***HIDDEN***'}
        
        return cls.sanitize_dict(config_dict, deep=True)


class SecurityValidator:
    """Validates security requirements"""
    
    @staticmethod
    def validate_api_key(api_key: str, exchange: str = 'binance') -> bool:
        """Validate API key format"""
        if not api_key:
            return False
            
        if exchange.lower() == 'binance':
            # Binance API keys are typically 64 characters
            return len(api_key) == 64 and api_key.isalnum()
        elif exchange.lower() == 'bybit':
            # Bybit API keys format
            return len(api_key) > 20 and api_key.replace('-', '').isalnum()
        
        return len(api_key) > 20  # Generic validation
    
    @staticmethod
    def validate_environment() -> Dict[str, bool]:
        """Validate security environment variables"""
        checks = {
            'master_key': bool(os.getenv('ATS_MASTER_KEY')),
            'api_keys_encrypted': bool(os.getenv('ENCRYPTED_API_KEYS')),
            'ssl_verify': os.getenv('SSL_VERIFY', 'true').lower() == 'true',
            'debug_mode': os.getenv('DEBUG', 'false').lower() == 'false',
        }
        
        # Check for exposed sensitive variables
        sensitive_vars = ['API_KEY', 'API_SECRET', 'PASSWORD']
        for var in sensitive_vars:
            if os.getenv(var):
                logger.warning(f"Sensitive variable {var} found in environment!")
                checks[f'exposed_{var.lower()}'] = False
        
        return checks


class RateLimiter:
    """Rate limiting for API calls"""
    
    def __init__(self, max_calls: int = 1200, period: int = 60):
        """
        Initialize rate limiter
        
        Args:
            max_calls: Maximum number of calls
            period: Time period in seconds
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = []
    
    async def acquire(self) -> bool:
        """Check if call is allowed"""
        import time
        now = time.time()
        
        # Remove old calls
        self.calls = [call_time for call_time in self.calls 
                     if now - call_time < self.period]
        
        # Check if limit reached
        if len(self.calls) >= self.max_calls:
            return False
        
        # Add new call
        self.calls.append(now)
        return True
    
    def reset(self):
        """Reset rate limiter"""
        self.calls = []


# Convenience functions
def safe_log(message: str, data: Any = None, level: str = 'info') -> None:
    """
    Log message with sanitized data
    
    Args:
        message: Log message
        data: Optional data to include (will be sanitized)
        level: Log level
    """
    log_func = getattr(logger, level, logger.info)
    
    if data:
        sanitized = LogSanitizer.sanitize_dict(data) if isinstance(data, (dict, list)) else str(data)
        log_func(f"{message}: {json.dumps(sanitized, default=str)}")
    else:
        log_func(message)


def mask_string(text: str, show_start: int = 4, show_end: int = 4) -> str:
    """
    Mask sensitive string showing only start and end
    
    Args:
        text: String to mask
        show_start: Number of characters to show at start
        show_end: Number of characters to show at end
    """
    if not text or len(text) <= (show_start + show_end):
        return '***'
    
    return f"{text[:show_start]}{'*' * (len(text) - show_start - show_end)}{text[-show_end:]}"