"""
ATS 2.0 - Retry and Circuit Breaker Module
Handles retries with exponential backoff and circuit breaker pattern
"""
import asyncio
import functools
import logging
import time
from typing import Any, Callable, Optional, Type, Union, Tuple
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failures exceeded threshold, blocking calls
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker pattern implementation
    Prevents cascading failures by blocking calls to failing services
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: Type[Exception] = Exception
    ):
        """
        Initialize circuit breaker
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to catch
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitState.CLOSED
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator for circuit breaker"""
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await self.call(func, *args, **kwargs)
        return wrapper
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
            else:
                raise Exception(f"Circuit breaker is OPEN for {func.__name__}")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time passed to attempt reset"""
        if not self.last_failure_time:
            return True
        return datetime.now() - self.last_failure_time > timedelta(seconds=self.recovery_timeout)
    
    def _on_success(self):
        """Handle successful call"""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
    
    def _on_failure(self):
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
    
    def reset(self):
        """Manually reset circuit breaker"""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED


class RetryConfig:
    """Configuration for retry behavior"""
    
    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.exceptions = exceptions


async def retry_with_backoff(
    func: Callable,
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable] = None
) -> Any:
    """
    Retry function with exponential backoff
    
    Args:
        func: Async function to retry
        config: Retry configuration
        on_retry: Optional callback on each retry
    
    Returns:
        Function result
    
    Raises:
        Last exception if all retries failed
    """
    if config is None:
        config = RetryConfig()
    
    last_exception = None
    
    for attempt in range(config.max_attempts):
        try:
            return await func()
        except config.exceptions as e:
            last_exception = e
            
            if attempt == config.max_attempts - 1:
                logger.error(f"All {config.max_attempts} attempts failed for {func.__name__}")
                raise
            
            # Calculate delay with exponential backoff
            delay = min(
                config.initial_delay * (config.exponential_base ** attempt),
                config.max_delay
            )
            
            # Add jitter to prevent thundering herd
            if config.jitter:
                import random
                delay *= (0.5 + random.random())
            
            logger.warning(
                f"Attempt {attempt + 1}/{config.max_attempts} failed for {func.__name__}: {e}. "
                f"Retrying in {delay:.2f}s..."
            )
            
            if on_retry:
                await on_retry(attempt, delay, e)
            
            await asyncio.sleep(delay)
    
    raise last_exception


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Decorator for retrying async functions
    
    Args:
        max_attempts: Maximum number of attempts
        delay: Initial delay between retries
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch
        on_retry: Optional callback on retry
    
    Example:
        @retry(max_attempts=3, delay=1.0, backoff=2.0)
        async def flaky_api_call():
            return await external_api.call()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            config = RetryConfig(
                max_attempts=max_attempts,
                initial_delay=delay,
                exponential_base=backoff,
                exceptions=exceptions
            )
            
            async def wrapped_func():
                return await func(*args, **kwargs)
            
            return await retry_with_backoff(wrapped_func, config, on_retry)
        
        return wrapper
    return decorator


class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, rate: float, capacity: int):
        """
        Initialize rate limiter
        
        Args:
            rate: Tokens per second
            capacity: Maximum tokens in bucket
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens, waiting if necessary
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            Time waited in seconds
        """
        async with self._lock:
            while tokens > self.tokens:
                # Calculate tokens to add
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                
                if tokens > self.tokens:
                    # Calculate wait time
                    deficit = tokens - self.tokens
                    wait_time = deficit / self.rate
                    await asyncio.sleep(wait_time)
            
            self.tokens -= tokens
            return 0.0
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator for rate limiting"""
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            await self.acquire()
            return await func(*args, **kwargs)
        return wrapper


class ExponentialBackoff:
    """Exponential backoff calculator"""
    
    def __init__(
        self,
        initial: float = 1.0,
        maximum: float = 60.0,
        multiplier: float = 2.0,
        jitter: bool = True
    ):
        self.initial = initial
        self.maximum = maximum
        self.multiplier = multiplier
        self.jitter = jitter
        self.attempt = 0
    
    def next_delay(self) -> float:
        """Calculate next delay"""
        delay = min(self.initial * (self.multiplier ** self.attempt), self.maximum)
        
        if self.jitter:
            import random
            delay *= (0.5 + random.random())
        
        self.attempt += 1
        return delay
    
    def reset(self):
        """Reset backoff"""
        self.attempt = 0


class BulkheadPattern:
    """
    Bulkhead pattern for isolating resources
    Limits concurrent executions to prevent resource exhaustion
    """
    
    def __init__(self, max_concurrent: int = 10):
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    def __call__(self, func: Callable) -> Callable:
        """Decorator for bulkhead pattern"""
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            async with self.semaphore:
                return await func(*args, **kwargs)
        return wrapper


# Convenience functions
async def safe_api_call(
    func: Callable,
    exchange_name: str,
    max_retries: int = 3,
    circuit_breaker: Optional[CircuitBreaker] = None
) -> Any:
    """
    Safe API call with retry and circuit breaker
    
    Args:
        func: API call function
        exchange_name: Exchange name for logging
        max_retries: Maximum retry attempts
        circuit_breaker: Optional circuit breaker instance
    """
    try:
        if circuit_breaker:
            return await circuit_breaker.call(func)
        else:
            config = RetryConfig(max_attempts=max_retries)
            return await retry_with_backoff(func, config)
    except Exception as e:
        logger.error(f"API call failed for {exchange_name}: {e}")
        raise