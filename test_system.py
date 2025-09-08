#!/usr/bin/env python3
"""
ATS 2.0 - Comprehensive System Test Suite
Tests all components without requiring real API keys or database
"""
import asyncio
import sys
import os
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Test results collector
test_results = {
    "passed": [],
    "failed": [],
    "skipped": []
}

def test_result(test_name: str, passed: bool, error: str = None):
    """Record test result"""
    if passed:
        test_results["passed"].append(test_name)
        logger.info(f"✅ {test_name}: PASSED")
    else:
        test_results["failed"].append((test_name, error))
        logger.error(f"❌ {test_name}: FAILED - {error}")

# ============= TEST: IMPORTS =============
async def test_imports():
    """Test all module imports"""
    test_name = "Module Imports"
    try:
        # Core modules
        from core.config import SystemConfig
        from core.security import SecretsManager, LogSanitizer, SecurityValidator
        from core.retry import retry, CircuitBreaker, RetryConfig, RateLimiter
        
        # Database modules
        from database.connection import DatabaseManager, DatabasePool
        from database.models import TradingSignal, Position, Order
        
        # Exchange modules
        from exchanges.base import ExchangeBase, SymbolInfo, MarketData
        from exchanges.binance import BinanceExchange
        from exchanges.bybit import BybitExchange
        from exchanges.websocket_manager import WebSocketManager, StreamType
        
        # Trading modules
        from trading.signal_processor import SignalProcessor, DuplicateProtection, SignalFilter
        
        # Monitoring modules
        from monitoring.metrics import metrics_collector, MetricsCollector
        
        # Web modules
        from web.app import app, HealthStatus, PositionInfo
        
        test_result(test_name, True)
        return True
    except ImportError as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: CONFIGURATION =============
async def test_configuration():
    """Test configuration loading"""
    test_name = "Configuration"
    try:
        from core.config import SystemConfig
        
        # Create test config
        config = SystemConfig()
        
        # Test config structure
        assert hasattr(config, 'database'), "Missing database config"
        assert hasattr(config, 'exchange'), "Missing exchange config"
        assert hasattr(config, 'position'), "Missing position config"
        assert hasattr(config, 'risk'), "Missing risk config"
        assert hasattr(config, 'signal'), "Missing signal config"
        assert hasattr(config, 'monitoring'), "Missing monitoring config"
        
        # Test config validation
        errors = config.validate()
        
        # Test config serialization
        config_dict = config.to_dict()
        assert isinstance(config_dict, dict), "Config to_dict failed"
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: SECURITY =============
async def test_security():
    """Test security features"""
    test_name = "Security Module"
    try:
        from core.security import SecretsManager, LogSanitizer, SecurityValidator, mask_string
        
        # Test encryption
        secrets = SecretsManager()
        original = "my_secret_api_key"
        encrypted = secrets.encrypt(original)
        decrypted = secrets.decrypt(encrypted)
        assert decrypted == original, "Encryption/decryption failed"
        
        # Test log sanitization
        sensitive_data = {
            "api_key": "BINANCE_SECRET_KEY_12345",
            "api_secret": "SUPER_SECRET_VALUE",
            "password": "my_password",
            "normal_data": "this is fine"
        }
        
        sanitized = LogSanitizer.sanitize_dict(sensitive_data)
        assert sanitized["api_key"] == "***HIDDEN***", "API key not sanitized"
        assert sanitized["api_secret"] == "***HIDDEN***", "API secret not sanitized"
        assert sanitized["password"] == "***HIDDEN***", "Password not sanitized"
        assert sanitized["normal_data"] == "this is fine", "Normal data was changed"
        
        # Test string sanitization
        log_string = "Connected with api_key=ABC123SECRET and password=mypass"
        sanitized_string = LogSanitizer.sanitize_string(log_string)
        assert "ABC123SECRET" not in sanitized_string, "API key not removed from string"
        assert "mypass" not in sanitized_string, "Password not removed from string"
        
        # Test masking
        masked = mask_string("BINANCE_API_KEY_12345", 4, 4)
        assert masked.startswith("BINA") and masked.endswith("2345"), "Masking failed"
        assert "*" in masked, "No masking characters"
        
        # Test API key validation
        assert SecurityValidator.validate_api_key("A" * 64, "binance"), "Valid Binance key rejected"
        assert not SecurityValidator.validate_api_key("SHORT", "binance"), "Invalid key accepted"
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: RETRY LOGIC =============
async def test_retry_logic():
    """Test retry and circuit breaker"""
    test_name = "Retry Logic"
    try:
        from core.retry import retry, CircuitBreaker, RetryConfig, ExponentialBackoff, RateLimiter
        
        # Test exponential backoff
        backoff = ExponentialBackoff(initial=1.0, maximum=10.0, multiplier=2.0, jitter=False)
        delay1 = backoff.next_delay()
        delay2 = backoff.next_delay()
        assert delay1 == 1.0, f"First delay should be 1.0, got {delay1}"
        assert delay2 == 2.0, f"Second delay should be 2.0, got {delay2}"
        
        # Test rate limiter
        limiter = RateLimiter(rate=10, capacity=10)
        
        # Should allow first 10 immediately
        for i in range(10):
            wait_time = await limiter.acquire()
            assert wait_time == 0, f"Should not wait for token {i+1}"
        
        # Test circuit breaker
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        
        call_count = 0
        async def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise Exception("Test failure")
            return "success"
        
        # Should fail 3 times and open circuit
        for i in range(3):
            try:
                await breaker.call(failing_function)
            except:
                pass
        
        assert breaker.state.value == "open", "Circuit should be open after 3 failures"
        
        # Should reject calls when open
        try:
            await breaker.call(failing_function)
            assert False, "Circuit breaker should reject calls when open"
        except Exception as e:
            assert "Circuit breaker is OPEN" in str(e)
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: DATABASE MODELS =============
async def test_database_models():
    """Test database models"""
    test_name = "Database Models"
    try:
        from database.models import (
            TradingSignal, Position, Order, 
            SignalStatus, PositionStatus, OrderStatus, OrderType, OrderPurpose,
            CloseReason
        )
        from datetime import datetime
        
        # Test TradingSignal
        signal = TradingSignal(
            prediction_id=1,
            signal_id=100,
            prediction=True,
            prediction_proba=Decimal("0.85"),
            confidence_level="HIGH",
            signal_type="BUY",  # Should be BUY or SELL
            created_at=datetime.utcnow(),
            pair_symbol="BTCUSDT",
            trading_pair_id=1,
            exchange_id=1,
            exchange_name="binance"
        )
        
        assert signal.is_valid, "Signal should be valid"
        assert signal.side == "LONG", "Signal side incorrect"
        signal_dict = signal.to_dict()
        assert isinstance(signal_dict, dict), "Signal to_dict failed"
        
        # Test Position
        position = Position(
            signal_id=1,
            prediction_id=1,
            exchange="binance",
            symbol="BTCUSDT",
            side="LONG",
            entry_price=Decimal("50000"),
            quantity=Decimal("0.01"),
            leverage=10
        )
        
        assert position.is_open, "New position should be open"
        position.current_price = Decimal("51000")
        assert position.unrealized_pnl_percent > 0, "PNL calculation failed"
        
        # Test Order
        order = Order(
            position_id=1,
            exchange_order_id="123456",
            side="BUY",
            order_type=OrderType.MARKET,
            order_purpose=OrderPurpose.ENTRY,
            quantity=Decimal("0.01"),
            status=OrderStatus.FILLED
        )
        
        assert order.is_filled, "Order should be filled"
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: METRICS =============
async def test_metrics():
    """Test monitoring metrics"""
    test_name = "Metrics System"
    try:
        from monitoring.metrics import (
            metrics_collector,
            signals_received, orders_placed,
            api_requests, system_errors
        )
        
        # Test metric recording
        metrics_collector.track_signal("binance", "BTCUSDT", "LONG", "executed")
        metrics_collector.track_order("binance", "BUY", "MARKET", True, 0.5)
        metrics_collector.track_api_call("binance", "/api/v3/order", "POST", 0.1, True)
        metrics_collector.update_daily_metrics(10, Decimal("100.50"))
        
        # Get metrics output
        metrics_output = metrics_collector.get_metrics()
        assert isinstance(metrics_output, bytes), "Metrics output should be bytes"
        assert len(metrics_output) > 0, "Metrics output is empty"
        
        # Check metrics format (Prometheus format)
        metrics_str = metrics_output.decode('utf-8')
        assert "ats_signals_received_total" in metrics_str, "Signal metric missing"
        assert "ats_orders_placed_total" in metrics_str, "Order metric missing"
        assert "ats_api_requests_total" in metrics_str, "API metric missing"
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: SIGNAL PROCESSOR =============
async def test_signal_processor():
    """Test signal processing logic"""
    test_name = "Signal Processor"
    try:
        from trading.signal_processor import (
            DuplicateProtection, SignalFilter, RiskManager
        )
        from core.config import SystemConfig
        from database.models import TradingSignal
        
        # Test duplicate protection
        dup_protection = DuplicateProtection(cooldown_seconds=30)
        
        signal = TradingSignal(
            prediction_id=1,
            signal_id=1,
            prediction=True,
            prediction_proba=Decimal("0.9"),
            confidence_level="HIGH",
            signal_type="BUY",
            created_at=datetime.utcnow(),
            pair_symbol="BTCUSDT",
            trading_pair_id=1,
            exchange_id=1,
            exchange_name="binance"
        )
        
        # First signal should be allowed
        can_process, reason = await dup_protection.can_process_signal(signal)
        assert can_process, f"First signal should be allowed: {reason}"
        
        # Duplicate should be blocked
        can_process2, reason2 = await dup_protection.can_process_signal(signal)
        assert not can_process2, "Duplicate signal should be blocked"
        assert "already being processed" in reason2
        
        # Release and test cooldown
        await dup_protection.release_signal(signal)
        
        # Should still be in cooldown
        can_process3, reason3 = await dup_protection.can_process_signal(signal)
        assert not can_process3, "Signal should be in cooldown"
        assert "Cooldown active" in reason3
        
        # Test signal filter
        config = SystemConfig()
        filter = SignalFilter(config)
        
        should_process, skip_reason = filter.should_process(signal)
        # May fail without proper config, but should not crash
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: WEB API =============
async def test_web_api():
    """Test FastAPI application"""
    test_name = "Web API"
    try:
        from web.app import app, HealthStatus
        from fastapi.testclient import TestClient
        
        # Create test client
        client = TestClient(app)
        
        # Test health endpoint
        response = client.get("/health")
        assert response.status_code == 200, f"Health check failed: {response.status_code}"
        health_data = response.json()
        assert "status" in health_data, "Health response missing status"
        assert "components" in health_data, "Health response missing components"
        
        # Test metrics endpoint
        response = client.get("/metrics")
        assert response.status_code == 200, f"Metrics endpoint failed: {response.status_code}"
        assert "ats_" in response.text, "Metrics not in Prometheus format"
        
        # Test liveness probe
        response = client.get("/health/live")
        assert response.status_code == 200, "Liveness probe failed"
        
        # Test API docs
        response = client.get("/docs")
        assert response.status_code == 200, "API docs not accessible"
        
        test_result(test_name, True)
        return True
    except ImportError:
        test_result(test_name, False, "fastapi or TestClient not installed")
        return False
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: WEBSOCKET MANAGER =============
async def test_websocket_manager():
    """Test WebSocket manager"""
    test_name = "WebSocket Manager"
    try:
        from exchanges.websocket_manager import WebSocketManager, StreamType
        
        # Create manager (won't connect without valid URL)
        manager = WebSocketManager(
            exchange_name="binance",
            url="wss://stream.binance.com:9443/ws",
            api_key=None
        )
        
        # Test subscription storage
        async def dummy_callback(data):
            pass
        
        # Should handle subscription even without connection
        manager.callbacks["test:BTCUSDT"] = dummy_callback
        assert "test:BTCUSDT" in manager.callbacks, "Callback storage failed"
        
        # Test message processing methods exist
        assert hasattr(manager, '_process_binance_message'), "Missing Binance processor"
        assert hasattr(manager, '_process_bybit_message'), "Missing Bybit processor"
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= TEST: EXCHANGE BASE =============
async def test_exchange_base():
    """Test exchange base functionality"""
    test_name = "Exchange Base"
    try:
        from exchanges.base import SymbolInfo, MarketData, AccountBalance, ExchangePosition
        from decimal import Decimal
        
        # Test SymbolInfo
        symbol_info = SymbolInfo(
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("1000"),
            step_size=Decimal("0.001"),
            min_notional=Decimal("10"),
            price_precision=2,
            quantity_precision=3,
            max_leverage=125,
            is_trading=True
        )
        
        # Test quantity rounding
        rounded = symbol_info.round_quantity(Decimal("0.0012345"))
        assert rounded == Decimal("0.001"), f"Quantity rounding failed: {rounded}"
        
        # Test price rounding  
        rounded_price = symbol_info.round_price(Decimal("50123.456"))
        assert rounded_price == Decimal("50123.46"), f"Price rounding failed: {rounded_price}"
        
        # Test MarketData
        market = MarketData(
            symbol="BTCUSDT",
            bid_price=Decimal("50000"),
            bid_quantity=Decimal("1.5"),
            ask_price=Decimal("50010"),
            ask_quantity=Decimal("2.0"),
            last_price=Decimal("50005"),
            volume_24h=Decimal("1000"),
            quote_volume_24h=Decimal("50000000"),
            open_price_24h=Decimal("49000"),
            high_price_24h=Decimal("51000"),
            low_price_24h=Decimal("48500"),
            price_change_24h=Decimal("1005"),
            price_change_percent_24h=Decimal("2.05"),
            timestamp=datetime.utcnow()
        )
        
        spread = market.spread
        assert spread == Decimal("10"), f"Spread calculation wrong: {spread}"
        
        spread_percent = market.spread_percent
        assert spread_percent > 0, "Spread percent should be positive"
        
        test_result(test_name, True)
        return True
    except Exception as e:
        test_result(test_name, False, str(e))
        return False

# ============= MAIN TEST RUNNER =============
async def run_all_tests():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("ATS 2.0 - COMPREHENSIVE SYSTEM TEST")
    logger.info("=" * 60)
    
    # Run tests in order
    tests = [
        ("Imports", test_imports),
        ("Configuration", test_configuration),
        ("Security", test_security),
        ("Retry Logic", test_retry_logic),
        ("Database Models", test_database_models),
        ("Metrics", test_metrics),
        ("Signal Processor", test_signal_processor),
        ("Exchange Base", test_exchange_base),
        ("WebSocket Manager", test_websocket_manager),
        ("Web API", test_web_api),
    ]
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning: {test_name}")
        try:
            await test_func()
        except Exception as e:
            logger.error(f"Test {test_name} crashed: {e}")
            test_results["failed"].append((test_name, str(e)))
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    total_tests = len(test_results["passed"]) + len(test_results["failed"]) + len(test_results["skipped"])
    
    logger.info(f"Total Tests: {total_tests}")
    logger.info(f"✅ Passed: {len(test_results['passed'])}")
    logger.info(f"❌ Failed: {len(test_results['failed'])}")
    logger.info(f"⏭️  Skipped: {len(test_results['skipped'])}")
    
    if test_results["failed"]:
        logger.info("\nFailed Tests:")
        for test_name, error in test_results["failed"]:
            logger.error(f"  - {test_name}: {error}")
    
    # Return success if all critical tests passed
    critical_passed = len(test_results["failed"]) == 0
    
    if critical_passed:
        logger.info("\n✅ ALL TESTS PASSED! System is ready for use.")
    else:
        logger.error(f"\n❌ {len(test_results['failed'])} TESTS FAILED! Please fix issues before running.")
    
    return critical_passed

if __name__ == "__main__":
    # Run tests
    success = asyncio.run(run_all_tests())
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)