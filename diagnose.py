#!/usr/bin/env python3
"""Diagnose ATS configuration"""
import os
from dotenv import load_dotenv

load_dotenv()

print("=" * 60)
print("ATS 2.0 CONFIGURATION DIAGNOSTICS")
print("=" * 60)

# Check testnet setting
testnet = os.getenv('BINANCE_TESTNET', 'false').lower() == 'true'
print(f"\n✓ BINANCE_TESTNET = {os.getenv('BINANCE_TESTNET')} -> testnet={testnet}")

# Check testnet thresholds
testnet_volume = float(os.getenv('MIN_VOLUME_24H_USD_TESTNET', 10000))
testnet_spread = float(os.getenv('MAX_SPREAD_PERCENT_TESTNET', 10.0))
print(f"✓ Testnet Volume Threshold: ${testnet_volume:,.0f}")
print(f"✓ Testnet Spread Threshold: {testnet_spread}%")

# Check production thresholds
prod_volume = float(os.getenv('MIN_VOLUME_24H_USD', 1000000))
prod_spread = float(os.getenv('MAX_SPREAD_PERCENT', 2.0))
print(f"✓ Production Volume Threshold: ${prod_volume:,.0f}")
print(f"✓ Production Spread Threshold: {prod_spread}%")

# Which will be used?
if testnet:
    print(f"\n🎯 WILL USE TESTNET SETTINGS:")
    print(f"   - Volume: ${testnet_volume:,.0f}")
    print(f"   - Spread: {testnet_spread}%")
else:
    print(f"\n🎯 WILL USE PRODUCTION SETTINGS:")
    print(f"   - Volume: ${prod_volume:,.0f}")
    print(f"   - Spread: {prod_spread}%")

print("\n" + "=" * 60)