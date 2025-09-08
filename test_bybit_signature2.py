#!/usr/bin/env python3
"""Test Bybit V5 API signature generation - version 2"""

import hmac
import hashlib
import time
import requests
from urllib.parse import urlencode

API_KEY = "WrG37DUFHbejVGkqU2"
API_SECRET = "ACUTQBTtKGfGKtH0D19IS9xYUCwiRuofLJf6"
TESTNET = True

def test_positions():
    """Test getting positions with correct signature"""
    base_url = "https://api-testnet.bybit.com" if TESTNET else "https://api.bybit.com"
    url = f"{base_url}/v5/position/list"
    
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    
    # Parameters for the request - Bybit expects specific order
    params = {
        "category": "linear",
        "settleCoin": "USDT", 
        "limit": "200"
    }
    
    # Create query string - use urlencode to ensure proper encoding
    param_str = urlencode(sorted(params.items()))
    
    # Bybit V5 signature: timestamp + api_key + recv_window + query_string
    sign_str = f"{timestamp}{API_KEY}{recv_window}{param_str}"
    print(f"Sign string: {sign_str}")
    
    signature = hmac.new(
        API_SECRET.encode(),
        sign_str.encode(),
        hashlib.sha256
    ).hexdigest()
    
    print(f"Signature: {signature}")
    
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window
    }
    
    response = requests.get(url, headers=headers, params=params)
    print(f"Status: {response.status_code}")
    
    data = response.json()
    if data.get('retCode') == 0:
        positions = data.get('result', {}).get('list', [])
        print(f"Success! Found {len(positions)} positions")
        for pos in positions[:5]:  # Show first 5
            print(f"  - {pos.get('symbol')}: {pos.get('side')} size={pos.get('size')} avgPrice={pos.get('avgPrice')}")
    else:
        print(f"API Error: {data.get('retMsg')}")
        print(f"Full response: {data}")

if __name__ == "__main__":
    test_positions()