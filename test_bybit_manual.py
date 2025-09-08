#!/usr/bin/env python3
"""Manual test of Bybit V5 API"""

import hmac
import hashlib
import time
import requests

API_KEY = "WrG37DUFHbejVGkqU2"
API_SECRET = "ACUTQBTtKGfGKtH0D19IS9xYUCwiRuofLJf6"

def test():
    url = "https://api-testnet.bybit.com/v5/position/list"
    
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    
    # Try without any parameters first
    param_str = "category=linear"
    
    # Build signature
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
    
    # Make request
    response = requests.get(url + "?category=linear", headers=headers)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")

if __name__ == "__main__":
    test()