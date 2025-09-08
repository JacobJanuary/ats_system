#!/usr/bin/env python3
"""Final test of Bybit V5 API with correct parameter order"""

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
    
    # Parameters in the exact order Bybit expects
    param_str = "category=linear&settleCoin=USDT"
    
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
    
    # Make request with parameters in URL
    full_url = f"{url}?{param_str}"
    response = requests.get(full_url, headers=headers)
    print(f"Status: {response.status_code}")
    
    data = response.json()
    if data.get('retCode') == 0:
        positions = data.get('result', {}).get('list', [])
        print(f"Success! Found {len(positions)} positions")
        for pos in positions[:5]:
            print(f"  - {pos.get('symbol')}: {pos.get('side')} size={pos.get('size')} avgPrice={pos.get('avgPrice')}")
    else:
        print(f"Error: {data}")

if __name__ == "__main__":
    test()