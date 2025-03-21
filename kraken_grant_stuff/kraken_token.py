import os
import time
from dotenv import load_dotenv
import base64
import hashlib
import hmac
import urllib.request
import json

def loadKrakenKeys():
    load_dotenv("/Users/grantlau/Documents/QuantStuff/kraken/.env")
    api_key = os.getenv('KRAKEN_API_KEY')
    api_secret = os.getenv('KRAKEN_API_SECRET')
    return api_key, api_secret
    
def get_websocket_token():
    """ Obtain a token from Kraken for WebSocket API. 
        Note this token must be used within 15 mins.
    """
    api_key, api_secret = loadKrakenKeys()
    api_path = '/0/private/GetWebSocketsToken'
    api_nonce = str(int(time.time()*1000))
    api_post = 'nonce=' + api_nonce

    # Cryptographic hash algorithms
    api_sha256 = hashlib.sha256(api_nonce.encode('utf-8') + api_post.encode('utf-8'))
    api_hmac = hmac.new(base64.b64decode(api_secret), api_path.encode('utf-8') + api_sha256.digest(), hashlib.sha512)
    # Encode signature into base64 format used in API-Sign value
    api_signature = base64.b64encode(api_hmac.digest())

    # HTTP request (POST)
    api_request = urllib.request.Request('https://api.kraken.com/0/private/GetWebSocketsToken', api_post.encode('utf-8'))
    api_request.add_header('API-Key', api_key)
    api_request.add_header('API-Sign', api_signature)
    api_response = urllib.request.urlopen(api_request).read().decode()

    json_api = json.loads(api_response)
    return json_api['result']['token']