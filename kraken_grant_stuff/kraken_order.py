import websocket
import threading
import signal
import sys
import requests
import time
import os
import json
import base64
import hashlib
import hmac
import urllib.request
from dotenv import load_dotenv
from kraken_balances import KrakenBalances
import kraken_executions
import kraken_l2

ws_url = "wss://ws-auth.kraken.com/v2"
api_token = None
prompt_event = threading.Event()

myKrakenBalances = None
ws = None

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

def create_subscription_message_balances(token, snapshot=True):
    return {
        "method": "subscribe",
        "params": {
            "channel": "balances",
            "snapshot": snapshot,
            "token": token
        }
    }

def create_subscription_message_executions(token):
    return {
        "method": "subscribe",
        "params": {
        "channel": "executions",
        "token": token,
        "snap_orders": True,
        "snap_trades": True
        }
    }

def create_unsubscribe_message_balances(token):
    return {
        "method": "unsubscribe",
        "params": {
            "channel": "balances",
            "token": token
        }
    }

def send_market_order(ws, side, order_qty, symbol, token):
    """
    Send a market order to Kraken WebSocket API with validation.
    
    :param ws: WebSocket connection
    :param token: Authenticated session token
    :param symbol: Symbol for the trading pair (e.g., 'BTC/USD')
    :param order_qty: Quantity of the order
    :param side: 'buy' or 'sell'
    """
    order_message = {
        "method": "add_order",
        "params": {
            "order_type": "market",
            "side": side,
            "order_qty": order_qty,
            "symbol": symbol,
            "token": token,
            "validate": True  # Ensures the order is only validated, not executed
        },
        "req_id": 1,  # Optional: Unique ID provided by the client
    }
    
    ws.send(json.dumps(order_message))
    return


def handle_manual_order():
    orderString = input("Enter side, order_qty, symbol, type:\n")
    orderStringList = [x.strip() for x in orderString.split(',')]
    assert len(orderStringList)==4, "Order input length should be 4"
    orderSide = orderStringList[0]
    assert orderSide in ["buy", "sell"]
    orderQty = float(orderStringList[1])
    orderSymbol = orderStringList[2]
    orderType = orderStringList[3]
    if orderType == "market":
        send_market_order(ws, orderSide, orderQty, orderSymbol, api_token)
    

def signal_handler(sig, frame):
    global ws
    if ws:
        ws.close()
    sys.exit(0)

def on_message(ws, message):
    global myKrakenBalances
    message = json.loads(message)
    if message.get('channel')!='heartbeat':
        #print(message)
        pass
    if message.get('channel')=='balances' and message.get('type')=='snapshot':
        myKrakenBalances = KrakenBalances()
        myKrakenBalances.snapshot_balances(message.get('data'))
        print("Snapshot Balances")
        myKrakenBalances.print_balances()

        prompt_event.set()
        
        #unsubscribe_message_balances = create_unsubscribe_message_balances(api_token)
        #ws.send(json.dumps(unsubscribe_message_balances))
    
    elif message.get('channel')=='balances' and message.get('type')=='update':
        myKrakenBalances.update_balances(message.get('data'))
        

    elif message.get('channel')=='executions' and message.get('type')=='snapshot':
        kraken_executions.handle_executions_snapshot(message.get('data'))
        #TODO look for snapshot balances and snapshot executions before prompt_event.set()

    elif message.get('channel')=='executions' and message.get('type')=='update':
        kraken_executions.handle_executions_update(message.get('data'))

    elif message.get('method')=='add_order':
        print("Order req_id: {}; Success: {}".format(message.get('result').get('order_id'), message.get('success')))
        prompt_event.set()

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, status_code, status_message):
    print(f"Connection closed with code {status_code}, message: {status_message}")


def on_open(ws):
    def run(*args):
        global api_token
        global myKrakenBalances
        api_token = get_websocket_token()
        subscription_message_balances = create_subscription_message_balances(api_token)
        ws.send(json.dumps(subscription_message_balances))

        subscription_message_executions = create_subscription_message_executions(api_token)
        ws.send(json.dumps(subscription_message_executions))

        while True:
            prompt_event.wait()

            action = input("Type 'order', 'balances' or 'exit':\n")
            if action == 'order':
                prompt_event.clear()
                handle_manual_order()
            elif action == 'balances':
                prompt_event.clear()
                myKrakenBalances.print_balances()
                prompt_event.set()

            elif action == 'test':
                pass
            elif action == 'exit':
                break
        ws.close()
    thread = threading.Thread(target=run)
    thread.start()

def main():
    #websocket.enableTrace(True)
    global ws
    signal.signal(signal.SIGINT, signal_handler)
    ws_thread_l2 = threading.Thread(target=kraken_l2.start_websocket, daemon=True)
    ws_thread_l2.start()
    ws = websocket.WebSocketApp(ws_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever(ping_interval = 30) #maybe consider ping_interval = 30

if __name__ == "__main__":
    main()