import websocket
import json
import pandas as pd
import signal
import sys

# Define the WebSocket URL for the Kraken API
ws_url = "wss://ws.kraken.com/v2"
symbols = ["BTC/USD"]  

#columns = ["timestamp", "symbol", "bid", "bid_qty", "ask", "ask_qty", "last", "volume", "vwap", "low", "high", "change", "change_pct"]
#ticker_data = pd.DataFrame(columns=columns)

def create_subscription_message(symbols):
    return {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": symbols
        }
    }

def on_message(ws, message):
    message = json.loads(message)
    print(message)
    print("\n")
    return

def on_error(ws, error):
    print("Error:", error)

def on_close(ws):
    print("Connection closed")

def on_open(ws):
    subscription_message = create_subscription_message(symbols)
    ws.send(json.dumps(subscription_message))
    print("Subscribed to:", symbols)


def signal_handler(sig, frame):
    sys.exit(0)

if __name__ == "__main__":
    #websocket.enableTrace(True)
    signal.signal(signal.SIGINT, signal_handler)
    
    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open  
    ws.run_forever()  