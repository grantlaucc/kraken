import websocket
import json
import pandas as pd
import signal
import sys

# Define the WebSocket URL for the Kraken API
ws_url = "wss://ws.kraken.com/v2"
symbols = ["BTC/USD"]  

columns = ["timestamp", "symbol", "bid", "bid_qty", "ask", "ask_qty", "last", "volume", "vwap", "low", "high", "change", "change_pct"]
ticker_data = pd.DataFrame(columns=columns)

def create_subscription_message(symbols):
    return {
        "method": "subscribe",
        "params": {
            "channel": "ticker",
            "symbol": symbols
        }
    }

def on_message(ws, message):
    if message['type']=='snapshot':
        print(message)
    return

def on_error(ws, error):
    print("Error:", error)

def on_close(ws):
    ticker_data.to_csv('ticker_data.csv', index=False)
    print("Connection closed")

def on_open(ws):
    subscription_message = create_subscription_message(symbols)
    ws.send(json.dumps(subscription_message))
    print("Subscribed to:", symbols)

def save_data():
    ticker_data.to_csv('ticker_data.csv', index=False)

def signal_handler(sig, frame):
    print("Signal received, saving data...")
    save_data()  
    sys.exit(0)

if __name__ == "__main__":
    #websocket.enableTrace(True)
    print("hi")
    signal.signal(signal.SIGINT, signal_handler)
    
    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open  
    ws.run_forever()  