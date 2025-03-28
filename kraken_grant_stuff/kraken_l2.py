import websocket
import json
import pandas as pd
import signal
import sys
from decimal import Decimal
import threading
import time
import datetime as dt
from order_book import OrderBook, generate_checksum, validate_checksum

# Define the WebSocket URL for the Kraken API
ws_url = "wss://ws.kraken.com/v2"
symbols = ["BTC/USD", "ETH/USD"]
ORDER_BOOK_DEPTH = 10  
WRITE_TO_DB = True


OrderBooks = {}

def queryOrderBook():
    while True:
        query_time = dt.datetime.now()
        for symbol in OrderBooks:
            OrderBooks[symbol].getQuote(query_time)
            OrderBooks[symbol].writeOrderBooktoDB(query_time)
        time.sleep(1)


def create_subscription_message(symbols):
    return {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": symbols,
            "depth": ORDER_BOOK_DEPTH
        }
    }

def create_unsubscribe_message(symbols):
    return {
        "method": "unsubscribe",
        "params": {
            "channel": "book",
            "symbol": symbols
        }
    }

def reset_websocket(ws, symbols):
    unsubscribe_message = create_unsubscribe_message(symbols)
    ws.send(json.dumps(unsubscribe_message))
    print(f"Unsubscribed from {symbols}")

    # Re-subscribe to the symbol
    subscription_message = create_subscription_message(symbols)
    ws.send(json.dumps(subscription_message))
    print(f"Re-subscribed to {symbols}")

def on_message(ws, message):
    message = json.loads(message, parse_float=Decimal)
    if message.get('channel')=='status':
        pass

    elif message.get('method')=='subscribe':
        print(message)
        newOrderBook = OrderBook(message['result']['symbol'], message['result']['depth'], lastUpdate=message['time_out'])
        OrderBooks[message['result']['symbol']]=newOrderBook

    elif message.get('type')=='snapshot': #order book snapshot
        print("Snapshot Message")
        messageData = message['data'][0]
        orderBook = OrderBooks[messageData['symbol']]
        orderBook.bids = [[entry['price'], entry['qty']] for entry in messageData['bids']]
        orderBook.asks = [[entry['price'], entry['qty']] for entry in messageData['asks']]
        orderBook.bidMap = dict(orderBook.bids)
        orderBook.askMap = dict(orderBook.asks)
        orderBook.checksum = generate_checksum(orderBook.bids, orderBook.asks)
        if not validate_checksum(orderBook.checksum, messageData['checksum']):
            print("{} Snapshot checksum error".format(messageData['symbol']))
            reset_websocket(ws, [messageData['symbol']])

    elif message.get('type')=='update': #order book update
        #print("Update Message")
        messageData = message['data'][0]
        orderBook = OrderBooks[messageData['symbol']]
        orderBook.updateOrderBook(messageData['bids'], messageData['asks'], messageData['timestamp'])
        orderBook.checksum = generate_checksum(orderBook.bids, orderBook.asks)
        if not validate_checksum(orderBook.checksum, messageData['checksum']):
            print("{} Update checksum error".format(messageData['symbol']))
            reset_websocket(ws, [messageData['symbol']])
        #orderBook.getQuote()
    return

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, status_code, status_message):
    print(f"Connection closed with code {status_code}, message: {status_message}")

def on_open(ws):
    subscription_message = create_subscription_message(symbols)
    ws.send(json.dumps(subscription_message))
    print("Subscribed to:", symbols)


def start_websocket():
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Start the thread that periodically queries the order book and writes to DB
    if WRITE_TO_DB:
        queryThread = threading.Thread(target=queryOrderBook, daemon=True)
        queryThread.start()

    # Blocking call - runs forever in this thread
    ws.run_forever()

if __name__ == "__main__":
    def signal_handler(sig, frame):
        print("Caught Ctrl+C, exiting...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    print("Running kraken_l2.py as a standalone script. Starting WebSocket...")
    start_websocket()
   
     