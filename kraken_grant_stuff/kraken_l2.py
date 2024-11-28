import websocket
import json
import pandas as pd
import signal
import sys
import bisect
import zlib
from decimal import Decimal
import matplotlib.pyplot as plt
import threading
import time
from datetime import datetime
import datetime as dt
import sqlite3
from data.queries import SQLConfig
from order_book import OrderBook, generate_checksum

# Define the WebSocket URL for the Kraken API
ws_url = "wss://ws.kraken.com/v2"
symbols = ["BTC/USD"]  

OrderBooks = {}

def queryOrderBook():
    while True:
        query_time = dt.datetime.now()
        print("queryOrderBook(): "+str(query_time))
        if OrderBooks.get('BTC/USD'):
            OrderBooks['BTC/USD'].getQuote(query_time)
            OrderBooks['BTC/USD'].writeOrderBooktoDB(query_time)
        time.sleep(1)


def create_subscription_message(symbols):
    return {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": symbols
        }
    }


def on_message(ws, message):
    message = json.loads(message, parse_float=Decimal)
    if message.get('channel')=='status':
        pass

    elif message.get('method')=='subscribe':
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
        assert orderBook.checksum == messageData['checksum'], "Snapshot checksum error"

    elif message.get('type')=='update': #order book update
        #print("Update Message")
        messageData = message['data'][0]
        orderBook = OrderBooks[messageData['symbol']]
        orderBook.updateOrderBook(messageData['bids'], messageData['asks'], messageData['checksum'], messageData['timestamp'])
        #orderBook.getQuote()
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
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    queryThread = threading.Thread(target=queryOrderBook, daemon=True)
    queryThread.start()

    ws.run_forever() 


   
     