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
import datetime

# Define the WebSocket URL for the Kraken API
ws_url = "wss://ws.kraken.com/v2"
symbols = ["BTC/USD"]  

#columns = ["timestamp", "symbol", "bid", "bid_qty", "ask", "ask_qty", "last", "volume", "vwap", "low", "high", "change", "change_pct"]
#ticker_data = pd.DataFrame(columns=columns)

OrderBooks = {}

def generate_checksum(bids, asks):
    # Step 1: Generate the formatted string for asks (sorted in ascending order)
    asks_str = ''
    for ask in asks:  # Top 10 asks, sorted from low to high price
        price_str = str(ask[0]).replace('.', '')
        qty_str = str(ask[1]).replace('.', '').lstrip('0')
        asks_str += price_str + qty_str

    # Step 2: Generate the formatted string for bids (sorted in descending order)
    bids_str = ''
    for bid in bids:  # Top 10 bids, sorted from high to low price
        price_str = str(bid[0]).replace('.', '')
        qty_str = str(bid[1]).replace('.', '').lstrip('0')
        bids_str += price_str + qty_str

    full_str = asks_str + bids_str
    checksum = zlib.crc32(full_str.encode('utf-8'))
    return checksum

class OrderBook:
    def __init__(self, symbol, depth, bids=[], asks=[], bidMap = {}, askMap = {}, checksum=None, lastUpdate=None):
        self.symbol = symbol
        self.depth = depth
        self.bids = bids #sorted list [[price1, qty1], [price2, qty2], etc.]
        self.asks = asks #sorted list [[price1, qty1], [price2, qty2], etc.]
        self.bidMap = bidMap
        self.askMap = askMap
        self.checksum = checksum
        self.lastUpdate = lastUpdate

    def updateOrderBook(self, updateBids, updateAsks, checksum, timestamp):
        #TODO checksum and timestamp
        for updateBid in updateBids:
            if updateBid['qty'] != 0.0: 
                self.bidMap.update({updateBid['price']:updateBid['qty']})
            else: #updateBid with qty = 0.0
                if updateBid['price'] in self.bidMap:
                    self.bidMap.pop(updateBid['price'])
            self.bids = [[price, qty] for price, qty in sorted(self.bidMap.items(), key=lambda x: x[0], reverse=True)][:self.depth]
            self.bidMap = dict(self.bids)

        for updateAsk in updateAsks:
            if updateAsk['qty'] != 0.0:
                self.askMap.update({updateAsk['price']: updateAsk['qty']})
            else:  # updateAsk with qty = 0.0
                if updateAsk['price'] in self.askMap:
                    self.askMap.pop(updateAsk['price'])
            self.asks = [[price, qty] for price, qty in sorted(self.askMap.items(), key=lambda x: x[0])][:self.depth]
            self.askMap = dict(self.asks)

        self.checksum = generate_checksum(self.bids, self.asks)
        assert self.checksum == checksum, "Update checksum error"
        self.lastUpdate = timestamp
        return
    
    def getQuote(self):
        if len(self.bids)>0 and len(self.asks)>0:
            print(str(self.bids[0][0])+"/"+str(self.asks[0][0])+"\t"
                +str(self.bids[0][1])+"x"+str(self.asks[0][1])
                )

def queryOrderBook():
    while True:
        print("queryOrderBook(): "+str(datetime.datetime.now()))
        if OrderBooks.get('BTC/USD'):
            OrderBooks['BTC/USD'].getQuote()
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


   
     