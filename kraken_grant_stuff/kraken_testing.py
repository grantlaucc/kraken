import threading
import signal
import sys
from kraken_balances import KrakenBalances
import kraken_l2
import time
import datetime as dt
import sqlite3
import csv
from order_book import OrderBook, generate_checksum, validate_checksum

from collections import deque

liveData = False
historicalDataFile = "/Users/grantlau/Documents/QuantStuff/CryptoData/kraken_quotes_25.csv"
testKrakenBalances = None
conn = None
cursor = None


#STRATEGY PARAMETERS
symbols = ["BTC/USD"]
IMBALANCE_MOVING_AVG_LEN = 10
IMBALANCE_SCALING_CONSTANT = 30
SIGNAL_THRESHOLD = 0.3
positionLimits = {"BTC/USD":10}



imbalances = deque(maxlen=IMBALANCE_MOVING_AVG_LEN)
def decision(orderBook, krakenBalances):
    currentBalance = krakenBalances.balances['BTC/USD']
    positionLimit = positionLimits['BTC/USD']

    bidVolume = sum([qty for _, qty in orderBook.bids])
    askVolume = sum([qty for _, qty in orderBook.asks])
    imbalances.append(bidVolume - askVolume)
    avgImbalance = sum(imbalances) / len(imbalances)
    targetPosition = float(avgImbalance/IMBALANCE_SCALING_CONSTANT)
    aggression = targetPosition - (currentBalance/positionLimit)
    aggression = max(-1, min(round(aggression,1), 1))

    #BUY
    askPrice = float(orderBook.asks[0][0])
    bidPrice = float(orderBook.bids[0][0])
    if aggression>SIGNAL_THRESHOLD and currentBalance+aggression<=positionLimit:
        qty = aggression
        record_transaction('buy', qty, askPrice)
        krakenBalances.balances['BTC/USD']+=qty
        krakenBalances.balances['USD']-=qty*askPrice

    #SELL
    elif aggression<-SIGNAL_THRESHOLD and currentBalance+aggression>=-positionLimit:
        qty = -aggression
        record_transaction('sell', qty, bidPrice)
        krakenBalances.balances['BTC/USD']-=qty
        krakenBalances.balances['USD']+=qty*bidPrice

    #Get Liquidation Value
    if krakenBalances.balances['BTC/USD']>0:
        valueBTC = bidPrice*krakenBalances.balances['BTC/USD']
    else:
        valueBTC = askPrice*krakenBalances.balances['BTC/USD']
    liquidationValue = valueBTC+krakenBalances.balances['USD']

    print(liquidationValue, krakenBalances.balances)
    return aggression

def setup_database():
    global conn, cursor
    conn = sqlite3.connect('kraken_testing.db')
    cursor = conn.cursor()
    # Create table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS transactions (
                        transaction_id INTEGER PRIMARY KEY,
                        action TEXT, 
                        quantity REAL,
                        price REAL)''')
    conn.commit()

def record_transaction(action, quantity, price):
    global cursor
    cursor.execute("INSERT INTO transactions (action, quantity, price) VALUES (?, ?, ?)",
                   (action, quantity, price))
    conn.commit()
    print("{} {} @ {}".format(action, quantity, price))


def testStrategyLive():
    while True:
        orderBook = kraken_l2.OrderBooks.get(symbols[0])
        if not orderBook:
            print(f"No data for {symbols[0]} yet...")
        else:
            orderBook.getQuote(dt.datetime.now())
            decision(orderBook, testKrakenBalances)
        time.sleep(1)

def testStrategyHistorical():
    historicalOrderBook = OrderBook("BTC/USD", 25)
    with open(historicalDataFile, 'r') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            #if i >= 10:  # Stop after reading n rows
                #break
            historicalOrderBook.populateHistorical(row)
            historicalOrderBook.getQuote(dt.datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S'))
            decision(historicalOrderBook, testKrakenBalances)
        return

def signal_handler(sig, frame):
    global conn
    conn.commit()
    conn.close()
    sys.exit(0)

def main():
    global testKrakenBalances
    #websocket.enableTrace(True)
    testKrakenBalances = KrakenBalances({"BTC/USD":0, "USD":1e6})
    setup_database()
    if liveData:
        signal.signal(signal.SIGINT, signal_handler)
        ws_thread_l2 = threading.Thread(target=kraken_l2.start_websocket, daemon=True)
        ws_thread_l2.start()
        testStrategyLive()
    else:
        testStrategyHistorical()
        return


if __name__ == "__main__":
    main()