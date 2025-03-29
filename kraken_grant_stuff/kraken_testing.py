import threading
import signal
import sys
from kraken_balances import KrakenBalances
import kraken_l2
import time
import datetime as dt
import sqlite3
import csv
from order_book import OrderBook

import Strategies.order_book_imbalance_strat as strat


liveData = False
historicalDataFile = "/Users/grantlau/Documents/QuantStuff/CryptoData/kraken_quotes_25.csv"
testKrakenBalances = None
conn = None
cursor = None

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
        orderBook = kraken_l2.OrderBooks.get(strat.symbols[0])
        if not orderBook:
            print(f"No data for {strat.symbols[0]} yet...")
        else:
            orderBook.getQuote(dt.datetime.now())
            action,qty,price = strat.decision(orderBook, testKrakenBalances)
            if action:
                record_transaction(action,qty,price)
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
            action,qty,price = strat.decision(historicalOrderBook, testKrakenBalances)
            if action:
                record_transaction(action,qty,price)
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