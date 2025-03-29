import sqlite3
from data.queries import SQLConfig
import zlib
import re
import requests
import urllib.parse as par

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
        self.db_url = 'http://localhost:9000'
        self.db_table_name = re.sub(r'\W', '_', self.symbol)
        self.create_table()

    def create_table(self):
        create_table_query = par.quote(SQLConfig.create_table_query(self.db_table_name, self.depth))
        try:
            response = requests.get(f"{self.db_url}/exec?query={create_table_query}")
            if response.status_code == 200:
                print(f"Table {self.db_table_name} created or already exists.")
            else:
                print(f"Failed to create table: {response.text}")
        except Exception as e:
            print(f"Error connecting to QuestDB: {e}")

    def updateOrderBook(self, updateBids, updateAsks, timestamp):
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
        
        self.lastUpdate = timestamp
        return
    
    def getMid(self):
        if len(self.bids)>0 and len(self.asks)>0:
            return (self.bids[0][0]+self.asks[0][0])/2

    def getQuote(self,query_time):
        if len(self.bids)>0 and len(self.asks)>0:
            print(query_time.strftime('%Y-%m-%d %H:%M:%S')+"\t"
                  +self.symbol+"\t"
                  +str(self.bids[0][0])+"/"+str(self.asks[0][0])+"\t"
                  +str(self.bids[0][1])+"x"+str(self.asks[0][1])
                )
            return
    
    def writeOrderBooktoDB(self, query_time):
        if len(self.bids)>0 and len(self.asks)>0:
            # Prepare the values for insertion
            values = []
            timestamp_str = f"'{query_time.strftime('%Y-%m-%d %H:%M:%S')}'"  # Wrap in single quotes
            symbol_str = f"'{self.symbol}'"  # Wrap symbol in single quotes
            values += [timestamp_str, symbol_str]
            for i in range(self.depth):
                values.extend([str(self.bids[i][0]), str(self.bids[i][1])])
            for i in range(self.depth):
                values.extend([str(self.asks[i][0]), str(self.asks[i][1])])

            #print(values)
            insert_query = SQLConfig.insert_table_query(self.db_table_name, self.depth).format(*values)
            response = requests.get(f"{self.db_url}/exec?query={insert_query}")
            if response.status_code != 200:
                print(f"Failed to insert data: {response.text}")


    
    def populateHistorical(self, row):
        """
        Populate the OrderBook object using a single row of data from the CSV file.
            
        :param row: A single row of data from the CSV (in the form of a dictionary).
        """
        # Extract bids and asks
        bids = []
        asks = []
        
        for i in range(1, 26):  # 25 bids and 25 asks
            bid_price = row[f'bid_price_{i}']
            bid_volume = row[f'bid_volume_{i}']
            ask_price = row[f'ask_price_{i}']
            ask_volume = row[f'ask_volume_{i}']
            
            # Add the bid and ask to their respective lists
            if bid_price and bid_volume:
                bids.append([float(bid_price), float(bid_volume)])
            if ask_price and ask_volume:
                asks.append([float(ask_price), float(ask_volume)])

        # Now populate the order book
        self.symbol = row['symbol']
        self.bids = bids
        self.asks = asks

def generate_checksum(bids, asks):
    # Step 1: Generate the formatted string for asks (sorted in ascending order)
    asks_str = ''
    for ask in asks[:10]:  # Top 10 asks, sorted from low to high price
        price_str = str(ask[0]).replace('.', '').lstrip('0')
        qty_str = str(ask[1]).replace('.', '').lstrip('0')
        asks_str += price_str + qty_str

    # Step 2: Generate the formatted string for bids (sorted in descending order)
    bids_str = ''
    for bid in bids[:10]:  # Top 10 bids, sorted from high to low price
        price_str = str(bid[0]).replace('.', '').lstrip('0')
        qty_str = str(bid[1]).replace('.', '').lstrip('0')
        bids_str += price_str + qty_str

    full_str = asks_str + bids_str
    checksum = zlib.crc32(full_str.encode('utf-8'))
    return checksum

def validate_checksum(calculatedChecksum, exchangeChecksum):
    return calculatedChecksum == exchangeChecksum

