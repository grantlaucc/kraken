import sqlite3
from data.queries import SQLConfig
import zlib
import re

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
        self.db_file = 'kraken_quotes_{}.db'.format(self.depth)
        self.db_table_name = re.sub(r'\W', '_', self.symbol)

        # Connect to SQLite database (or create it)
        self.conn = sqlite3.connect(self.db_file,check_same_thread=False)
        self.cursor = self.conn.cursor()
        print("db connected")
        # Create the table if it doesn't exist
        self.cursor.execute(SQLConfig.create_table_query(self.db_table_name, self.depth))
        self.conn.commit()

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
            values+=[query_time.strftime('%Y-%m-%d %H:%M:%S'),self.symbol]
            for i in range(self.depth):
                values.extend([str(self.bids[i][0]), str(self.bids[i][1])])
            for i in range(self.depth):
                values.extend([str(self.asks[i][0]), str(self.asks[i][1])])

            #print(values)
            self.cursor.execute(SQLConfig.insert_table_query(self.db_table_name, self.depth), values)
            self.conn.commit()

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