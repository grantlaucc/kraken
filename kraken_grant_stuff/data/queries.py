class SQLConfig:
    @staticmethod
    def create_table_query(table_name):
        return f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,currency TEXT,
            bid_price_1 REAL, bid_volume_1 REAL,
            bid_price_2 REAL, bid_volume_2 REAL,
            bid_price_3 REAL, bid_volume_3 REAL,
            bid_price_4 REAL, bid_volume_4 REAL,
            bid_price_5 REAL, bid_volume_5 REAL,
            bid_price_6 REAL, bid_volume_6 REAL,
            bid_price_7 REAL, bid_volume_7 REAL,
            bid_price_8 REAL, bid_volume_8 REAL,
            bid_price_9 REAL, bid_volume_9 REAL,
            bid_price_10 REAL, bid_volume_10 REAL,
            ask_price_1 REAL, ask_volume_1 REAL,
            ask_price_2 REAL, ask_volume_2 REAL,
            ask_price_3 REAL, ask_volume_3 REAL,
            ask_price_4 REAL, ask_volume_4 REAL,
            ask_price_5 REAL, ask_volume_5 REAL,
            ask_price_6 REAL, ask_volume_6 REAL,
            ask_price_7 REAL, ask_volume_7 REAL,
            ask_price_8 REAL, ask_volume_8 REAL,
            ask_price_9 REAL, ask_volume_9 REAL,
            ask_price_10 REAL, ask_volume_10 REAL
        );
        '''
    
    @staticmethod
    def insert_table_query(table_name):
        return f'''
        INSERT INTO {table_name} (
            timestamp, currency,
            bid_price_1, bid_volume_1,
            bid_price_2, bid_volume_2,
            bid_price_3, bid_volume_3,
            bid_price_4, bid_volume_4,
            bid_price_5, bid_volume_5,
            bid_price_6, bid_volume_6,
            bid_price_7, bid_volume_7,
            bid_price_8, bid_volume_8,
            bid_price_9, bid_volume_9,
            bid_price_10, bid_volume_10,
            ask_price_1, ask_volume_1,
            ask_price_2, ask_volume_2,
            ask_price_3, ask_volume_3,
            ask_price_4, ask_volume_4,
            ask_price_5, ask_volume_5,
            ask_price_6, ask_volume_6,
            ask_price_7, ask_volume_7,
            ask_price_8, ask_volume_8,
            ask_price_9, ask_volume_9,
            ask_price_10, ask_volume_10
        ) VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?);
        '''
