class SQLConfig:
    @staticmethod
    def create_table_query(table_name, depth):
        """
        Creates a table with `depth` levels of bid/ask columns.
        Example columns: bid_price_1, bid_volume_1, ..., bid_price_n, bid_volume_n
                         ask_price_1, ask_volume_1, ..., ask_price_n, ask_volume_n
        """
        # Basic columns
        columns = [
            "timestamp TIMESTAMP",
            "symbol SYMBOL"
        ]
        
        # Bid columns
        for i in range(1, depth + 1):
            columns.append(f"bid_price_{i} REAL")
            columns.append(f"bid_volume_{i} REAL")
        
        # Ask columns
        for i in range(1, depth + 1):
            columns.append(f"ask_price_{i} REAL")
            columns.append(f"ask_volume_{i} REAL")
        
        # Join all column definitions with commas
        columns_str = ",\n    ".join(columns)
        
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_str}
        );
        """

    @staticmethod
    def insert_table_query(table_name, depth):
        """
        Generates an INSERT query with placeholders for `depth` levels of bid/ask columns.
        """
        # Build list of column names (excluding the 'id' which autoincrements):
        columns = ["timestamp", "symbol"]
        
        for i in range(1, depth + 1):
            columns.append(f"bid_price_{i}")
            columns.append(f"bid_volume_{i}")
        
        for i in range(1, depth + 1):
            columns.append(f"ask_price_{i}")
            columns.append(f"ask_volume_{i}")
        
        # Generate the comma-separated list of columns
        column_list_str = ", ".join(columns)
        # Generate the placeholders for values in the SQL query
        placeholders = ", ".join(["{}"] * len(columns))  # Using '{}' as placeholder for formatting
        
        return f"""
        INSERT INTO {table_name} (
            {column_list_str}
        ) VALUES ({placeholders});
        """
    
    @staticmethod
    def create_executions_table_query():
        """
        Creates an executions table with columns for order_id, exec_id, trade_id, symbol, side, price, qty, and timestamp.
        """
        # Basic columns
        columns = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "timestamp DATETIME",
            "symbol TEXT",
            "order_id TEXT",
            "exec_id TEXT",
            "exec_type TEXT",
            "trade_id INTEGER",
            "side TEXT",
            "last_qty REAL",
            "last_price REAL",
            "liquidity_ind TEXT",
            "cost REAL",
            "order_userref INTEGER",
            "order_status TEXT",
            "order_type TEXT",
            "fee_usd_equiv REAL"
        ]
        
        # Join all column definitions with commas
        columns_str = ",\n    ".join(columns)
        
        return f"""
        CREATE TABLE IF NOT EXISTS EXECUTIONS (
            {columns_str}
        );
        """
    
    @staticmethod
    def insert_executions_table_query(trade):
        """
        Static method that generates the SQL INSERT or UPDATE query string for a single trade.
        
        :param trade: A dictionary containing trade information.
        :return: A string containing the SQL INSERT or UPDATE query.
        """
        # Default values for missing fields in trade data
        order_id = trade.get('order_id', '')
        exec_id = trade.get('exec_id', '')
        exec_type = trade.get('exec_type', '')
        trade_id = trade.get('trade_id', 'NULL')
        symbol = trade.get('symbol', '')
        side = trade.get('side', '')
        last_qty = trade.get('last_qty', 'NULL')
        last_price = trade.get('last_price', 'NULL')
        liquidity_ind = trade.get('liquidity_ind', '')
        cost = trade.get('cost', 'NULL')
        order_userref = trade.get('order_userref', 'NULL')
        order_status = trade.get('order_status', '')
        order_type = trade.get('order_type', '')
        fee_usd_equiv = trade.get('fee_usd_equiv', 'NULL')
        timestamp = trade.get('timestamp', '')

        # SQL INSERT or UPDATE statement (UPSERT behavior)
        insert_or_update_query = f'''
        INSERT OR REPLACE INTO EXECUTIONS (
            order_id, exec_id, exec_type, trade_id, symbol, side, last_qty, 
            last_price, liquidity_ind, cost, order_userref, order_status, 
            order_type, fee_usd_equiv, timestamp
        ) VALUES (
            '{order_id}', '{exec_id}', '{exec_type}', {trade_id}, 
            '{symbol}', '{side}', {last_qty}, 
            {last_price}, '{liquidity_ind}', {cost}, {order_userref}, 
            '{order_status}', '{order_type}', {fee_usd_equiv}, 
            '{timestamp}'
        );
        '''
        return insert_or_update_query

    @staticmethod
    def delete_executions_by_order_id_query(order_id):
        """
        Delete the trade with the specified order_id, to handle the case of replacing old trades.
        """
        return f"DELETE FROM EXECUTIONS WHERE order_id = '{order_id}';"