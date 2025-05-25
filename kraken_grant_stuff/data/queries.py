execution_update_mapping = {
    "snapshot": dict(timestamp="timestamp", symbol="symbol", order_id="order_id", exec_id="exec_id", exec_type="exec_type", trade_id="trade_id", side="side", qty="last_qty", price="last_price", liquidity_ind="liquidity_ind", cost="cost", order_userref="order_userref", order_status="order_status", order_type="order_type", fee_usd_equiv="fee_usd_equiv"),
    "update_pending_new": dict(timestamp="timestamp", symbol="symbol", order_id="order_id", exec_id="exec_id", exec_type="exec_type", trade_id="trade_id", side="side", qty="order_qty", cost="cum_cost", order_userref="order_userref", order_status="order_status", order_type="order_type", fee_usd_equiv="fee_usd_equiv"),
    "update_new": dict(timestamp="timestamp", symbol="symbol", order_id="order_id", exec_id="exec_id", exec_type="exec_type", order_userref="order_userref", order_status="order_status", fee_usd_equiv="fee_usd_equiv"),
    "update_partially_filled": dict(timestamp="timestamp", symbol="symbol", order_id="order_id", exec_id="exec_id", exec_type="exec_type", trade_id="trade_id", side="side", qty="cum_qty", price="avg_price", liquidity_ind="liquidity_ind", cost="cum_cost", order_userref="order_userref", order_status="order_status", fee_usd_equiv="fee_usd_equiv"),
    "update_filled": dict(timestamp="timestamp", order_id="order_id", exec_type="exec_type", qty="cum_qty", price="avg_price", cost="cum_cost", order_userref="order_userref", order_status="order_status", fee_usd_equiv="fee_usd_equiv")
    }

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
            "timestamp TIMESTAMP",
            "sequence INTEGER",
            "symbol SYMBOL",
            "order_id TEXT",
            "exec_id TEXT",
            "exec_type SYMBOL",
            "trade_id INTEGER",
            "side SYMBOL",
            "qty REAL",
            "price REAL",
            "liquidity_ind SYMBOL",
            "cost REAL",
            "order_userref INTEGER",
            "order_status SYMBOL",
            "order_type SYMBOL",
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
    def insert_executions_table_query(event, trade, sequence=None):
        """
        Static method that generates the SQL INSERT or UPDATE query string for a single trade.
        :param event: String type of even calling the insert. Either snapshot or order_status
        :param trade: A dictionary containing trade information.
        :return: A string containing the SQL INSERT or UPDATE query.
        """

        # Default values for missing fields in trade data
        timestamp = trade.get(execution_update_mapping[event].get('timestamp',""), "")
        sequence = 'NULL' if sequence is None else sequence
        symbol = trade.get(execution_update_mapping[event].get('symbol',""), "")
        order_id = trade.get(execution_update_mapping[event].get('order_id',""), "")
        exec_id = trade.get(execution_update_mapping[event].get('exec_id',""), "")
        exec_type = trade.get(execution_update_mapping[event].get('exec_type',""), "")
        trade_id = trade.get(execution_update_mapping[event].get('trade_id',"NULL"), "NULL")
        side = trade.get(execution_update_mapping[event].get('side',""), "")
        qty = trade.get(execution_update_mapping[event].get('qty',"NULL"), "NULL")
        price = trade.get(execution_update_mapping[event].get('price',"NULL"), "NULL")
        liquidity_ind = trade.get(execution_update_mapping[event].get('liquidity_ind',""), "")
        cost = trade.get(execution_update_mapping[event].get('cost',"NULL"), "NULL")
        order_userref = trade.get(execution_update_mapping[event].get('order_userref',"NULL"), "NULL")
        order_status = trade.get(execution_update_mapping[event].get('order_status',""), "")
        order_type = trade.get(execution_update_mapping[event].get('order_type',""), "")
        fee_usd_equiv = trade.get(execution_update_mapping[event].get('fee_usd_equiv',"NULL"), "NULL")

        # SQL INSERT or UPDATE statement (UPSERT behavior)
        insert_or_update_query = f'''
        INSERT INTO EXECUTIONS (
            timestamp, sequence, symbol,
            order_id, exec_id, exec_type, trade_id, side, qty, 
            price, liquidity_ind, cost, order_userref, order_status, 
            order_type, fee_usd_equiv
        ) VALUES (
            '{timestamp}', {sequence}, '{symbol}',
            '{order_id}', '{exec_id}', '{exec_type}', {trade_id}, 
            '{side}', {qty}, 
            {price}, '{liquidity_ind}', {cost}, {order_userref}, 
            '{order_status}', '{order_type}', {fee_usd_equiv}
        );
        '''
        #print(insert_or_update_query)
        return insert_or_update_query

    @staticmethod
    def delete_executions_by_order_id_query(order_id):
        """
        Delete the trade with the specified order_id, to handle the case of replacing old trades.
        """
        return f"DELETE FROM EXECUTIONS WHERE order_id = '{order_id}';"