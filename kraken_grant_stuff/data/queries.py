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
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "timestamp DATETIME",
            "symbol TEXT"
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
        # Make the same number of '?' placeholders
        placeholders = ", ".join(["?"] * len(columns))
        
        return f"""
        INSERT INTO {table_name} (
            {column_list_str}
        ) VALUES ({placeholders});
        """
