import sqlite3
import requests
from data.queries import SQLConfig
import urllib.parse as par

db_url = 'http://localhost:9000'

def handle_executions_snapshot(tradeData, writeToDB = False):
    tradeData.reverse() #Reverse tradeData to get oldest to newest.

    #Write to DB - only for first time. Otherwise executions snapshot should not modify db.
    if writeToDB:
        #Create executions table if doesn't exist
        create_query = SQLConfig.create_executions_table_query()
        create_response = requests.get(f"{db_url}/exec?query={create_query}")
        if create_response.status_code != 200:
            print("Failed to create executions table")

        for trade in tradeData:
            insert_query = SQLConfig.insert_executions_table_query("snapshot", trade)
            response = requests.get(f"{db_url}/exec?query={insert_query}")
            if response.status_code != 200:
                print(f"Failed to insert trade {trade['order_id']} with exec_id {trade['exec_id']}")

    #check tail of executions DB matches snapshot
    tail_order_ids = [trade['order_id'] for trade in tradeData]
    tail_exec_ids = [trade['exec_id'] for trade in tradeData]
    
    # Fetch the last `len(tradeData)` trades from the database
    tail_db_trades = []
    query = f"""
    SELECT order_id, exec_id FROM EXECUTIONS
    WHERE order_status IN ('filled', 'partially_filled')
    AND exec_id != ''
    ORDER BY timestamp DESC LIMIT {len(tradeData)};
    """
    query = par.quote(query)
    response = requests.get(f"{db_url}/exec?query={query}")
    data = response.json()
    tail_db_trades = data.get('dataset')[::-1]

    # Check if the trades in the DB match with tradeData based on order_id and exec_id
    tail_db_order_ids = [trade[0] for trade in tail_db_trades]
    tail_db_exec_ids = [trade[1] for trade in tail_db_trades]

    # Check if the order_ids and exec_ids match
    if tail_db_order_ids != tail_order_ids or tail_db_exec_ids != tail_exec_ids:
        print("Mismatch between executions DB and executions snapshot in last {} trades.".format(len(tradeData)))

    return

def handle_executions_update(tradeData, sequence = None):
    for trade in tradeData:
        event = "update_" + trade.get('order_status')
        insert_query = SQLConfig.insert_executions_table_query(event, trade, sequence)
        encoded_query = par.quote(insert_query)
        response = requests.get(f"{db_url}/exec?query={encoded_query}")
        if response.status_code != 200:
            print(f"Failed to insert trade {trade.get('order_id')} with exec_id {trade.get('exec_id')}")