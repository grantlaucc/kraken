import sqlite3
from data.queries import SQLConfig

krakenExecutionsDB = "kraken_executions.db"

def handle_executions_snapshot(tradeData, writeToDB = False):
    conn = sqlite3.connect(krakenExecutionsDB)
    cursor = conn.cursor()
    cursor.execute(SQLConfig.create_executions_table_query())
    conn.commit()

    tradeData.reverse() #Reverse tradeData to get oldest to newest.

    #Write to DB - only for first time. Otherwise executions snapshot should not modify db.
    if writeToDB:
        conn = sqlite3.connect(krakenExecutionsDB)
        cursor = conn.cursor()
        for trade in tradeData:
            cursor.execute(SQLConfig.insert_executions_table_query(trade))
        conn.commit()

    #check tail of executions DB matches snapshot
    tail_order_ids = [trade['order_id'] for trade in tradeData]
    tail_exec_ids = [trade['exec_id'] for trade in tradeData]
    
    # Fetch the last `len(tradeData)` trades from the database
    cursor.execute('''
        SELECT order_id, exec_id FROM EXECUTIONS
        WHERE order_status = 'filled'
        ORDER BY id DESC LIMIT ?
    ''', (len(tradeData),))

    # Get the results from the query
    tail_db_trades = cursor.fetchall()[::-1] #get newest n trades but then reverse the list

    # Check if the trades in the DB match with tradeData based on order_id and exec_id
    tail_db_order_ids = [trade[0] for trade in tail_db_trades]
    tail_db_exec_ids = [trade[1] for trade in tail_db_trades]

    # Check if the order_ids and exec_ids match
    if tail_db_order_ids != tail_order_ids or tail_db_exec_ids != tail_exec_ids:
        print("Mismatch between executions DB and executions snapshot in last {} trades.".format(len(tradeData)))

    conn.close()
    return

def handle_executions_update(tradeData):
    conn = sqlite3.connect(krakenExecutionsDB)
    cursor = conn.cursor()
    for trade in tradeData:
        cursor.execute(SQLConfig.insert_executions_table_query(trade))
    conn.commit()
    return