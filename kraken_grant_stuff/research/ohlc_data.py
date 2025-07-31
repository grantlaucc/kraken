import requests
from datetime import datetime, timezone
import urllib.parse as par
from questdb.ingress import Sender
import pandas as pd


def get_ohlc_data(pair="BTC/USD", interval=1440, since = None):
    url = "https://api.kraken.com/0/public/OHLC"
    params = {
        "pair": pair,
        "interval": interval,
    }
    if since is not None:
        params["since"] = since

    response = requests.get(url, params=params)
    data = response.json()

    if data["error"]:
        raise Exception(f"API returned errors: {data['error']}")
    
    pair_key = list(data["result"].keys())[0]
    ohlc_data = data["result"][pair_key]
    return ohlc_data

def format_timestamp(ts):
    # Convert Unix timestamp to QuestDB-compatible ISO format
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def ohlc_to_df(ohlc):
    df = pd.DataFrame(ohlc, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count'
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    for col in ['open', 'high', 'low', 'close', 'vwap', 'volume']:
        df[col] = df[col].astype(float)
    df['count'] = df['count'].astype(int)
    return df

def create_table_if_not_exists(table_name, questdb_url="http://localhost:9000/exec"):
    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            vwap DOUBLE,
            volume DOUBLE,
            count LONG
        ) timestamp(timestamp) PARTITION BY DAY WAL;
    """
    encoded_query = par.quote(query)
    response = requests.get(f"{questdb_url}?query={encoded_query}")
    
    if response.status_code != 200:
        raise Exception(f"Failed to create table {table_name}: {response.text}")

def insert_ohlc_to_questdb_ilp(ticker, ohlc_df, interval, host="localhost", port=9000, new_only=True):
    questdb_url = "http://localhost:9000/exec"
    table_name = f"{ticker.replace('/', '_')}_{interval}"

    # Ensure timestamp column is datetime
    ohlc_df['timestamp'] = pd.to_datetime(ohlc_df['timestamp'], utc=True)

    if new_only:
        # Query for latest timestamp in the table
        max_ts_query = f"SELECT max(timestamp) FROM {table_name};"
        response = requests.get(questdb_url, params={"query": max_ts_query})
        if response.status_code != 200:
            raise Exception(f"Failed to get max timestamp: {response.text}")

        try:
            result = response.json()["dataset"]
            if result and result[0][0] is not None:
                latest_ts = pd.to_datetime(result[0][0])
                ohlc_df = ohlc_df[ohlc_df['timestamp'] > latest_ts].copy()
        except Exception as e:
            raise Exception(f"Failed to parse max timestamp response: {response.text} ({e})")

    if ohlc_df.empty:
        print(f"No new rows to insert for {ticker}")
        return

    # Add a symbol column if needed
    ohlc_df['symbol'] = ticker

    with Sender.from_conf(f"http::addr={host}:{port};") as sender:
        sender.dataframe(
            ohlc_df,
            table_name=table_name,
            symbols=['symbol'],      # or [] if no symbol columns
            at='timestamp'           # must be datetime64
        )
    print(f"Inserted {len(ohlc_df)} rows into {table_name} using ILP")

def load_ohlc_data_to_df(ticker, interval=1440, questdb_url="http://localhost:9000/exec", startDate=None, endDate=None):
    table_name = f"{ticker.replace('/', '_')}_{interval}"

    # Build WHERE clause based on start and end date
    where_clauses = []
    if startDate:
        start_iso = pd.to_datetime(startDate).tz_convert("UTC").isoformat()
        where_clauses.append(f"timestamp >= '{start_iso}'")
    if endDate:
        end_iso = pd.to_datetime(endDate).tz_convert("UTC").isoformat()
        where_clauses.append(f"timestamp <= '{end_iso}'")

    where_clause = ""
    if where_clauses:
        where_clause = "WHERE " + " AND ".join(where_clauses)

    query = f"SELECT * FROM {table_name} {where_clause} ORDER BY timestamp"

    response = requests.get(questdb_url, params={"query": query, "format": "json"})
    if response.status_code != 200:
        raise Exception(f"Failed to query QuestDB: {response.text}")

    json_data = response.json()
    columns = [col["name"] for col in json_data["columns"]]
    data = json_data["dataset"]

    df = pd.DataFrame(data, columns=columns)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        for col in ['open', 'high', 'low', 'close', 'vwap', 'volume']:
            df[col] = df[col].astype(float)
        df['count'] = df['count'].astype(int)

    df = df.set_index('timestamp')
    return df

def download_ohlc_data(interval, tickers, startTimestamp, new_only=True):
    for ticker in tickers:
        ohlc = get_ohlc_data(ticker, interval, since=startTimestamp)
        ohlcDF = ohlc_to_df(ohlc)
        insert_ohlc_to_questdb_ilp(ticker, ohlcDF, interval, new_only=new_only)

if __name__ == "__main__":
    interval = 1440
    with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usdc_pairs.txt", "r") as file:
        usdc_pairs = [line.strip() for line in file]
    startTimestamp = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())

    download_ohlc_data(interval=interval, tickers=usdc_pairs,startTimestamp=startTimestamp, new_only=True)