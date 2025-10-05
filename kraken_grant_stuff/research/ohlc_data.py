import requests
from datetime import datetime, timezone
import urllib.parse as par
from questdb.ingress import Sender
import pandas as pd
import io
from typing import Iterable, Union, List


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

def insert_ohlc_to_csv(ticker, ohlcDF, column, csvFile):
    # Build clean Series: timestamp -> value
    idx = pd.to_datetime(ohlcDF["timestamp"] if "timestamp" in ohlcDF.columns else ohlcDF.index, utc=True)
    s = (pd.Series(pd.to_numeric(ohlcDF[column], errors="coerce").values, index=idx, name=ticker)
           .sort_index()
           .dropna())
    s = s[~s.index.duplicated(keep="last")]  # guard against duplicate timestamps

    # Load (or init) wide CSV
    try:
        df = pd.read_csv(csvFile, parse_dates=["timestamp"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp").sort_index()
    except FileNotFoundError:
        df = pd.DataFrame(index=pd.DatetimeIndex([], name="timestamp", tz="UTC"))

    # Only rows strictly after the last existing timestamp for this ticker
    last_dt = df[ticker].last_valid_index() if ticker in df.columns else None
    s_new = s[s.index > last_dt] if last_dt is not None else s

    n = len(s_new)
    if n:
        # expand index and assign (no column overlap issues)
        df = df.reindex(df.index.union(s_new.index)).sort_index()
        df.loc[s_new.index, ticker] = s_new.values
        df.reset_index().to_csv(csvFile, index=False)

    print(f"Inserted {n} row(s) into column '{ticker}' in csv.")
    return n

def load_ohlc_data_to_df(ticker, interval=1440, questdb_url="http://localhost:9000/exec", startDate=None, endDate=None, selectCols = None,
                         read_csv = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/all_closes.csv"):
    #Direct csv load branch
    if read_csv is not None:
        df_wide = pd.read_csv(read_csv, parse_dates=["timestamp"])
        df_wide["timestamp"] = pd.to_datetime(df_wide["timestamp"], utc=True)
        df_wide = df_wide.set_index("timestamp").sort_index()

        # apply date filters (optional)
        if startDate is not None:
            df_wide = df_wide[df_wide.index >= pd.to_datetime(startDate, utc=True)]
        if endDate is not None:
            df_wide = df_wide[df_wide.index <= pd.to_datetime(endDate, utc=True)]

        # direct, exact match
        if ticker not in df_wide.columns:
            raise KeyError(f"Ticker column '{ticker}' not found in CSV")

        out = pd.DataFrame({"close": pd.to_numeric(df_wide[ticker], errors="coerce")})
        return out
    
    #QuestDB query branch
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
    if isinstance(selectCols, list):
        if "timestamp" not in selectCols:
            selectCols.append("timestamp")
            selectColsString = ", ".join(selectCols)
        query = f"SELECT {selectColsString} FROM {table_name} {where_clause} ORDER BY timestamp"
    else:
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
        float_cols = {"open", "high", "low", "close", "vwap", "volume"} & set(df.columns)
        for col in float_cols:
            df[col] = df[col].astype(float)
        if "count" in df.columns:
            df['count'] = df['count'].astype(int)

    df = df.set_index('timestamp')
    return df

def read_csv_data(
    tickers: Union[str, Iterable[str]],
    csv_path: str = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/all_closes.csv",
    startDate: str | pd.Timestamp | None = None,
    endDate: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Load a wide CSV of close prices and return a DataFrame of closes for the requested tickers,
    filtered to [startDate, endDate] (inclusive). The CSV must have a 'timestamp' column
    and one column per ticker (values = close prices).

    Parameters
    ----------
    csv_path : path to CSV
    tickers  : a ticker symbol or an iterable of symbols (must match CSV column names)
    startDate, endDate : optional date/datetime strings or pd.Timestamp; interpreted in UTC

    Returns
    -------
    pd.DataFrame indexed by UTC timestamps with columns = requested tickers (float).
    """
    # Normalize tickers to a list
    if isinstance(tickers, str):
        tickers = [tickers]
    else:
        tickers = list(tickers)

    # Load and set index
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    if "timestamp" not in df.columns:
        raise ValueError("CSV must have a 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.set_index("timestamp").sort_index()

    # Date filters (inclusive)
    if startDate is not None:
        df = df[df.index >= pd.to_datetime(startDate, utc=True)]
    if endDate is not None:
        df = df[df.index <= pd.to_datetime(endDate, utc=True)]

    # Validate tickers exist
    missing: List[str] = [t for t in tickers if t not in df.columns]
    if missing:
        raise KeyError(f"Ticker(s) not found in CSV: {missing}")

    # Select and coerce to float
    out = df[tickers].apply(pd.to_numeric, errors="coerce")

    return out

def download_ohlc_data(interval, tickers, startTimestamp, new_only=True):
    for ticker in tickers:
        ohlc = get_ohlc_data(ticker, interval, since=startTimestamp)
        ohlcDF = ohlc_to_df(ohlc)
        insert_ohlc_to_questdb_ilp(ticker, ohlcDF, interval, new_only=new_only)
        insert_ohlc_to_csv(ticker, ohlcDF, column = 'close', csvFile = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/all_closes.csv")

def export_closes_to_csv_2(
    tickers,
    csv_path="all_closes.csv",
    interval=1440,
    questdb_base="http://localhost:9000",
    chunk_months=3,
    iso_z=True,
    incremental=True,     # <— NEW: only fetch after last timestamp in existing CSV
    atomic=True           # <— NEW: write to temp then replace
):
    """
    Build a wide CSV with one 'close' column per ticker (full history).
    - Streams CSV from QuestDB and fetches in date chunks to avoid GC/heap errors.
    - If incremental=True and csv_path exists, fetch only data newer than each
      ticker's last timestamp in the existing file.
    """
    import io, os
    import pandas as pd
    import requests

    exec_url = questdb_base.rstrip("/") + "/exec"
    exp_url  = questdb_base.rstrip("/") + "/exp"

    def _to_z(dt):
        return pd.to_datetime(dt, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def _fetch_csv(sql, timeout=120):
        # Prefer POST /exec (fmt=csv). Fallback to GET /exp if 404.
        try:
            r = requests.post(exec_url, data={"query": sql, "fmt": "csv"}, timeout=timeout)
            r.raise_for_status()
            return pd.read_csv(io.BytesIO(r.content))
        except requests.HTTPError as e:
            txt = getattr(e.response, "text", "")
            if "404" in str(e) or "Not Found" in txt:
                r2 = requests.get(exp_url, params={"query": sql}, timeout=timeout, stream=True)
                r2.raise_for_status()
                return pd.read_csv(io.BytesIO(r2.content))
            raise

    def _get_range(table_name):
        # min/max timestamp (tiny query)
        sql = f"SELECT min(timestamp) AS tmin, max(timestamp) AS tmax FROM {table_name}"
        try:
            r = requests.post(exec_url, data={"query": sql, "fmt": "csv"}, timeout=30)
            r.raise_for_status()
            rng = pd.read_csv(io.BytesIO(r.content))
        except requests.HTTPError:
            r2 = requests.get(exp_url, params={"query": sql}, timeout=30)
            r2.raise_for_status()
            rng = pd.read_csv(io.BytesIO(r2.content))
        if rng.empty or pd.isna(rng.loc[0, "tmin"]) or pd.isna(rng.loc[0, "tmax"]):
            return None, None
        tmin = pd.to_datetime(rng.loc[0, "tmin"], utc=True)
        tmax = pd.to_datetime(rng.loc[0, "tmax"], utc=True)
        return tmin, tmax

    def _chunk_bounds(start_ts, end_ts, months):
        bounds, cur = [], pd.to_datetime(start_ts, utc=True)
        end = pd.to_datetime(end_ts, utc=True)
        while cur <= end:
            nxt = (cur + pd.DateOffset(months=months)).normalize()
            if nxt <= cur:
                nxt = cur + pd.Timedelta(days=1)
            stop = min(nxt - pd.Timedelta(microseconds=1), end)
            bounds.append((cur, stop))
            cur = nxt
        return bounds

    # Load existing CSV (if any)
    wide = None
    if incremental and os.path.exists(csv_path):
        prev = pd.read_csv(csv_path, parse_dates=["timestamp"])
        prev["timestamp"] = pd.to_datetime(prev["timestamp"], utc=True)
        prev = prev.set_index("timestamp").sort_index()
        wide = prev  # start from previous data

    for t in tickers:
        try:
            table = f"{t.replace('/','_')}_{int(interval)}"
            tmin, tmax = _get_range(table)
            if tmin is None:
                print(f"Skipping {t}: no data (range not available)")
                continue

            # Find last timestamp we already have for this ticker
            last_have = None
            if wide is not None and t in wide.columns:
                last_have = wide[t].dropna().index.max()

            # Decide start bound
            start_from = tmin if (not incremental or last_have is None) else max(
                tmin, last_have + pd.Timedelta(microseconds=1)
            )
            if start_from > tmax:
                # Up-to-date; nothing to fetch. Keep existing column if present.
                print(f"{t}: up-to-date through {last_have}")
                continue

            # Fetch new data in chunks
            parts = []
            for s_ts, e_ts in _chunk_bounds(start_from, tmax, chunk_months):
                sql = (
                    f"SELECT timestamp, close FROM {table} "
                    f"WHERE timestamp BETWEEN '{_to_z(s_ts)}' AND '{_to_z(e_ts)}' "
                    f"ORDER BY timestamp"
                )
                df = _fetch_csv(sql)
                if df.empty:
                    continue
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
                ser = pd.to_numeric(df["close"], errors="coerce")
                ser.index = df["timestamp"]
                ser = ser[~ser.index.duplicated(keep="last")]
                parts.append(ser.rename(t))

            if not parts:
                print(f"{t}: no new rows retrieved")
                continue

            s_new = pd.concat(parts).sort_index()
            s_new = s_new[~s_new.index.duplicated(keep="last")]

            # Merge new with existing
            if wide is None:
                wide = s_new.to_frame()
            else:
                # If column already exists, combine and dedup
                if t in wide.columns:
                    s_old = wide[t]
                    s_all = pd.concat([s_old.dropna(), s_new]).sort_index()
                    s_all = s_all[~s_all.index.duplicated(keep="last")]
                    wide = wide.drop(columns=[t]).join(s_all.to_frame(name=t), how="outer")
                else:
                    wide = wide.join(s_new.to_frame(name=t), how="outer")

            print(f"{t}: added {len(s_new):,} new rows (now through {tmax})")

        except Exception as e:
            print(f"Skipping {t}: {e}")
            continue

    if wide is None or wide.empty:
        raise ValueError("No data fetched or updated for any ticker.")

    wide = wide.sort_index()

    # Write CSV (atomic optional)
    out = wide.copy()
    if iso_z:
        out.index = out.index.tz_convert("UTC")
        out.index = out.index.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        out.index = out.index.tz_convert(None)

    if atomic:
        tmp_path = f"{csv_path}.tmp"
        out.to_csv(tmp_path, index_label="timestamp", float_format="%.10g")
        os.replace(tmp_path, csv_path)  # atomic on POSIX
    else:
        out.to_csv(csv_path, index_label="timestamp", float_format="%.10g")

    print(f"Wrote {len(out):,} rows × {len(out.columns)} columns to {csv_path}")
    return out


if __name__ == "__main__":
    interval = 1440
    with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usd_pairs.txt", "r") as file:
        usdc_pairs = [line.strip() for line in file]
    startTimestamp = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())

    #for ticker in usdc_pairs:
        #tableName = ticker.replace("/","_")+"_"+str(interval)
        #create_table_if_not_exists(tableName)
    #download_ohlc_data(interval=interval, tickers=usdc_pairs,startTimestamp=startTimestamp, new_only=True)
    #print(load_ohlc_data_to_df("BTC/USD"))
    #export_closes_to_csv_2(tickers=usdc_pairs)
    ticker = ["BTC/USD", "ETH/USD"]
    startDate = pd.Timestamp.now(tz="UTC").normalize()
    df = read_csv_data(ticker, startDate = startDate)
    print(df)
    #ohlc = get_ohlc_data(ticker, interval, since=startTimestamp)
    #ohlcDF = ohlc_to_df(ohlc)
    #print(ohlcDF)

