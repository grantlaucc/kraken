import requests
import pandas as pd

BASE_URL = "http://localhost:9000/exec"
TIMEOUT  = 60

def list_tables():
    r = requests.get(BASE_URL, params={"query": "SHOW TABLES"}, timeout=TIMEOUT)
    r.raise_for_status()
    js = r.json()
    cols = [c["name"] for c in js.get("columns", [])]
    idx = cols.index("table") if "table" in cols else 0
    return [row[idx] for row in js.get("dataset", [])]

def earliest_ts(table):
    q = f'SELECT min(timestamp) FROM "{table}";'
    r = requests.get(BASE_URL, params={"query": q}, timeout=TIMEOUT)
    r.raise_for_status()
    ds = r.json().get("dataset", [])
    if ds and ds[0][0] is not None:
        return pd.to_datetime(ds[0][0], utc=True)
    return None

def table_to_pair(table):
    """
    Convert table name like 'btc_usd_1440' -> 'BTC/USD'.
    If the name doesn't match that pattern, just return the table name.
    """
    parts = table.split("_")
    if len(parts) >= 3 and parts[-1].isdigit():
        base, quote = parts[0], parts[1]
        return f"{base.upper()}/{quote.upper()}"
    return table.upper()

def pairs_with_data_before(min_date_str: str, interval_suffix="1440"):
    """
    min_date_str: e.g. '2019-01-01' or '2019-01-01T00:00:00Z'
    Returns a list of pair symbols (e.g., 'BTC/USD') that have data earlier than min_date.
    """
    min_dt = pd.to_datetime(min_date_str, utc=True)

    all_tables = list_tables()
    # keep only tables ending with _<interval_suffix>
    candidate_tables = [t for t in all_tables if t.endswith(f"_{interval_suffix}")]
    eligible_pairs = []

    for t in candidate_tables:
        ts0 = earliest_ts(t)
        if ts0 is None:
            continue
        if ts0 < min_dt:
            eligible_pairs.append(table_to_pair(t))

    return eligible_pairs
