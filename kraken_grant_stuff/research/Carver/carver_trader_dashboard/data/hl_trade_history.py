import os
from datetime import datetime, timezone
from threading import Lock

import pandas as pd

# Field names confirmed against hyperliquid-python-sdk's Fill TypedDict (utils/types.py) --
# "tid" (trade id) is the per-fill unique key; "oid" is the order it belongs to and can repeat
# across multiple partial fills of the same order.
FILL_COLS = ["time", "coin", "side", "px", "sz", "dir", "closedPnl", "fee", "feeToken", "oid", "tid", "hash"]
# Funding record fields per Hyperliquid's docs (time, coin, usdc, szi, fundingRate) -- not yet
# observed against a real funding payment (probe wallet had no open positions), see the plan.
FUNDING_COLS = ["time", "coin", "usdc", "szi", "fundingRate"]

_FILLS_LOCK = Lock()
_FUNDING_LOCK = Lock()


def fills_log_path(strategy_dir: str) -> str:
    return os.path.join(strategy_dir, "hl_fills.csv")


def funding_log_path(strategy_dir: str) -> str:
    return os.path.join(strategy_dir, "hl_funding_payments.csv")


def _append_dedup(path: str, lock: Lock, row: dict, cols: list[str], dedup_key: str) -> bool:
    """Append `row` to the CSV at `path` unless a row with the same dedup_key value already
    exists. Returns True if the row was written. A websocket can redeliver a message after a
    reconnect, so dedup on the venue's own unique id rather than trusting "only arrives once"."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with lock:
        if os.path.exists(path):
            existing = pd.read_csv(path)
            if dedup_key in existing.columns and row.get(dedup_key) is not None:
                if (existing[dedup_key].astype(str) == str(row[dedup_key])).any():
                    return False
            out = pd.concat([existing, pd.DataFrame([row])[cols]], ignore_index=True)
        else:
            out = pd.DataFrame([row])[cols]
        out.to_csv(path, index=False)
        return True


def record_fill(strategy_dir: str, fill: dict) -> bool:
    """fill: a single raw item from a userFills websocket message (see FILL_COLS)."""
    row = {c: fill.get(c) for c in FILL_COLS}
    row["_recorded_at"] = datetime.now(timezone.utc).isoformat()
    return _append_dedup(fills_log_path(strategy_dir), _FILLS_LOCK, row, FILL_COLS + ["_recorded_at"], "tid")


def record_funding(strategy_dir: str, funding: dict) -> bool:
    """funding: a single raw item from a userFundings websocket message (see FUNDING_COLS)."""
    row = {c: funding.get(c) for c in FUNDING_COLS}
    row["_recorded_at"] = datetime.now(timezone.utc).isoformat()
    # No single unique id field documented for a funding record -- (time, coin) is unique since
    # Hyperliquid settles funding once per coin per hour.
    dedup_key = f"{row.get('time')}|{row.get('coin')}"
    row["_dedup_key"] = dedup_key
    return _append_dedup(funding_log_path(strategy_dir), _FUNDING_LOCK, row,
                         FUNDING_COLS + ["_recorded_at", "_dedup_key"], "_dedup_key")


def load_recent_fills(strategy_dir: str, n: int = 20) -> pd.DataFrame:
    path = fills_log_path(strategy_dir)
    if not os.path.exists(path):
        return pd.DataFrame(columns=FILL_COLS)
    with _FILLS_LOCK:
        df = pd.read_csv(path)
    return df.tail(n).iloc[::-1].reset_index(drop=True)


def load_recent_funding(strategy_dir: str, n: int = 20) -> pd.DataFrame:
    path = funding_log_path(strategy_dir)
    if not os.path.exists(path):
        return pd.DataFrame(columns=FUNDING_COLS)
    with _FUNDING_LOCK:
        df = pd.read_csv(path)
    return df.tail(n).iloc[::-1].reset_index(drop=True)


def summarize_pnl(strategy_dir: str) -> dict:
    """Running totals: realized PnL (sum of closedPnl across fills) and total funding paid/received
    (sum of usdc across funding records; Hyperliquid's sign convention is already "amount credited
    to the account", so a simple sum is the net cash flow -- no re-negation needed)."""
    fills_path = fills_log_path(strategy_dir)
    funding_path = funding_log_path(strategy_dir)
    realized_pnl = 0.0
    total_funding = 0.0
    if os.path.exists(fills_path):
        with _FILLS_LOCK:
            df = pd.read_csv(fills_path)
        if "closedPnl" in df.columns:
            realized_pnl = pd.to_numeric(df["closedPnl"], errors="coerce").sum()
    if os.path.exists(funding_path):
        with _FUNDING_LOCK:
            df = pd.read_csv(funding_path)
        if "usdc" in df.columns:
            total_funding = pd.to_numeric(df["usdc"], errors="coerce").sum()
    return {"realized_pnl": float(realized_pnl), "total_funding": float(total_funding)}
