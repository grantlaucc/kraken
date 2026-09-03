import os
import pandas as pd
from datetime import datetime, timezone
from threading import Lock

ORDER_COLS = ["timestamp","asset","side","qty","order_type","limit_price","status","req_id", "order_id"]
_ORDERS_LOCK = Lock()

def orders_log_path(strategy_dir: str) -> str:
    return os.path.join(strategy_dir, "orders_submitted.csv")

def record_order(strategy_dir: str, *, asset: str, side: str, qty: float,
                 order_type: str, limit_price, status: str, req_id: int | None):
    os.makedirs(strategy_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = pd.DataFrame([{
        "timestamp": ts, "asset": asset, "side": side, "qty": float(qty),
        "order_type": order_type, "limit_price": ("" if limit_price is None else float(limit_price)),
        "status": status, "req_id": req_id, "order_id": ""
    }])
    with _ORDERS_LOCK:
        path = orders_log_path(strategy_dir)
        if os.path.exists(path):
            # Align to ORDER_COLS in case the existing file is missing columns (e.g., order_id)
            existing = pd.read_csv(path)
            for c in ORDER_COLS:
                if c not in existing.columns:
                    existing[c] = "" if c != "qty" else 0.0
            # Reorder both and append
            existing = existing[ORDER_COLS]
            row = row[ORDER_COLS]
            out = pd.concat([existing, row], ignore_index=True)
            out.to_csv(path, index=False)
        else:
            row[ORDER_COLS].to_csv(path, index=False)

def load_recent_orders(strategy_dir: str, n: int = 20) -> pd.DataFrame:
    path = orders_log_path(strategy_dir)
    with _ORDERS_LOCK:
        if not os.path.exists(path):
            return pd.DataFrame(columns=ORDER_COLS)
        try:
            df = pd.read_csv(path, usecols=ORDER_COLS)
        except Exception:
            df = pd.read_csv(path)
            for c in ORDER_COLS:
                if c not in df.columns:
                    df[c] = "" if c != "qty" else 0.0
            df = df[ORDER_COLS]
        if "order_id" in df.columns:
            df["order_id"] = df["order_id"].astype("string").fillna("")
    return df.tail(n).iloc[::-1].reset_index(drop=True)

def update_status_by_reqid(strategy_dir: str, req_id: int, new_status: str) -> None:
    path = orders_log_path(strategy_dir)
    if not os.path.exists(path): return
    with _ORDERS_LOCK:
        df = pd.read_csv(path)
        if "req_id" not in df.columns: return
        idxes = df.index[df["req_id"] == req_id]
        if len(idxes) == 0: return
        df.loc[idxes[-1], "status"] = new_status
        df.to_csv(path, index=False)

def set_order_id_by_reqid(strategy_dir: str, req_id: int, order_id: str) -> None:
    """After add-order ACK, persist Kraken order_id onto the matching row."""
    path = orders_log_path(strategy_dir)
    if not os.path.exists(path): return
    with _ORDERS_LOCK:
        df = pd.read_csv(path)
        if "order_id" not in df.columns:
            df["order_id"] = ""
        df["order_id"] = df["order_id"].astype("string").fillna("")
        if "req_id" not in df.columns:
            return
        idxes = df.index[df["req_id"] == req_id]
        if len(idxes) == 0:
            return
        df.loc[idxes[-1], "order_id"] = str(order_id)
        df.to_csv(path, index=False)

def update_status_by_order_id(strategy_dir: str, order_id: str, new_status: str) -> None:
    """Update status using Kraken order_id (e.g., after cancel ACK)."""
    path = orders_log_path(strategy_dir)
    if not os.path.exists(path): return
    with _ORDERS_LOCK:
        df = pd.read_csv(path)
        if "order_id" not in df.columns:
            return
        idxes = df.index[df["order_id"] == str(order_id)]
        if len(idxes) == 0:
            return
        df.loc[idxes[-1], "status"] = new_status
        df.to_csv(path, index=False)

def get_asset_by_order_id(strategy_dir: str, order_id: str) -> str | None:
    """Look up the asset/coin recorded for an order_id -- Hyperliquid cancels need the coin
    (to derive its asset index) alongside the order id, unlike Kraken's order_id-only cancel."""
    path = orders_log_path(strategy_dir)
    if not os.path.exists(path):
        return None
    with _ORDERS_LOCK:
        df = pd.read_csv(path)
        if "order_id" not in df.columns or "asset" not in df.columns:
            return None
        rows = df[df["order_id"].astype(str) == str(order_id)]
        if rows.empty:
            return None
        return str(rows.iloc[-1]["asset"])


def update_last_order_status(strategy_dir: str, symbol: str, new_status: str) -> None:
    path = orders_log_path(strategy_dir)
    if not os.path.exists(path): return
    with _ORDERS_LOCK:
        df = pd.read_csv(path)
        if "status" not in df.columns: return
        mask = (df.get("asset") == symbol) & (df.get("status").isin(["pending","submitted"]))
        idxes = df[mask].index
        if len(idxes) == 0:
            idxes = df[df.get("asset") == symbol].index
            if len(idxes) == 0: return
        df.loc[idxes[-1], "status"] = new_status
        df.to_csv(path, index=False)
