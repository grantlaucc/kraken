# carver_trader_dashboard.py
import os
import sys
import argparse
import webbrowser
import threading
import json
import time
from datetime import datetime, timezone
from secrets import randbits

import pandas as pd
from flask import Flask, request, redirect, url_for, render_template_string, flash

# === Kraken WS + order imports ===
import websocket
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import kraken_token
import kraken_order

ORDER_COLS = ["timestamp", "asset", "side", "qty", "order_type", "limit_price", "status", "req_id"]
ALLOWED_QUOTES = ["USD", "USDC", "CAD"]
WS_URL = "wss://ws-auth.kraken.com/v2"

ORDERMIN_CSV_PATH = os.environ.get(
    "KRAKEN_ORDERMIN_CSV",
    "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/kraken_usd_pairs_ordermin.csv"
)
APP_STRATEGY_DIR = None

# --- WS globals (protected by WS_LOCK) ---
WS_LOCK = threading.Lock()
ORDERS_LOCK = threading.Lock() 
WS_APP = None               # websocket.WebSocketApp
WS_THREAD = None            # Thread running run_forever
API_TOKEN = None            # refreshed in on_open
WS_CONNECTED = threading.Event()  # set when socket is open and token fetched

TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Trader Dashboard — {{ strategy_name }}</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin-bottom: 0; }
    small { color: #666; }
    form, table { margin-top: 16px; }
    input, select { padding: 6px 8px; font-size: 14px; }
    button { padding: 8px 12px; font-size: 14px; cursor: pointer; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #eee; padding: 8px; text-align: left; }
    .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .card { border: 1px solid #eee; padding: 16px; border-radius: 8px; }
    .right { text-align: right; }
    .muted { color: #888; }
    .ok { color: #0a7; }
    .warn { color: #a70; }
    .err { color: #c33; }
  </style>
</head>
<body>
  <h1>Trader Dashboard</h1>
  <small>Strategy: <b>{{ strategy_name }}</b> &nbsp;•&nbsp; Dir: <code>{{ strategy_dir }}</code></small>

  <div class="card">
    <h3 style="margin-top:0">Submit Order</h3>
    <form method="post" action="{{ url_for('submit_order') }}">
      <div class="row">
        {% if bases %}
          <label>Base
            <select name="base">
              {% for b in bases %}
                <option value="{{ b }}">{{ b }}</option>
              {% endfor %}
            </select>
          </label>
        {% else %}
          <label>Base <input name="base" placeholder="e.g., BTC" required></label>
        {% endif %}

        <label>Quote
          <select name="quote">
            {% for q in quotes %}
              <option value="{{ q }}">{{ q }}</option>
            {% endfor %}
          </select>
        </label>

        <label>Side
          <select name="side">
            <option value="BUY">BUY</option>
            <option value="SELL">SELL</option>
          </select>
        </label>

        <label>Type
          <select name="order_type">
            <option value="market">market</option>
            <option value="limit">limit</option>
          </select>
        </label>

        <label>Limit Px
          <input name="limit_price" type="number" step="any" placeholder="only for limit">
        </label>

        <label title="If checked, Kraken will only validate the order without executing it.">
          <input type="checkbox" name="validate" value="true" checked> Validate
        </label>

        <label>Quantity <input name="qty" type="number" step="any" min="0" placeholder="e.g., 0.05" required></label>
        <button type="submit">Submit</button>
      </div>
      {% if not ws_ready %}
        <p class="warn">WebSocket not fully ready yet (connecting/token)… You can submit, but it may fail until connected.</p>
      {% endif %}
    </form>
    {% with messages = get_flashed_messages() %}
      {% if messages %}
        <p class="ok">{{ messages[-1] }}</p>
      {% endif %}
    {% endwith %}
    <p class="muted">Orders are also logged to <code>orders_submitted.csv</code> in your strategy folder.</p>
  </div>

  <div class="card" style="margin-top:16px">
    <h3 style="margin-top:0">Recent Orders</h3>
    {% if has_orders %}
      <table>
        <thead>
          <tr><th>Time (UTC)</th><th>Asset</th><th>Side</th><th>Type</th><th class="right">Qty</th><th>Limit</th><th>Status</th></tr>
        </thead>
        <tbody>
          {% for r in orders %}
            <tr>
              <td>{{ r['timestamp'] }}</td>
              <td>{{ r['asset'] }}</td>
              <td>{{ r['side'] }}</td>
              <td>{{ r.get('order_type','') }}</td>
              <td class="right">{{ "{:.8g}".format(r['qty']) }}</td>
              <td class="right">{{ r.get('limit_price','') }}</td>
              <td>{{ r.get('status', '') }}</td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    {% else %}
      <p class="muted">No orders yet.</p>
    {% endif %}
  </div>
</body>
</html>
"""

def update_order_status_by_reqid(strategy_dir: str, req_id: int, new_status: str) -> None:
    path = orders_log_path(strategy_dir)
    if not os.path.exists(path):
        return
    with ORDERS_LOCK:
        df = pd.read_csv(path)
        if "req_id" not in df.columns:
            return
        idxes = df.index[df["req_id"] == req_id]
        if len(idxes) == 0:
            return
        df.loc[idxes[-1], "status"] = new_status
        df.to_csv(path, index=False)

def _extract_base(col: str) -> str | None:
    s = col.strip().upper()
    for sep in ("/", "_", "-"):
        parts = [p for p in s.split(sep) if p]
        if len(parts) >= 2:
            for p in parts[1:]:
                p_alpha = "".join(ch for ch in p if ch.isalpha())
                if p_alpha in ALLOWED_QUOTES:
                    return parts[0]
    for q in sorted(ALLOWED_QUOTES, key=len, reverse=True):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)]
    if s.isalpha() and s not in {"CASH", "PORTFOLIO", "VALUE", "NOTIONAL"}:
        return s
    return None

def load_bases_from_position_file(strategy_dir: str) -> list[str]:
    path = os.path.join(strategy_dir, "position_file.csv")
    try:
        df = pd.read_csv(path, index_col=0)
        drop_cols = {"CASH", "PORTFOLIO VALUE", "PORTFOLIO_VALUE", "NOTIONAL", "PORTFOLIO", "VALUE"}
        cols = [c for c in df.columns if str(c).strip().upper() not in drop_cols]
        bases = {
            b for c in cols
            for b in [_extract_base(str(c))]
            if b
        }
        return sorted(bases)
    except Exception:
        return []

def orders_log_path(strategy_dir: str) -> str:
    return os.path.join(strategy_dir, "orders_submitted.csv")

def record_order(strategy_dir: str, asset: str, side: str, qty: float, order_type: str, limit_price, status: str = "queued", req_id: int | None = None) -> None:
    path = orders_log_path(strategy_dir)
    os.makedirs(strategy_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = pd.DataFrame([{
        "timestamp": ts, "asset": asset, "side": side,
        "qty": float(qty), "order_type": order_type,
        "limit_price": ("" if limit_price is None else float(limit_price)),
        "status": status, "req_id": req_id
    }])
    with ORDERS_LOCK:
        if os.path.exists(path):
            row.to_csv(path, mode="a", header=False, index=False)
        else:
            row.to_csv(path, index=False)

def load_recent_orders(strategy_dir: str, n: int = 20) -> pd.DataFrame:
    #TODO make this from the KrakenAPI not the orders log path
    path = orders_log_path(strategy_dir)
    cols = ["timestamp", "asset", "side", "qty", "order_type", "limit_price", "status", "req_id"]
    with ORDERS_LOCK:
        if not os.path.exists(path):
            return pd.DataFrame(columns=cols)
        try:
            df = pd.read_csv(path, usecols=cols)
        except Exception:
            # Legacy/misaligned CSV fallback
            df = pd.read_csv(path)
            for c in cols:
                if c not in df.columns:
                    df[c] = "" if c != "qty" else 0.0
            df = df[cols]
    return df.tail(n).iloc[::-1].reset_index(drop=True)

def load_ordermin_map(csv_path: str) -> dict[str, float]:
    """
    Read a CSV with columns: Ticker, Base, OrderMin
    and return {BASE: min_order_size}. If multiple rows per base, take the minimum.
    """
    try:
        df = pd.read_csv(csv_path)
        if "Base" not in df.columns or "OrderMin" not in df.columns:
            raise ValueError("CSV must contain 'Base' and 'OrderMin' columns")
        # normalize base to upper-case and numeric OrderMin
        df["Base"] = df["Base"].astype(str).str.upper().str.strip()
        df["OrderMin"] = pd.to_numeric(df["OrderMin"], errors="coerce")
        df = df.dropna(subset=["Base", "OrderMin"])
        # If duplicates, pick the smallest
        ser = df.groupby("Base")["OrderMin"].min()
        return ser.to_dict()
    except Exception as e:
        print(f"[ordermin] failed to load '{csv_path}': {e}")
        return {}
    
def update_last_order_status(strategy_dir: str, symbol: str, new_status: str) -> None:
    """
    Update the most recent row for this symbol whose status is 'pending' (or 'submitted')
    to the provided new_status. Best-effort; silently returns if file not present.
    """
    path = orders_log_path(strategy_dir)
    if not os.path.exists(path):
        return
    with ORDERS_LOCK:
        df = pd.read_csv(path)
        if "status" not in df.columns:
            return
        # find last pending-like row for this symbol
        mask = (df.get("asset") == symbol) & (df.get("status").isin(["pending", "submitted"]))
        idxes = df[mask].index
        if len(idxes) == 0:
            # fallback: update the last row for this symbol
            idxes = df[df.get("asset") == symbol].index
            if len(idxes) == 0:
                return
        idx = idxes[-1]
        df.loc[idx, "status"] = new_status
        df.to_csv(path, index=False)

# =========================
# Kraken WS connection bits
# =========================
def _ws_on_open(wsapp):
    global API_TOKEN
    try:
        # Fetch a fresh token when socket opens
        API_TOKEN = kraken_token.get_websocket_token()
        # Optional: subscribe to executions to see order acks in console
        wsapp.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "executions", "token": API_TOKEN, "snap_orders": True, "snap_trades": True}
        }))
        WS_CONNECTED.set()
        print(">> WS connected & token acquired")
    except Exception as e:
        print("!! token fetch failed:", e)

def _ws_on_message(wsapp, message):
    try:
        msg = json.loads(message)
    except Exception:
        print("<< raw:", message[:160])
        return
    ch = msg.get("channel")
    typ = msg.get("type")
    if ch == "executions":
        if typ == "snapshot":
            print("<< executions snapshot")
        elif typ == "update":
            seq = msg.get("sequence")
            data = msg.get("data", {})
            print(f"<< executions update (seq {seq}) orders={len(data.get('orders',[]))} trades={len(data.get('trades',[]))}")
    elif msg.get("method") == "add_order":
        ok  = msg.get("success")
        res = msg.get("result") or {}
        err = msg.get("error") or res.get("error")
        req = msg.get("req_id")
        sym = msg.get("symbol") or (res.get("descr") or {}).get("pair") or ""
        print(f"<< add_order ack: success={ok}, order_id={res.get('order_id')}, req_id={req}, error={err}")
        if req is not None:
            update_order_status_by_reqid(APP_STRATEGY_DIR, int(req), "accepted" if ok else f"error: {err}")
        elif sym:
            # fallback if req_id missing for any reason
            update_last_order_status(APP_STRATEGY_DIR, sym, "accepted" if ok else f"error: {err}")
        return


def _ws_on_error(wsapp, error):
    print("!! WS error:", error)

def _ws_on_close(wsapp, code, msg):
    print(f"xx WS closed: code={code}, msg={msg}")
    WS_CONNECTED.clear()
    # light auto-reconnect loop
    def _reconnector():
        time.sleep(2.0)
        _start_ws_thread()
    threading.Thread(target=_reconnector, daemon=True).start()

def _start_ws_thread():
    """Idempotent: start WS thread if not already running."""
    global WS_APP, WS_THREAD
    with WS_LOCK:
        if WS_THREAD and WS_THREAD.is_alive():
            return
        WS_APP = websocket.WebSocketApp(
            WS_URL,
            on_open=_ws_on_open,
            on_message=_ws_on_message,
            on_error=_ws_on_error,
            on_close=_ws_on_close,
        )
        def _run():
            # Note: run_forever blocks; keep in daemon thread
            WS_APP.run_forever(ping_interval=30)
        WS_THREAD = threading.Thread(target=_run, daemon=True, name="KrakenWS")
        WS_THREAD.start()

def _place_order(symbol: str, side: str, order_type: str, qty: float, limit_price, validate: bool, req_id: int | None = None):
    """Thread-safe wrapper around kraken_order.order using the shared WS + token."""
    if not WS_CONNECTED.is_set():
        raise RuntimeError("Kraken WS not connected yet (or token unavailable).")
    with WS_LOCK:
        # kraken_order expects 'buy'/'sell' and 'market'/'limit'
        kraken_order.order(
            ws=WS_APP,
            order_type=order_type.lower(),
            side=side.lower(),
            qty=qty,
            limit_price=limit_price,
            symbol=symbol,
            token=API_TOKEN,
            validate=validate,
            req_id=req_id,
        )

# ==============
# Flask app bits
# ==============
def create_app(strategy_name: str):
    app = Flask(__name__)
    app.secret_key = "dev-secret"

    strategy_dir = os.path.join("strategies", strategy_name)
    global APP_STRATEGY_DIR
    APP_STRATEGY_DIR = strategy_dir
    os.makedirs(strategy_dir, exist_ok=True)
    bases = load_bases_from_position_file(strategy_dir)

    ordmin_map = load_ordermin_map(ORDERMIN_CSV_PATH)

    # Start WS at app creation
    _start_ws_thread()

    @app.route("/", methods=["GET"])
    def index():
        orders_df = load_recent_orders(strategy_dir)
        has_orders = not orders_df.empty
        orders = orders_df.to_dict("records")
        return render_template_string(
            TEMPLATE,
            strategy_name=strategy_name,
            strategy_dir=strategy_dir,
            bases=bases,
            quotes=ALLOWED_QUOTES,
            has_orders=has_orders,
            orders=orders,
            ws_ready=WS_CONNECTED.is_set(),
            ordmin_count=len(ordmin_map),
        )

    @app.route("/submit", methods=["POST"])
    def submit_order():
        base  = (request.form.get("base") or "").strip().upper()
        quote = (request.form.get("quote") or "").strip().upper()
        side  = (request.form.get("side") or "BUY").strip().upper()
        otype = (request.form.get("order_type") or "market").strip().lower()
        qty_s = (request.form.get("qty") or "").strip()
        limit_s = (request.form.get("limit_price") or "").strip()
        validate = (request.form.get("validate") == "true")

        # Parse
        try:
            qty = float(qty_s)
        except Exception:
            qty = None
        limit_price = None
        if otype == "limit" and limit_s:
            try:
                limit_price = float(limit_s)
            except Exception:
                flash("Invalid limit price.")
                return redirect(url_for("index"))

        # Validate
        if not base or quote not in ALLOWED_QUOTES:
            flash("Invalid base/quote.")
            return redirect(url_for("index"))
        if side not in {"BUY", "SELL"} or qty is None or qty <= 0:
            flash("Invalid side/quantity.")
            return redirect(url_for("index"))
        if otype not in {"market", "limit"}:
            flash("Invalid order type.")
            return redirect(url_for("index"))
        if otype == "limit" and limit_price is None:
            flash("Limit price required for limit orders.")
            return redirect(url_for("index"))
        
        min_qty = ordmin_map.get(base)
        if min_qty is not None and qty < float(min_qty):
            flash(f"Qty {qty} is below minimum {min_qty} for base {base}.")
            return redirect(url_for("index"))

        symbol = f"{base}/{quote}"

        # Place order via Kraken WS
        try:
            req_id = randbits(31)
            _place_order(symbol, side, otype, qty, limit_price, validate, req_id=req_id)
            status = "pending"
            record_order(strategy_dir, symbol, side, qty, otype, limit_price, status=status, req_id=req_id)
            flash(f"{status.upper()}: {side} {qty:.8g} {symbol}" + (f" @ {limit_price}" if limit_price else ""))
        except Exception as e:
            record_order(strategy_dir, symbol, side, qty, otype, limit_price, status=f"error: {e}", req_id=req_id if 'req_id' in locals() else None)
            flash(f"Order error: {e}")

        return redirect(url_for("index"))

    return app

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-name", required=True, help="Strategy folder under ./strategies/")
    parser.add_argument("--port", type=int, default=5055)
    parser.add_argument("--no-open", action="store_true", help="Do not auto-open the browser")
    args = parser.parse_args()

    app = create_app(args.strategy_name)
    url = f"http://127.0.0.1:{args.port}/"
    if not args.no_open:
        webbrowser.open(url, new=2)
    # Important: disable reloader so WS thread doesn't duplicate
    app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
