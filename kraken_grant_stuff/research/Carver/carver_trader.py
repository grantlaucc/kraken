# kraken_ws_min.py
import json
import signal
import sys
import os
import argparse
import websocket
import threading
import research.Carver.carver_trader_helper_old as helper
import threading
import webbrowser
from research.Carver.carver_trader_dashboard.app import create_app
import kraken_token  # must provide get_websocket_token()
from kraken_balances import KrakenBalances
import research.ohlc_data as ohlc_data

EXCLUDE_BALANCES = {"BTC":0.01046193, "ETH":0.22, "ETHW":0.22}
_FX_CACHE = {"USD/CAD": {"date": None, "rate": None}}

WS_URL = "wss://ws-auth.kraken.com/v2"

BAL_LOCK = threading.Lock()

STRATEGY_DIR = None
ws = None              # so we can close on Ctrl+C
myKrakenBalances = None

# --- target positions loaded from CSV ---
TARGET_POS = {}         # dict[str, float]  normalized asset -> target units
TARGET_DATE_STR = ""    # index of last row (printed once)

# --- live balances from WS ---
LIVE_POS = {}           # dict[str, float]  normalized asset -> current units


def start_dashboard_thread(strategy_name: str, port: int = 5055, no_open: bool = False) -> threading.Thread:
    app = create_app(strategy_name)

    def _run():
        # Important: use_reloader=False so Flask doesn't spawn a second process/thread
        app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)

    t = threading.Thread(target=_run, daemon=True, name="DashboardThread")
    t.start()

    if not no_open:
        try:
            webbrowser.open(f"http://127.0.0.1:{port}/", new=2)
        except Exception:
            pass

    return t

def cli_loop():  # NEW: simple stdin command loop
    print("\n[commands] flash | plan | bal | quit\n", flush=True)
    while True:
        try:
            cmd = input().strip().lower()
        except EOFError:
            break

        if cmd in ("flash", "f"):
            live = helper.get_live_positions_from_ws(BAL_LOCK, myKrakenBalances, _FX_CACHE, exclude=EXCLUDE_BALANCES)
            if not live:
                print("No live balances yet; try after first snapshot/update.")
                continue
            path = os.path.join(STRATEGY_DIR, "live_positions.csv")
            helper.write_live_positions_to_csv(path, live)
            print(f">> Flashed positions to {path}")
        elif cmd in ("plan", "p"):
            live = helper.get_live_positions_from_ws(BAL_LOCK, myKrakenBalances, _FX_CACHE, exclude=EXCLUDE_BALANCES)
            plan = helper.compute_trade_plan(TARGET_POS, live)
            helper.print_trade_plan(plan, TARGET_POS, live, TARGET_DATE_STR)
        elif cmd in ("bal", "b"):
            with BAL_LOCK:
                if myKrakenBalances:
                    helper.print_balances_with_total(myKrakenBalances, exclude_balances=EXCLUDE_BALANCES, fx_cache=_FX_CACHE)
                else:
                    print("Balances not available yet.")
        elif cmd in ("quit", "q", "exit"):
            if ws:
                ws.close()
            break
        elif cmd in ("help", "h", "?"):
            print("[commands] flash | plan | bal | quit")
        else:
            if cmd:
                print(f"Unknown command: {cmd}. Type 'help'.")


def sub_balances(token, snapshot=True):
    return {"method": "subscribe", "params": {"channel": "balances", "snapshot": snapshot, "token": token}}

def sub_executions(token):
    return {
        "method": "subscribe",
        "params": {"channel": "executions", "token": token, "snap_orders": True, "snap_trades": True},
    }


def on_open(wsapp):
    try:
        token = kraken_token.get_websocket_token()
        wsapp.send(json.dumps(sub_balances(token)))
        wsapp.send(json.dumps(sub_executions(token)))
        print(">> subscribed to balances & executions")
    except Exception as e:
        print("subscribe failed:", e)
        wsapp.close()


def on_message(wsapp, message):
    global myKrakenBalances
    try:
        msg = json.loads(message)
    except Exception:
        print("<< raw:", message[:200])
        return

    # ignore heartbeats, status updates, and subscribe acks
    if msg.get("channel") == "heartbeat":
        return
    if msg.get("channel") == "status":
        return
    if msg.get("method") == "subscribe":
        return

    ch = msg.get("channel")
    typ = msg.get("type") or msg.get("method")

    if ch == "balances":
        # Ensure we have a balances object
        if myKrakenBalances is None:
            myKrakenBalances = KrakenBalances()

        if typ == "snapshot":
            with BAL_LOCK:
                myKrakenBalances.snapshot_balances(msg.get("data"))
            print("\n=== Balances (snapshot) ===")
            helper.print_balances_with_total(myKrakenBalances, exclude_balances=EXCLUDE_BALANCES, fx_cache=_FX_CACHE)

            live = helper.get_live_positions_from_ws(BAL_LOCK, myKrakenBalances, _FX_CACHE, exclude=EXCLUDE_BALANCES)
            plan = helper.compute_trade_plan(TARGET_POS, live)
            helper.print_trade_plan(plan, TARGET_POS, live, TARGET_DATE_STR)

        elif typ == "update":
            with BAL_LOCK:
                myKrakenBalances.update_balances(msg.get("data"))
            print("\n=== Balances (update) ===")
            helper.print_balances_with_total(myKrakenBalances, exclude_balances=EXCLUDE_BALANCES, fx_cache=_FX_CACHE)


            live = helper.get_live_positions_from_ws(BAL_LOCK, myKrakenBalances, _FX_CACHE, exclude=EXCLUDE_BALANCES)
            plan = helper.compute_trade_plan(TARGET_POS, live)
            helper.print_trade_plan(plan, TARGET_POS, live, TARGET_DATE_STR)
        else:
            print("<< balances:", json.dumps(msg, ensure_ascii=False))

    elif ch == "executions":
        data = msg.get("data", {})
        orders = len(data.get("orders", [])) if isinstance(data, dict) else 0
        trades = len(data.get("trades", [])) if isinstance(data, dict) else 0
        seq = msg.get("sequence")

        if typ == "snapshot":
            print(f"<< executions snapshot: {orders} orders, {trades} trades")
        elif typ == "update":
            print(f"<< executions update (seq {seq}): {orders} orders, {trades} trades")
        else:
            print("<< executions:", json.dumps(msg, ensure_ascii=False))

    else:
        # subscription status / errors, etc.
        print("<<", json.dumps(msg, ensure_ascii=False))


def on_error(wsapp, error):
    print("!! error:", error)

def on_close(wsapp, code, msg):
    print(f"xx closed: code={code}, msg={msg}")

def _sigint(sig, frame):
    global ws
    if ws:
        ws.close()
    sys.exit(0)


def main():
    global ws, STRATEGY_DIR, TARGET_DATE_STR, TARGET_POS
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--strategy-name",
        required=True,
        help="Strategy name; reads targets from strategies/<name>/position_file.csv and writes live_positions.csv there."
    )
    parser.add_argument("--dashboard-port", type=int, default=5055)
    parser.add_argument("--no-open", action="store_true", help="Do not auto-open the dashboard in a browser")
    args = parser.parse_args()

    STRATEGY_DIR = os.path.join("strategies", args.strategy_name)

    if not os.path.isdir(STRATEGY_DIR):
        raise FileNotFoundError(f"Strategy folder not found: {STRATEGY_DIR}")
    
    target_position_file = os.path.join(STRATEGY_DIR, "position_file.csv")
    if not os.path.exists(target_position_file):
        raise FileNotFoundError(f"Missing target file: {target_position_file}")
      
    TARGET_DATE_STR, TARGET_POS = helper.load_target_positions(target_position_file)
    signal.signal(signal.SIGINT, _sigint)

    # Start the dashboard (non-blocking)
    start_dashboard_thread(args.strategy_name, port=args.dashboard_port, no_open=args.no_open)

    # Start CLI listener
    threading.Thread(target=cli_loop, daemon=True).start()

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=30)


if __name__ == "__main__":
    main()

