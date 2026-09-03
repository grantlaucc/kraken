# kraken_ws_min.py
import json
import signal
import sys
import os
import time
import argparse
import websocket
import threading
import research.Carver.carver_trader_helper_old as helper
import hyperliquid_trader_helper as hl_helper
import threading
import webbrowser
from dotenv import load_dotenv
from eth_account import Account
from research.Carver.carver_trader_dashboard.app import create_app
from research.Carver.carver_trader_dashboard.services.hl_ws_manager import HLWSManager
import kraken_token  # must provide get_websocket_token()
from kraken_balances import KrakenBalances
import research.ohlc_data as ohlc_data

EXCLUDE_BALANCES = {"BTC":0.01046193, "ETH":0.22, "ETHW":0.22}
_FX_CACHE = {"USD/CAD": {"date": None, "rate": None}}

WS_URL = "wss://ws-auth.kraken.com/v2"
HL_ENV_PATH = "/Users/grantlau/Documents/QuantStuff/kraken/.env"

BAL_LOCK = threading.Lock()

STRATEGY_DIR = None
VENUE = "kraken"        # set from --venue in main(); drives every branch below
ws = None                # Kraken raw WS app (kraken venue only) -- so we can close on Ctrl+C
hl_ws_mgr = None          # HLWSManager (hyperliquid venue only) -- owns its own WS + live account state
myKrakenBalances = None

# --- target positions loaded from CSV ---
TARGET_POS = {}         # dict[str, float]  normalized asset -> target units
TARGET_DATE_STR = ""    # index of last row (printed once)
TRADING_CAPITAL = None  # hyperliquid venue only -- base capital position_file.csv was sized against

# --- live balances from WS ---
LIVE_POS = {}           # dict[str, float]  normalized asset -> current units

# --- trade plan prefill data, shared with the dashboard ---
TRADE_PLAN_LOCK = threading.Lock()
TRADE_PLAN_DETAILS = {}  # dict[str, dict]  base -> {side, qty, limit_price}


def refresh_trade_plan_details(live_pos: dict) -> None:
    """Recompute the trade plan and stash prefill details for the dashboard to read."""
    if VENUE == "hyperliquid":
        scaled_target = hl_helper.scale_target_positions(TARGET_POS, hl_ws_mgr.account, TRADING_CAPITAL)
        plan = hl_helper.compute_trade_plan(scaled_target, live_pos)
        details = hl_helper.get_trade_plan_details(plan)
    else:
        plan = helper.compute_trade_plan(TARGET_POS, live_pos)
        details = helper.get_trade_plan_details(plan)
    with TRADE_PLAN_LOCK:
        TRADE_PLAN_DETAILS.clear()
        TRADE_PLAN_DETAILS.update(details)


def get_trade_plan_snapshot() -> dict:
    with TRADE_PLAN_LOCK:
        return dict(TRADE_PLAN_DETAILS)


def start_dashboard_thread(strategy_name: str, port: int = 5055, no_open: bool = False) -> threading.Thread:
    if VENUE == "hyperliquid":
        app = create_app(strategy_name, venue="hyperliquid", get_trade_plan=get_trade_plan_snapshot, hl_ws_mgr=hl_ws_mgr)
    else:
        app = create_app(strategy_name, get_trade_plan=get_trade_plan_snapshot)

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
            if VENUE == "hyperliquid":
                print("Not applicable for hyperliquid -- there's no notional file for this venue yet; "
                      "the trade plan already scales live off account value on every refresh instead.")
                continue
            live = helper.get_live_positions_from_ws(BAL_LOCK, myKrakenBalances, _FX_CACHE, exclude=EXCLUDE_BALANCES)
            if not live:
                print("No live balances yet; try after first snapshot/update.")
                continue
            path = os.path.join(STRATEGY_DIR, "live_positions.csv")
            helper.write_live_positions_to_csv(path, live)
            print(f">> Flashed positions to {path}")
        elif cmd in ("plan", "p"):
            if VENUE == "hyperliquid":
                live = hl_helper.get_live_positions_from_hl(hl_ws_mgr.account)
                scaled_target = hl_helper.scale_target_positions(TARGET_POS, hl_ws_mgr.account, TRADING_CAPITAL)
                plan = hl_helper.compute_trade_plan(scaled_target, live)
                hl_helper.print_trade_plan(plan, scaled_target, live, TARGET_DATE_STR, hl_ws_mgr.account)
                refresh_trade_plan_details(live)
            else:
                live = helper.get_live_positions_from_ws(BAL_LOCK, myKrakenBalances, _FX_CACHE, exclude=EXCLUDE_BALANCES)
                plan = helper.compute_trade_plan(TARGET_POS, live)
                helper.print_trade_plan(plan, TARGET_POS, live, TARGET_DATE_STR)
                refresh_trade_plan_details(live)
        elif cmd in ("bal", "b"):
            if VENUE == "hyperliquid":
                hl_helper.print_positions(hl_ws_mgr.account)
            else:
                with BAL_LOCK:
                    if myKrakenBalances:
                        helper.print_balances_with_total(myKrakenBalances, exclude_balances=EXCLUDE_BALANCES, fx_cache=_FX_CACHE)
                    else:
                        print("Balances not available yet.")
        elif cmd in ("quit", "q", "exit"):
            if VENUE == "hyperliquid" and hl_ws_mgr and hl_ws_mgr.ws_app:
                hl_ws_mgr.ws_app.close()
            elif ws:
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
            refresh_trade_plan_details(live)

        elif typ == "update":
            with BAL_LOCK:
                myKrakenBalances.update_balances(msg.get("data"))
            print("\n=== Balances (update) ===")
            helper.print_balances_with_total(myKrakenBalances, exclude_balances=EXCLUDE_BALANCES, fx_cache=_FX_CACHE)


            live = helper.get_live_positions_from_ws(BAL_LOCK, myKrakenBalances, _FX_CACHE, exclude=EXCLUDE_BALANCES)
            plan = helper.compute_trade_plan(TARGET_POS, live)
            helper.print_trade_plan(plan, TARGET_POS, live, TARGET_DATE_STR)
            refresh_trade_plan_details(live)
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
    if VENUE == "hyperliquid" and hl_ws_mgr and hl_ws_mgr.ws_app:
        hl_ws_mgr.ws_app.close()
    elif ws:
        ws.close()
    sys.exit(0)


def _confirm_hl_mainnet(address: str) -> None:
    print(f"\n!! MAINNET selected for Hyperliquid -- wallet {address} -- this trades REAL FUNDS.")
    if input("Type 'yes' to continue: ").strip().lower() != "yes":
        sys.exit("Aborted.")


def _run_hyperliquid(args) -> None:
    global hl_ws_mgr, TARGET_DATE_STR, TARGET_POS, TRADING_CAPITAL

    target_position_file = os.path.join(STRATEGY_DIR, "position_file.csv")
    if not os.path.exists(target_position_file):
        raise FileNotFoundError(f"Missing target file: {target_position_file}")

    load_dotenv(HL_ENV_PATH)
    private_key = os.getenv("HYPERLIQUID_PRIVATE_KEY")
    if not private_key:
        sys.exit(f"Set HYPERLIQUID_PRIVATE_KEY in {HL_ENV_PATH} first (see place_test_order.py's docstring "
                  "for setup notes -- prefer an API/agent wallet key, not your main wallet's).")
    wallet = Account.from_key(private_key)
    if args.mainnet:
        _confirm_hl_mainnet(wallet.address)
    else:
        print(f"[testnet] Hyperliquid wallet {wallet.address} -- no real funds at risk. Pass --mainnet to trade live.")

    TRADING_CAPITAL = hl_helper.load_strategy_trading_capital(STRATEGY_DIR)
    TARGET_DATE_STR, TARGET_POS = hl_helper.load_target_positions(target_position_file)
    signal.signal(signal.SIGINT, _sigint)

    hl_ws_mgr = HLWSManager(wallet, mainnet=args.mainnet)
    hl_ws_mgr.on_account_update = lambda: refresh_trade_plan_details(hl_helper.get_live_positions_from_hl(hl_ws_mgr.account))
    hl_ws_mgr.start()
    if not hl_ws_mgr.connected.wait(timeout=15):
        print("!! Hyperliquid WS did not connect within 15s -- check network/credentials, continuing anyway.")

    # Start the dashboard (non-blocking) -- shares this same hl_ws_mgr instance
    start_dashboard_thread(args.strategy_name, port=args.dashboard_port, no_open=args.no_open)

    # Start CLI listener
    threading.Thread(target=cli_loop, daemon=True).start()

    # Dashboard + Hyperliquid WS both run on daemon threads; keep the main thread alive for Ctrl+C.
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        _sigint(None, None)


def main():
    global ws, STRATEGY_DIR, TARGET_DATE_STR, TARGET_POS, VENUE
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--strategy-name",
        required=True,
        help="Strategy name; reads targets from strategies/<name>/position_file.csv and writes live_positions.csv there."
    )
    parser.add_argument("--venue", choices=["kraken", "hyperliquid"], default="kraken",
                        help="Which venue this process trades. One process per venue -- run a second "
                             "process/port for the other venue if you want both live at once.")
    parser.add_argument("--mainnet", action="store_true",
                        help="Hyperliquid only: trade real funds instead of testnet (default). Requires typed confirmation at startup.")
    parser.add_argument("--dashboard-port", type=int, default=5055)
    parser.add_argument("--no-open", action="store_true", help="Do not auto-open the dashboard in a browser")
    args = parser.parse_args()

    VENUE = args.venue
    STRATEGY_DIR = os.path.join("strategies", args.strategy_name)

    if not os.path.isdir(STRATEGY_DIR):
        raise FileNotFoundError(f"Strategy folder not found: {STRATEGY_DIR}")

    if VENUE == "hyperliquid":
        _run_hyperliquid(args)
        return

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

