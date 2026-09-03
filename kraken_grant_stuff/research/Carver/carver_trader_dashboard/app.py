import logging
import os
from flask import Flask
from .config import SETTINGS, get_settings
from .services.ws_manager import WSManager
from .services.hl_ws_manager import HLWSManager
from .routes import init_routes


class _SuppressTradePlanPollLog(logging.Filter):
    def filter(self, record):
        return "trade_plan.json" not in record.getMessage()


def create_app(strategy_name: str, venue: str = "kraken", get_trade_plan=None,
               hl_wallet=None, hl_mainnet: bool = False, hl_ws_mgr=None, on_fill=None, on_funding=None):
    """
    venue: "kraken" (default, unchanged behavior) or "hyperliquid".
    get_trade_plan: optional zero-arg callable returning dict[str, dict] of
    base -> {side, qty, limit_price}, used to prefill the order form.
    hl_ws_mgr: pass an already-constructed HLWSManager to share its live account/positions state
    with code outside the dashboard (carver_trader.py's CLI loop reads the same object). If
    omitted, one is built here from hl_wallet/hl_mainnet -- fine for using the dashboard
    standalone, but carver_trader.py always injects its own so both surfaces see one connection.
    on_fill/on_funding: forwarded to routes.py for venue="hyperliquid" (see its docstring).
    """
    logging.getLogger("werkzeug").addFilter(_SuppressTradePlanPollLog())

    app = Flask(__name__)
    app.secret_key = get_settings(venue).secret_key
    strategy_dir = os.path.join("strategies", strategy_name)
    os.makedirs(strategy_dir, exist_ok=True)
    app.config["STRATEGY_DIR"] = strategy_dir

    if venue == "kraken":
        ws_mgr = WSManager()
    elif venue == "hyperliquid":
        if hl_ws_mgr is not None:
            ws_mgr = hl_ws_mgr
        elif hl_wallet is not None:
            ws_mgr = HLWSManager(hl_wallet, mainnet=hl_mainnet)
        else:
            raise ValueError("hl_ws_mgr or hl_wallet is required for venue='hyperliquid'")
    else:
        raise ValueError(f"Unknown venue: {venue!r}")
    ws_mgr.start()  # start WS thread immediately (no-op if already started, e.g. hl_ws_mgr injected pre-started)
    init_routes(app, ws_mgr=ws_mgr, strategy_name=strategy_name, venue=venue,
                get_trade_plan=get_trade_plan, on_fill=on_fill, on_funding=on_funding)
    return app
