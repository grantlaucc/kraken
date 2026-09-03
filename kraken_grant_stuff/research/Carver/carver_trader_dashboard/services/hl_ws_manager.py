import json
import threading
import time
from typing import Callable

import websocket
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL

import hyperliquid_balances


def _ws_url_for(base_url: str) -> str:
    return base_url.replace("https://", "wss://") + "/ws"


class HLWSManager:
    """
    Hyperliquid analogue of ws_manager.py's WSManager: same connected Event / ws_lock /
    reconnect-on-close shape, but subscribes to the four user-scoped channels that cover
    everything the Kraken WSManager gets from "executions" -- positions/margin
    (clearinghouseState), order acks/status (orderUpdates), fills (userFills), and funding
    payments (userFundings) -- confirmed live against testnet (see hyperliquid_balances.py's
    docstring for what's been directly observed vs. taken from docs).

    Holds `wallet` (the signing account) and `base_url` (mainnet/testnet REST base) so
    order_exec.py can read them off this object the same way it reads `token` off the Kraken
    WSManager.
    """

    def __init__(self, wallet, mainnet: bool = False):
        self.wallet = wallet
        self.address = wallet.address
        self.mainnet = mainnet
        self.base_url = MAINNET_API_URL if mainnet else TESTNET_API_URL
        self.url = _ws_url_for(self.base_url)

        self.ws_app: websocket.WebSocketApp | None = None
        self.ws_thread: threading.Thread | None = None
        self.ws_lock = threading.Lock()
        self.connected = threading.Event()

        self.account = hyperliquid_balances.HyperliquidAccount()

        # external hooks -- each called with the raw per-item dict (a single fill / funding
        # record / order-update entry, not the whole websocket message)
        self.on_order_update: Callable[[dict], None] | None = None
        self.on_fill: Callable[[dict], None] | None = None
        self.on_funding: Callable[[dict], None] | None = None
        # zero-arg hook, called after self.account is updated from a clearinghouseState message --
        # lets carver_trader.py refresh its live trade plan whenever account value/positions change,
        # the Hyperliquid analogue of Kraken's balances-snapshot/update triggering the same refresh.
        self.on_account_update: Callable[[], None] | None = None

    # === internal callbacks ===
    def _subscribe_all(self, wsapp):
        for sub_type in ("clearinghouseState", "orderUpdates", "userFills", "userFundings"):
            wsapp.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": sub_type, "user": self.address},
            }))

    def _on_open(self, wsapp):
        self._subscribe_all(wsapp)
        self.connected.set()
        print(f">> Hyperliquid WS connected ({'MAINNET' if self.mainnet else 'testnet'}), subscribed for {self.address}")

    def _on_message(self, wsapp, message: str):
        try:
            msg = json.loads(message)
        except Exception:
            print("<< raw:", message[:160])
            return
        if not isinstance(msg, dict):
            print("<< ignoring non-dict message:", str(msg)[:160])
            return

        ch = msg.get("channel")
        if ch == "clearinghouseState":
            self.account.update_from_clearinghouse_state(msg)
            if self.on_account_update:
                self.on_account_update()
        elif ch == "userFills":
            data = msg.get("data", {})
            if data.get("isSnapshot"):
                print(f"<< userFills snapshot: {len(data.get('fills', []))} fills")
            for fill in data.get("fills", []):
                if self.on_fill:
                    self.on_fill(fill)
        elif ch == "userFundings":
            data = msg.get("data", {})
            if data.get("isSnapshot"):
                print(f"<< userFundings snapshot: {len(data.get('fundings', []))} records")
            for funding in data.get("fundings", []):
                if self.on_funding:
                    self.on_funding(funding)
        elif ch == "orderUpdates":
            data = msg.get("data")
            updates = data if isinstance(data, list) else ([data] if data else [])
            for upd in updates:
                if self.on_order_update:
                    self.on_order_update(upd)
        elif ch == "subscriptionResponse":
            pass  # ack for our own subscribe calls, nothing to do
        else:
            print("<<", str(msg)[:200])

    def _on_error(self, wsapp, error):
        print("!! Hyperliquid WS error:", error)

    def _on_close(self, wsapp, code, msg):
        print(f"xx Hyperliquid WS closed: code={code}, msg={msg}")
        self.connected.clear()

        def _reconnector():
            time.sleep(2.0)
            self.start()
        threading.Thread(target=_reconnector, daemon=True).start()

    # === public ===
    def start(self):
        with self.ws_lock:
            if self.ws_thread and self.ws_thread.is_alive():
                return
            self.ws_app = websocket.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            def _run():
                self.ws_app.run_forever(ping_interval=30)
            self.ws_thread = threading.Thread(target=_run, daemon=True, name="HyperliquidWS")
            self.ws_thread.start()
