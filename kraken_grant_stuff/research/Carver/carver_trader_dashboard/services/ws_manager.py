import json, time, threading, websocket
from typing import Callable
from ..config import SETTINGS
import kraken_token

class WSManager:
    def __init__(self, url: str = SETTINGS.ws_url):
        self.url = url
        self.ws_app: websocket.WebSocketApp | None = None
        self.ws_thread: threading.Thread | None = None
        self.ws_lock = threading.Lock()
        self.connected = threading.Event()
        self.token: str | None = None
        # external hooks
        self.on_add_order_ack: Callable[[dict], None] | None = None
        self.on_cancel_order_ack = None

    # === internal callbacks ===
    def _on_open(self, wsapp):
        try:
            self.token = kraken_token.get_websocket_token()
            wsapp.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "executions", "token": self.token, "snap_orders": True, "snap_trades": True}
            }))
            self.connected.set()
            print(">> WS connected & token acquired")
        except Exception as e:
            print("!! token fetch failed:", e)

    def _on_message(self, wsapp, message: str):
        try:
            msg = json.loads(message)
        except Exception:
            print("<< raw:", message[:160]); return

        # If Kraken sent a list of messages, handle each dict within it
        if isinstance(msg, list):
            for item in msg:
                if isinstance(item, dict):
                    self._on_message(wsapp, json.dumps(item))  # recurse on dict items
                else:
                    print("<< ignoring non-dict item in list:", str(item)[:120])
            return

        if not isinstance(msg, dict):
            print("<< ignoring non-dict message:", str(msg)[:160]); return
        ch, typ = msg.get("channel"), msg.get("type")
        if ch == "executions":
            if typ == "snapshot":
                print("<< executions snapshot")
            elif typ == "update":
                seq = msg.get("sequence")
                data = msg.get("data", {})
                print(f"<< executions update (seq {seq}) orders={len(data.get('orders',[]))} trades={len(data.get('trades',[]))}")
        elif msg.get("method") == "add_order":
            if self.on_add_order_ack:
                self.on_add_order_ack(msg)
        elif msg.get("method") == "cancel_order":
            if self.on_cancel_order_ack:
                self.on_cancel_order_ack(msg)

    def _on_error(self, wsapp, error):
        print("!! WS error:", error)

    def _on_close(self, wsapp, code, msg):
        print(f"xx WS closed: code={code}, msg={msg}")
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
            self.ws_thread = threading.Thread(target=_run, daemon=True, name="KrakenWS")
            self.ws_thread.start()

    def send_add_order(self, payload: dict):
        if not self.connected.is_set():
            raise RuntimeError("Kraken WS not connected yet.")
        with self.ws_lock:
            self.ws_app.send(json.dumps(payload))
