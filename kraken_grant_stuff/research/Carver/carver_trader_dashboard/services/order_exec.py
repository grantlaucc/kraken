from secrets import randbits
import kraken_order
import hyperliquid_order


def place_order(ws_mgr, *, venue: str, symbol: str, side: str, order_type: str,
                qty: float, limit_price, validate: bool, req_id: int | None = None):
    """
    Returns (req_id, status, order_id). Kraken's add_order is async over the websocket -- status
    is always "pending" here and order_id is None until routes.py's on_add_order_ack callback
    fills them in later. Hyperliquid's /exchange endpoint is a synchronous REST call, so status
    and order_id are already final by the time this returns; the on_order_update hook (wired in
    routes.py) will still overwrite status again once the authoritative orderUpdates message
    arrives, but the UI doesn't have to wait for it to show something.
    """
    if req_id is None:
        req_id = randbits(31)

    if venue == "kraken":
        if not ws_mgr.connected.is_set():
            raise RuntimeError("Kraken WS not connected yet.")
        if not ws_mgr.token:
            raise RuntimeError("Missing Kraken WS auth token.")
        with ws_mgr.ws_lock:
            kraken_order.order(
                ws=ws_mgr.ws_app,
                order_type=order_type.lower(),
                side=side.lower(),
                qty=qty,
                limit_price=limit_price,
                symbol=symbol,
                token=ws_mgr.token,
                validate=bool(validate),
                req_id=req_id,
            )
        return req_id, "pending", None

    if venue == "hyperliquid":
        coin = symbol  # Hyperliquid orders are base-only; routes.py passes the bare coin here
        try:
            result = hyperliquid_order.order(
                coin=coin, side=side, qty=qty, order_type=order_type, limit_price=limit_price,
                wallet=ws_mgr.wallet, base_url=ws_mgr.base_url, reduce_only=False, req_id=req_id,
            )
        except Exception as e:
            return req_id, f"error: {e}", None

        try:
            statuses = result["response"]["data"]["statuses"]
            status_obj = statuses[0] if statuses else {}
        except (KeyError, IndexError, TypeError):
            return req_id, f"error: unexpected response shape: {result}", None

        if "error" in status_obj:
            return req_id, f"error: {status_obj['error']}", None
        if "filled" in status_obj:
            return req_id, "filled", str(status_obj["filled"].get("oid")) if status_obj["filled"].get("oid") is not None else None
        if "resting" in status_obj:
            return req_id, "resting", str(status_obj["resting"].get("oid")) if status_obj["resting"].get("oid") is not None else None
        return req_id, f"error: unrecognized status: {status_obj}", None

    raise ValueError(f"Unknown venue: {venue!r}")


def cancel_order(ws_mgr, *, venue: str, order_id: str, coin: str | None = None) -> None:
    if venue == "kraken":
        if not ws_mgr.connected.is_set():
            raise RuntimeError("Kraken WS not connected yet.")
        if not ws_mgr.token:
            raise RuntimeError("Missing Kraken WS auth token.")
        with ws_mgr.ws_lock:
            kraken_order.cancel_order(
                ws=ws_mgr.ws_app,
                token=ws_mgr.token,
                order_ids=order_id,
                req_id=None,
            )
        return

    if venue == "hyperliquid":
        if not coin:
            raise ValueError("coin is required to cancel a Hyperliquid order")
        hyperliquid_order.cancel_order(coin=coin, order_id=int(order_id), wallet=ws_mgr.wallet, base_url=ws_mgr.base_url)
        return

    raise ValueError(f"Unknown venue: {venue!r}")
