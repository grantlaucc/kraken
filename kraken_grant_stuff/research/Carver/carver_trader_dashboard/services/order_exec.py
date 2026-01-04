from secrets import randbits
import kraken_order

def place_order(ws_mgr, *, symbol: str, side: str, order_type: str,
                qty: float, limit_price, validate: bool, req_id: int | None = None) -> int:
    """
    Place an order using the existing kraken_order.order(ws=..., token=..., ...).
    Returns the req_id used so callers can log/track status.
    """
    if req_id is None:
        req_id = randbits(31)

    # Ensure WS is ready and we have a token
    if not ws_mgr.connected.is_set():
        raise RuntimeError("Kraken WS not connected yet.")
    if not ws_mgr.token:
        raise RuntimeError("Missing Kraken WS auth token.")

    # Serialize through the WS lock to avoid interleaving messages
    with ws_mgr.ws_lock:
        kraken_order.order(
            ws=ws_mgr.ws_app,
            order_type=order_type.lower(),
            side=side.lower(),
            qty=qty,                              # your helper sends as "order_qty"
            limit_price=limit_price,              # your helper adds only if limit
            symbol=symbol,
            token=ws_mgr.token,
            validate=bool(validate),
            req_id=req_id,
        )

    return req_id

def cancel_order(ws_mgr, *, order_id: str) -> None:
    if not ws_mgr.connected.is_set():
        raise RuntimeError("Kraken WS not connected yet.")
    if not ws_mgr.token:
        raise RuntimeError("Missing Kraken WS auth token.")
    # use your helper
    with ws_mgr.ws_lock:
        kraken_order.cancel_order(
            ws=ws_mgr.ws_app,
            token=ws_mgr.token,
            order_ids=order_id,   # accepts str or list[str]
            req_id=None           # optional
        )
