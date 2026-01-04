import json

def order(ws, order_type, side, qty, limit_price, symbol, token, validate = True, req_id=None):
    """
    Send a market order to Kraken WebSocket API with validation.
    
    :param ws: WebSocket connection
    :param token: Authenticated session token
    :param symbol: Symbol for the trading pair (e.g., 'BTC/USD')
    :param order_qty: Quantity of the order
    :param side: 'buy' or 'sell'
    """
    order_message = {
        "method": "add_order",
        "params": {
            "order_type": order_type,
            "side": side,
            "order_qty": qty,
            "symbol": symbol,
            "token": token,
            "validate": validate  # Ensures the order is only validated, not executed
        },
        "req_id": req_id,  # Optional: Unique ID provided by the client
    }

    if order_type == "limit":
        order_message["params"]["limit_price"] = limit_price
    
    ws.send(json.dumps(order_message))
    return

def _as_list(x):
    """Normalize a scalar or iterable to a list (None stays None)."""
    if x is None:
        return None
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]


def cancel_order(ws, token, order_ids=None, cl_ord_ids=None, order_userrefs=None, req_id=None):
    """
    Cancel one or more open orders via Kraken WebSocket API v2 (cancel_order).

    At least one of order_ids, cl_ord_ids, or order_userrefs must be provided.

    :param ws: WebSocket connection
    :param token: Authenticated session token
    :param order_ids: Kraken order_id or list of order_id strings
    :param cl_ord_ids: Client cl_ord_id or list of cl_ord_id strings
    :param order_userrefs: Client order_userref or list of integers
    :param req_id: Optional client request id echoed back in the ACK
    """
    if not token:
        raise ValueError("cancel_order: token is required")

    order_id_list = _as_list(order_ids)
    cl_ord_id_list = _as_list(cl_ord_ids)
    order_userref_list = _as_list(order_userrefs)

    if not (order_id_list or cl_ord_id_list or order_userref_list):
        raise ValueError(
            "cancel_order: provide at least one of order_ids, cl_ord_ids, order_userrefs"
        )

    msg = {
        "method": "cancel_order",
        "params": {"token": token}
    }
    if req_id is not None:
        msg["req_id"]=int(req_id)
    if order_id_list:
        msg["params"]["order_id"] = order_id_list
    if cl_ord_id_list:
        msg["params"]["cl_ord_id"] = cl_ord_id_list
    if order_userref_list:
        msg["params"]["order_userref"] = order_userref_list

    ws.send(json.dumps(msg))
    return