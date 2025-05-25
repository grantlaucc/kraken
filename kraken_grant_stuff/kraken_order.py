import json

def order(ws, order_type, side, qty, limit_price, symbol, token, validate = True):
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
        "req_id": 1,  # Optional: Unique ID provided by the client
    }

    if order_type == "limit":
        order_message["params"]["limit_price"] = limit_price
    
    ws.send(json.dumps(order_message))
    return