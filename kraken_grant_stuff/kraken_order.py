import json

def market_order(ws, side, order_qty, symbol, token):
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
            "order_type": "market",
            "side": side,
            "order_qty": order_qty,
            "symbol": symbol,
            "token": token,
            "validate": True  # Ensures the order is only validated, not executed
        },
        "req_id": 1,  # Optional: Unique ID provided by the client
    }
    
    ws.send(json.dumps(order_message))
    return