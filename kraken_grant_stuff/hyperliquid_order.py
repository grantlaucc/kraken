"""
Hyperliquid order placement/cancellation primitives, mirroring kraken_order.py's shape so
carver_trader_dashboard/services/order_exec.py can dispatch to either venue with minimal
branching.

Deliberately does NOT use the hyperliquid-python-sdk's Info/Exchange classes: Info() eagerly
parses the full spot universe and crashes on delisted tokens with out-of-range indices (same
issue hyperliquid_funding_data.py and place_test_order.py work around). Uses only the SDK's
low-level signing utilities plus plain REST calls, same pattern as the rest of this project's
Hyperliquid code -- this module lifts that exact approach out of place_test_order.py so it's
reusable from the dashboard, not just a standalone CLI scaffold.
"""
import time

import requests
from hyperliquid.utils.signing import (
    sign_l1_action,
    order_request_to_order_wire,
    order_wires_to_order_action,
    get_timestamp_ms,
)
from hyperliquid.utils.constants import MAINNET_API_URL

_ASSET_INDEX_CACHE: dict[str, dict[str, int]] = {}  # base_url -> {coin: index}
_ASSET_INDEX_CACHED_AT: dict[str, float] = {}
ASSET_INDEX_TTL_SECONDS = 300  # Hyperliquid occasionally adds/delists perps; don't cache forever


def _get_universe(base_url: str) -> dict[str, int]:
    now = time.monotonic()
    cached_at = _ASSET_INDEX_CACHED_AT.get(base_url, 0.0)
    if base_url not in _ASSET_INDEX_CACHE or (now - cached_at) > ASSET_INDEX_TTL_SECONDS:
        resp = requests.post(f"{base_url}/info", json={"type": "meta"})
        resp.raise_for_status()
        universe = resp.json()["universe"]
        _ASSET_INDEX_CACHE[base_url] = {m["name"]: i for i, m in enumerate(universe)}
        _ASSET_INDEX_CACHED_AT[base_url] = now
    return _ASSET_INDEX_CACHE[base_url]


def get_asset_index(coin: str, base_url: str) -> int:
    universe = _get_universe(base_url)
    if coin not in universe:
        raise ValueError(f"'{coin}' not found in perp universe")
    return universe[coin]


def get_mid_price(coin: str, base_url: str) -> float:
    resp = requests.post(f"{base_url}/info", json={"type": "allMids"})
    resp.raise_for_status()
    mids = resp.json()
    if coin not in mids:
        raise ValueError(f"No mid price for '{coin}'")
    return float(mids[coin])


def order(*, coin: str, side: str, qty: float, order_type: str, limit_price: float | None,
          wallet, base_url: str, slippage: float = 0.05, reduce_only: bool = False,
          req_id: int | None = None) -> dict:
    """
    Place an order on Hyperliquid. Mirrors kraken_order.order's call shape (coin instead of
    symbol -- Hyperliquid perps are USDC-margined, there's no quote leg to encode).

    order_type: "market" or "limit" (Hyperliquid has no native market order -- "market" here is
    submitted as an aggressive IOC limit against the current mid, same as place_test_order.py).
    req_id is accepted for call-shape parity with kraken_order.order but isn't used by Hyperliquid's
    API (Hyperliquid orders are tracked by the returned order id ("oid"), not a client req_id).
    """
    is_buy = side.lower() == "buy"

    if order_type == "market":
        mid = get_mid_price(coin, base_url)
        raw_px = mid * (1 + slippage) if is_buy else mid * (1 - slippage)
        wire_px = round(float(f"{raw_px:.5g}"), 6)
        order_type_wire = {"limit": {"tif": "Ioc"}}
    elif order_type == "limit":
        if limit_price is None:
            raise ValueError("limit_price is required for order_type='limit'")
        wire_px = limit_price
        order_type_wire = {"limit": {"tif": "Gtc"}}
    else:
        raise ValueError(f"Unknown order_type: {order_type!r}")

    asset = get_asset_index(coin, base_url)
    order_req = {
        "coin": coin,
        "is_buy": is_buy,
        "sz": qty,
        "limit_px": wire_px,
        "order_type": order_type_wire,
        "reduce_only": reduce_only,
    }
    order_wire = order_request_to_order_wire(order_req, asset)
    action = order_wires_to_order_action([order_wire])
    timestamp = get_timestamp_ms()
    is_mainnet = base_url == MAINNET_API_URL
    signature = sign_l1_action(wallet, action, None, timestamp, None, is_mainnet)

    payload = {"action": action, "nonce": timestamp, "signature": signature, "vaultAddress": None}
    resp = requests.post(f"{base_url}/exchange", json=payload)
    resp.raise_for_status()
    result = resp.json()
    # Hyperliquid returns request-level errors (bad wallet, bad signature, etc.) as a 200 with
    # {"status": "err", "response": "<message>"} -- raise_for_status() above won't catch this,
    # confirmed against a real error response during development. Per-order errors (e.g. this
    # specific order rejected for insufficient margin) are nested one level deeper, inside
    # response.data.statuses[i], and are left for the caller to inspect since a batch request can
    # have some orders succeed and others fail.
    if result.get("status") != "ok":
        raise RuntimeError(f"Hyperliquid order request failed: {result.get('response')}")
    return result


def cancel_order(*, coin: str, order_id: int, wallet, base_url: str) -> dict:
    """Cancel a single open order by its Hyperliquid order id ("oid")."""
    asset = get_asset_index(coin, base_url)
    action = {"type": "cancel", "cancels": [{"a": asset, "o": int(order_id)}]}
    timestamp = get_timestamp_ms()
    is_mainnet = base_url == MAINNET_API_URL
    signature = sign_l1_action(wallet, action, None, timestamp, None, is_mainnet)

    payload = {"action": action, "nonce": timestamp, "signature": signature, "vaultAddress": None}
    resp = requests.post(f"{base_url}/exchange", json=payload)
    resp.raise_for_status()
    result = resp.json()
    if result.get("status") != "ok":
        raise RuntimeError(f"Hyperliquid cancel request failed: {result.get('response')}")
    try:
        statuses = result["response"]["data"]["statuses"]
        status_obj = statuses[0] if statuses else {}
    except (KeyError, IndexError, TypeError):
        raise RuntimeError(f"Hyperliquid cancel: unexpected response shape: {result}")
    if isinstance(status_obj, dict) and "error" in status_obj:
        raise RuntimeError(f"Hyperliquid cancel rejected: {status_obj['error']}")
    return result
