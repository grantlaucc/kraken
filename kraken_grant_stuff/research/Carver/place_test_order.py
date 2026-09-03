"""
Minimal, standalone script to place a single test order on Hyperliquid.
This is a scaffold to prove out order placement, not real trading infra --
no position tracking, no order management, no retries.

Deliberately does NOT use the hyperliquid-python-sdk's Info/Exchange classes:
Info() eagerly parses the full spot universe and crashes on delisted tokens
with out-of-range indices (same issue hyperliquid_funding_data.py works around).
Uses only the SDK's low-level signing utilities plus plain REST calls, same
pattern as the rest of this project's Hyperliquid code.

Setup:
    Add to /Users/grantlau/Documents/QuantStuff/kraken/.env:
        HYPERLIQUID_PRIVATE_KEY=0x...
    Strongly prefer an API/agent wallet's key (Hyperliquid Settings -> API),
    not your main wallet's key -- an agent key can trade but can't withdraw,
    and can be revoked independently of your main account.

Usage (defaults to TESTNET -- no real money):
    python place_test_order.py --coin BTC --side buy --qty 0.001 --type limit --price 50000
    python place_test_order.py --coin BTC --side buy --qty 0.001 --type market

Add --mainnet for real funds -- this requires a typed confirmation before it submits.
"""
import argparse
import os
import sys

import requests
from dotenv import load_dotenv
from eth_account import Account

from hyperliquid.utils.signing import (
    sign_l1_action,
    order_request_to_order_wire,
    order_wires_to_order_action,
    get_timestamp_ms,
)
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL

ENV_PATH = "/Users/grantlau/Documents/QuantStuff/kraken/.env"


def get_asset_index(coin: str, base_url: str) -> int:
    resp = requests.post(f"{base_url}/info", json={"type": "meta"})
    resp.raise_for_status()
    universe = resp.json()["universe"]
    for i, m in enumerate(universe):
        if m["name"] == coin:
            return i
    raise ValueError(f"'{coin}' not found in perp universe")


def get_mid_price(coin: str, base_url: str) -> float:
    resp = requests.post(f"{base_url}/info", json={"type": "allMids"})
    resp.raise_for_status()
    mids = resp.json()
    if coin not in mids:
        raise ValueError(f"No mid price for '{coin}'")
    return float(mids[coin])


def place_order(coin, is_buy, sz, order_type_wire, limit_px, wallet, base_url, reduce_only=False):
    asset = get_asset_index(coin, base_url)
    order = {
        "coin": coin,
        "is_buy": is_buy,
        "sz": sz,
        "limit_px": limit_px,
        "order_type": order_type_wire,
        "reduce_only": reduce_only,
    }
    order_wire = order_request_to_order_wire(order, asset)
    action = order_wires_to_order_action([order_wire])
    timestamp = get_timestamp_ms()
    is_mainnet = base_url == MAINNET_API_URL
    signature = sign_l1_action(wallet, action, None, timestamp, None, is_mainnet)

    payload = {"action": action, "nonce": timestamp, "signature": signature, "vaultAddress": None}
    resp = requests.post(f"{base_url}/exchange", json=payload)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Minimal Hyperliquid test order placer")
    parser.add_argument("--coin", required=True, help="e.g. BTC")
    parser.add_argument("--side", choices=["buy", "sell"], default="buy")
    parser.add_argument("--qty", type=float, required=True)
    parser.add_argument("--type", choices=["limit", "market"], default="limit")
    parser.add_argument("--price", type=float, default=None, help="required for --type limit")
    parser.add_argument("--slippage", type=float, default=0.05, help="market order max slippage tolerance, default 5%%")
    parser.add_argument("--reduce-only", action="store_true")
    parser.add_argument("--mainnet", action="store_true", help="DANGER: real funds. Defaults to testnet.")
    args = parser.parse_args()

    if args.type == "limit" and args.price is None:
        parser.error("--price is required for --type limit")

    load_dotenv(ENV_PATH)
    private_key = os.getenv("HYPERLIQUID_PRIVATE_KEY")
    if not private_key:
        sys.exit(f"Set HYPERLIQUID_PRIVATE_KEY in {ENV_PATH} first (see module docstring).")

    wallet = Account.from_key(private_key)
    base_url = MAINNET_API_URL if args.mainnet else TESTNET_API_URL
    is_buy = args.side == "buy"

    if args.type == "market":
        mid = get_mid_price(args.coin, base_url)
        raw_px = mid * (1 + args.slippage) if is_buy else mid * (1 - args.slippage)
        limit_px = round(float(f"{raw_px:.5g}"), 6)  # aggressive IOC limit order, Hyperliquid's own "market order"
        order_type_wire = {"limit": {"tif": "Ioc"}}
        px_display = f"~{limit_px:,.2f} (market, {args.slippage:.0%} slippage cap on mid {mid:,.2f})"
    else:
        limit_px = args.price
        order_type_wire = {"limit": {"tif": "Gtc"}}
        px_display = f"{limit_px:,.2f}"

    print(f"[{'MAINNET -- REAL FUNDS' if args.mainnet else 'testnet'}] "
          f"{args.side.upper()} {args.qty} {args.coin} @ {px_display}"
          f"{' (reduce-only)' if args.reduce_only else ''}")

    if args.mainnet:
        if input("Type 'yes' to submit this MAINNET order: ").strip().lower() != "yes":
            print("Aborted.")
            return

    result = place_order(args.coin, is_buy, args.qty, order_type_wire, limit_px, wallet, base_url,
                          reduce_only=args.reduce_only)
    print(result)


if __name__ == "__main__":
    main()
