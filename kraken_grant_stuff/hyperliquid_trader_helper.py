"""
Hyperliquid-side equivalent of carver_trader_helper_old.py's Kraken-specific pieces
(get_live_positions_from_ws, print_balances_with_total, etc.), sourced from a
HyperliquidAccount / HLWSManager instead of KrakenBalances/websocket balance messages.

Deliberately a separate top-level module rather than added into carver_trader_helper_old.py --
that file's name is already confusing (it's load-bearing for Kraken despite the "_old"), and
piling Hyperliquid logic into it would only make that worse.

Reuses compute_trade_plan / base_from_symbol / load_target_positions from
carver_trader_helper_old.py as-is: those are already venue-agnostic dict/CSV math with no
Kraken-specific assumptions (see their definitions there).
"""
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.append("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/Carver")
from research.Carver.carver_trader_helper_old import compute_trade_plan, base_from_symbol, load_target_positions  # noqa: F401  (re-exported for callers)

sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding")
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding/Hyperliquid")
import hyperliquid_price_data
import hyperliquid_balances


def load_strategy_trading_capital(strategy_dir: str) -> float:
    """Read the base trading_capital a strategy's position_file.csv was sized against, written
    by run_carver_hyperliquid.py alongside its other outputs."""
    meta_path = os.path.join(strategy_dir, "strategy_meta.json")
    with open(meta_path) as f:
        meta = json.load(f)
    return float(meta["trading_capital"])


def scale_target_positions(target_pos: dict[str, float], account: "hyperliquid_balances.HyperliquidAccount",
                           trading_capital: float) -> dict[str, float]:
    """
    Scale TARGET_POS (sized against a fixed base trading_capital when the strategy was
    generated) by (live account value / trading_capital) at trade-plan time. Hyperliquid's
    websocket account-value feed makes this a live, no-lag alternative to the Kraken flow's
    yesterday's-EOD live_positions.csv scaling (run_carver_helper.load_live_position_notional_scale_series) --
    the position sizes react immediately to deposits/withdrawals/PnL swings instead of a day
    behind.
    """
    if trading_capital <= 0:
        raise ValueError(f"trading_capital must be positive, got {trading_capital}")
    scale = account.account_value / trading_capital
    return {base: units * scale for base, units in target_pos.items()}


def get_live_positions_from_hl(account: "hyperliquid_balances.HyperliquidAccount") -> dict[str, float]:
    """coin -> signed size, the Hyperliquid analogue of carver_trader_helper_old's
    get_live_positions_from_ws (base -> units)."""
    return dict(account.get_live_positions())


def _get_latest_hl_prices(bases: list[str]) -> pd.Series:
    prices = {}
    for base in bases:
        try:
            df = hyperliquid_price_data.load_hyperliquid_price_to_df(base, selectCols=["open"])
            prices[base] = float(df["open"].dropna().iloc[-1])
        except (FileNotFoundError, ValueError, KeyError, IndexError):
            prices[base] = np.nan
    return pd.Series(prices, dtype=float)


def get_trade_plan_details(plan: dict[str, float]) -> dict[str, dict]:
    """Per-base order prefill info (side, qty, limit_price) for the dashboard's order form --
    same shape as carver_trader_helper_old.get_trade_plan_details, priced from Hyperliquid's own
    last close instead of Kraken's all_opens.csv."""
    px = _get_latest_hl_prices(list(plan.keys()))
    details = {}
    for base, delta in plan.items():
        price = float(px.get(base, np.nan))
        details[base] = {
            "side": "BUY" if delta > 0 else "SELL",
            "qty": round(abs(delta), 6),
            "limit_price": None if np.isnan(price) else price,
        }
    return details


def print_trade_plan(plan: dict[str, float], target_pos: dict[str, float], live_pos: dict[str, float],
                     target_date_str: str, account: "hyperliquid_balances.HyperliquidAccount") -> None:
    if not plan:
        print(f"\n=== Trade Plan ({target_date_str}) ===\nOK: live ≈ target")
        return

    px = _get_latest_hl_prices(list(plan.keys()))

    def fmt_qty(x: float) -> str:
        return "0" if abs(x) < 1e-8 else f"{x:.8g}"

    rows = []
    for base, delta in plan.items():
        live = float(live_pos.get(base, 0.0))
        tgt = float(target_pos.get(base, 0.0))
        price = float(px.get(base, np.nan))
        notional = np.nan if np.isnan(price) else delta * price
        rows.append((base, delta, live, tgt, price, notional))

    priced = [r for r in rows if not np.isnan(r[5])]
    unpriced = [r for r in rows if np.isnan(r[5])]
    priced.sort(key=lambda r: abs(r[5]), reverse=True)

    print(f"\n=== Trade Plan ({target_date_str}) ===")
    for base, delta, live, tgt, price, notional in priced:
        side = "BUY" if delta > 0 else "SELL"
        price_str = f"${price:,.6g}" if price < 1 else f"${price:,.2f}"
        print(f"{base}: {live:.8g} -> {tgt:.8g}  |  {side} {fmt_qty(delta)} @ {price_str}  |  ${notional:,.2f}")
    for base, delta, live, tgt, price, _ in unpriced:
        side = "BUY" if delta > 0 else "SELL"
        print(f"{base}: {live:.8g} -> {tgt:.8g}  |  {side} {fmt_qty(delta)}   (price N/A)")

    print(f"\nAccount Value: ${account.account_value:,.2f}   Withdrawable: ${account.withdrawable:,.2f}   Margin Used: ${account.total_margin_used:,.2f}")


def print_positions(account: "hyperliquid_balances.HyperliquidAccount") -> None:
    if not account.positions:
        print("No open Hyperliquid positions.")
    else:
        df = pd.DataFrame.from_dict(account.positions, orient="index")
        df.index.name = "coin"
        with pd.option_context('display.float_format', '{:,.4f}'.format):
            print(df)
    print(f"\nAccount Value: ${account.account_value:,.2f}   Withdrawable: ${account.withdrawable:,.2f}   Margin Used: ${account.total_margin_used:,.2f}")
