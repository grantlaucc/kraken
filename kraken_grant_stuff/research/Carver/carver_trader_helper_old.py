import sys
import os
import pandas as pd
import numpy as np
from kraken_balances import KrakenBalances
import research.ohlc_data as ohlc_data

def base_from_symbol(col: str) -> str:
    # "BTC/USDC" -> "BTC", else just upper()
    return col.split("/")[0].upper() if "/" in col else col.upper()

def _get_usd_cad(fx_cache: dict | None = None) -> float:
    """
    Return USD/CAD (CAD per 1 USD). If fx_cache is provided, cache per UTC day to avoid repeated IO.
    Falls back to last available close if today's candle isn't present; final fallback = 1.0.
    """
    today = pd.Timestamp.now(tz="UTC").normalize()

    # Use cache if available & fresh
    if isinstance(fx_cache, dict):
        cached = fx_cache.get("USD/CAD", {})
        if cached.get("date") == today.date() and cached.get("rate") is not None:
            return float(cached["rate"])

    try:
        # Try today's bar first (daily interval)
        df_today = ohlc_data.ohlc_to_df(ohlc_data.get_ohlc_data("USD/CAD", 1440, since=today))
        if not df_today.empty and df_today["open"].notna().any():
            rate = float(df_today["open"].dropna().iloc[-1])
        else:
            # Fallback: last available close
            df_all = ohlc_data.load_ohlc_data_to_df("USD/CAD", selectCols=["open"])
            rate = float(df_all["open"].dropna().iloc[-1])
    except Exception as e:
        print(f"!! USD/CAD FX fetch failed: {e}. Using 1.0 as fallback.")
        rate = 1.0

    # Update cache if provided
    if isinstance(fx_cache, dict):
        fx_cache["USD/CAD"] = {"date": today.date(), "rate": rate}

    return rate


def load_target_positions(csv_path: str):
    """Read CSV, take last row, build TARGET_POS (normalized by base asset)."""
    df = pd.read_csv(csv_path, index_col=0)
    if df.empty:
        raise ValueError(f"{csv_path} is empty")

    last_idx = df.index[-1]
    target_date_str = str(last_idx)

    row = df.iloc[-1]
    # Build mapping base-asset -> units; coerce numeric and drop NaNs
    target_pos = {}
    for col, val in row.items():
        try:
            x = float(val)
        except (TypeError, ValueError):
            continue
        if pd.isna(x):
            continue
        base = base_from_symbol(str(col))
        if not base:
            continue
        target_pos[base] = target_pos.get(base, 0.0) + x  # sum if duplicates
    return target_date_str, target_pos

def get_live_positions_from_ws(bal_lock, myKrakenBalances, fx_cache, exclude=None, aggregate_cash=True) -> dict[str, float]:
    """Collapse raw WS balances to normalized base -> units (floats)."""
    live = {}
    with bal_lock:
        if not (myKrakenBalances and getattr(myKrakenBalances, "balances", None)):
            return live
        for asset, bal in myKrakenBalances.balances.items():
            live[asset] = live.get(asset, 0.0) + float(bal)
    if exclude:
        for a, amt in exclude.items():
            live[a] = live.get(a, 0.0) - float(amt)

    if aggregate_cash:
        usd  = live.get("USD", 0.0) + live.get("USDC", 0.0)
        cad  = live.get("CAD", 0.0)
        if cad != 0.0:
            usd_cad = _get_usd_cad(fx_cache)            # CAD per USD
            usd += (cad / usd_cad)              # convert CAD -> USD
        for k in ("USD", "USDC", "CAD"):
            if k in live:
                del live[k]
        live["CASH"] = usd
    return live

def write_live_positions_to_csv(csv_path: str, live_pos: dict, tz_utc: bool = True):
    """
    Upsert today's row into csv_path with live_pos columns; index is today's date.
    """
    ts = pd.Timestamp.now(tz="UTC" if tz_utc else None).normalize()
    row = pd.DataFrame([live_pos], index=[ts])

    if os.path.exists(csv_path):
        try:
            existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        except Exception:
            existing = pd.read_csv(csv_path, index_col=0)
            existing.index = pd.to_datetime(existing.index, errors="coerce")

        if isinstance(existing.index, pd.DatetimeIndex):
            if existing.index.tz is not None:
                existing.index = existing.index.tz_convert(None)
            existing.index = existing.index.normalize()

        cols = sorted(set(existing.columns) | set(row.columns))
        existing = existing.reindex(columns=cols)
        row = row.reindex(columns=cols)
        key = ts.tz_convert(None) if ts.tzinfo else ts
        if key in existing.index:
            # Merge: today's non-NaN values overwrite; otherwise keep existing (preserves Notional)
            merged = row.iloc[0].combine_first(existing.loc[key])
            existing.loc[key] = merged
        else:
            existing.loc[key] = row.iloc[0]
        out = existing.sort_index()
    else:
        if ts.tzinfo is not None:
            row.index = row.index.tz_convert(None)
        out = row

    out.to_csv(csv_path)

def compute_trade_plan(target_pos: dict[str, float],
                       live_pos: dict[str, float]):
    """
    Returns {BASE: delta_units} needed to reach target (positive=BUY, negative=SELL).
    Optionally rounds to OrderMin steps and drops tiny deltas.
    """
    plan = {}
    for base, tgt in target_pos.items():
        live = float(live_pos.get(base, 0.0))
        delta = tgt - live
        plan[base] = delta
    return plan

def print_trade_plan(
    plan: dict[str, float],
    target_pos: dict[str, float],
    live_pos: dict[str, float],
    target_date_str: str,
    price_csv: str = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/all_opens.csv",
):
    if not plan:
        print(f"\n=== Trade Plan ({target_date_str}) ===\nOK: live ≈ target")
        return

    # Load last close for each "{ASSET}/USD" column (skip CASH)
    price_cols = [f"{a}/USD" for a in plan.keys() if a != "CASH"]
    px_last = pd.Series(dtype=float)
    px_last = get_latest_closes(price_cols, price_csv)

    def fmt_qty(x: float) -> str:
        return "0" if abs(x) < 1e-8 else f"{x:.8g}"

    # Build rows: (asset, delta, live, target, price, notional)
    rows = []
    for a, d in plan.items():
        live = float(live_pos.get(a, 0.0))
        tgt  = float(target_pos.get(a, 0.0))
        if a == "CASH":
            price = 1.0
        else:
            price = float(px_last.get(f"{a}/USD", np.nan))
        notional = np.nan if np.isnan(price) or a == "CASH" else d * price
        rows.append((a, d, live, tgt, price, notional))

    # Split and sort: priced (non-CASH) by |notional| desc; then unpriced; cash summary last
    priced      = [r for r in rows if (r[0] != "CASH" and not np.isnan(r[5]))]
    unpriced    = [r for r in rows if (r[0] != "CASH" and  np.isnan(r[5]))]
    priced.sort(key=lambda r: abs(r[5]), reverse=True)

    print(f"\n=== Trade Plan ({target_date_str}) ===")

    net_notional = 0.0  # only non-CASH priced trades
    for a, d, live, tgt, price, notional in priced:
        side = "BUY" if d > 0 else "SELL"
        price_str = f"${price:,.6g}" if price < 1 else f"${price:,.2f}"
        net_notional += notional
        print(f"{a}: {live:.8g} -> {tgt:.8g}  |  {side} {fmt_qty(d)} @ {price_str}  |  ${notional:,.2f}")

    for a, d, live, tgt, price, _ in unpriced:
        side = "BUY" if d > 0 else "SELL"
        print(f"{a}: {live:.8g} -> {tgt:.8g}  |  {side} {fmt_qty(d)}   (price N/A)")

    # Cash summary at the end (approx; ignores fees/slippage)
    live_cash = float(live_pos.get("CASH", 0.0))
    end_cash  = live_cash - net_notional
    print(f"\nCASH: {live_cash:,.2f} -> {end_cash:,.2f}  |  ${net_notional:,.2f}")

def get_trade_plan_details(
    plan: dict[str, float],
    price_csv: str = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/all_opens.csv",
) -> dict[str, dict]:
    """
    Per-base order prefill info derived from the trade plan: side, qty (abs, rounded 4dp),
    and limit_price (per-unit price, not notional). Used to prefill the dashboard order form.
    """
    price_cols = [f"{a}/USD" for a in plan.keys() if a != "CASH"]
    px_last = get_latest_closes(price_cols, price_csv)

    details = {}
    for a, d in plan.items():
        if a == "CASH":
            continue
        price = float(px_last.get(f"{a}/USD", np.nan))
        details[a] = {
            "side": "BUY" if d > 0 else "SELL",
            "qty": round(abs(d), 4),
            "limit_price": None if np.isnan(price) else price,
        }
    return details

def get_latest_closes(
    price_cols: list[str],
    price_csv: str,
    read_fn = ohlc_data.read_csv_data,   # inject for testing if you want
) -> pd.Series:
    """
    Return a Series of the last available close for each requested column.
    If no data or empty input, returns an empty/NaN-filled float Series.
    """
    if not price_cols:
        return pd.Series(dtype=float)

    try:
        df = read_fn(price_cols, csv_path=price_csv)
    except KeyError:
        # If some columns are missing, fall back to whatever exists
        # (comment this out if you prefer hard failure)
        hdr = pd.read_csv(price_csv, nrows=0).columns
        present = [c for c in price_cols if c in hdr]
        if not present:
            return pd.Series({c: np.nan for c in price_cols}, dtype=float)
        df = read_fn(present, csv_path=price_csv)

    if df.empty:
        return pd.Series({c: np.nan for c in price_cols}, dtype=float)

    # Last valid close per column, keep requested order, float dtype
    return df.ffill().iloc[-1].reindex(price_cols).astype(float)

def balances_with_notionals(
    myKrakenBalances: KrakenBalances,
    price_csv: str = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/all_opens.csv",
    exclude_balances: dict[str, float] | None = None,
    cash_assets: tuple[str, ...] = ("CAD", "USD", "USDC"),
    fx_cache: dict | None = None
) -> pd.DataFrame:
    """
    Returns a DataFrame with columns: ['units','price','notional'] for each asset.
    Prices come from get_latest_closes([{ASSET}/USD], price_csv).
    cash_assets are priced at 1.0. Assets without a price column return NaN price/notional.
    """
    # 1) starting balances (apply exclusions if provided)
    units = {a: float(u) for a, u in myKrakenBalances.balances.items()}
    if exclude_balances:
        for a, amt in exclude_balances.items():
            units[a] = units.get(a, 0.0) - float(amt)

    # 2) load last closes for non-cash assets present
    want_cols = [f"{a}/USD" for a in units.keys() if a not in cash_assets]
    px_last = get_latest_closes(want_cols, price_csv) if want_cols else pd.Series(dtype=float)

    # 3) build dataframe
    rows = []
    for a, u in units.items():
        if a in cash_assets:
            if a == "CAD":
                price = 1/(_get_usd_cad(fx_cache))
            else:
                price = 1.0
        else:
            price = float(px_last.get(f"{a}/USD", np.nan))
        notional = u * price if not np.isnan(price) else np.nan
        rows.append((a, u, price, notional))

    df = pd.DataFrame(rows, columns=["asset", "units", "price", "notional"]).set_index("asset")
    return df.sort_index()

def print_balances_with_total(
    myKrakenBalances: KrakenBalances,
    price_csv: str = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/all_opens.csv",
    exclude_balances: dict[str, float] | None = None,
    cash_assets: tuple[str, ...] = ("CAD", "USD", "USDC"),
    fx_cache: dict | None = None,
) -> None:
    df = balances_with_notionals(
        myKrakenBalances=myKrakenBalances,
        price_csv=price_csv,
        exclude_balances=exclude_balances,
        cash_assets=cash_assets,
        fx_cache=fx_cache,
    )

    total_notional = df["notional"].sum(skipna=True)  # ignores NaNs
    total_row = pd.DataFrame(
        {"units": [np.nan], "price": [np.nan], "notional": [total_notional]},
        index=["TOTAL"],
    )
    df_out = pd.concat([df, total_row])

    # Pretty print
    fmt_units = lambda x: "" if pd.isna(x) else f"{x:.8g}"
    fmt_price = lambda x: "" if pd.isna(x) else (f"${x:,.6g}" if x < 1 else f"${x:,.2f}")
    fmt_usd   = lambda x: "" if pd.isna(x) else f"${x:,.2f}"

    print(
        df_out.to_string(
            formatters={
                "units": fmt_units,
                "price": fmt_price,
                "notional": fmt_usd,
            }
        )
    )
