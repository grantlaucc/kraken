import os
import pandas as pd
from research.Carver.carver_trader_helper_old import base_from_symbol
import research.ohlc_data as ohlc_data

def load_notional_scale_series(path: str,
                                base_capital: float,
                                align_index: pd.DatetimeIndex) -> pd.Series:
    """
    Load a CSV of notionals/equity and return a scale series aligned to align_index:
        scale_t = notional_t / base_capital
    Uses YESTERDAY's scale for TODAY (shift by 1) to mirror backtest behavior.
    """
    # Read CSV and try to parse first column as datetime index
    df = pd.read_csv(path)
    # Heuristics for datetime in first column
    first_col = df.columns[0]
    try:
        df[first_col] = pd.to_datetime(df[first_col], utc=True, errors="coerce")
        df = df.set_index(first_col).sort_index()
    except Exception as e:
        raise ValueError(f"Failed to parse datetime index from first column '{first_col}': {e}")

    # Pick a numeric column for notional
    notional = df["Portfolio Value"].astype(float)

    # Make index naive to match typical price/position indices
    if isinstance(notional.index, pd.DatetimeIndex) and notional.index.tz is not None:
        notional.index = notional.index.tz_convert(None)

    # Align to target index and forward-fill last known notional
    notional_aligned = notional.reindex(align_index).ffill()

    # Scale by base capital
    scale = notional_aligned / float(base_capital)

    # Use yesterday's equity to size today's target (first day -> scale 1.0)
    scale = scale.shift(1).fillna(1.0)

    return scale


def load_live_position_notional_scale_series(path, base_capital, align_index):
    df = pd.read_csv(path, index_col=0, parse_dates=True)

    # Make both sides tz-naive daily for the join
    idx_csv = (df.index.tz_localize(None) if df.index.tz is not None else df.index).normalize()
    ai = pd.DatetimeIndex(align_index)
    ai_join = (ai.tz_localize(None) if ai.tz is not None else ai).normalize()

    notional = pd.to_numeric(df["Notional"], errors="coerce")
    notional_aligned = notional.set_axis(idx_csv).reindex(ai_join).ffill().bfill()

    scale = (notional_aligned / float(base_capital)).astype(float)

    # Preserve the original align_index (including time/tz)
    scale.index = ai
    return scale

def updateYesterdayNotional(live_positions_filepath: str, tickers: list[str], date_override=None) -> float:
    """
    Compute today's Notional based on *yesterday's* recorded positions and *today's* prices,
    then upsert it into today's row (column 'Notional') of live_positions.csv.

    Returns:
      - The computed notional (float). Raises on missing yesterday row or missing file.
    """
    if not os.path.isfile(live_positions_filepath):
        raise FileNotFoundError(f"Positions file not found: {live_positions_filepath}")

    # Load existing positions file
    df = pd.read_csv(live_positions_filepath, index_col=0, parse_dates=[0])
    df.index = pd.to_datetime(df.index).normalize() 

    if df.empty:
        raise ValueError(f"{live_positions_filepath} is empty")

    today = (
    pd.to_datetime(date_override, utc=True).normalize().tz_convert(None)
    if date_override is not None
    else pd.Timestamp.now(tz="UTC").normalize().tz_convert(None))
    yday = today - pd.Timedelta(days=1)

    if yday not in df.index:
        raise ValueError(f"No positions found for yesterday ({yday.date()}) in {live_positions_filepath}")

    pos_yday = df.loc[yday]

    # Build notional from yesterday's units × today's prices
    notional = 0.0
    startDate = (today - pd.Timedelta(days=10))  # safety window for price lookup

    for t in tickers:
        base = base_from_symbol(t)
        units = float(pos_yday.get(base, 0.0))
        if (units == 0.0 or units != units):
            continue

        try:
            px_df = ohlc_data.load_ohlc_data_to_df(t, selectCols=["open"], startDate=startDate, endDate=today)
            price = float(px_df["open"].dropna().iloc[-1])
            notional += units * price
        except Exception as e:
            print(f"!! price fetch failed for {t}: {e} (skipping)")

    # Add cash if present
    cash = float(pos_yday.get("CASH", 0.0))
    notional += cash

    # Upsert today's row with 'Notional' only (don’t touch other columns for today)
    if today in df.index:
        df.loc[today, "Notional"] = notional
    else:
        # Create a minimal row for today
        insert = pd.DataFrame({"Notional": [notional]}, index=[today])
        # Union columns
        cols = sorted(set(df.columns) | {"Notional"})
        df = df.reindex(columns=cols)
        insert = insert.reindex(columns=cols)
        df = pd.concat([df, insert], axis=0).sort_index()

    df.to_csv(live_positions_filepath)
    return notional

