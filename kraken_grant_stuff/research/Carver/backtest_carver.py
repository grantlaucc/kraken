#%%
import numpy as np
import pandas as pd
import sys
import research.Carver.trading_system.forecast as forecast
import research.Carver.trading_system.trading_system as trading_system
import research.Carver.trading_system.coin_universe as coin_universe
import research.ohlc_data as ohlc_data
import matplotlib.pyplot as plt
import quantstats_lumi as qs
import yfinance as yf
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding/Hyperliquid")
import hyperliquid_price_data
import research.Carver.run_carver_hyperliquid as run_carver_hyperliquid

# Same 10-coin universe as before, just re-sourced from coin_universe.csv instead of
# hardcoded ticker strings -- refresh via `python -m research.Carver.trading_system.coin_universe`.
LIVE_TRADING_BASES = ['LTC', 'LINK', 'BCH', 'ADA', 'ETH', 'XTZ', 'ATOM', 'XRP', 'BTC', 'DOGE']
tickers = coin_universe.to_kraken_tickers([c for c in coin_universe.load_universe() if c.base in LIVE_TRADING_BASES])

quoteCurrency = "USD"
trading_capital = 2500  # TODO
volatility_target = 0.50
fee_rate = 0.0004      # Kraken trading fees
slippage = 0.0003
TRADE_INERTIA = 0.10  # only trade if |target - current| > 10% of |current|
ORDER_MIN = True
AVOID_NEGATIVE_CASH = False
MIN_CASH_BUFFER = 0.0

## Backtest Helper Functions
def log_trades(date, current_units: pd.Series, target_units: pd.Series, trades_units: pd.Series):
    moved = trades_units[trades_units != 0.0]
    if moved.empty:
        return
    print(f"\n{date} — trades")
    for t in moved.index:
        cur = float(current_units.get(t, 0.0))
        tgt = float(target_units.get(t, 0.0))
        act = float(trades_units[t])
        print(f"  {t:12s}: {cur:.6f} → {tgt:.6f} | action {act:+.6f}")

def enforce_cash_constraint(trades_units: pd.Series,
                            prices: pd.Series,
                            cash0: float,
                            fee_rate: float,
                            slippage: float = 0.0,
                            min_cash: float = 0.0) -> tuple[pd.Series, float]:
    """
    Keep end-of-trade cash >= min_cash by scaling BUY legs only.
    Returns (adjusted_trades_units, alpha_used) where alpha in [0,1].
    """
    if trades_units.empty:
        return trades_units, 1.0

    buys  = trades_units.clip(lower=0.0)
    sells = trades_units.clip(upper=0.0)

    buy_notional  = float((buys  * prices * (1.0 + slippage)).sum())
    sell_notional = float((-sells * prices * (1.0 - slippage)).sum())

    if buy_notional <= 1e-15:
        return trades_units, 1.0

    numer = cash0 + sell_notional * (1.0 - fee_rate) - min_cash
    denom = buy_notional * (1.0 + fee_rate)

    if denom <= 1e-15:
        return sells, 0.0

    alpha = max(0.0, min(1.0, numer / denom))
    if alpha >= 0.999999:
        return trades_units, 1.0

    adjusted = sells + buys * alpha
    return adjusted, alpha

def get_daily_funding_rate_series(ticker):
    """
    Real (un-negated) daily mean hourly funding rate for a ticker -- the actual
    rate that would be paid/received holding a perp, independent of whichever
    signal is driving the position. Positive = longs pay shorts. Reuses
    CarrySignal's stitched BitMEX/Hyperliquid loader since that IS the
    realized funding history for these instruments; returns empty if neither
    source covers the ticker (e.g. XTZ).
    """
    funding = forecast.CarrySignal().load_funding_series(ticker)
    if funding.dropna().empty:
        return pd.Series(dtype=float)
    return funding.resample('1D').mean().iloc[:, 0]

def tickers_with_forecast_data(candidate_tickers, forecasts):
    """
    Coin selection for a given signal set: keep a ticker only if at least one
    of the forecasts actually has usable data for it. Signals don't need to
    share a universe -- e.g. CarrySignal has no BitMEX/Hyperliquid funding data
    for XTZ, but EWMACSignal (price-only) covers it fine; a combined run should
    still trade XTZ on EWMAC alone rather than drop it, while a carry-only run
    has no choice but to drop it.
    """
    keep = []
    for ticker in candidate_tickers:
        has_data = False
        for f in forecasts:
            try:
                if not f(ticker).dropna().empty:
                    has_data = True
                    break
            except (FileNotFoundError, ValueError, KeyError):
                continue
        if has_data:
            keep.append(ticker)
        else:
            print(f"  {ticker}: dropped, no forecast in this set has data for it")
    return keep

def load_execution_price_series(ticker, venue):
    """Execution price series for the backtest's own trade simulation (separate
    from whatever price_source a signal itself uses to build its forecast)."""
    if venue == "kraken":
        df_px = ohlc_data.load_ohlc_data_to_df(ticker, selectCols=['open'])
    elif venue == "hyperliquid_perp":
        df_px = hyperliquid_price_data.load_hyperliquid_price_to_df(ticker, selectCols=['open'])
    else:
        raise ValueError(f"Unknown venue: {venue!r}")
    return df_px['open'].astype(float)

def run_backtest(tickers, forecasts, forecast_weights, output_name, long_only, venue="kraken", volatility_target=volatility_target):
    """
    venue: "kraken" (spot, no funding) or "hyperliquid_perp" (perp, funding
    cash flow applied daily). Controls both the execution price series used
    for this backtest's own trade simulation AND whether funding accrues --
    NOT the price_source each signal itself uses to build its forecast, which
    is set independently on each forecast object. Keep the two in sync unless
    you specifically want e.g. a Kraken-priced signal executed against
    Hyperliquid fills (unusual) -- a mismatch prints a warning below.
    """
    print(f"\n=== {output_name} ===")
    print("Universe:", tickers, " Venue:", venue)

    expected_price_source = "hyperliquid" if venue == "hyperliquid_perp" else "kraken"
    for f in forecasts:
        ps = getattr(f, "price_source", None)
        if ps is not None and ps != expected_price_source:
            print(f"  WARNING: {type(f).__name__} has price_source={ps!r} but venue={venue!r} normally pairs with {expected_price_source!r}")

    if venue == "hyperliquid_perp":
        print("  NOTE: fee_rate/slippage below are still Kraken-calibrated; Hyperliquid's real perp fee schedule isn't modeled yet.")

    instrument_weights = np.full(len(tickers), 1 / len(tickers))

    ## Build base targets (open-based)
    subsystem_portfolio_df = trading_system.run_trading_system(
        tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights,
        price_source=expected_price_source
    )
    base_capital = trading_capital
    subsystem_portfolio_df_base = subsystem_portfolio_df.copy()

    steps = None
    if ORDER_MIN and venue == "kraken":
        ordermin = pd.read_csv(
            "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/kraken_usd_pairs_ordermin.csv"
        ).set_index("Ticker")["OrderMin"]
        steps = ordermin.reindex(subsystem_portfolio_df_base.columns).dropna()
    elif ORDER_MIN:
        print("  NOTE: order-min rounding skipped -- Hyperliquid's own minimum order sizes aren't modeled yet.")

    # --- Build price_df from OPEN prices (as Series) and drop duplicate timestamps  ---
    # dropna(how='all') rather than the default: tickers can have very different listing
    # dates (e.g. BTC since 2023-02, XMR since 2026-01), and requiring every column
    # non-null would collapse the whole shared calendar down to the latest-listed ticker's
    # start date. Each ticker instead joins the day it actually has data.
    price_df = pd.DataFrame()
    for ticker in tickers:
        s = load_execution_price_series(ticker, venue)
        price_df = price_df.reindex(price_df.index.union(s.index)).sort_index()
        price_df[ticker] = s
    # ffill first: some tickers have real interior gaps (e.g. LTC's Kraken history has
    # ~179 missing days shortly after its 2013 listing) separate from the "not listed yet"
    # leading NaNs. Without this, a gap day would mark that ticker's held position at $0
    # (see the pd.notna guards below) even though it's not actually flat, then jump back to
    # its real value once data resumes -- an artificial cliff/rebound in portfolio value that
    # was never a real price move. ffill only ever propagates forward from a ticker's own
    # first valid price, so leading (not-yet-listed) NaNs are untouched.
    price_df = price_df.ffill().dropna(how='all')

    # Align price and targets
    common_index = subsystem_portfolio_df_base.index.intersection(price_df.index)
    subsystem_portfolio_df_base = subsystem_portfolio_df_base.loc[common_index]
    price_df = price_df.loc[common_index]

    # Real (un-negated) daily funding rate per ticker -- only accrues on the real perp.
    # Kraken spot has no funding, so this is left as all-zero for that venue.
    if venue == "hyperliquid_perp":
        funding_rate_df = pd.DataFrame({t: get_daily_funding_rate_series(t) for t in tickers})
        funding_rate_df = funding_rate_df.reindex(common_index).fillna(0.0)
    else:
        funding_rate_df = pd.DataFrame(0.0, index=common_index, columns=tickers)

    # Initialize positions (units) and cash
    positions = {ticker: 0.0 for ticker in subsystem_portfolio_df_base.columns}
    positions[quoteCurrency] = trading_capital

    portfolio_value_history = []
    equity_pre_history = []   # record equity before trading (T-1 units * T prices)
    cash_history = []
    fees_history = []
    slippage_cost_history = []
    funding_cashflow_history = []
    funding_by_ticker_history = []
    positions_history = []
    notional_history = []

    for date in common_index:
        prices = price_df.loc[date]

        # --- PRE-TRADE EQUITY at day T: cash_{T-1} + sum(units_{T-1} * price_T) ---
        cur_units_series = pd.Series({t: positions.get(t, 0.0) for t in prices.index})
        equity_pre = positions[quoteCurrency] + float((cur_units_series * prices).sum())
        equity_pre_history.append(equity_pre)

        # Scale today's target by PRE-TRADE equity (at T prices)
        scale = equity_pre / base_capital
        # Tickers not yet listed (no forecast/price history yet) are NaN here -- treat as
        # "no position" rather than propagating NaN into cash/position bookkeeping below.
        target_units = (subsystem_portfolio_df_base.loc[date] * scale).fillna(0.0)

        # Constraints AFTER scaling
        if long_only:
            target_units = target_units.clip(lower=0.0)

        if steps is not None:
            idx = steps.index.intersection(target_units.index)
            target_units.loc[idx] = (
                target_units.loc[idx].div(steps[idx]).round().mul(steps[idx])
            )

        # Current units and deltas
        current_units = pd.Series({ticker: positions[ticker] for ticker in target_units.index})
        delta_units = target_units - current_units
        denom = current_units.abs().replace(0, 1e-12)
        rel_gap = delta_units.abs() / denom
        trade_mask = rel_gap > TRADE_INERTIA
        trades_units = delta_units.where(trade_mask, 0.0)

        # Optional: guard against negative cash
        if AVOID_NEGATIVE_CASH:
            trades_units, alpha_used = enforce_cash_constraint(
                trades_units=trades_units,
                prices=prices,
                cash0=positions[quoteCurrency],
                fee_rate=fee_rate,
                slippage=slippage,
                min_cash=MIN_CASH_BUFFER
            )
            if alpha_used < 1.0:
                print(f"{date} — cash guard active: scaled BUY legs by alpha={alpha_used:.4f} to respect cash≥{MIN_CASH_BUFFER:.2f}")

        # Execute at open ± slippage, compute cash flows and fees
        buys  = trades_units.clip(lower=0.0)
        sells = trades_units.clip(upper=0.0)

        buy_mid_notional  = float((buys * prices).sum())
        sell_mid_notional = float((-sells * prices).sum())
        buy_exec_notional  = float((buys  * prices * (1.0 + slippage)).sum())
        sell_exec_notional = float((-sells * prices * (1.0 - slippage)).sum())
        slippage_cost = (buy_exec_notional - buy_mid_notional) + (sell_mid_notional - sell_exec_notional)

        trade_cost  = buy_exec_notional - sell_exec_notional
        trading_fee = fee_rate * (buy_exec_notional + sell_exec_notional)

        positions[quoteCurrency] -= trade_cost
        positions[quoteCurrency] -= trading_fee

        # Update units only where we traded
        for tkr in target_units.index:
            if trade_mask.loc[tkr]:
                positions[tkr] = positions[tkr] + trades_units.loc[tkr]

        # Funding cash flow for the day, on the post-trade (today's held) position.
        # Positive rate = longs pay shorts, so a long position's cash flow is negative.
        # daily_funding_rate * 24 approximates the day's ~24 hourly settlements.
        # A ticker not yet listed has NaN price but must have 0 position (never traded),
        # so its true contribution is 0 -- skip it rather than let 0 * NaN corrupt the sum.
        # Tracked per-ticker (not just the total) so performance can be broken down by coin later.
        funding_cashflow = 0.0
        funding_row = {}
        for tkr in target_units.index:
            if pd.isna(prices[tkr]):
                funding_row[tkr] = 0.0
                continue
            rate = funding_rate_df.at[date, tkr] if tkr in funding_rate_df.columns else 0.0
            tkr_funding = -positions[tkr] * prices[tkr] * rate * 24
            funding_row[tkr] = tkr_funding
            funding_cashflow += tkr_funding
        positions[quoteCurrency] += funding_cashflow
        funding_cashflow_history.append(funding_cashflow)
        funding_by_ticker_history.append(funding_row)

        # Mark to market at today's open (post-trade)
        notional_row = {t: (positions[t] * prices[t] if pd.notna(prices[t]) else 0.0) for t in target_units.index}
        notional_row[quoteCurrency] = positions[quoteCurrency]
        notional_row["Portfolio Value"] = sum(notional_row.values())
        notional_history.append(notional_row)

        portfolio_value = notional_row["Portfolio Value"]
        portfolio_value_history.append(portfolio_value)
        cash_history.append(positions[quoteCurrency])
        fees_history.append(trading_fee)
        slippage_cost_history.append(slippage_cost)
        positions_history.append(positions.copy())

    # Histories to DataFrames
    backtest_df = pd.DataFrame({
        "Equity_PreTrade": equity_pre_history,            # pre-trade equity at T
        "Portfolio Value": portfolio_value_history,       # post-trade equity at T
        "Cash": cash_history,
        "Funding_Cashflow": funding_cashflow_history,
        "Fees": fees_history,
        "Slippage_Cost": slippage_cost_history,
    }, index=common_index)

    positions_df = pd.DataFrame(positions_history, index=common_index)
    notional_df  = pd.DataFrame(notional_history,  index=common_index)
    funding_by_ticker_df = pd.DataFrame(funding_by_ticker_history, index=common_index)

    # Clean and standardize index -> tz-naive DatetimeIndex
    backtest_df = backtest_df[~backtest_df.index.duplicated(keep="last")]

    # Force to datetimes in UTC, drop non-parsable
    backtest_df.index = pd.to_datetime(backtest_df.index, utc=True, errors="coerce")
    backtest_df = backtest_df.loc[backtest_df.index.notna()]

    # Drop timezone to make QuantStats happy
    backtest_df.index = backtest_df.index.tz_convert(None)

    # Build returns
    strategy = backtest_df["Portfolio Value"]
    returns = strategy.pct_change()
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    qs.reports.html(returns, output=output_name + '.html')
    total_funding = float(backtest_df["Funding_Cashflow"].sum())
    print(f"CAGR: {qs.stats.cagr(returns):.2%}  Sharpe: {qs.stats.sharpe(returns):.2f}  Max DD: {qs.stats.max_drawdown(returns):.2%}  Total funding P&L: ${total_funding:,.2f}")

    return {
        "returns": returns,
        "backtest_df": backtest_df,
        "positions_df": positions_df,
        "notional_df": notional_df,
        "subsystem_portfolio_df": subsystem_portfolio_df,
        "price_df": price_df,
        "funding_by_ticker_df": funding_by_ticker_df,
    }

def compute_pnl_breakdown(result):
    """
    Per-coin daily P&L split into two buckets: price ("equity") P&L and funding
    P&L. Price P&L for day D = the position already held going into D (i.e.
    yesterday's post-trade units, since today's own trade hasn't experienced
    any price move yet) x the price change from D-1 to D. Funding P&L is the
    actual daily funding cash flow already tracked per ticker in run_backtest.
    Trading fees/slippage are NOT split into either bucket -- they're a
    separate cost, not part of "equity return" or "funding"; see the total
    reconciliation printed by print_pnl_breakdown.
    """
    positions_df = result["positions_df"]
    price_df = result["price_df"]
    funding_df = result["funding_by_ticker_df"]

    tickers = [c for c in positions_df.columns if c in price_df.columns]

    held_position = positions_df[tickers].shift(1).fillna(0.0)
    price_change = price_df[tickers].diff()
    price_pnl_df = held_position * price_change

    funding_pnl_df = funding_df[tickers].reindex(price_pnl_df.index).fillna(0.0)

    return price_pnl_df, funding_pnl_df

def summarize_pnl_by_coin(result):
    """One row per coin: total price P&L, total funding P&L, and their sum."""
    price_pnl_df, funding_pnl_df = compute_pnl_breakdown(result)

    summary = pd.DataFrame({
        "Price P&L": price_pnl_df.sum(),
        "Funding P&L": funding_pnl_df.sum(),
    })
    summary["Total P&L"] = summary["Price P&L"] + summary["Funding P&L"]
    return summary.sort_values("Total P&L", ascending=False)

def print_pnl_breakdown(result, label=""):
    summary = summarize_pnl_by_coin(result)
    total_price = summary["Price P&L"].sum()
    total_funding = summary["Funding P&L"].sum()
    total_fees = float(result["backtest_df"]["Fees"].sum())
    total_slippage = float(result["backtest_df"]["Slippage_Cost"].sum())

    print(f"\n=== P&L breakdown by coin: {label} ===")
    with pd.option_context('display.float_format', '{:,.2f}'.format):
        print(summary)

    equity_change = float(result["backtest_df"]["Portfolio Value"].iloc[-1] - result["backtest_df"]["Portfolio Value"].iloc[0])
    reconciled = total_price + total_funding - total_fees - total_slippage
    print(f"\nPrice P&L: ${total_price:,.2f}   Funding P&L: ${total_funding:,.2f}   Fees: ${-total_fees:,.2f}   Slippage: ${-total_slippage:,.2f}")
    print(f"Reconciled total: ${reconciled:,.2f}   Actual portfolio value change: ${equity_change:,.2f}   (diff: ${reconciled - equity_change:,.2f})")
    return summary


"""
#%% EWMAC-only, long/short
ewmac_forecasts = [forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3)]
ewmac_tickers = tickers_with_forecast_data(tickers, ewmac_forecasts)
ewmac_weights = np.array([1.0])
ewmac_result = run_backtest(ewmac_tickers, ewmac_forecasts, ewmac_weights, "EWMAC_LS_Test_V1", long_only=False)

#%% Carry-only, long/short
carry_forecasts = [forecast.CarrySignal()]
carry_tickers = tickers_with_forecast_data(tickers, carry_forecasts)
carry_weights = np.array([1.0])
carry_result = run_backtest(carry_tickers, carry_forecasts, carry_weights, "Carry_LS_Test_V1", long_only=False)

#%% Combined EWMAC + Carry, long/short (union universe -- XTZ trades EWMAC-only)
combined_forecasts_list = [forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3), forecast.CarrySignal()]
combined_tickers = tickers_with_forecast_data(tickers, combined_forecasts_list)
combined_weights = np.array([0.5, 0.5])
combined_result = run_backtest(combined_tickers, combined_forecasts_list, combined_weights, "Combined_LS_Test_V1", long_only=False)

#%% Carry-only, long/short, trading the real perp on Hyperliquid (funding applies)
carry_hl_forecasts = [forecast.CarrySignal(price_source="hyperliquid")]
carry_hl_tickers = tickers_with_forecast_data(tickers, carry_hl_forecasts)
carry_hl_weights = np.array([1.0])
carry_hl_result = run_backtest(carry_hl_tickers, carry_hl_forecasts, carry_hl_weights, "Carry_Hyperliquid_LS_Test_V1", long_only=False, venue="hyperliquid_perp")

"""
#%% Backtest mirroring run_carver_hyperliquid.py's live EWMAC_Carry_Hyperliquid_LS_V1
# strategy as closely as possible: same universe-selection rule (both price and funding
# data present and not stale -- which is what currently excludes AI16Z and TON, whose
# candleSnapshot data stopped in 2025-11-06 / 2026-06-15 even though funding kept
# settling), same signals/weights/start date, run here at 30% vol target instead of 50%.
hl_all_universe = run_carver_hyperliquid.select_hyperliquid_universe(coin_universe.load_universe())

ewmac_hl_forecasts = [forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3, start_date=run_carver_hyperliquid.start_ts, price_source="hyperliquid"),
                      forecast.CarrySignal(carry_span=8, forecast_scalar=30, start_date=run_carver_hyperliquid.start_ts, price_source="hyperliquid")]
ewmac_hl_tickers = tickers_with_forecast_data(hl_all_universe, ewmac_hl_forecasts)
ewmac_hl_weights = np.array([0.6, 0.4])
ewmac_hl_result = run_backtest(ewmac_hl_tickers, ewmac_hl_forecasts, ewmac_hl_weights, "EWMAC_Carry_Hyperliquid_LS_V1_Backtest_30vol", long_only=False, venue="hyperliquid_perp", volatility_target=0.30)
print_pnl_breakdown(ewmac_hl_result, "EWMAC+Carry, Hyperliquid perp, all coins, 30% vol")
# %%
