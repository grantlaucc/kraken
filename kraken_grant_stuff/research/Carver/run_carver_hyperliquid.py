import sys
import os
import json
import numpy as np
import pandas as pd
import argparse
import research.Carver.trading_system.forecast as forecast
import research.Carver.trading_system.trading_system as trading_system
import research.Carver.trading_system.coin_universe as coin_universe
from file_helper import write_incremental_csv
import research.ohlc_data as ohlc_data
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding")
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding/Hyperliquid")
import hyperliquid_price_data
import hyperliquid_funding_data
import update_hyperliquid_data

STRATEGY_NAME = "EWMAC_Carry_Hyperliquid_LS_V1"

# Hyperliquid's oldest real perp data (BTC/ETH/ATOM) starts 2023-02-26 -- floor the
# lookback there (comfortably more than the "at least 2 years" requirement as of any
# run date past ~2025). Younger coins just pick up NaN-padded from their own inception,
# same as everywhere else in this codebase (see backtest_carver.py's price_df handling).
start_ts = pd.Timestamp("2023-01-01", tz="UTC")

forecasts = [
    forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3, start_date=start_ts, price_source="hyperliquid"),
    forecast.CarrySignal(carry_span=8, forecast_scalar=30, start_date=start_ts, price_source="hyperliquid"),
]
forecast_weights = np.array([0.5, 0.5])

trading_capital = 2500  # TODO
volatility_target = 0.50
strategy_folder = os.path.join("strategies", STRATEGY_NAME)
TRADE_INERTIA = 0.10    # only trade if |target - current| > 10% of |current|
LONG_ONLY = False        # can trade long/short
ORDER_MIN = True         # round to Hyperliquid's own per-coin minimum size increments
MAX_DATA_STALENESS_DAYS = 5  # a coin whose HL price or funding data hasn't updated in this long is dropped


def select_hyperliquid_universe(coins: list[coin_universe.Coin], max_staleness_days: int = MAX_DATA_STALENESS_DAYS) -> list[str]:
    """
    Universe = any coin with BOTH real Hyperliquid price data and Hyperliquid funding
    data currently being updated. Checked directly against the data files (not just
    coin_universe.csv metadata) since a coin can have a hyperliquid_ticker on record but
    have stopped receiving price updates -- confirmed for TON/AI16Z, whose candleSnapshot
    data stalled months ago even though funding kept settling. That combination would
    otherwise leave a coin's EWMAC forecast permanently stale, so it's excluded here
    rather than hardcoded, so this self-corrects as coverage changes going forward.
    """
    now = pd.Timestamp.now(tz="UTC")
    usable = []
    for c in coins:
        if c.hyperliquid_ticker is None:
            continue
        ticker = f"{c.base}/USD"
        try:
            price = hyperliquid_price_data.load_hyperliquid_price_to_df(ticker, selectCols=["open"])["open"].dropna()
            funding = hyperliquid_funding_data.load_funding_data_to_df(c.base).iloc[:, 0].dropna()
        except (FileNotFoundError, ValueError, KeyError):
            continue
        if price.empty or funding.empty:
            continue
        price_stale = (now - price.index.max()) > pd.Timedelta(days=max_staleness_days)
        funding_stale = (now - funding.index.max()) > pd.Timedelta(days=max_staleness_days)
        if price_stale or funding_stale:
            print(f"  {c.base}: skipping, stale data (price last {price.index.max().date()}, funding last {funding.index.max().date()})")
            continue
        usable.append(ticker)
    return usable


def run_carver(skip_update: bool, run_early: bool = False):
    ###Update Data
    if not skip_update:
        interval = 1440
        with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usd_pairs.txt", "r") as file:
            usdc_pairs = [line.strip() for line in file]
        startTimestamp = int(start_ts.timestamp())
        # Not every symbol in usd_pairs.txt is actually a live Kraken pair (e.g. AI16Z has
        # no Kraken listing) -- get_ohlc_data raises hard on an invalid pair, so filter down
        # to ones coin_universe.csv confirmed against Kraken's own AssetPairs API.
        known_kraken_pairs = {c.kraken_ticker for c in coin_universe.load_universe() if c.kraken_ticker is not None}
        usdc_pairs = [p for p in usdc_pairs if p in known_kraken_pairs]
        if run_early:
            ohlc_data.download_run_early_data(interval=interval, tickers=usdc_pairs, startTimestamp=startTimestamp, column='close')
        else:
            ohlc_data.download_ohlc_data(interval=interval, tickers=usdc_pairs, startTimestamp=startTimestamp, new_only=True, column='open')

        # Hyperliquid price + funding, incremental from each file's own last saved
        # timestamp (full backfill for any coin with no existing column yet).
        hl_bases = [c.base for c in coin_universe.load_universe() if c.hyperliquid_ticker is not None]
        update_hyperliquid_data.update_funding(hl_bases)
        update_hyperliquid_data.update_price(hl_bases)

    ###Universe
    coins = coin_universe.load_universe()
    tickers = select_hyperliquid_universe(coins)
    print(f"Universe ({len(tickers)} coins with live price+funding data): {tickers}")
    instrument_weights = np.full(len(tickers), 1 / len(tickers))

    ###Run
    subsystem_portfolio_df = trading_system.run_trading_system(
        tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights,
        correlation_file=None, save_dir=strategy_folder, price_source="hyperliquid"
    )

    if LONG_ONLY:
        subsystem_portfolio_df = subsystem_portfolio_df.clip(lower=0.0)
    if ORDER_MIN:
        hl_min_size = {f"{c.base}/USD": c.hyperliquid_min_size for c in coins if c.hyperliquid_min_size is not None}
        steps = pd.Series(hl_min_size).reindex(subsystem_portfolio_df.columns).dropna()
        subsystem_portfolio_df.loc[:, steps.index] = (
            subsystem_portfolio_df.loc[:, steps.index].div(steps, axis=1).round().mul(steps, axis=1)
        )
    if TRADE_INERTIA > 0:
        out = subsystem_portfolio_df.copy()
        out.iloc[0] = out.iloc[0].fillna(0.0)
        for i in range(1, len(out)):
            prev = out.iloc[i - 1]
            tgt = out.iloc[i]
            denom = prev.abs()
            denom = denom.replace(0, np.nan)
            rel = (tgt - prev).abs().div(denom)
            rel = rel.replace([np.inf, -np.inf], np.inf).fillna(np.inf)
            mask = (rel > TRADE_INERTIA) | (prev.isna() & tgt.notna())
            out.iloc[i] = tgt.where(mask, prev)
        subsystem_portfolio_df = out
    subsystem_portfolio_df.round(decimals=6)
    info4 = write_incremental_csv(subsystem_portfolio_df, strategy_folder, "position_file.csv")
    print(f"  position_file -> {info4}")

    # Lets carver_trader.py (--venue hyperliquid) discover this strategy's base capital without
    # hardcoding it a second place -- it scales TARGET_POS by (live account value / trading_capital)
    # at trade-plan time using this file, rather than baking a lagged scale factor into
    # position_file.csv itself the way the Kraken live_positions.csv flow does.
    meta_path = os.path.join(strategy_folder, "strategy_meta.json")
    with open(meta_path, "w") as f:
        json.dump({"trading_capital": trading_capital, "venue": "hyperliquid"}, f, indent=2)
    print(f"  strategy_meta -> {meta_path}")

    #TODO get deltas to trade based on current positions
    #TODO track executions


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-update",
        action="store_true",
        help="Skip refreshing prices/funding"
    )
    parser.add_argument(
        "--run-early",
        action="store_true",
        help="Run Carver early before typical execution time."
    )
    args = parser.parse_args()
    run_carver(args.skip_update, args.run_early)
