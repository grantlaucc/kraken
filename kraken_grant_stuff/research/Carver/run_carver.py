import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import argparse
import research.Carver.trading_system.forecast as forecast
import research.Carver.trading_system.trading_system as trading_system
from file_helper import write_incremental_csv
import run_carver_helper
import research.ohlc_data as ohlc_data
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding")
import bitmex_funding_data

STRATEGY_NAME = "EWMAC_8_32_LO_TEST_V5"

tickers = ['LTC/USD', 'LINK/USD', 'BCH/USD', 'ADA/USD', 'ETH/USD', 'XTZ/USD', 'ATOM/USD', 'XRP/USD', 'BTC/USD', 'DOGE/USD']
quoteCurrency = "USD"

end_ts = pd.Timestamp.now(tz="UTC").normalize()           # e.g., 2025-09-02 00:00:00+00:00
start_ts = end_ts - pd.DateOffset(years=2)

forecasts = [forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3, start_date=start_ts)]
forecast_weights = np.array([1])

trading_capital = 2500 #TODO
LIVE_POSITION_NOTIONAL = True 
volatility_target = 0.50
correlation_file = "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/Carver/correlations_2023_2025.csv"
strategy_folder = os.path.join("strategies", STRATEGY_NAME)
TRADE_INERTIA = 0.10    # only trade if |target - current| > 10% of |current|
LONG_ONLY = True
ORDER_MIN = True

instrument_weights = np.full(len(tickers), 1 / len(tickers))

def run_carver(skip_update: bool, notional_file: str | None = None, run_early: bool = False):
    ###Update Data
    if not skip_update:
        interval = 1440
        startTimestamp = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usd_pairs.txt", "r") as file:
            usdc_pairs = [line.strip() for line in file]
        if run_early:
            ohlc_data.download_run_early_data(interval=interval, tickers=usdc_pairs, startTimestamp=startTimestamp, column='close')
        else:
            ohlc_data.download_ohlc_data(interval=interval, tickers=usdc_pairs,startTimestamp=startTimestamp, new_only=True, column='open')
        #Download Bitmex funding data
        bitmex_tickers = [bitmex_funding_data.convert_symbol(t) for t in usdc_pairs]
        bitmex_funding_data.download_funding_data(tickers=bitmex_tickers)

    #TODO get trading capital based on today's prices and yesterday's real positions
    live_positions_filepath = os.path.join(strategy_folder, "live_positions.csv")
    todayNotional = run_carver_helper.updateYesterdayNotional(live_positions_filepath, tickers)
    print("TODAY NOTIONAL", todayNotional)

    ###Run 
    subsystem_portfolio_df = trading_system.run_trading_system(tickers, forecasts, forecast_weights, trading_capital, 
                                                               volatility_target, instrument_weights, 
                                                               correlation_file=correlation_file, save_dir=strategy_folder)
    
        ### Scale by notional time series (if provided)
    if notional_file:
        scale_series = run_carver_helper.load_notional_scale_series(
            path=notional_file,
            base_capital=trading_capital,
            align_index=subsystem_portfolio_df.index
        )
        # Multiply each day's target units by that day's (previous EOD) scale
        subsystem_portfolio_df = subsystem_portfolio_df.mul(scale_series, axis=0)
    
    elif LIVE_POSITION_NOTIONAL:
        scale_series = run_carver_helper.load_live_position_notional_scale_series(path=live_positions_filepath, 
                                                                                  base_capital=trading_capital,
                                                                                  align_index=subsystem_portfolio_df.index)
        subsystem_portfolio_df = subsystem_portfolio_df.mul(scale_series, axis=0)

    #TODO elif no notional file scale using live positions. Back fill notional series for dates subsystem_porfolio_df index dates

    if LONG_ONLY:
        subsystem_portfolio_df = subsystem_portfolio_df.clip(lower=0.0)
    if ORDER_MIN:
        ordermin = pd.read_csv("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/kraken_usd_pairs_ordermin.csv").set_index("Ticker")["OrderMin"]
        steps = ordermin.reindex(subsystem_portfolio_df.columns).dropna()
        subsystem_portfolio_df.loc[:, steps.index] = (subsystem_portfolio_df.loc[:, steps.index].div(steps, axis=1).round().mul(steps, axis=1))
    if TRADE_INERTIA>0:
        out = subsystem_portfolio_df.copy()
        # Make the very first row a valid baseline (choose your policy: 0 or keep)
        # Using 0.0 ensures we don't start with NaNs that propagate.
        out.iloc[0] = out.iloc[0].fillna(0.0)
        for i in range(1, len(out)):
            prev = out.iloc[i - 1]
            tgt  = out.iloc[i]
            # Relative change, but make denom robust:
            # - prev==0 -> inf relative change (update)
            # - prev is NaN -> inf relative change (update)
            denom = prev.abs()
            denom = denom.replace(0, np.nan)
            rel = (tgt - prev).abs().div(denom)
            rel = rel.replace([np.inf, -np.inf], np.inf).fillna(np.inf)
            # Update if relative change > inertia OR if prev was NaN and tgt is not
            mask = (rel > TRADE_INERTIA) | (prev.isna() & tgt.notna())
            # Where mask True -> take tgt; else keep prev
            out.iloc[i] = tgt.where(mask, prev)
        subsystem_portfolio_df = out
    subsystem_portfolio_df.round(decimals=6)
    info4 = write_incremental_csv(subsystem_portfolio_df, strategy_folder, "position_file.csv")
    print(f"  position_file -> {info4}")

    #TODO get deltas to trade based on current positions
    #TODO track executions
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-update",
        action="store_true",
        help="Skip refreshing prices"
    )
    parser.add_argument(
        "--notional-file",
        type=str,
        default=None,
        help="Path to CSV with time series of notionals/equity (first column = datetime)."
    )
    parser.add_argument(
        "--run-early",
        action="store_true",
        help="Run Carver early before typical execution time."
    )
    args = parser.parse_args()
    run_carver(args.skip_update, args.notional_file, args.run_early)