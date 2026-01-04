#%%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import research.Carver.trading_system.forecast as forecast
import research.Carver.trading_system.combined_forecast as combined_forecast
import research.Carver.trading_system.position_sizing as position_sizing
import research.Carver.trading_system.subsystem_portfolio as subsystem_portfolio
import research.ohlc_data as ohlc_data
from datetime import datetime, timezone, timedelta
from research.Carver.file_helper import write_incremental_csv



def run_trading_system(tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights, correlation_file=None, save_dir=False):
    #Combined Forecast
    combined_forecasts_df = combined_forecast.get_combined_forecasts(tickers, forecasts, forecast_weights)
    print("combined_forecasts_df complete")
    print(combined_forecasts_df)

    #Volatility Targeting

    #Position Sizing
    position_sizing_df = position_sizing.get_position_sizing(tickers, combined_forecasts_df, trading_capital, volatility_target)
    print("position_sizing_df complete")
    print(position_sizing_df.tail())

    #Subsystem Portfolio
    subsystem_portfolio_df = subsystem_portfolio.get_subsystem_portfolio(tickers, instrument_weights, position_sizing_df, 
                                                                        correlation_file = correlation_file)
    print("subsytem_portfolio_df complete")
    print(subsystem_portfolio_df.tail())
    

    final_display_df = pd.DataFrame({
        "Combined Forecast": combined_forecasts_df.iloc[-1],
        "Position Sizing": position_sizing_df.iloc[-1],
        "Subsystem Portfolio": subsystem_portfolio_df.iloc[-1]
    })
    last_close_dict = {}

    for ticker in tickers:
        last_close_series = ohlc_data.load_ohlc_data_to_df(ticker, startDate=combined_forecasts_df.index[-1], selectCols=["open"]).squeeze("columns")
        if not last_close_series.empty:
            last_close_dict[ticker] = last_close_series.iloc[0]

    notional_series = pd.Series({
        ticker: final_display_df.loc[ticker, "Subsystem Portfolio"] * last_close_dict[ticker]
        for ticker in final_display_df.index if ticker in last_close_dict
    })

    final_display_df["Notional"] = notional_series

    with pd.option_context('display.float_format', '{:,.4f}'.format):
        print(final_display_df)

    if save_dir:
        # These frames look like your example (timestamp index, asset columns)
        info1 = write_incremental_csv(combined_forecasts_df, save_dir, "combined_forecasts.csv")
        info2 = write_incremental_csv(position_sizing_df, save_dir, "position_sizing.csv")
        info3 = write_incremental_csv(subsystem_portfolio_df, save_dir, "subsystem_portfolio.csv")
        print("[Data export]")
        print(f"  combined_forecasts -> {info1}")
        print(f"  position_sizing    -> {info2}")
        print(f"  subsystem_portfolio-> {info3}")

    return subsystem_portfolio_df

if __name__ == "__main__":
    end_ts = pd.Timestamp.now(tz="UTC").normalize()           # e.g., 2025-09-02 00:00:00+00:00
    start_ts = end_ts - pd.DateOffset(years=2)
    #tickers = ['BTC/USD', 'ADA/USD', 'BCH/USD', 'XTZ/USD', 'ATOM/USD', 'LTC/USD', 'XMR/USD', 'DOGE/USD', 'LINK/USD', 'XRP/USD', 'ETH/USD']
    tickers = ["BTC/USD", "ETH/USD"]
    forecasts = [forecast.EWMACSignal(L_fast=16, L_slow=64, forecast_scalar=3.75, start_date=start_ts), 
                forecast.EWMACSignal(L_fast=32, L_slow=128, forecast_scalar=2.65, start_date=start_ts)]
    forecast_weights = np.array([0.6, 0.4])
    trading_capital = 100000
    volatility_target = 0.50
    instrument_weights = np.full(len(tickers), 1 / len(tickers))

    df = run_trading_system(tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights)


