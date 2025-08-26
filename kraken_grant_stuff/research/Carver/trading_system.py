#%%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import forecast
import combined_forecast
import position_sizing
import subsystem_portfolio
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import ohlc_data
from datetime import datetime, timezone, timedelta


def run_trading_system(tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights):
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
                                                                        correlation_file = None)
    print("subsytem_portfolio_df complete")
    print(subsystem_portfolio_df.tail())
    

    final_display_df = pd.DataFrame({
        "Combined Forecast": combined_forecasts_df.iloc[-1],
        "Position Sizing": position_sizing_df.iloc[-1],
        "Subsystem Portfolio": subsystem_portfolio_df.iloc[-1]
    })
    last_close_dict = {}

    for ticker in tickers:
        last_close_series = ohlc_data.load_ohlc_data_to_df(ticker, startDate=combined_forecasts_df.index[-1], selectCols=["close"]).squeeze("columns")
        if not last_close_series.empty:
            last_close_dict[ticker] = last_close_series.iloc[0]

    notional_series = pd.Series({
        ticker: final_display_df.loc[ticker, "Subsystem Portfolio"] * last_close_dict[ticker]
        for ticker in final_display_df.index if ticker in last_close_dict
    })

    final_display_df["Notional"] = notional_series

    with pd.option_context('display.float_format', '{:,.4f}'.format):
        print(final_display_df)

    return subsystem_portfolio_df

if __name__ == "__main__":
    tickers = ['BTC/USD', 'ADA/USD', 'BCH/USD', 'XTZ/USD', 'ATOM/USD', 'LTC/USD', 'XMR/USD', 'DOGE/USD', 'LINK/USD', 'XRP/USD', 'ETH/USD']
    #tickers = ["BTC/USD", "ETH/USD"]
    forecasts = [forecast.EWMACSignal(L_fast=16, L_slow=64, forecast_scalar=3.75), 
                forecast.EWMACSignal(L_fast=32, L_slow=128, forecast_scalar=2.65)]
    forecast_weights = np.array([0.6, 0.4])
    trading_capital = 100000
    volatility_target = 0.50
    instrument_weights = np.full(len(tickers), 1 / len(tickers))

    df = run_trading_system(tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights)


'''
overwriteExisting = True

startDate = (datetime.now(timezone.utc) - timedelta(days=300)).replace(hour=0, minute=0, second=0, microsecond=0)
ewmac_cs_df = ewmac_cross_sectional(usdc_pairs, startDate=None)
if overwriteExisting:
    existing_df = pd.read_excel("ewmac_signals.xlsx", index_col=0, parse_dates=True)
    last_timestamp = existing_df.index.max()
    #Ensure last_timestamp is tz-aware (UTC)
    if last_timestamp.tzinfo is None:
        last_timestamp = last_timestamp.tz_localize("UTC")
    print(f"Last saved timestamp: {last_timestamp}")
    new_rows = ewmac_cs_df[ewmac_cs_df.index > last_timestamp]

    if new_rows.empty:
        print("No new rows to append.")
    else:
        print(f"Appending {len(new_rows)} new row(s).")
        updated_df = pd.concat([existing_df, new_rows])
        updated_df = updated_df[~updated_df.index.duplicated(keep='last')]
        updated_df.index = pd.to_datetime(updated_df.index, utc=True).tz_convert(None)# remove tz for Excel
        updated_df.to_excel("ewmac_signals.xlsx")

else:
    ewmac_cs_df.index = ewmac_cs_df.index.tz_convert(None) #remove timezones for safe excel writing
    ewmac_cs_df.to_excel("ewmac_signals2.xlsx")

'''
# %%
