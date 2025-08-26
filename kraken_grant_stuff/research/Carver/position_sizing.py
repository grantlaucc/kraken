import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import ohlc_data
import numpy as np
import pandas as pd


def get_position_sizing(tickers, combined_forecasts_df, trading_capital, volatility_target, trading_frequency=365):
    daily_vol_target = trading_capital*volatility_target/np.sqrt(trading_frequency) #in dollars

    position_sizing_df = pd.DataFrame(index=combined_forecasts_df.index)

    for ticker in tickers:
        # Load close prices for this ticker
        price_series = ohlc_data.load_ohlc_data_to_df(ticker, startDate=combined_forecasts_df.index.min(),
    endDate=combined_forecasts_df.index.max(), selectCols=['close']).squeeze("columns")

        # EWMA volatility (35-day based on Carver pg. 254) of PRICES [$]
        daily_returns = price_series.diff()
        instrument_vol = daily_returns.ewm(span=35, adjust=False).std()

        vol_scalar = daily_vol_target / instrument_vol

        # Position = combined forecast × vol_scalar
        combined_forecast_series = combined_forecasts_df[ticker]
        position = (combined_forecast_series * vol_scalar)/10 #10 long run average of forecast
        position_sizing_df[ticker] = position

    return position_sizing_df