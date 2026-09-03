import sys
import research.ohlc_data as ohlc_data
import numpy as np
import pandas as pd
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding/Hyperliquid")
import hyperliquid_price_data


def _load_price_series(price_source, ticker, startDate, endDate):
    """price_source: "kraken" (spot) or "hyperliquid" (perp) -- mirrors forecast.py's dispatch,
    so the vol-targeting scalar is computed from whichever venue is actually being traded."""
    if price_source == "kraken":
        return ohlc_data.load_ohlc_data_to_df(ticker, startDate=startDate, endDate=endDate, selectCols=['open']).squeeze("columns")
    if price_source == "hyperliquid":
        return hyperliquid_price_data.load_hyperliquid_price_to_df(ticker, startDate=startDate, endDate=endDate, selectCols=['open']).squeeze("columns")
    raise ValueError(f"Unknown price_source: {price_source!r}")


def get_position_sizing(tickers, combined_forecasts_df, trading_capital, volatility_target, trading_frequency=365, price_source="kraken"):
    daily_vol_target = trading_capital*volatility_target/np.sqrt(trading_frequency) #in dollars

    position_sizing_df = pd.DataFrame(index=combined_forecasts_df.index)

    for ticker in tickers:
        # Load open prices for this ticker
        price_series = _load_price_series(price_source, ticker, combined_forecasts_df.index.min(), combined_forecasts_df.index.max())

        # EWMA volatility (35-day based on Carver pg. 254) of PRICES [$]
        daily_returns = price_series.diff()
        instrument_vol = daily_returns.ewm(span=35, adjust=False).std()

        vol_scalar = daily_vol_target / instrument_vol

        # Position = combined forecast × vol_scalar
        combined_forecast_series = combined_forecasts_df[ticker]
        position = (combined_forecast_series * vol_scalar)/10 #10 long run average of forecast
        position_sizing_df[ticker] = position

    return position_sizing_df