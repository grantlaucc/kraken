import numpy as np
import pandas as pd

def get_combined_forecasts(tickers, forecasts, forecast_weights):
    """
    Per-ticker forecast blend. Forecasts don't all need to cover the same
    universe -- e.g. a funding-rate-based signal may have no data for a ticker
    that a price-based signal covers fine. For each ticker, only the forecasts
    that actually have data are blended, with weights renormalized over just
    those; a ticker with no usable forecast at all is dropped entirely.
    """
    assert len(forecasts) == len(forecast_weights), "number of forecasts != number of forecast weights"
    assert np.isclose(forecast_weights.sum(), 1.0), "Forecast weights must sum to 1"

    combined_forecasts = {}

    for ticker in tickers:
        # Get forecast series from all models. A forecast can raise instead of
        # returning empty (e.g. a Hyperliquid-sourced signal for a ticker with
        # no Hyperliquid price/funding coverage) -- treat that the same as no data.
        signal_list = []
        for f in forecasts:
            try:
                signal_list.append(f(ticker))
            except (FileNotFoundError, ValueError, KeyError):
                signal_list.append(pd.DataFrame())
        usable = [i for i, s in enumerate(signal_list) if not s.dropna().empty]
        if not usable:
            print(f"  Skipping {ticker}: no forecast has usable data for it")
            continue

        sub_weights = forecast_weights[usable]
        sub_weights = sub_weights / sub_weights.sum()

        forecast_matrix = pd.concat([signal_list[i] for i in usable], axis=1)
        forecast_matrix.columns = [f"Model_{i}" for i in usable]

        # Average pairwise correlation
        corr_matrix = forecast_matrix.corr()
        H = corr_matrix.values.copy()
        H[H < 0] = 0  # floor negative correlations
        fdm = 1 / np.sqrt(np.dot(sub_weights.T, np.dot(H, sub_weights)))
        fdm = min(fdm, 2.5)

        # Weighted forecast, rescaled, clipped
        weighted_forecast = forecast_matrix.dot(sub_weights)
        rescaled_forecast = weighted_forecast * fdm
        combined_forecast = rescaled_forecast.clip(lower=-20, upper=20)

        combined_forecasts[ticker] = combined_forecast

    return pd.DataFrame(combined_forecasts)