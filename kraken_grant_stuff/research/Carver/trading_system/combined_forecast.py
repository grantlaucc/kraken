import numpy as np
import pandas as pd

def get_combined_forecasts(tickers, forecasts, forecast_weights):
    assert len(forecasts) == len(forecast_weights), "number of forecasts != number of forecast weights"
    assert np.isclose(forecast_weights.sum(), 1.0), "Forecast weights must sum to 1"

    combined_forecasts = {}

    for ticker in tickers:
        # Get forecast series from all models
        signal_list = [forecast(ticker) for forecast in forecasts]
        forecast_matrix = pd.concat(signal_list, axis=1)
        forecast_matrix.columns = [f"Model_{i}" for i in range(len(forecasts))]

        # Average pairwise correlation
        corr_matrix = forecast_matrix.corr()
        H = corr_matrix.values.copy()
        H[H < 0] = 0  # floor negative correlations
        fdm = 1 / np.sqrt(np.dot(forecast_weights.T, np.dot(H, forecast_weights)))
        fdm = min(fdm, 2.5)

        # Weighted forecast, rescaled, clipped
        weighted_forecast = forecast_matrix.dot(forecast_weights)
        rescaled_forecast = weighted_forecast * fdm
        combined_forecast = rescaled_forecast.clip(lower=-20, upper=20)

        combined_forecasts[ticker] = combined_forecast

    return pd.DataFrame(combined_forecasts)