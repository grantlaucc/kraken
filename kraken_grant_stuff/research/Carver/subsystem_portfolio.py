import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import ohlc_data
import numpy as np
import pandas as pd

def get_subsystem_portfolio(tickers, instrument_weights, position_sizing_df, correlation_file = None, dynamic_trading_system_adjustment=0.7):
    assert len(tickers) == len(instrument_weights), "number of instruments != number of instrument weights"
    assert np.isclose(instrument_weights.sum(), 1.0), "Instrument weights must sum to 1"

    if correlation_file is not None:
        corr_matrix = pd.read_csv(correlation_file, index_col=0)
        corr_matrix = corr_matrix.loc[tickers, tickers]
    else:
        return_df = pd.DataFrame()
        for ticker in tickers:
            price_series = ohlc_data.load_ohlc_data_to_df(ticker, selectCols=['close']).squeeze("columns")
            return_series = price_series.pct_change().rename(ticker)
            return_df = pd.concat([return_df, return_series], axis=1)
        
        corr_matrix = return_df.corr()

    H = corr_matrix.values.copy()
    H[H < 0] = 0  # floor negative correlations
    idm = 1 / np.sqrt(np.dot(instrument_weights.T, np.dot(H, instrument_weights)))
    subsystem_portfolio_df = position_sizing_df*instrument_weights*idm
    return subsystem_portfolio_df

#with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usdc_pairs.txt", "r") as file:
    #usdc_pairs = [line.strip() for line in file]

#print(get_subsystem_portfolio(usdc_pairs))
#corr_matrix.to_csv("correlation_matrix.csv")
