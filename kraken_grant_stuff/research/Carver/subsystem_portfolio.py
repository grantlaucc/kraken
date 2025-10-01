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
        corr_matrix = get_corr_matrix(tickers)

    H = corr_matrix.values.copy()
    H[H < 0] = 0  # floor negative correlations
    idm = 1 / np.sqrt(np.dot(instrument_weights.T, np.dot(H, instrument_weights)))
    subsystem_portfolio_df = position_sizing_df*instrument_weights*idm
    return subsystem_portfolio_df

def get_corr_matrix(tickers, startDate = None, endDate = None, outputFile = None):
    return_df = pd.DataFrame()
    for ticker in tickers:
        price_series = ohlc_data.load_ohlc_data_to_df(ticker, startDate=startDate, endDate=endDate, selectCols=['close']).squeeze("columns")
        return_series = price_series.pct_change(fill_method=None).rename(ticker)
        return_df = pd.concat([return_df, return_series], axis=1)
    corr_matrix = return_df.corr()

    if outputFile is not None:
        corr_matrix.to_csv(outputFile)

    return corr_matrix

if __name__ == "__main__":
    with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usd_pairs.txt", "r") as file:
        usd_pairs = [line.strip() for line in file]
    startDate = "2021-01-01T00:00:00Z"
    endDate = "2022-12-31T23:59:59Z"
    print(get_corr_matrix(usd_pairs, startDate, endDate, "correlations_2021_2023.csv"))
