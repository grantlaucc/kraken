import sys
import research.ohlc_data as ohlc_data
import numpy as np
import pandas as pd
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding/Hyperliquid")
import hyperliquid_price_data

def _load_price_series(price_source, ticker, startDate, endDate):
    """price_source: "kraken" (spot) or "hyperliquid" (perp) -- mirrors forecast.py's dispatch,
    so correlation is computed from whichever venue is actually being traded."""
    if price_source == "kraken":
        return ohlc_data.load_ohlc_data_to_df(ticker, startDate=startDate, endDate=endDate, selectCols=['open']).squeeze("columns")
    if price_source == "hyperliquid":
        return hyperliquid_price_data.load_hyperliquid_price_to_df(ticker, startDate=startDate, endDate=endDate, selectCols=['open']).squeeze("columns")
    raise ValueError(f"Unknown price_source: {price_source!r}")

def get_subsystem_portfolio(tickers, instrument_weights, position_sizing_df, correlation_file = None, dynamic_trading_system_adjustment=0.7, price_source="kraken"):
    assert len(tickers) == len(instrument_weights), "number of instruments != number of instrument weights"
    assert np.isclose(instrument_weights.sum(), 1.0), "Instrument weights must sum to 1"

    if correlation_file is not None:
        corr_matrix = pd.read_csv(correlation_file, index_col=0)
        corr_matrix = corr_matrix.loc[tickers, tickers]
    else:
        corr_matrix = get_corr_matrix(tickers, price_source=price_source)

    H = corr_matrix.values.copy()
    H[H < 0] = 0  # floor negative correlations
    idm = 1 / np.sqrt(np.dot(instrument_weights.T, np.dot(H, instrument_weights)))
    idm = min(idm, 2.5)  # Carver's cap -- sample correlations understate real co-movement in stress, so don't trust diversification benefit past this regardless of what the raw formula implies
    subsystem_portfolio_df = position_sizing_df*instrument_weights*idm
    return subsystem_portfolio_df

def get_corr_matrix(tickers, startDate = None, endDate = None, outputFile = None, price_source="kraken"):
    return_df = pd.DataFrame()
    for ticker in tickers:
        price_series = _load_price_series(price_source, ticker, startDate, endDate)
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
