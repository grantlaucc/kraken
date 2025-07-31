import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import ohlc_data
from plot_utils import plot_price_with_indicators
import pandas as pd
from datetime import datetime, timezone, timedelta

#fast and slow lookbacks
L_fast = 16
L_slow = 64
vol_span = 25
forecast_scalar = 3.75 #Carver Appendix B
lower_cap = -20
upper_cap = 20

class EWMACSignal:
    def __init__(self, L_fast=16, L_slow=64, vol_span=25,
                 forecast_scalar=3.75, lower_cap=-20, upper_cap=20,
                 start_date=None, end_date=None):
        self.L_fast = L_fast
        self.L_slow = L_slow
        self.vol_span = vol_span
        self.forecast_scalar = forecast_scalar
        self.lower_cap = lower_cap
        self.upper_cap = upper_cap
        self.start_date = start_date
        self.end_date = end_date

    def __call__(self, ticker):
        price_series = self.load_price_series(ticker, "close")
        signal = self.get_signal(price_series)
        return signal

    def load_price_series(self, ticker, columnName):
        df = ohlc_data.load_ohlc_data_to_df(ticker, startDate=self.start_date, endDate=self.end_date)
        return df[columnName]
    
    def get_signal(self, price_series):
        fast_ewma = price_series.ewm(span=self.L_fast).mean()
        slow_ewma = price_series.ewm(span=self.L_slow).mean()
        ewmac_raw = fast_ewma - slow_ewma

        price_returns = price_series.diff()
        vol = price_returns.ewm(span=self.vol_span, adjust=False).std()

        raw_signal = self.forecast_scalar * ewmac_raw / vol
        return raw_signal.clip(lower=self.lower_cap, upper=self.upper_cap)

    def plot(self, ticker):
        price_series = self.load_price_series(ticker, "close")
        signal = self.get_signal(price_series)
        plot_price_with_indicators(price_series, {"EWMAC": signal}, title=f"{ticker} Price and EWMAC Signal")
    '''
    # Plotting
    ax = df.plot(x='timestamp', y='close', label='Close', legend=True)
    df.plot(x='timestamp', y='ewmac_signal', secondary_y=True, ax=ax, label='EWMAC Signal', legend=True)
    plt.title(f"{ticker} Close Price and EWMAC Signal")

    plt.show() #blocking
    '''

def ewmac_cross_sectional(tickers, startDate=None):
    ewmac_dict = {}

    for ticker in tickers:
        try:
            ewmac_series = ewmac(ticker, startDate = startDate)
            ewmac_series.name = ticker  # set column name
            ewmac_dict[ticker] = ewmac_series
        except Exception as e:
            print(f"Failed to process {ticker}: {e}")

    ewmac_df = pd.DataFrame(ewmac_dict)
    return ewmac_df



