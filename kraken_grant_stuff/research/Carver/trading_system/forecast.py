import research.ohlc_data as ohlc_data
from research.Carver.plot_utils import plot_price_with_indicators
import pandas as pd
from datetime import datetime, timezone, timedelta
import sys
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding")
import bitmex_funding_data

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
        price_series = self.load_price_series(ticker, "open")
        signal = self.get_signal(price_series)
        return signal

    def load_price_series(self, ticker, columnName):
        df = ohlc_data.load_ohlc_data_to_df(ticker, startDate=self.start_date, endDate=self.end_date, selectCols=[columnName])
        return df
    
    def get_signal(self, price_series):
        fast_ewma = price_series.ewm(span=self.L_fast).mean()
        slow_ewma = price_series.ewm(span=self.L_slow).mean()
        ewmac_raw = fast_ewma - slow_ewma

        price_returns = price_series.diff()
        vol = price_returns.ewm(span=self.vol_span, adjust=False).std()

        raw_signal = self.forecast_scalar * ewmac_raw / vol
        return raw_signal.clip(lower=self.lower_cap, upper=self.upper_cap)

    def plot(self, ticker):
        price_series = self.load_price_series(ticker, "open")
        signal = self.get_signal(price_series)
        plot_price_with_indicators(price_series, {"EWMAC": signal}, title=f"{ticker} Price and EWMAC Signal")
    '''
    # Plotting
    ax = df.plot(x='timestamp', y='close', label='Close', legend=True)
    df.plot(x='timestamp', y='ewmac_signal', secondary_y=True, ax=ax, label='EWMAC Signal', legend=True)
    plt.title(f"{ticker} Close Price and EWMAC Signal")

    plt.show() #blocking
    '''

class CarrySignal:
    def __init__(self, carry_span=8, vol_span=25, forecast_scalar=50,
                 lower_cap=-20, upper_cap=20, 
                 start_date=None, end_date=None):
        self.carry_span = carry_span
        self.vol_span = vol_span
        self.forecast_scalar = forecast_scalar
        self.lower_cap = lower_cap
        self.upper_cap = upper_cap
        self.start_date = start_date
        self.end_date = end_date
        pass
    
    def __call__(self, ticker):
        bitmexTicker = bitmex_funding_data.convert_symbol(ticker)
        funding_series = self.load_funding_series(bitmexTicker)
        price_series = self.load_price_series(ticker, "open")
        signal = self.get_signal(funding_series, price_series)
        return signal
    
    def load_funding_series(self, ticker):
        df = bitmex_funding_data.load_funding_data_to_df(ticker, self.start_date, self.end_date)
        return df
    
    def load_price_series(self, ticker, columnName):
        df = ohlc_data.load_ohlc_data_to_df(ticker, startDate=self.start_date, endDate=self.end_date, selectCols=[columnName])
        return df

    def get_signal(self, funding_series, price_series):
        #Average funding rate by day
        eight_hour_funding = funding_series.resample('1D').mean()
        #Annualize the funding rate (multiply by 3*365) and convert into price points (multiply by spot price)
        annualized_funding = eight_hour_funding*3*365
        carry_price_points = (price_series.mul(annualized_funding.iloc[:, 0], axis=0).dropna())
        #Get EWMA of daily funding rate
        carry_ewma = carry_price_points.ewm(span=self.vol_span).mean()
        #Get price returns and annualized price vol
        price_returns = price_series.diff()
        vol = price_returns.ewm(span=self.vol_span, adjust=False).std()
        vol*=(365**0.5)
        #Multiply EWMA funding by forecast scalar and divide by vol
        raw_signal = self.forecast_scalar * carry_ewma / vol
        clipped_signal = raw_signal.clip(lower=self.lower_cap, upper=self.upper_cap)
        #Shift the date by one day since today's signal is going to be based on the data up to yesterday
        signal = clipped_signal.shift(1)

        return signal
