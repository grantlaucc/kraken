import research.ohlc_data as ohlc_data
from research.Carver.plot_utils import plot_price_with_indicators
import pandas as pd
from datetime import datetime, timezone, timedelta
import sys
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding")
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding/Hyperliquid")
import bitmex_funding_data
import hyperliquid_funding_data
import hyperliquid_price_data

#fast and slow lookbacks
L_fast = 16
L_slow = 64
vol_span = 25
forecast_scalar = 3.75 #Carver Appendix B
lower_cap = -20
upper_cap = 20

def _load_price_series(price_source, ticker, columnName, start_date, end_date):
    """
    price_source: "kraken" (spot, via ohlc_data) or "hyperliquid" (perp, via
    hyperliquid_price_data). Kept as one dispatch point so both signal classes
    stay in sync on which venues exist.
    """
    if price_source == "kraken":
        return ohlc_data.load_ohlc_data_to_df(ticker, startDate=start_date, endDate=end_date, selectCols=[columnName])
    if price_source == "hyperliquid":
        return hyperliquid_price_data.load_hyperliquid_price_to_df(ticker, startDate=start_date, endDate=end_date, selectCols=[columnName])
    raise ValueError(f"Unknown price_source: {price_source!r}")

class EWMACSignal:
    def __init__(self, L_fast=16, L_slow=64, vol_span=25,
                 forecast_scalar=3.75, lower_cap=-20, upper_cap=20,
                 start_date=None, end_date=None, price_source="kraken"):
        self.L_fast = L_fast
        self.L_slow = L_slow
        self.vol_span = vol_span
        self.forecast_scalar = forecast_scalar
        self.lower_cap = lower_cap
        self.upper_cap = upper_cap
        self.start_date = start_date
        self.end_date = end_date
        self.price_source = price_source

    def __call__(self, ticker):
        price_series = self.load_price_series(ticker, "open")
        signal = self.get_signal(price_series)
        return signal

    def load_price_series(self, ticker, columnName):
        return _load_price_series(self.price_source, ticker, columnName, self.start_date, self.end_date)

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
    def __init__(self, carry_span=8, vol_span=25, forecast_scalar=30,
                 lower_cap=-20, upper_cap=20,
                 start_date=None, end_date=None, price_source="kraken"):
        self.carry_span = carry_span
        self.vol_span = vol_span
        self.forecast_scalar = forecast_scalar
        self.lower_cap = lower_cap
        self.upper_cap = upper_cap
        self.start_date = start_date
        self.end_date = end_date
        self.price_source = price_source
        pass
    
    def __call__(self, ticker):
        funding_series = self.load_funding_series(ticker)
        price_series = self.load_price_series(ticker, "open")
        signal = self.get_signal(funding_series, price_series)
        return signal

    def load_funding_series(self, ticker):
        """
        Hourly-rate funding series, stitched from two sources: the static
        BitMEX archive (bitmex_funding.csv, pre-converted from its native 8h-period
        rate to an hourly-equivalent rate) for history before Hyperliquid coverage
        begins, then Hyperliquid's native hourly rate from that point on. No longer
        pulls new BitMEX data live -- see run_carver.py.
        """
        base = ticker.split("/")[0]
        bitmex_ticker = bitmex_funding_data.convert_symbol(ticker)

        empty = pd.Series(dtype=float, name=ticker, index=pd.DatetimeIndex([], tz="UTC"))

        try:
            bitmex_df = bitmex_funding_data.load_funding_data_to_df(bitmex_ticker, self.start_date, self.end_date)
            bitmex_hourly = bitmex_df.iloc[:, 0].rename(ticker)
        except (FileNotFoundError, ValueError, KeyError):
            bitmex_hourly = empty

        try:
            hl_df = hyperliquid_funding_data.load_funding_data_to_df(base, self.start_date, self.end_date)
            hl_hourly = hl_df.iloc[:, 0].rename(ticker)
        except (FileNotFoundError, ValueError, KeyError):
            hl_hourly = empty

        if not hl_hourly.empty:
            # BitMEX archive only fills in the history before Hyperliquid coverage begins
            bitmex_hourly = bitmex_hourly[bitmex_hourly.index < hl_hourly.index.min()]

        combined = pd.concat([bitmex_hourly, hl_hourly]).sort_index()
        return combined.to_frame()

    def load_price_series(self, ticker, columnName):
        return _load_price_series(self.price_source, ticker, columnName, self.start_date, self.end_date)

    def get_signal(self, funding_series, price_series):
        daily_funding = funding_series.resample('1D').mean()
        #Negated: on a perp, positive funding means longs pay shorts (negative carry for a long),
        #so positive carry corresponds to a negative funding rate.
        annualized_funding = -daily_funding*24*365
        carry_price_points = (price_series.mul(annualized_funding.iloc[:, 0], axis=0).dropna())
        carry_ewma = carry_price_points.ewm(span=self.carry_span).mean()
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
