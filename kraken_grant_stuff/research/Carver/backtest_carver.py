import sys
import os
import numpy as np
import pandas as pd
import forecast
import trading_system
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import ohlc_data
import matplotlib.pyplot as plt
import quantstats_lumi as qs
import yfinance as yf
import pair_selection

with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usdc_pairs.txt", "r") as file:
    usd_pairs = [line.strip() for line in file]

tickers = pair_selection.pairs_with_data_before("2020-01-01")
print("Running Backtest for: ", tickers)
quoteCurrency = "USD"
forecasts = [forecast.EWMACSignal(L_fast=16, L_slow=64, forecast_scalar=3.75), 
             forecast.EWMACSignal(L_fast=32, L_slow=128, forecast_scalar=2.65)]
forecast_weights = np.array([0.6, 0.4])

trading_capital = 10000 #TODO 
volatility_target = 0.50

instrument_weights = np.full(len(tickers), 1 / len(tickers))

subsystem_portfolio_df = trading_system.run_trading_system(tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights)


price_df = pd.DataFrame()
for ticker in tickers:
    price_df[ticker] = ohlc_data.load_ohlc_data_to_df(ticker, selectCols=['close'])
price_df = price_df.dropna()




# Align price and position data
common_index = subsystem_portfolio_df.index.intersection(price_df.index)
subsystem_portfolio_df = subsystem_portfolio_df.loc[common_index]
price_df = price_df.loc[common_index]

# Initialize positions dictionary
positions = {ticker: 0.0 for ticker in subsystem_portfolio_df.columns}
positions[quoteCurrency] = trading_capital

portfolio_value_history = []
cash_history = []
positions_history = []

for date in common_index:
    target_units = subsystem_portfolio_df.loc[date]
    prices = price_df.loc[date]

    # Calculate trades and trade cost
    trades_units = target_units - pd.Series({ticker: positions[ticker] for ticker in target_units.index})
    trade_cost = (trades_units * prices).sum()

    # Update cash
    positions[quoteCurrency] -= trade_cost

    # Update positions
    for ticker in target_units.index:
        positions[ticker] = target_units[ticker]

    # Calculate portfolio value
    portfolio_value = sum(positions[ticker] * prices[ticker] for ticker in target_units.index) + positions[quoteCurrency]
    portfolio_value_history.append(portfolio_value)
    cash_history.append(positions[quoteCurrency])

    # Record positions (deep copy so it doesn't mutate)
    positions_history.append(positions.copy())

# Convert portfolio value and cash to DataFrame
backtest_df = pd.DataFrame({
    "Portfolio Value": portfolio_value_history,
    "Cash": cash_history
}, index=common_index)

# Convert positions history to a DataFrame
positions_df = pd.DataFrame(positions_history, index=common_index)

strategy = backtest_df["Portfolio Value"]
returns = strategy.pct_change().dropna()
returns.index = returns.index.tz_convert(None)

# Now safe to call full()
qs.reports.html(returns, output='strategy_tearsheet.html')

#plt.figure(figsize=(12, 6))
#plt.plot(backtest_df["Portfolio Value"], label="Portfolio Value")
##plt.plot(backtest_df["Cash"], label="Cash", linestyle="--")
#plt.ylabel("USD")
#plt.title("Backtest with Position Tracking")
#plt.legend()
#plt.grid(True)
#plt.tight_layout()
#plt.show()
