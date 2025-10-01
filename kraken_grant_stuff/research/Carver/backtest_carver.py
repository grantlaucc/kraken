#%%
import sys
import os
import numpy as np
import pandas as pd
import forecast
import trading_system
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import ohlc_data
import matplotlib.pyplot as plt
#import quantstats_lumi as qs
import yfinance as yf
import pair_selection

STRATEGY_NAME = "EWMAC_8_32_LO_TEST_V2"

tickers = pair_selection.pairs_with_data_before("2020-01-01")
print("Running Backtest for: ", tickers)
quoteCurrency = "USD"
forecasts = [forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3)]
forecast_weights = np.array([1])

trading_capital = 2500 #TODO 
volatility_target = 0.50
fee_rate = 0.006 #Kraken trading fees
TRADE_INERTIA = 0.10    # only trade if |target - current| > 10% of |current|
LONG_ONLY = True
ORDER_MIN = True
AVOID_NEGATIVE_CASH = False
MIN_CASH_BUFFER = 0.0

instrument_weights = np.full(len(tickers), 1 / len(tickers))

##Backetest Helper Functions
def log_trades(date, current_units: pd.Series, target_units: pd.Series, trades_units: pd.Series):
    moved = trades_units[trades_units != 0.0]
    if moved.empty:
        return
    print(f"\n{date} — trades")
    for t in moved.index:
        cur = float(current_units.get(t, 0.0))
        tgt = float(target_units.get(t, 0.0))
        act = float(trades_units[t])
        print(f"  {t:12s}: {cur:.6f} → {tgt:.6f} | action {act:+.6f}")

def enforce_cash_constraint(trades_units: pd.Series,
                            prices: pd.Series,
                            cash0: float,
                            fee_rate: float,
                            min_cash: float = 0.0) -> tuple[pd.Series, float]:
    """
    Ensures end-of-trade cash >= min_cash by scaling BUY legs only.
    Returns (adjusted_trades_units, alpha_used) where alpha in [0,1].
    """
    if trades_units.empty:
        return trades_units, 1.0

    # Split buys/sells
    buys = trades_units.clip(lower=0.0)
    sells = trades_units.clip(upper=0.0)

    # Dollar notionals (all nonnegative)
    buy_notional  = float((buys  * prices).sum())        # == cost_buys
    sell_notional = float((-sells * prices).sum())       # == cash_from_sells

    # No buys -> cash only improves or stays; nothing to scale
    if buy_notional <= 1e-15:
        return trades_units, 1.0

    # Cash after applying alpha to BUY legs (SELLS unchanged):
    # cash_after(alpha) = cash0
    #                   + sell_notional
    #                   - alpha * buy_notional
    #                   - fee_rate * (alpha * buy_notional + sell_notional)
    #                   = cash0 + sell_notional*(1 - fee_rate)
    #                     - alpha * buy_notional*(1 + fee_rate)
    numer = cash0 + sell_notional * (1.0 - fee_rate) - min_cash
    denom = buy_notional * (1.0 + fee_rate)

    if denom <= 1e-15:
        # Degenerate, but guard anyway
        return sells, 0.0

    # Choose smallest shrink alpha in [0,1] that satisfies cash_after >= min_cash
    alpha = max(0.0, min(1.0, numer / denom))

    if alpha >= 0.999999:
        return trades_units, 1.0

    adjusted = sells + buys * alpha
    return adjusted, alpha

##End of Backtest Helper Functions

subsystem_portfolio_df = trading_system.run_trading_system(tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights)
base_capital = trading_capital  # e.g., 2500 you already passed into run_trading_system
subsystem_portfolio_df_base = subsystem_portfolio_df.copy()

steps = None
if ORDER_MIN:
    ordermin = pd.read_csv("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/kraken_usd_pairs_ordermin.csv")\
                 .set_index("Ticker")["OrderMin"]
    steps = ordermin.reindex(subsystem_portfolio_df_base.columns).dropna()

price_df = pd.DataFrame()
for ticker in tickers:
    price_df[ticker] = ohlc_data.load_ohlc_data_to_df(ticker, selectCols=['close'])
price_df = price_df.dropna()


# Align price and position data
common_index = subsystem_portfolio_df_base.index.intersection(price_df.index)
subsystem_portfolio_df_base = subsystem_portfolio_df_base.loc[common_index]
price_df = price_df.loc[common_index]

# Initialize positions dictionary
positions = {ticker: 0.0 for ticker in subsystem_portfolio_df_base.columns}
positions[quoteCurrency] = trading_capital

portfolio_value_history = []
cash_history = []
fees_history = []
positions_history = []
notional_history = []

for date in common_index:
    prices = price_df.loc[date]

    # Scale today's target by yesterday's equity
    equity_tm1 = portfolio_value_history[-1] if portfolio_value_history else base_capital
    scale = equity_tm1 / base_capital

    # Get base plan for today and scale it
    target_units = subsystem_portfolio_df_base.loc[date] * scale

    # Apply constraints AFTER scaling (recommended)
    if LONG_ONLY:
        target_units = target_units.clip(lower=0.0)

    if steps is not None:
        idx = steps.index.intersection(target_units.index)
        target_units.loc[idx] = (
            target_units.loc[idx]
            .div(steps[idx]).round().mul(steps[idx])
        )

    # Current units for these tickers
    current_units = pd.Series({ticker: positions[ticker] for ticker in target_units.index})
    # Compute deltas
    delta_units = target_units - current_units
    # Relative gap vs. current position; if current is 0, treat any nonzero target as 100% away
    denom = current_units.abs().replace(0, 1e-12)
    rel_gap = delta_units.abs() / denom
    # Only trade where gap > threshold
    trade_mask = rel_gap > TRADE_INERTIA
    trades_units = delta_units.where(trade_mask, 0.0)

    if AVOID_NEGATIVE_CASH:
        trades_units, alpha_used = enforce_cash_constraint(
            trades_units=trades_units,
            prices=prices,
            cash0=positions[quoteCurrency],
            fee_rate=fee_rate,
            min_cash=MIN_CASH_BUFFER
        )
        if alpha_used < 1.0:
            print(f"{date} — cash guard active: scaled BUY legs by alpha={alpha_used:.4f} to respect cash≥{MIN_CASH_BUFFER:.2f}")

    # Optional: log what will actually be traded
    #log_trades(date, current_units, target_units, trades_units)

    # Calculate trades and trade cost
    #trades_units = target_units - pd.Series({ticker: positions[ticker] for ticker in target_units.index})
    trade_cost = (trades_units * prices).sum()

    notional_traded = (trades_units.abs() * prices).sum()
    trading_fee = fee_rate * notional_traded

    # Update cash
    positions[quoteCurrency] -= trade_cost
    positions[quoteCurrency] -= trading_fee

    # Update positions ONLY for traded tickers
    for ticker in target_units.index:
        if trade_mask.loc[ticker]:
            positions[ticker] = positions[ticker] + trades_units.loc[ticker]
        # else: leave unchanged
    
    notional_row = {t: positions[t] * prices[t] for t in target_units.index}  # asset notionals
    notional_row[quoteCurrency] = positions[quoteCurrency]                    # cash
    notional_row["Portfolio Value"] = sum(notional_row.values())
    notional_history.append(notional_row)

    # Calculate portfolio value
    portfolio_value = notional_row["Portfolio Value"]
    portfolio_value_history.append(portfolio_value)
    cash_history.append(positions[quoteCurrency])
    fees_history.append(trading_fee)

    # Record positions (deep copy so it doesn't mutate)
    positions_history.append(positions.copy())

# Convert portfolio value and cash to DataFrame
backtest_df = pd.DataFrame({
    "Portfolio Value": portfolio_value_history,
    "Cash": cash_history
}, index=common_index)

# Convert positions history to a DataFrame
positions_df = pd.DataFrame(positions_history, index=common_index)

#build notional_df
notional_df = pd.DataFrame(notional_history, index=common_index)

strategy = backtest_df["Portfolio Value"]
returns = strategy.pct_change().dropna()
returns.index = returns.index.tz_convert(None)

# Now safe to call full()
qs.reports.html(returns, output=STRATEGY_NAME+'.html')



# %%
backtest_df.to_csv("backtest_df.csv")
notional_df.to_csv("notional_df.csv")
subsystem_portfolio_df.to_csv("subsystem_portfolio_df.csv")
# %%
