#%%
import numpy as np
import pandas as pd
import research.Carver.trading_system.forecast as forecast
import research.Carver.trading_system.trading_system as trading_system
import research.ohlc_data as ohlc_data
import matplotlib.pyplot as plt
import quantstats_lumi as qs
import yfinance as yf
import pair_selection

STRATEGY_NAME = "EWMAC_8_32_LO_TEST_V4"

tickers = pair_selection.pairs_with_data_before("2020-01-01")
tickers = ['LTC/USD', 'LINK/USD', 'BCH/USD', 'ADA/USD', 'ETH/USD', 'XTZ/USD', 'ATOM/USD', 'XRP/USD', 'BTC/USD', 'DOGE/USD']

#tickers = ['LTC/USD', 'BCH/USD', 'ADA/USD', 'ETH/USD', 'XRP/USD', 'BTC/USD', 'DOGE/USD', 'SOL/USD'] #Funding Tickers
print("Running Backtest for: ", tickers)
quoteCurrency = "USD"
forecasts = [forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3)]
#forecasts = [forecast.EWMACSignal(L_fast=8, L_slow=32, forecast_scalar=5.3), forecast.CarrySignal()]
forecast_weights = np.array([1.0])

trading_capital = 2500  # TODO
volatility_target = 0.50
fee_rate = 0.004      # Kraken trading fees
slippage = 0.002
TRADE_INERTIA = 0.10  # only trade if |target - current| > 10% of |current|
LONG_ONLY = True
ORDER_MIN = True
AVOID_NEGATIVE_CASH = False
MIN_CASH_BUFFER = 0.0

instrument_weights = np.full(len(tickers), 1 / len(tickers))

## Backtest Helper Functions
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
                            slippage: float = 0.0,
                            min_cash: float = 0.0) -> tuple[pd.Series, float]:
    """
    Keep end-of-trade cash >= min_cash by scaling BUY legs only.
    Returns (adjusted_trades_units, alpha_used) where alpha in [0,1].
    """
    if trades_units.empty:
        return trades_units, 1.0

    buys  = trades_units.clip(lower=0.0)
    sells = trades_units.clip(upper=0.0)

    buy_notional  = float((buys  * prices * (1.0 + slippage)).sum())
    sell_notional = float((-sells * prices * (1.0 - slippage)).sum())

    if buy_notional <= 1e-15:
        return trades_units, 1.0

    numer = cash0 + sell_notional * (1.0 - fee_rate) - min_cash
    denom = buy_notional * (1.0 + fee_rate)

    if denom <= 1e-15:
        return sells, 0.0

    alpha = max(0.0, min(1.0, numer / denom))
    if alpha >= 0.999999:
        return trades_units, 1.0

    adjusted = sells + buys * alpha
    return adjusted, alpha

## Build base targets (open-based)
subsystem_portfolio_df = trading_system.run_trading_system(
    tickers, forecasts, forecast_weights, trading_capital, volatility_target, instrument_weights
)
base_capital = trading_capital
subsystem_portfolio_df_base = subsystem_portfolio_df.copy()

steps = None
if ORDER_MIN:
    ordermin = pd.read_csv(
        "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/kraken_usd_pairs_ordermin.csv"
    ).set_index("Ticker")["OrderMin"]
    steps = ordermin.reindex(subsystem_portfolio_df_base.columns).dropna()

# --- Build price_df from OPEN prices (as Series) and drop duplicate timestamps  ---  # <<< CHANGED
price_df = pd.DataFrame()
for ticker in tickers:
    df_px = ohlc_data.load_ohlc_data_to_df(ticker, selectCols=['open'])
    s = df_px['open'].astype(float)
    price_df = price_df.reindex(price_df.index.union(s.index)).sort_index()
    price_df[ticker] = s
price_df = price_df.dropna()

# Align price and targets
common_index = subsystem_portfolio_df_base.index.intersection(price_df.index)
subsystem_portfolio_df_base = subsystem_portfolio_df_base.loc[common_index]
price_df = price_df.loc[common_index]

# Initialize positions (units) and cash
positions = {ticker: 0.0 for ticker in subsystem_portfolio_df_base.columns}
positions[quoteCurrency] = trading_capital

portfolio_value_history = []
equity_pre_history = []   # <<< CHANGED: record equity before trading (T-1 units * T prices)
cash_history = []
fees_history = []
positions_history = []
notional_history = []

for date in common_index:
    prices = price_df.loc[date]

    # --- PRE-TRADE EQUITY at day T: cash_{T-1} + sum(units_{T-1} * price_T) ---  # <<< CHANGED
    cur_units_series = pd.Series({t: positions.get(t, 0.0) for t in prices.index})
    equity_pre = positions[quoteCurrency] + float((cur_units_series * prices).sum())
    equity_pre_history.append(equity_pre)

    # Scale today's target by PRE-TRADE equity (at T prices)                      # <<< CHANGED
    scale = equity_pre / base_capital
    target_units = subsystem_portfolio_df_base.loc[date] * scale

    # Constraints AFTER scaling
    if LONG_ONLY:
        target_units = target_units.clip(lower=0.0)

    if steps is not None:
        idx = steps.index.intersection(target_units.index)
        target_units.loc[idx] = (
            target_units.loc[idx].div(steps[idx]).round().mul(steps[idx])
        )

    # Current units and deltas
    current_units = pd.Series({ticker: positions[ticker] for ticker in target_units.index})
    delta_units = target_units - current_units
    denom = current_units.abs().replace(0, 1e-12)
    rel_gap = delta_units.abs() / denom
    trade_mask = rel_gap > TRADE_INERTIA
    trades_units = delta_units.where(trade_mask, 0.0)

    # Optional: guard against negative cash
    if AVOID_NEGATIVE_CASH:
        trades_units, alpha_used = enforce_cash_constraint(
            trades_units=trades_units,
            prices=prices,
            cash0=positions[quoteCurrency],
            fee_rate=fee_rate,
            slippage=slippage,
            min_cash=MIN_CASH_BUFFER
        )
        if alpha_used < 1.0:
            print(f"{date} — cash guard active: scaled BUY legs by alpha={alpha_used:.4f} to respect cash≥{MIN_CASH_BUFFER:.2f}")

    # Execute at open ± slippage, compute cash flows and fees
    buys  = trades_units.clip(lower=0.0)
    sells = trades_units.clip(upper=0.0)

    buy_exec_notional  = float((buys  * prices * (1.0 + slippage)).sum())
    sell_exec_notional = float((-sells * prices * (1.0 - slippage)).sum())

    trade_cost  = buy_exec_notional - sell_exec_notional
    trading_fee = fee_rate * (buy_exec_notional + sell_exec_notional)

    positions[quoteCurrency] -= trade_cost
    positions[quoteCurrency] -= trading_fee

    # Update units only where we traded
    for tkr in target_units.index:
        if trade_mask.loc[tkr]:
            positions[tkr] = positions[tkr] + trades_units.loc[tkr]

    # Mark to market at today's open (post-trade)
    notional_row = {t: positions[t] * prices[t] for t in target_units.index}
    notional_row[quoteCurrency] = positions[quoteCurrency]
    notional_row["Portfolio Value"] = sum(notional_row.values())
    notional_history.append(notional_row)

    portfolio_value = notional_row["Portfolio Value"]
    portfolio_value_history.append(portfolio_value)
    cash_history.append(positions[quoteCurrency])
    fees_history.append(trading_fee)
    positions_history.append(positions.copy())

# Histories to DataFrames
backtest_df = pd.DataFrame({
    "Equity_PreTrade": equity_pre_history,            # <<< CHANGED: pre-trade equity at T
    "Portfolio Value": portfolio_value_history,       # post-trade equity at T
    "Cash": cash_history
}, index=common_index)

positions_df = pd.DataFrame(positions_history, index=common_index)
notional_df  = pd.DataFrame(notional_history,  index=common_index)

# Clean and standardize index -> tz-naive DatetimeIndex
backtest_df = backtest_df[~backtest_df.index.duplicated(keep="last")]
print(backtest_df)

# Force to datetimes in UTC, drop non-parsable
backtest_df.index = pd.to_datetime(backtest_df.index, utc=True, errors="coerce")
backtest_df = backtest_df.loc[backtest_df.index.notna()]

# Drop timezone to make QuantStats happy
backtest_df.index = backtest_df.index.tz_convert(None)

# Build returns
strategy = backtest_df["Portfolio Value"]
returns = strategy.pct_change()
returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

qs.reports.html(returns, output=STRATEGY_NAME + '.html')

# %%
#backtest_df.to_csv("backtest_df.csv")
#notional_df.to_csv("notional_df.csv")
#subsystem_portfolio_df.to_csv("subsystem_portfolio_df.csv")
# %%