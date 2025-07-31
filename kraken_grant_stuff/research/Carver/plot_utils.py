import matplotlib.pyplot as plt

def plot_price_with_indicators(price_series, indicators, title="Price and Indicators"):
    """
    Plot price (left y-axis) and one or more indicators (right y-axis).
    
    Parameters:
        price_series (pd.Series): Time series of prices.
        indicators (dict): {name: series} of indicators.
        title (str): Plot title.
    """
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot price
    ax1.plot(price_series.index, price_series, label="Price", color="blue")
    ax1.set_ylabel("Price", color="blue")
    ax1.tick_params(axis='y', labelcolor="blue")
    
    # Plot indicators on second axis
    ax2 = ax1.twinx()
    for name, series in indicators.items():
        ax2.plot(series.index, series, label=name, color='orange')
    
    ax2.set_ylabel("Indicators", color="black")
    ax2.tick_params(axis='y', labelcolor="black")

    # Combined legend
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc="upper left")

    ax1.set_title(title)
    fig.tight_layout()
    plt.show()
