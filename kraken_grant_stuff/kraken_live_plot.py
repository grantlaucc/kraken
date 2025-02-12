import threading
import time
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import signal
import sys
from matplotlib.animation import FuncAnimation
from kraken_l2 import OrderBooks, start_websocket

symbol = "XRP/USD"

def build_depth_arrays(order_book):
    """
    Given an OrderBook with .bids and .asks, return
    (bid_prices, bid_depth, ask_prices, ask_depth) for plotting.

    - Bids are typically sorted descending by price, so we reverse them
      so that the lowest bid is on the left, moving to the highest on the right.
    - Asks are typically sorted ascending by price.
    - Then we do a cumulative sum of quantities to get the 'depth'.
    """

    # Bids: descending => reverse them to get ascending by price
    # so the leftmost point is the lowest bid price
    if order_book.bids:
        #rev_bids = list(reversed(order_book.bids))  # now ascending by price
        bid_prices = [float(b[0]) for b in order_book.bids]
        bid_sizes  = [float(b[1]) for b in order_book.bids]
        bid_depth  = np.cumsum(bid_sizes)
    else:
        bid_prices = []
        bid_depth  = []

    # Asks: already ascending by price
    if order_book.asks:
        ask_prices = [float(a[0]) for a in order_book.asks]
        ask_sizes  = [float(a[1]) for a in order_book.asks]
        ask_depth  = np.cumsum(ask_sizes)
    else:
        ask_prices = []
        ask_depth  = []

    return bid_prices, bid_depth, ask_prices, ask_depth


def update_plot(frame, ax, symbol=symbol):
    """
    Matplotlib animation callback to plot the *depth* of the order book.
    It fetches the current bids/asks from OrderBooks[symbol],
    builds cumulative 'depth' arrays, and plots them.
    """
    # Clear the axes
    ax.clear()

    # Get the order book from the global dictionary
    order_book = OrderBooks.get(symbol)
    if not order_book:
        ax.set_title(f"No data for {symbol} yet...")
        return
    
    # Build the depth arrays for bids and asks
    bid_prices, bid_depth, ask_prices, ask_depth = build_depth_arrays(order_book)

    # Plot bids (if any)
    if bid_prices:
        # We'll do a step plot for depth
        ax.step(bid_prices, bid_depth, where='post', color='green')

    # Plot asks (if any)
    if ask_prices:
        ax.step(ask_prices, ask_depth, where='post', color='red')

    ax.set_xlabel("Price")
    ax.set_ylabel("Cumulative Size")
    ax.set_title(f"Kraken {symbol} {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", loc='left', fontsize='small')
    ax.set_title(f"Mid: {order_book.getMid()}", loc='right', fontsize='small')

def signal_handler(sig, frame):
    plt.close('all')
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    # 1) Start the WebSocket in a background thread
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    # 2) Set up the Matplotlib figure
    global fig, ax
    fig, ax = plt.subplots()

    # 3) Start FuncAnimation
    ani = FuncAnimation(fig, update_plot, fargs=(ax,), interval=1000)  # update every 1s

    # 4) Show the plot
    plt.show()


if __name__ == "__main__":
    main()
