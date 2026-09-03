"""
Hyperliquid account/positions state, mirroring kraken_balances.py's shape so
carver_trader_helper-style code can treat both venues similarly.

Field shapes below are grounded in a live testnet probe of the `clearinghouseState` websocket
subscription (confirmed nesting: msg["data"]["clearinghouseState"]["marginSummary"/"assetPositions"])
plus Hyperliquid's documented REST clearinghouseState response for the per-position fields, since
the probe wallet had no open positions to observe directly. Parsing is defensive (.get with
fallbacks, tolerant of missing/renamed fields) so an unexpected shape logs and skips rather than
crashing the websocket thread it's fed from.
"""


class HyperliquidAccount:
    def __init__(self):
        self.positions: dict[str, dict] = {}   # coin -> {szi, entry_px, unrealized_pnl, margin_used}
        self.account_value: float = 0.0
        self.total_margin_used: float = 0.0
        self.withdrawable: float = 0.0
        self.last_update_time: int | None = None

    def update_from_clearinghouse_state(self, msg: dict) -> None:
        """msg is a raw `{"channel": "clearinghouseState", "data": {...}}` websocket message."""
        data = msg.get("data", {})
        state = data.get("clearinghouseState", data)  # tolerate a flatter shape if it ever changes

        margin_summary = state.get("marginSummary", {})
        try:
            self.account_value = float(margin_summary.get("accountValue", 0.0))
            self.total_margin_used = float(margin_summary.get("totalMarginUsed", 0.0))
            self.withdrawable = float(state.get("withdrawable", 0.0))
        except (TypeError, ValueError) as e:
            print(f"!! HyperliquidAccount: failed to parse marginSummary: {e} ({margin_summary})")

        self.last_update_time = state.get("time")

        positions = {}
        for entry in state.get("assetPositions", []):
            pos = entry.get("position", entry)  # tolerate either {"position": {...}} or a flat dict
            coin = pos.get("coin")
            if not coin:
                continue
            try:
                positions[coin] = {
                    "szi": float(pos.get("szi", 0.0)),
                    "entry_px": float(pos["entryPx"]) if pos.get("entryPx") not in (None, "") else None,
                    "unrealized_pnl": float(pos.get("unrealizedPnl", 0.0)),
                    "margin_used": float(pos.get("marginUsed", 0.0)),
                }
            except (TypeError, ValueError) as e:
                print(f"!! HyperliquidAccount: failed to parse position for {coin}: {e} ({pos})")
        self.positions = positions

    def get_live_positions(self) -> dict[str, float]:
        """coin -> signed size (szi), the Hyperliquid analogue of KrakenBalances' base->units map."""
        return {coin: p["szi"] for coin, p in self.positions.items()}
