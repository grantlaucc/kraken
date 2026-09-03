"""
Coin metadata: which venues cover a coin, min order sizes, fees, inception dates,
market cap -- persisted locally so it's viewable and doesn't need re-fetching on
every run, plus a selection layer for pulling a universe by criteria (e.g. "top 25
by market cap") instead of a hardcoded ticker list.

Static metadata lives in coin_universe.csv (one row per coin, refreshed on demand via
refresh_universe_csv()). Time series (price/returns/vol/funding) are NOT duplicated
here -- Coin's get_* methods read live from the existing data stores (ohlc_data,
hyperliquid_price_data, CarrySignal's stitched funding loader), so there's a single
source of truth for the actual data.
"""
import sys
import time
from dataclasses import dataclass, asdict, fields

import numpy as np
import pandas as pd
import requests

import research.ohlc_data as ohlc_data
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding")
sys.path.append("/Users/grantlau/Documents/QuantStuff/CryptoFunding/Hyperliquid")
import hyperliquid_price_data
import hyperliquid_funding_data
import research.Carver.trading_system.forecast as forecast

UNIVERSE_CSV = "/Users/grantlau/Documents/QuantStuff/CryptoFunding/coin_universe.csv"
COINGECKO_URL = "https://api.coingecko.com/api/v3"
KRAKEN_URL = "https://api.kraken.com/0/public"

# Hyperliquid's fee schedule is a flat maker/taker tier uniform across every perp
# (not per-coin like Kraken's), so it's a shared constant rather than a fetched field.
HYPERLIQUID_TAKER_FEE = 0.00045
HYPERLIQUID_MAKER_FEE = 0.00015

# Base -> CoinGecko id overrides for cases symbol-matching gets wrong or misses entirely,
# e.g. Toncoin's CoinGecko symbol is now "gram" after a rename, so a plain symbol search
# for "ton" finds unrelated bridged/wrapped tokens instead.
COINGECKO_ID_OVERRIDES = {
    "TON": "the-open-network",
}

# Kraken's asset code for Bitcoin is XBT, not BTC.
KRAKEN_BASE_OVERRIDES = {
    "BTC": "XBT",
}


@dataclass
class Coin:
    base: str
    hyperliquid_ticker: str | None = None
    kraken_ticker: str | None = None
    hyperliquid_min_size: float | None = None
    kraken_min_size: float | None = None
    kraken_min_notional: float | None = None
    kraken_fee_pct: float | None = None
    hyperliquid_inception: pd.Timestamp | None = None
    kraken_inception: pd.Timestamp | None = None
    market_cap: float | None = None
    circulating_supply: float | None = None
    volume_24h: float | None = None
    coingecko_id: str | None = None
    last_updated: pd.Timestamp | None = None

    def get_price(self, venue="hyperliquid", column="open", start=None, end=None) -> pd.Series:
        ticker = self.hyperliquid_ticker if venue == "hyperliquid" else self.kraken_ticker
        if ticker is None:
            raise ValueError(f"{self.base} has no {venue} ticker")
        if venue == "hyperliquid":
            df = hyperliquid_price_data.load_hyperliquid_price_to_df(ticker, startDate=start, endDate=end, selectCols=[column])
        else:
            df = ohlc_data.load_ohlc_data_to_df(ticker, startDate=start, endDate=end, selectCols=[column])
        return df[column]

    def get_returns(self, venue="hyperliquid", start=None, end=None) -> pd.Series:
        return self.get_price(venue=venue, start=start, end=end).pct_change()

    def get_vol(self, venue="hyperliquid", span=35, start=None, end=None) -> pd.Series:
        price = self.get_price(venue=venue, start=start, end=end)
        return price.diff().ewm(span=span, adjust=False).std()

    def get_funding(self, start=None, end=None) -> pd.Series:
        """Real (un-negated) hourly funding rate, stitched BitMEX/Hyperliquid history --
        reuses CarrySignal's loader so this isn't a second implementation of the same logic."""
        ticker = f"{self.base}/USD"
        signal = forecast.CarrySignal(start_date=start, end_date=end)
        df = signal.load_funding_series(ticker)
        return df.iloc[:, 0]


def _get_coingecko_coin_list() -> list[dict]:
    resp = requests.get(f"{COINGECKO_URL}/coins/list")
    resp.raise_for_status()
    return resp.json()


def _get_coingecko_markets(ids: list[str], max_retries: int = 6) -> dict:
    """Fetches market data for `ids` in as few batched calls as possible. CoinGecko's
    free tier rate-limits fairly aggressively (hit a 429 with just ~15 sequential
    single-symbol calls during testing), so this retries with backoff and callers
    should batch everything they need into one call up front rather than one per coin."""
    if not ids:
        return {}
    out = {}
    for i in range(0, len(ids), 200):
        chunk = ids[i:i + 200]
        for attempt in range(max_retries):
            resp = requests.get(f"{COINGECKO_URL}/coins/markets", params={"vs_currency": "usd", "ids": ",".join(chunk)})
            if resp.status_code == 429:
                wait = min(10 * (attempt + 1), 60)
                print(f"  CoinGecko rate limited, retrying in {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            for row in resp.json():
                out[row["id"]] = row
            break
        else:
            raise RuntimeError(f"CoinGecko rate limited after {max_retries} retries")
        time.sleep(2.0)
    return out


def resolve_coingecko_ids(bases: list[str], coin_list: list[dict]) -> dict[str, str | None]:
    """Symbol match for every base at once, disambiguated by market cap when multiple
    coins share a ticker (bridged/wrapped/meme tokens commonly do) -- one batched
    market-cap lookup covers every ambiguous base rather than one call each.
    COINGECKO_ID_OVERRIDES take priority for known symbol renames/collisions a plain
    search gets wrong (e.g. Toncoin's CoinGecko symbol is now "gram", not "ton")."""
    by_symbol: dict[str, list[str]] = {}
    for c in coin_list:
        by_symbol.setdefault(c["symbol"].lower(), []).append(c["id"])

    candidates_by_base = {}
    ambiguous_ids = []
    for base in bases:
        if base in COINGECKO_ID_OVERRIDES:
            continue
        candidates = by_symbol.get(base.lower(), [])
        candidates_by_base[base] = candidates
        if len(candidates) > 1:
            ambiguous_ids.extend(candidates)

    markets = _get_coingecko_markets(sorted(set(ambiguous_ids)))

    resolved = {}
    for base in bases:
        if base in COINGECKO_ID_OVERRIDES:
            resolved[base] = COINGECKO_ID_OVERRIDES[base]
            continue
        candidates = candidates_by_base[base]
        if not candidates:
            resolved[base] = None
        elif len(candidates) == 1:
            resolved[base] = candidates[0]
        else:
            ranked = sorted(candidates, key=lambda i: (markets.get(i, {}).get("market_cap") or 0), reverse=True)
            resolved[base] = ranked[0]
    return resolved


def get_hyperliquid_meta() -> dict:
    """name -> {szDecimals, maxLeverage} for every Hyperliquid perp."""
    resp = requests.post(f"{hyperliquid_funding_data.HL_INFO_URL}", json={"type": "meta"})
    resp.raise_for_status()
    return {m["name"]: m for m in resp.json()["universe"]}


def get_kraken_asset_pair(base: str) -> dict | None:
    kraken_base = KRAKEN_BASE_OVERRIDES.get(base, base)
    altname = f"{kraken_base}USD"
    hyperliquid_funding_data._throttle()  # reuse the shared rate limiter; Kraken's public API is also best hit gently
    resp = requests.get(f"{KRAKEN_URL}/AssetPairs", params={"pair": altname})
    resp.raise_for_status()
    data = resp.json()
    if data["error"]:
        return None
    return list(data["result"].values())[0]


def _first_valid_date(series: pd.Series) -> pd.Timestamp | None:
    s = series.dropna()
    return s.index.min() if not s.empty else None


def build_universe(bases: list[str]) -> pd.DataFrame:
    """Fetch everything fresh for the given bases. Missing/unavailable data on either
    venue is left as None rather than raising -- e.g. AI16Z has no live Kraken pair."""
    hl_meta = get_hyperliquid_meta()
    coingecko_coins = _get_coingecko_coin_list()
    coingecko_ids = resolve_coingecko_ids(bases, coingecko_coins)
    markets = _get_coingecko_markets(sorted(set(i for i in coingecko_ids.values() if i)))

    rows = []
    now = pd.Timestamp.now(tz="UTC")
    for base in bases:
        row = {"base": base, "last_updated": now}

        hl_info = hl_meta.get(base)
        if hl_info is not None:
            row["hyperliquid_ticker"] = base
            row["hyperliquid_min_size"] = 10 ** -hl_info["szDecimals"]
            try:
                candles = hyperliquid_price_data.get_daily_candles(base)
                row["hyperliquid_inception"] = candles.index.min() if not candles.empty else None
            except Exception as e:
                print(f"  {base}: hyperliquid inception lookup failed: {e}")

        kraken_pair = get_kraken_asset_pair(base)
        if kraken_pair is not None:
            row["kraken_ticker"] = f"{base}/USD"
            row["kraken_min_size"] = float(kraken_pair["ordermin"])
            row["kraken_min_notional"] = float(kraken_pair["costmin"])
            row["kraken_fee_pct"] = float(kraken_pair["fees_maker"][0][1]) / 100.0
            try:
                px = ohlc_data.load_ohlc_data_to_df(f"{base}/USD", selectCols=["open"])
                row["kraken_inception"] = _first_valid_date(px["open"])
            except Exception as e:
                print(f"  {base}: kraken inception lookup failed: {e}")

        cg_id = coingecko_ids.get(base)
        if cg_id and cg_id in markets:
            m = markets[cg_id]
            row["coingecko_id"] = cg_id
            row["market_cap"] = m.get("market_cap")
            row["circulating_supply"] = m.get("circulating_supply")
            row["volume_24h"] = m.get("total_volume")

        rows.append(row)
        print(f"  {base}: HL={'y' if hl_info else 'n'} Kraken={'y' if kraken_pair else 'n'} "
              f"CoinGecko={cg_id or 'n/a'}")

    field_names = [f.name for f in fields(Coin)]
    df = pd.DataFrame(rows).set_index("base").reindex(columns=field_names[1:])
    return df


def refresh_universe_csv(bases: list[str] | None = None, path: str = UNIVERSE_CSV):
    if bases is None:
        existing = pd.read_csv(path, index_col=0)
        bases = existing.index.tolist()
    print(f"Refreshing universe for {len(bases)} bases...")
    fresh = build_universe(bases)
    fresh.to_csv(path)
    print(f"Wrote {path} ({fresh.shape[0]} rows x {fresh.shape[1]} cols)")
    return fresh


def load_universe(path: str = UNIVERSE_CSV) -> list[Coin]:
    df = pd.read_csv(path, index_col=0)
    date_cols = ["hyperliquid_inception", "kraken_inception", "last_updated"]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    coins = []
    for base, row in df.iterrows():
        data = row.where(pd.notna(row), None).to_dict()
        coins.append(Coin(base=base, **data))
    return coins


def select_universe(coins: list[Coin], top_n: int | None = None, sort_by: str | None = "market_cap",
                     ascending: bool = False, min_inception_before=None, require_venue: str | None = None,
                     **field_filters) -> list[Coin]:
    """Composable filter/sort over an already-loaded universe (from load_universe()).
    field_filters: exact-match filters on any Coin field, e.g. coingecko_id='bitcoin'."""
    result = list(coins)

    if require_venue == "hyperliquid":
        result = [c for c in result if c.hyperliquid_ticker is not None]
    elif require_venue == "kraken":
        result = [c for c in result if c.kraken_ticker is not None]

    if min_inception_before is not None:
        cutoff = pd.to_datetime(min_inception_before, utc=True)
        def listed_before_cutoff(c):
            dates = [d for d in (c.hyperliquid_inception, c.kraken_inception) if d is not None]
            return any(d <= cutoff for d in dates)
        result = [c for c in result if listed_before_cutoff(c)]

    for key, value in field_filters.items():
        result = [c for c in result if getattr(c, key) == value]

    if sort_by is not None:
        # Split out Nones first: sorting a mix of None and real values crashes (Python
        # can't compare None to None once a tie-break falls through to a second None),
        # and Nones should sort last regardless of direction, not participate in it.
        present = [c for c in result if getattr(c, sort_by) is not None]
        missing = [c for c in result if getattr(c, sort_by) is None]
        present.sort(key=lambda c: getattr(c, sort_by), reverse=not ascending)
        result = present + missing

    if top_n is not None:
        result = result[:top_n]
    return result


def to_kraken_tickers(coins: list[Coin]) -> list[str]:
    return [c.kraken_ticker for c in coins if c.kraken_ticker is not None]


def to_hyperliquid_tickers(coins: list[Coin]) -> list[str]:
    return [f"{c.hyperliquid_ticker}/USD" for c in coins if c.hyperliquid_ticker is not None]


if __name__ == "__main__":
    with open("/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/usd_pairs.txt") as f:
        seed_bases = [line.strip().split("/")[0] for line in f]
    refresh_universe_csv(seed_bases)
