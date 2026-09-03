import pandas as pd
import research.Carver.trading_system.coin_universe as coin_universe

def load_ordermin_map(csv_path: str) -> dict[str, float]:
    try:
        df = pd.read_csv(csv_path)
        if "Base" not in df.columns or "OrderMin" not in df.columns:
            raise ValueError("CSV must contain 'Base' and 'OrderMin'")
        df["Base"] = df["Base"].astype(str).str.upper().str.strip()
        df["OrderMin"] = pd.to_numeric(df["OrderMin"], errors="coerce")
        df = df.dropna(subset=["Base","OrderMin"])
        return df.groupby("Base")["OrderMin"].min().to_dict()
    except Exception as e:
        print(f"[ordermin] failed to load '{csv_path}': {e}")
        return {}


def load_hyperliquid_ordermin_map() -> dict[str, float]:
    """Same {base: min_size} shape as load_ordermin_map, sourced from coin_universe.csv's
    hyperliquid_min_size field (already used in run_carver_hyperliquid.py) instead of a CSV --
    Hyperliquid's per-coin size increments aren't published as a downloadable file anywhere."""
    try:
        coins = coin_universe.load_universe()
        return {c.base: c.hyperliquid_min_size for c in coins if c.hyperliquid_min_size is not None}
    except Exception as e:
        print(f"[ordermin] failed to load Hyperliquid min sizes: {e}")
        return {}
