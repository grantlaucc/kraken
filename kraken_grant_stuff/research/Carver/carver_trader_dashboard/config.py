# config.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    allowed_quotes: tuple[str, ...] = ("USD", "USDC", "CAD")
    ws_url: str = "wss://ws-auth.kraken.com/v2"
    ordermin_csv_path: str = os.environ.get(
        "KRAKEN_ORDERMIN_CSV",
        "/Users/grantlau/Documents/QuantStuff/kraken/kraken_grant_stuff/research/kraken_usd_pairs_ordermin.csv"
    )
    secret_key: str = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

# Kept as SETTINGS (not renamed) so ws_manager.py and any other Kraken-only code keeps working
# unchanged -- this dataclass is Kraken's settings specifically, not shared/generic.
SETTINGS = Settings()


@dataclass(frozen=True)
class HyperliquidSettings:
    # Perps are USDC-margined -- no quote leg to pick, unlike Kraken's BASE/QUOTE pairs. Kept as
    # a tuple for symmetry with Settings.allowed_quotes so shared template code doesn't special-case it.
    allowed_quotes: tuple[str, ...] = ("USD",)
    secret_key: str = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

HYPERLIQUID_SETTINGS = HyperliquidSettings()


def get_settings(venue: str):
    if venue == "kraken":
        return SETTINGS
    if venue == "hyperliquid":
        return HYPERLIQUID_SETTINGS
    raise ValueError(f"Unknown venue: {venue!r}")
