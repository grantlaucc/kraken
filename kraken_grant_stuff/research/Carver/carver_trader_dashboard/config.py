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

SETTINGS = Settings()
