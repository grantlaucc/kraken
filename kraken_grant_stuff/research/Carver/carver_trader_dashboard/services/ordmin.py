import pandas as pd

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
