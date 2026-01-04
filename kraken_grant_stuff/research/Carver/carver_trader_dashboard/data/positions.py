import os
import pandas as pd
from ..utils.parsing import extract_base

DROP_COLS = {"CASH","PORTFOLIO VALUE","PORTFOLIO_VALUE","NOTIONAL","PORTFOLIO","VALUE"}

def load_bases_from_position_file(strategy_dir: str, allowed_quotes: tuple[str, ...]) -> list[str]:
    path = os.path.join(strategy_dir, "position_file.csv")
    try:
        df = pd.read_csv(path, index_col=0)
        cols = [c for c in df.columns if str(c).strip().upper() not in DROP_COLS]
        bases = { b for c in cols for b in [extract_base(str(c), allowed_quotes)] if b }
        return sorted(bases)
    except Exception:
        return []
