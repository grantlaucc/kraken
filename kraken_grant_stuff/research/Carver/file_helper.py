# fileHelper.py
import os
from typing import Union, Optional
import pandas as pd

def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path

def _read_existing_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    # Find timestamp column (index was written to CSV as a column)
    ts_col = "timestamp" if "timestamp" in df.columns else df.columns[0]
    if ts_col != "timestamp":
        df = df.rename(columns={ts_col: "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.set_index("timestamp").sort_index()
    return df

def _standardize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Ensure DatetimeIndex in UTC named 'timestamp'
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, utc=True, errors="coerce")
    else:
        out.index = out.index.tz_localize("UTC") if out.index.tz is None else out.index.tz_convert("UTC")
    out.index.name = "timestamp"
    return out.sort_index()

def write_incremental_csv(
    df: pd.DataFrame,
    folder: Union[str, os.PathLike],
    filename: str,
    allow_schema_update: bool = True,
) -> dict:
    """
    Append only *new* rows (by timestamp index) of `df` to {folder}/{filename}.
    If file doesn't exist, create it. If columns changed, rewrites file with the
    union of columns when `allow_schema_update=True`.

    Returns: {'path': str, 'rows_written': int, 'schema_rewritten': bool}
    """
    folder = ensure_dir(str(folder))
    path = os.path.join(folder, filename)
    cur = _standardize(df)
    existing = _read_existing_csv(path)

    info = {"path": path, "rows_written": 0, "schema_rewritten": False}

    if existing is None:
        cur.to_csv(path, index=True)
        info["rows_written"] = len(cur)
        return info

    last_ts = existing.index.max()
    new = cur.loc[cur.index > last_ts]
    if new.empty:
        return info  # nothing to append

    # If column sets match, append directly in existing column order
    if set(new.columns) == set(existing.columns):
        new = new.reindex(columns=existing.columns)
        new.to_csv(path, mode="a", header=False)
        info["rows_written"] = len(new)
        return info

    if not allow_schema_update:
        # Drop unknown columns and append
        new = new.reindex(columns=existing.columns)
        new.to_csv(path, mode="a", header=False)
        info["rows_written"] = len(new)
        return info

    # Schema changed: rewrite with union of columns
    union_cols = sorted(set(existing.columns).union(new.columns))
    rewritten = pd.concat(
        [existing.reindex(columns=union_cols), new.reindex(columns=union_cols)]
    )
    rewritten.to_csv(path, index=True)
    info["rows_written"] = len(new)
    info["schema_rewritten"] = True
    return info
