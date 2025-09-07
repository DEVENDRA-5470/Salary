import pandas as pd
from utils import _canon, normalize_spaces

CHQREF_HEADERS = [
    "Chq No/ReF No", "Chq No/Ref No", "Chq No / Ref No", "Chq/Ref No",
    "Cheque Ref No", "Cheque No/Ref No"
]

def find_any_column(df: pd.DataFrame, hints: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    cmap = {_canon(c): c for c in cols}
    for h in hints:
        ch = _canon(h)
        if ch in cmap:
            return cmap[ch]
    norm_map = {normalize_spaces(str(c)).lower(): c for c in cols}
    for h in hints:
        key = normalize_spaces(h).lower()
        for norm, original in norm_map.items():
            if key and (key in norm or norm in key):
                return original
    return None

def find_chqref_col(df: pd.DataFrame) -> str | None:
    return find_any_column(df, CHQREF_HEADERS)
