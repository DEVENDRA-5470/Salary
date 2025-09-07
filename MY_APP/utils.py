import re
from datetime import datetime

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).replace("\xa0", " ").strip()

def _canon(s: str) -> str:
    """lowercase + remove non-alnum (for robust header compare)."""
    return re.sub(r"[^a-z0-9]", "", normalize_spaces(str(s)).lower())

def normalize_ref_id(s: str) -> str:
    """
    Pull the core reference id from a noisy cell and normalize it.
    Prefer a single-letter prefix followed by digits (e.g., S46715881).
    """
    txt = normalize_spaces(s).upper()

    m = re.findall(r"[A-Z]\d{6,}", txt)
    if m:
        cand = next((x for x in m if x.startswith("S")), m[0])
        return re.sub(r"[\s\-/_.]", "", cand)

    m = re.findall(r"[A-Z]{2}\d{6,}", txt)
    if m:
        return re.sub(r"[\s\-/_.]", "", m[0])

    m = re.findall(r"\d{8,}", txt)
    if m:
        return re.sub(r"[\s\-/_.]", "", m[0])

    m = re.findall(r"[A-Z0-9][A-Z0-9\-/]{7,}", txt)
    if m:
        return re.sub(r"[\s\-/_.]", "", m[0])

    return re.sub(r"[\s\-/_.]", "", txt)

def parse_date_any(s: str):
    s = normalize_spaces(s)
    if not s:
        return None
    fmts = (
        "%d/%m/%Y","%d-%m-%Y","%d.%m.%Y","%d %b %Y","%d %B %Y",
        "%d/%b/%Y","%d-%b-%Y","%d/%B/%Y","%d-%B-%Y","%Y-%m-%d","%m/%d/%Y",
        "%d/%m/%y","%d-%m-%y","%y-%m-%d"
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    m = re.search(r"(\d{1,2})[.\-/ ]([A-Za-z]{3,}|\d{1,2})[.\-/ ](\d{2,4})", s)
    if m:
        for fmt in ("%d-%b-%Y","%d-%m-%Y","%d/%b/%Y","%d/%m/%Y","%d-%m-%y","%d/%m/%y"):
            try:
                return datetime.strptime(m.group(0), fmt).date()
            except Exception:
                continue
    return None

def month_key(d):
    return f"{d.year:04d}-{d.month:02d}"

def _plausible_ref(v: str) -> bool:
    v = (v or "").strip().upper()
    if not v or re.search(r"[^A-Z0-9]", v):
        return False
    if v.isdigit():
        return len(v) >= 8
    return len(v) >= 7
