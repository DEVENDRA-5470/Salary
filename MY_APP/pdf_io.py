import io, re
import pdfplumber  # type: ignore
import pandas as pd

from utils import normalize_spaces, parse_date_any, month_key
from payroll import extract_payroll_label

def extract_with_pdfplumber_tables_from_bytes(data: bytes, password: str | None):
    frames = []
    with pdfplumber.open(io.BytesIO(data), password=password) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            for t in (page.extract_tables() or []):
                if not t or len(t) < 2:
                    continue
                try:
                    df = pd.DataFrame(t[1:], columns=t[0])
                    df["__page"] = page_idx
                    frames.append(df)
                except Exception:
                    continue
    return frames

def check_via_text_from_bytes(data: bytes, password: str | None, sal_pattern):
    hits, date_pat = [], re.compile(
        r"\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|July|Aug|Sep|Sept|Oct|Nov|Dec|January|February|March|April|June|July|August|September|October|November|December)\s+(\d{4})\b",
        re.IGNORECASE,
    )
    with pdfplumber.open(io.BytesIO(data), password=password) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            lines = (page.extract_text() or "").splitlines()
            last_date, last_date_idx = None, -999
            for i, raw in enumerate(lines):
                ln = normalize_spaces(raw)
                m = date_pat.search(ln)
                if m:
                    d = parse_date_any(m.group(0))
                    if d:
                        last_date, last_date_idx = d, i
                if sal_pattern.search(ln) and last_date is not None and (i - last_date_idx) <= 2:
                    hits.append({"date": last_date, "month": month_key(last_date),
                                 "page": page_idx, "line": ln, "label": extract_payroll_label(ln)})
    return hits

def pdf_to_csv_bytes(data: bytes, password: str | None = None) -> bytes:
    """
    Pdfplumber se saare tables nikaal ke single CSV bana deta hai.
    __page column preserve hota hai taaki later page info rahe.
    Returns UTF-8 CSV bytes (ready to download / read_csv).
    """
    from pdf_io import extract_with_pdfplumber_tables_from_bytes  # or relative import if split
    frames = extract_with_pdfplumber_tables_from_bytes(data, password)
    if not frames:
        return b""

    def tidy(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [normalize_spaces(str(c)) for c in df.columns]
        return df

    frames = [tidy(df) for df in frames]
    big = pd.concat(frames, ignore_index=True)

    buf = io.StringIO()
    big.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8-sig")