# app.py
#!/usr/bin/env python3
from flask import Flask, render_template, request, render_template_string
import io, re
import pandas as pd
from datetime import datetime


app = Flask(__name__)

# ---------- helpers ----------
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).replace("\xa0", " ").strip()

def parse_date_any(s: str):
    s = normalize_spaces(s)
    if not s:
        return None
    fmts = (
        "%d/%m/%Y","%d-%m-%Y","%d.%m.%Y",
        "%d %b %Y","%d %B %Y",
        "%d/%b/%Y","%d-%b-%Y","%d/%B/%Y","%d-%B-%Y",
        "%Y-%m-%d","%m/%d/%Y",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    m = re.search(r"(\d{1,2})[.\-/ ]([A-Za-z]{3,}|\d{1,2})[.\-/ ](\d{2,4})", s)
    if m:
        for fmt in ("%d-%b-%Y","%d-%m-%Y","%d/%b/%Y","%d/%m/%Y"):
            try:
                return datetime.strptime(m.group(0), fmt).date()
            except:
                continue
    return None

def month_key(d): 
    return f"{d.year:04d}-{d.month:02d}"

def find_any_column(df: pd.DataFrame, hints: list[str]) -> str | None:
    cols = list(df.columns)
    norm_map = {normalize_spaces(str(c)).lower(): c for c in cols}
    for hint in hints:
        key = normalize_spaces(hint).lower()
        if key in norm_map:
            return norm_map[key]
    for hint in hints:
        key = normalize_spaces(hint).lower()
        for norm, original in norm_map.items():
            if key and (key in norm or norm in key):
                return original
    return None

# ---------- payroll-label extraction ----------
MONTH_MAP = {
    "JAN":"JAN","JANUARY":"JAN",
    "FEB":"FEB","FEBRUARY":"FEB",
    "MAR":"MAR","MARCH":"MAR",
    "APR":"APR","APRIL":"APR",
    "MAY":"MAY",
    "JUN":"JUN","JUNE":"JUN",
    "JUL":"JUL","JULY":"JUL",
    "AUG":"AUG","AUGUST":"AUG",
    "SEP":"SEP","SEPT":"SEP","SEPTEMBER":"SEP",
    "OCT":"OCT","OCTOBER":"OCT",
    "NOV":"NOV","NOVEMBER":"NOV",
    "DEC":"DEC","DECEMBER":"DEC",
}

def extract_payroll_label(line: str) -> str | None:
    """Find label like 'SALARY MAY' / 'SAL MAY' / 'PAYROLL MAY'."""
    txt = (line or "").upper()
    if "SAL" not in txt and "PAYROLL" not in txt:
        return None
    # try to find a month token anywhere after SAL/PAYROLL
    # greedy but practical for statements
    # e.g. "... /SALARY MAY", "PAYROLL: JUN", "SAL SEP 2025"
    m = re.search(r"(?:SAL(?:ARY)?|PAYROLL)[^A-Z]*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC|JANUARY|FEBRUARY|MARCH|APRIL|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\b", txt)
    if m:
        token = m.group(1).upper()
        return MONTH_MAP.get(token, None)
    # fallback: if both words anywhere
    for word in ("SALARY","SAL","PAYROLL"):
        if word in txt:
            for k in MONTH_MAP.keys():
                if re.search(rf"\b{k}\b", txt):
                    return MONTH_MAP[k]
    return None

# ---------- extractors from BYTES ----------
def extract_with_pdfplumber_tables_from_bytes(data: bytes, password: str | None):
    """Return list of DataFrames; each DF has a __page column."""
    try:
        import pdfplumber
    except Exception:
        return []
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
                except:
                    continue
    return frames

def check_via_text_from_bytes(data: bytes, password: str | None, sal_pattern):
    """Return list of hits: dict(date, month, page, line, label)."""
    import pdfplumber
    hits = []
    # e.g. "09 June 2025" or "8 Jul 2025"
    date_pat = re.compile(
        r"\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|July|Aug|Sep|Sept|Oct|Nov|Dec|"
        r"January|February|March|April|June|July|August|September|October|November|December)\s+(\d{4})\b",
        re.IGNORECASE,
    )
    with pdfplumber.open(io.BytesIO(data), password=password) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            lines = (page.extract_text() or "").splitlines()
            last_date = None
            last_date_idx = -999
            for i, raw in enumerate(lines):
                ln = normalize_spaces(raw)
                m = date_pat.search(ln)
                if m:
                    d = parse_date_any(m.group(0))
                    if d:
                        last_date = d
                        last_date_idx = i
                # keyword line within 2 lines after a date → treat as that date's txn
                if sal_pattern.search(ln) and last_date is not None and (i - last_date_idx) <= 2:
                    hits.append({
                        "date": last_date,
                        "month": month_key(last_date),
                        "page": page_idx,
                        "line": ln,
                        "label": extract_payroll_label(ln),
                    })
    return hits

# ---------- analysis ----------
def analyze_frames(frames, sal_pattern, date_hints, desc_hints):
    """Return list of hits (date, month, page?, line, label) found via tables."""
    hits = []
    for df in frames:
        date_col = find_any_column(df, date_hints)
        desc_col = find_any_column(df, desc_hints)
        if not date_col or not desc_col:
            continue
        keep_cols = [date_col, desc_col]
        if "__page" in df.columns: keep_cols.append("__page")
        sdf = df[keep_cols].copy()
        sdf[date_col] = sdf[date_col].astype(str).map(normalize_spaces)
        sdf[desc_col] = sdf[desc_col].astype(str).map(normalize_spaces)
        dates = sdf[date_col].map(parse_date_any)
        sdf = sdf[dates.notna()].copy()
        if sdf.empty:
            continue
        sdf["_month"] = dates[dates.notna()].map(month_key)
        sdf["_has_keyword"] = sdf[desc_col].str.contains(sal_pattern, na=False)
        sub = sdf[sdf["_has_keyword"]]
        for _, r in sub.iterrows():
            hits.append({
                "date": parse_date_any(r[date_col]),
                "month": r["_month"],
                "page": int(r.get("__page") or 0),
                "line": r[desc_col],
                "label": extract_payroll_label(r[desc_col]),
            })
    return hits

def dedup_hits(hits):
    seen = set()
    out = []
    for h in hits:
        key = (h.get("date"), h.get("page"), normalize_spaces(h.get("line","")))
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out

def summarize_by_txn_month(hits, min_occ):
    from collections import defaultdict
    by_month = defaultdict(list)
    for h in hits:
        if h.get("month"):
            by_month[h["month"]].append(h)
    out = []
    for m, arr in by_month.items():
        if len(arr) >= min_occ:
            pages = sorted({int(x.get("page") or 0) for x in arr if x.get("page")})
            examples = sorted(arr, key=lambda x: x["date"] or datetime.min)[:5]
            out.append({
                "month": m,
                "count": len(arr),
                "pages": pages,
                "examples": [
                    {"date": (e["date"].isoformat() if e["date"] else ""), "page": e.get("page"), "line": e["line"]}
                    for e in examples
                ]
            })
    out.sort(key=lambda d: d["month"])
    return out

def summarize_by_payroll_label(hits, min_occ):
    from collections import defaultdict
    by_label = defaultdict(list)
    for h in hits:
        if h.get("label"):
            by_label[h["label"]].append(h)
    out = []
    for lab, arr in by_label.items():
        if len(arr) >= min_occ:
            pages = sorted({int(x.get("page") or 0) for x in arr if x.get("page")})
            examples = sorted(arr, key=lambda x: (x["date"] or datetime.min, x.get("page") or 0))[:5]
            out.append({
                "label": lab,               # e.g., "MAY"
                "count": len(arr),
                "pages": pages,
                "examples": [
                    {"date": (e["date"].isoformat() if e["date"] else ""), "page": e.get("page"), "line": e["line"]}
                    for e in examples
                ]
            })
    # sort by calendar order JAN..DEC
    order = {m:i for i,m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"])}
    out.sort(key=lambda d: order.get(d["label"], 99))
    return out

# ---------- UI (HTML) ----------
# with open("index.html", "r", encoding="utf-8") as f:
#     BASE_HTML = f.read()


@app.get("/")
def index():
    return render_template("index.html", result=None, duplicates_txn=None, duplicates_label=None)

@app.post("/check")
def check():
    f = request.files.get("pdf")
    if not f or f.filename == "":
        return render_template("index.html", result=False, duplicates_txn=[], duplicates_label=[])
    data = f.read()
    if not data:
        return render_template("index.html", result=False, duplicates_txn=[], duplicates_label=[])

    # defaults
    password = None
    min_occ = 2
    date_hints = ["Date", "Txn Date", "Transaction Date", "Value Date"]
    desc_hints = ["Particulars", "Description", "Narration", "Details"]

    # salary keywords
    sal_pattern = re.compile(r"(?:SAL(?:ARY)?|PAYROLL)", re.IGNORECASE)

    # 1) tables
    frames = extract_with_pdfplumber_tables_from_bytes(data, password)
    hits = analyze_frames(frames, sal_pattern, date_hints, desc_hints)

    # 2) text fallback
    hits.extend(check_via_text_from_bytes(data, password, sal_pattern))

    # de-duplicate (table+text might overlap)
    hits = dedup_hits(hits)

    # summaries
    dupes_txn = summarize_by_txn_month(hits, min_occ)
    dupes_label = summarize_by_payroll_label(hits, min_occ)

    result = bool(dupes_txn or dupes_label)
    return render_template("index.html", result=result, duplicates_txn=dupes_txn, duplicates_label=dupes_label)

if __name__ == "__main__":
    # Run: pip install flask pdfplumber pandas
    # then: python app.py  → open http://localhost:8000
    app.run(host="0.0.0.0", port=8000)
