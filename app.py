#!/usr/bin/env python3
from flask import Flask, render_template, request
import io, re
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# ---------- helpers ----------
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).replace("\xa0", " ").strip()

def _canon(s: str) -> str:
    """lowercase + remove non-alnum (for robust header compare)."""
    return re.sub(r"[^a-z0-9]", "", normalize_spaces(str(s)).lower())

def normalize_ref_id(s: str) -> str:
    """Uppercase and remove spaces and common separators."""
    return re.sub(r"[\s\-/_.]", "", normalize_spaces(str(s))).upper()

# ID-like token inside a cell (e.g., S46715881, 12345678, AB1234567, etc.)
ID_TOKEN_RE = re.compile(r"(?ix)"
    r"(?:[A-Z]{1,3}\d{6,})"        # letter(s) + 6+ digits   e.g. S46715881, AB1234567
    r"|(?:\d{8,})"                 # long numeric (>=8)
    r"|(?:[A-Z0-9][A-Z0-9\-/]{7,})" # generic 8+ of A-Z0-9-/ (handles bank refs)
)

# put near the other helpers
# put near the other helpers
def normalize_ref_id(s: str) -> str:
    """
    Pull the core reference id from a noisy cell and normalize it.
    Prefer a single-letter prefix followed by digits (e.g., S46715881).
    Falls back sensibly for other formats.
    """
    txt = normalize_spaces(s).upper()

    # 1) Best: one letter + >=6 digits (this catches S46715881 even inside text)
    m = re.findall(r"[A-Z]\d{6,}", txt)
    if m:
        # Prefer ones starting with 'S', else the first
        cand = next((x for x in m if x.startswith("S")), m[0])
        return re.sub(r"[\s\-/_.]", "", cand)

    # 2) Next: two letters + digits (AB1234567 etc.)
    m = re.findall(r"[A-Z]{2}\d{6,}", txt)
    if m:
        return re.sub(r"[\s\-/_.]", "", m[0])

    # 3) Long numeric only
    m = re.findall(r"\d{8,}", txt)
    if m:
        return re.sub(r"[\s\-/_.]", "", m[0])

    # 4) Generic long token (last resort)
    m = re.findall(r"[A-Z0-9][A-Z0-9\-/]{7,}", txt)
    if m:
        return re.sub(r"[\s\-/_.]", "", m[0])

    # Nothing matched -> normalized whole string (unlikely)
    return re.sub(r"[\s\-/_.]", "", txt)





CHQREF_HEADERS = [
    "Chq No/ReF No", "Chq No/Ref No", "Chq No / Ref No", "Chq/Ref No",
    "Cheque Ref No", "Cheque No/Ref No"
]

def find_any_column(df: pd.DataFrame, hints: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    # exact canonical match first
    cmap = {_canon(c): c for c in cols}
    for h in hints:
        ch = _canon(h)
        if ch in cmap:
            return cmap[ch]
    # soft substring fallback
    norm_map = {normalize_spaces(str(c)).lower(): c for c in cols}
    for h in hints:
        key = normalize_spaces(h).lower()
        for norm, original in norm_map.items():
            if key and (key in norm or norm in key):
                return original
    return None

def find_chqref_col(df: pd.DataFrame) -> str | None:
    return find_any_column(df, CHQREF_HEADERS)

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

# ---------- payroll label ----------
MONTH_MAP = {
    "JAN":"JAN","JANUARY":"JAN","FEB":"FEB","FEBRUARY":"FEB","MAR":"MAR","MARCH":"MAR",
    "APR":"APR","APRIL":"APR","MAY":"MAY","JUN":"JUN","JUNE":"JUN","JUL":"JUL","JULY":"JUL",
    "AUG":"AUG","AUGUST":"AUG","SEP":"SEP","SEPT":"SEP","SEPTEMBER":"SEP","OCT":"OCT","OCTOBER":"OCT",
    "NOV":"NOV","NOVEMBER":"NOV","DEC":"DEC","DECEMBER":"DEC"
}

def extract_payroll_label(line: str) -> str | None:
    txt = (line or "").upper()
    if "SAL" not in txt and "PAYROLL" not in txt:
        return None
    m = re.search(
        r"(?:SAL(?:ARY)?|PAYROLL)[^A-Z]*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC|JANUARY|FEBRUARY|MARCH|APRIL|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\b",
        txt
    )
    if m:
        return MONTH_MAP.get(m.group(1).upper(), None)
    for w in ("SALARY","SAL","PAYROLL"):
        if w in txt:
            for k in MONTH_MAP.keys():
                if re.search(rf"\b{k}\b", txt):
                    return MONTH_MAP[k]
    return None

# ---------- PDF helpers ----------
def extract_with_pdfplumber_tables_from_bytes(data: bytes, password: str | None):
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
                except Exception:
                    continue
    return frames

def check_via_text_from_bytes(data: bytes, password: str | None, sal_pattern):
    try:
        import pdfplumber
    except Exception:
        return []
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

# ---------- analysis (salary) ----------
def analyze_frames(frames, sal_pattern, date_hints, desc_hints):
    hits = []
    for df in frames:
        date_col = find_any_column(df, date_hints)
        desc_col = find_any_column(df, desc_hints)
        if not date_col or not desc_col:
            continue
        keep = [date_col, desc_col]
        if "__page" in df.columns:
            keep.append("__page")
        sdf = df[keep].copy()
        sdf[date_col] = sdf[date_col].astype(str).map(normalize_spaces)
        sdf[desc_col] = sdf[desc_col].astype(str).map(normalize_spaces)
        dates = sdf[date_col].map(parse_date_any)
        sdf = sdf[dates.notna()].copy()
        if sdf.empty:
            continue
        sdf["_month"] = dates[dates.notna()].map(month_key)
        sdf["_has_keyword"] = sdf[desc_col].str.contains(sal_pattern, na=False)
        for _, r in sdf[sdf["_has_keyword"]].iterrows():
            hits.append({"date": parse_date_any(r[date_col]), "month": r["_month"],
                         "page": int(r.get("__page") or 0), "line": r[desc_col],
                         "label": extract_payroll_label(r[desc_col])})
    return hits

def dedup_hits(hits):
    seen, out = set(), []
    for h in hits:
        key = (h.get("date"), h.get("page"), normalize_spaces(h.get("line", "")))
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
            out.append({"month": m, "count": len(arr), "pages": pages,
                        "examples": [{"date": (e["date"].isoformat() if e["date"] else ""),
                                      "page": e.get("page"), "line": e["line"]} for e in examples]})
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
            out.append({"label": lab, "count": len(arr), "pages": pages,
                        "examples": [{"date": (e["date"].isoformat() if e["date"] else ""),
                                      "page": e.get("page"), "line": e["line"]} for e in examples]})
    order = {m: i for i, m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"])}
    out.sort(key=lambda d: order.get(d["label"], 99))
    return out

# ---------- EXACT Chq No/ReF No (table mode) ----------
def extract_chqref_values(frames, date_hints):
    rows = []
    any_found = False
    print("\n---- Chq No/ReF No values (by page) [table mode] ----")
    for df in frames:
        col = find_chqref_col(df)
        if not col:
            continue
        any_found = True
        page = int(df["__page"].iloc[0]) if "__page" in df.columns and len(df) else None
        date_col = find_any_column(df, date_hints)
        dates = df[date_col].astype(str).map(normalize_spaces) if date_col else None

        for i, raw in enumerate(df[col].astype(str)):
            val = normalize_spaces(raw)
            if not val or val in {"-", "--", "NA", "N/A"}:
                continue
            date_str = dates.iloc[i] if dates is not None and i < len(dates) else ""
            nid = normalize_ref_id(val)
            if not nid:
                continue
            rows.append({"value": val, "nid": nid, "page": page, "date": date_str})
            if page is not None:
                print(f"[page {page}] {val}  → nid: {nid}  {('— ' + date_str) if date_str else ''}")
            else:
                print(f"{val}  → nid: {nid}")

    if not any_found:
        print("No 'Chq No/ReF No' column found in tables.")
    print("---- end ----\n")
    return rows



def _plausible_ref(v: str) -> bool:
    v = (v or "").strip().upper()
    # after normalization we removed separators; allow A–Z0–9, length >= 7,
    # or >=8 if all digits
    if not v or re.search(r"[^A-Z0-9]", v):
        return False
    if v.isdigit():
        return len(v) >= 8
    return len(v) >= 7




# ---------- XY fallback (header-anchored) ----------
def extract_chqref_values_xy(data: bytes, password: str | None):
    """
    Geometry fallback: detect header, compute x-band for Chq/Ref, pull body values.
    """
    try:
        import pdfplumber
    except Exception:
        return []

    import statistics
    def cluster_rows(words, tol=3):
        rows = []
        for w in sorted(words, key=lambda x: (x["top"], x["x0"])):
            if not rows or abs(w["top"] - rows[-1]["top"]) > tol:
                rows.append({"top": w["top"], "bottom": w["bottom"], "items": [w]})
            else:
                rows[-1]["items"].append(w)
                rows[-1]["top"] = min(rows[-1]["top"], w["top"])
                rows[-1]["bottom"] = max(rows[-1]["bottom"], w["bottom"])
        return rows
    def cx(w): return 0.5 * (w["x0"] + w["x1"])

    KEY = {
        "date": lambda s: "date" in s,
        "particulars": lambda s: "particul" in s,
        "chq": lambda s: ("chq" in s) or ("cheque" in s) or ("ref" in s and "no" in s),
        "withdrawal": lambda s: "withdraw" in s,
        "deposit": lambda s: "deposit" in s,
        "balance": lambda s: "balan" in s,
    }

    out = []
    print("\n---- Chq No/ReF No values (by page) [XY fallback] ----")
    with pdfplumber.open(io.BytesIO(data), password=password) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            words = page.extract_words(x_tolerance=1.5, y_tolerance=1.5, keep_blank_chars=False, use_text_flow=True)
            if not words:
                continue

            rows = cluster_rows(words, tol=3)
            # find header row with most hits
            header, hits_best = None, 0
            for r in rows:
                hits = 0
                for w in r["items"]:
                    s = w["text"].strip().lower()
                    if any(fn(s) for fn in KEY.values()):
                        hits += 1
                if hits > hits_best:
                    header, hits_best = r, hits
            if not header or hits_best < 3:
                continue

            # anchors
            anchors = {}
            meds = [cx(x) for x in header["items"]]
            med = statistics.median(meds) if meds else 0
            for name, pred in KEY.items():
                cand = [w for w in header["items"] if pred(w["text"].strip().lower())]
                if cand:
                    anchors[name] = min(cand, key=lambda w: abs(cx(w) - med))

            if not {"particulars", "chq", "withdrawal"} <= anchors.keys():
                continue

            x_part = cx(anchors["particulars"]); x_chq = cx(anchors["chq"]); x_with = cx(anchors["withdrawal"])
            x_left  = 0.5 * (x_part + x_chq)
            x_right = 0.5 * (x_chq  + x_with)
            header_bottom = header["bottom"]

            x_date_l = x_date_r = None
            if "date" in anchors:
                xd = cx(anchors["date"])
                x_date_l  = min(xd, x_part) - 2
                x_date_r  = 0.5 * (xd + x_part)

            body = [r for r in rows if r["top"] > header_bottom + 1]
            for r in body:
                line_words = sorted(r["items"], key=lambda w: w["x0"])
                toks = [w["text"] for w in line_words if x_left <= cx(w) <= x_right]
                val = "".join(toks).strip()
                if not val or any(k in val.lower() for k in ("withdraw", "deposit", "balance", "particular", "date")):
                    continue
                if not re.fullmatch(r"[A-Za-z0-9/\-]{6,}", val):
                    continue

                date_str = ""
                if x_date_l is not None:
                    dtoks = [w["text"] for w in line_words if x_date_l <= cx(w) <= x_date_r]
                    date_str = " ".join(dtoks).strip()

                nid = normalize_ref_id(val)
                out.append({"value": val, "nid": nid, "page": pno, "date": date_str})
                print(f"[page {pno}] {val}  → nid: {nid}  {('— '+date_str) if date_str else ''}")




    if not out:
        print("No values extracted by XY fallback.")
    print("---- end ----\n")
    return out

# ---------- Chq No/ReF No duplicate summarizer ----------
def summarize_chqref_dupes(values, min_occ=2):
    from collections import defaultdict, Counter
    by_nid = defaultdict(list)
    for r in values:
        nid = (r.get("nid") or "").strip()
        if not nid:
            continue
        by_nid[nid].append(r)

    out = []
    for nid, arr in by_nid.items():
        if len(arr) >= min_occ:
            pages = sorted({int(x.get("page") or 0) for x in arr if x.get("page")})
            disp = Counter([a.get("value", nid) for a in arr]).most_common(1)[0][0]
            examples = arr[:5]
            out.append({
                "value": disp,   # pretty
                "nid": nid,      # core
                "count": len(arr),
                "pages": pages,
                "examples": [{"date": normalize_spaces(e.get("date","")) or None,
                              "page": e.get("page")} for e in examples]
            })
    out.sort(key=lambda d: (-d["count"], d["value"]))
    return out




# ---------- routes ----------
@app.get("/")
def index():
    return render_template(
        "index.html",
        result=None,
        duplicates_txn=[],
        duplicates_label=[],
        chqref_dupes=[],
    )

@app.post("/check")
def check():
    f = request.files.get("pdf")
    if not f or f.filename == "":
        return render_template("index.html", result=False,
                               duplicates_txn=[], duplicates_label=[], chqref_dupes=[])
    data = f.read()
    if not data:
        return render_template("index.html", result=False,
                               duplicates_txn=[], duplicates_label=[], chqref_dupes=[])

    # defaults
    password = None
    min_occ = 2  # >1
    date_hints = ["Date", "Txn Date", "Transaction Date", "Value Date"]
    desc_hints = ["Particulars", "Description", "Narration", "Details"]
    sal_pattern = re.compile(r"(?:SAL(?:ARY)?|PAYROLL)", re.IGNORECASE)

    # tables
    frames = extract_with_pdfplumber_tables_from_bytes(data, password)

    # 1) exact Chq No/ReF No via table mode
    table_vals = extract_chqref_values(frames, date_hints)
    bad = [v for v in table_vals if not _plausible_ref(v.get("nid",""))]

    use_fallback = (not table_vals) or (len(bad) > len(table_vals) // 2)
    if use_fallback:
        print("Using XY fallback for Chq/Ref detection (table empty or noisy).")
        xy_vals = extract_chqref_values_xy(data, password)
        chosen_vals = [r for r in xy_vals if _plausible_ref(r.get("nid",""))]
    else:
        print("Using table extraction for Chq/Ref detection.")
        chosen_vals = [r for r in table_vals if _plausible_ref(r.get("nid",""))]

    chqref_dupes = summarize_chqref_dupes(chosen_vals, min_occ=2)  # >1 only




    # 3) duplicate summary for Chq No/ReF No (NO prior dedup)
    chqref_dupes = summarize_chqref_dupes(chosen_vals, min_occ=min_occ)

    # salary detection (unchanged)
    hits = analyze_frames(frames, sal_pattern, date_hints, desc_hints)
    hits.extend(check_via_text_from_bytes(data, password, sal_pattern))
    hits = dedup_hits(hits)
    dupes_txn = summarize_by_txn_month(hits, min_occ)
    dupes_label = summarize_by_payroll_label(hits, min_occ)

    # global result flag
    result = bool(dupes_txn or dupes_label or chqref_dupes)

    return render_template(
        "index.html",
        result=result,
        duplicates_txn=dupes_txn,
        duplicates_label=dupes_label,
        chqref_dupes=chqref_dupes,
    )

if __name__ == "__main__":
    # pip install flask pdfplumber pandas
    app.run(host="0.0.0.0", port=8000)
