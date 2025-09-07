import pandas as pd
from utils import normalize_spaces, parse_date_any, month_key
from columns import find_any_column

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
                         "label": None})  # label is computed elsewhere if needed
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
    from datetime import datetime
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
    from datetime import datetime
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
