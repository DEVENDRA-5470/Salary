import io, re, statistics
import pdfplumber  # type: ignore
import pandas as pd

from utils import normalize_spaces, normalize_ref_id
from columns import find_any_column, find_chqref_col

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

def extract_chqref_values_xy(data: bytes, password: str | None):
    """
    Geometry fallback: detect header, compute x-band for Chq/Ref, pull body values.
    """
    def cluster_rows(words, tol=3):
        rows = []
        for w in sorted(words, key=lambda x: (x["top"], x["x0"])):  # type: ignore
            if not rows or abs(w["top"] - rows[-1]["top"]) > tol:
                rows.append({"top": w["top"], "bottom": w["bottom"], "items": [w]})
            else:
                rows[-1]["items"].append(w)
                rows[-1]["top"] = min(rows[-1]["top"], w["top"])
                rows[-1]["bottom"] = max(rows[-1]["bottom"], w["bottom"])
        return rows

    def cx(w): return 0.5 * (w["x0"] + w["x1"])  # type: ignore

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
    with pdfplumber.open(io.BytesIO(data), password=password) as pdf:  # type: ignore
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
