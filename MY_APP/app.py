#!/usr/bin/env python3
from flask import Flask, render_template, request
import re

from pdf_io import extract_with_pdfplumber_tables_from_bytes, check_via_text_from_bytes
from payroll import extract_payroll_label
from salary_analysis import analyze_frames, dedup_hits, summarize_by_txn_month, summarize_by_payroll_label
from chqref_extract import extract_chqref_values, extract_chqref_values_xy
from chqref_dupes import summarize_chqref_dupes
from utils import _plausible_ref
from flask import send_file
from pdf_io import pdf_to_csv_bytes

app = Flask(__name__)



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

    # 1) extract tables
    frames = extract_with_pdfplumber_tables_from_bytes(data, password)

    # 2) gather Chq/Ref values
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

    chqref_dupes = summarize_chqref_dupes(chosen_vals, min_occ=min_occ)

    # 3) salary detection (unchanged)
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
    app.run(host="0.0.0.0", port=5000)

