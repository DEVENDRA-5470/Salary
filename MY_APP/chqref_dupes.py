from collections import defaultdict, Counter
from utils import normalize_spaces

def summarize_chqref_dupes(values, min_occ=2):
    """
    Group by exact normalized id (nid) and return only those with count >= min_occ.
    """
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
                "value": disp,   # pretty display
                "nid": nid,      # core id
                "count": len(arr),
                "pages": pages,
                "examples": [{"date": normalize_spaces(e.get("date","")) or None,
                              "page": e.get("page")} for e in examples]
            })
    out.sort(key=lambda d: (-d["count"], d["value"]))
    return out

