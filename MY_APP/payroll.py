import re
from utils import normalize_spaces

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
