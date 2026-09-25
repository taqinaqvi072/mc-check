"""Motus Daily FMCSA Register (PDF) integration for MC Scout.

This is the ORIGINAL approach: FMCSA's "Daily Register" publication lists
every new operating-authority APPLICATION as it's filed -- i.e. "who
applied today", regardless of whether authority has been granted yet.
Most of these will show as pending/not-authorized when checked against
SAFER, since the review process takes time -- that's expected and normal
for this feed. This is intentionally kept SEPARATE from motus.py
(AuthHist -- "who was newly GRANTED authority"), since the client wants
both views available side by side.

Install: pip install pdfplumber
"""
import io
import re

import requests
import pdfplumber

MOTUS_API_BASE = "https://motus.dot.gov/api/report/getSignedUrlByTypeAndDateRange/REGISTER"

MOTUS_INCLUDE_CATEGORIES = [
    "MOTOR CARRIER OF PROPERTY",
    "MOTOR CARRIER OF PASSENGERS",
]

_ROW_START_RE = re.compile(r"^(\d{5,8})\s+(.*)")

_SKIP_PREFIXES = (
    "Run Date", "Run Time", "Page ", "USDOT Number", "U.S. Department",
    "Federal Motor", "REGISTER", "NOTICES RELEASED", "The FMCSA Register",
    "Disclaimer:", "General Information", "Applicants will receive",
    "Under 49 CFR", "In accordance with", "1. The applicant",
    "2. The applicant", "3. Granting", "Failure to timely",
    "If no opposition",
)


def _extract_url(entry):
    """Each item in the "Register" list returned by the Motus API is a
    dict like {"date": "2026-09-18", "url": "https://...signed-s3-url..."}."""
    if isinstance(entry, dict):
        for key in ("url", "signedUrl", "Url", "SignedUrl", "downloadUrl"):
            if entry.get(key):
                return entry[key]
        return None
    if isinstance(entry, str):
        return entry
    return None


def fetch_register_urls(from_date, to_date):
    url = f"{MOTUS_API_BASE}/{from_date}/{to_date}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def download_pdf_bytes(signed_url):
    r = requests.get(signed_url, timeout=30)
    r.raise_for_status()
    return r.content


def _is_category_header(line):
    if len(line) < 6 or line[0].isdigit():
        return False
    if any(line.startswith(p) for p in _SKIP_PREFIXES):
        return False
    letters = [c for c in line if c.isalpha()]
    if not letters:
        return False
    return all(c.isupper() for c in letters)


def parse_register_pdf(pdf_bytes, include_categories=None):
    """Returns a list of dicts: {"usdot", "raw", "category"} -- NOT yet
    deduped across multiple days' PDFs."""
    include_categories = [c.upper() for c in (include_categories or MOTUS_INCLUDE_CATEGORIES)]

    rows = []
    current_category = ""

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.split("\n"):
                line = raw_line.strip()
                if not line:
                    continue
                if any(line.startswith(p) for p in _SKIP_PREFIXES):
                    continue

                if _is_category_header(line):
                    current_category = line
                    continue

                m = _ROW_START_RE.match(line)
                if m and current_category and any(cat in current_category.upper() for cat in include_categories):
                    rows.append({
                        "usdot": m.group(1),
                        "raw": m.group(2).strip(),
                        "category": current_category,
                    })

    return rows


def dedupe_rows(rows):
    seen = set()
    out = []
    for row in rows:
        if row["usdot"] in seen:
            continue
        seen.add(row["usdot"])
        out.append(row)
    return out


def fetch_and_parse_range(from_date, to_date, include_categories=None):
    """Raises requests.RequestException on network failure."""
    urls = fetch_register_urls(from_date, to_date)
    register_entries = urls.get("Register", []) or []

    print(f"[motus_register] Register entries raw: {register_entries!r}")

    all_rows = []
    skipped = 0
    for entry in register_entries:
        signed_url = _extract_url(entry)
        if not signed_url:
            skipped += 1
            continue
        pdf_bytes = download_pdf_bytes(signed_url)
        all_rows.extend(parse_register_pdf(pdf_bytes, include_categories))

    if skipped:
        print(f"[motus_register] Skipped {skipped} Register entr(ies) with no extractable URL")

    deduped = dedupe_rows(all_rows)
    print(f"[motus_register] Unique USDOT rows after filtering+dedupe: {len(deduped)}")
    return deduped, len(register_entries)
