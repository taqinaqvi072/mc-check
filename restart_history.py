"""Restarted Carriers — FMCSA AuthHist "All With History" integration.

Source: FMCSA "AuthHist - All With History" dataset on Socrata
(https://catalog.data.gov/dataset/authhist-all-with-history), dataset id
9mw4-x3tu.

CONFIRMED SCHEMA (from a live 400-error response, since Socrata echoes the
resolved column list, and re-confirmed against a live sample fetch): this
dataset has ONLY these columns —

    docket_number, dot_number, sub_number, mod_col_1,
    original_action_desc, orig_served_date,
    disp_action_desc, disp_decided_date, disp_served_date

There is NO usdot_number/op_auth_type/reason/status_change_date here (that
schema belongs to motus.py's DAILY-DIFFERENCE dataset, dm5j-zc6c — a
different dataset). Each row in THIS dataset is one full authority record
for one docket, carrying both:
  - its start:  original_action_desc (e.g. "GRANTED") + orig_served_date
  - its end:    disp_action_desc (e.g. "REVOKED")     + disp_decided_date
                (disp_served_date as a fallback when disp_decided_date is
                blank)
There is no carrier-type/category column on this dataset, so category
filtering (Property/Passengers) is NOT possible here.

Dates come back from Socrata as plain "MM/DD/YYYY" strings (no time
component) — confirmed against a live sample fetch — which is why a
server-side "$where date >= '...'" filter never matched (see below);
_clean_date() below normalises this (and a couple of other formats) to
"YYYY-MM-DD" so grouping/sorting/year-filtering work correctly.

RESTART DEFINITION: for the same (dot_number, docket_number), one row's
authority record ENDS (has a disp_decided_date/disp_served_date), and a
LATER row for that same docket STARTS again afterwards (orig_served_date
after that end date). That later start is the "restart".

Because this dataset holds decades of history and has no per-carrier
category to narrow with, we fetch the WHOLE dataset (paged) and do all
grouping/restart-detection/year-filtering in Python. A server-side
$where date filter was tried first but silently returned 0 rows every
time — the date columns here don't appear to be a real Socrata date
type, so a ">=" comparison against an ISO literal never matched. Once
fetched, MIN_HISTORY_YEAR trims anything with no date at all in-range
(see _row_relevant_year() below) before restart-detection runs, and the
caller's requested year narrows the final output.

--------------------------------------------------------------------------
FIXES APPLIED (2026-09-27), after pulling a live sample from the dataset
and tracing real dockets (e.g. MC124003 / DOT 00012312) through the
restart-detection logic by hand:

1. The MIN_HISTORY_YEAR trim used to drop rows by orig_served_date only.
   That silently discarded rows where an OLD authority (granted well
   before MIN_HISTORY_YEAR) was REVOKED recently (e.g. granted 1996,
   revoked 2023) — exactly the rows needed to detect a same-year restart.
   Fixed by keeping a row if EITHER its orig_served_date OR its
   disposition date falls within range (see _row_relevant_year()).
   Blank orig_served_date rows (which used to be dropped outright because
   "".isdigit() is False) are also now correctly kept when they carry a
   relevant disposition date.

2. "DISCONTINUED REVOCATION" was being treated as a genuine
   authority-ending disposition (it wasn't in
   DISPOSITION_EXCLUDE_KEYWORDS). It shouldn't be: it means an
   involuntary-revocation NOTICE was issued and then withdrawn/cancelled
   — the carrier's authority never actually stopped. Treating it as a
   real end corrupts the pending "paused" record with a bogus date/reason
   and can cause a genuine restart to be matched against (or reset by)
   the wrong event. Added to DISPOSITION_EXCLUDE_KEYWORDS.

KNOWN REMAINING SIMPLIFICATION (not fixed here, flagging for later):
grouping is still by (dot_number, docket_number) only, ignoring
sub_number/mod_col_1. Distinct authority "categories" under the same
docket (e.g. COMMON vs CONTRACT vs BROKER) get treated as one continuous
chronological chain. FMCSA's own category labels for the same docket are
inconsistent across decades (e.g. "MOTOR PROPERTY COMMON CARRIER" vs
plain "COMMON"), so splitting groups by mod_col_1 as-is would likely
create MORE false splits than it fixes. Leaving this alone until we have
a reliable way to normalise mod_col_1 across eras.
--------------------------------------------------------------------------
"""
import requests

AUTHHIST_HISTORY_API = "https://data.transportation.gov/resource/9mw4-x3tu.json"

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 50000  # Socrata max per page; paged with $offset

# Don't bother pulling authority records with NO date at all in-range —
# restarts from decades ago aren't useful, and it keeps the one-time fetch
# smaller. NOTE: a row is kept if ANY of its dates (orig OR disposition)
# is >= this year — see _row_relevant_year(). Do not filter on
# orig_served_date alone; see fix #1 in the module docstring above.
MIN_HISTORY_YEAR = 2010

# Keywords marking a disposition as the authority genuinely ending (as
# opposed to e.g. a clerical/administrative disposition that isn't really
# a pause, or a revocation notice that was itself cancelled). Kept
# permissive — any row with a disposition date at all is treated as
# "ended"; these keywords only exclude a few disposition types that
# shouldn't count as a real pause.
DISPOSITION_EXCLUDE_KEYWORDS = [
    "DISMISS",
    "WITHDRAWN BY APPLICANT PRE-GRANT",
    # Fix #2: this means the revocation NOTICE was cancelled/withdrawn —
    # the authority never actually stopped, so it must not be treated as
    # a real "pause" end date.
    "DISCONTINUED REVOCATION",
]

# Keywords marking a later record's start as a genuine (re)start, as
# opposed to some other administrative original-action type.
RESTART_KEYWORDS = ["GRANT", "REINSTAT"]


def _normalise(value):
    return str(value or "").strip().upper()


import re

_MDY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _clean_date(value):
    """Format-flexible date normaliser -> 'YYYY-MM-DD' or ''. Handles ISO
    timestamps/dates, compact YYYYMMDD, and MM/DD/YYYY (this dataset's
    date columns turned out to be silently unfilterable via $where — a
    strong sign they're stored as plain text rather than a real Socrata
    date type; confirmed via a live sample fetch to be plain
    'MM/DD/YYYY' with no time component)."""
    value = str(value or "").strip()
    if not value:
        return ""
    if "T" in value:
        return value.split("T")[0]
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    m = _MDY_RE.match(value)
    if m:
        month, day, year = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    if len(value) == 10 and value[4] == "-":
        return value  # already YYYY-MM-DD
    return value


def fetch_authhist_history_rows():
    """Pages through the ENTIRE AuthHist "All With History" dataset.

    NOTE: an earlier version tried to filter server-side with
    "$where orig_served_date >= '...'" — that silently returned 0 rows
    every time, with no error. The likely cause: this column isn't a real
    Socrata date/timestamp type, so ">=" was doing a plain STRING
    comparison against our ISO-format literal, which never matches a
    MM/DD/YYYY-formatted value. Filtering by year is therefore done in
    Python (see fetch_and_parse_restarts) after normalising every row's
    dates with _clean_date() above, which understands both formats.
    """
    all_rows = []
    offset = 0
    page_num = 0
    while True:
        params = {
            "$limit": PAGE_LIMIT,
            "$offset": offset,
            "$order": "dot_number ASC, docket_number ASC",
        }
        response = requests.get(AUTHHIST_HISTORY_API, params=params, timeout=REQUEST_TIMEOUT)

        if response.status_code >= 400:
            raise ValueError(
                f"AuthHist All-With-History API returned {response.status_code}: {response.text[:500]}"
            )

        page_rows = response.json()
        if not isinstance(page_rows, list):
            raise ValueError(f"Unexpected AuthHist response type: {type(page_rows).__name__}")

        if page_num == 0 and page_rows:
            print(f"[restart_history] Sample raw row: {page_rows[0]}")

        all_rows.extend(page_rows)
        print(f"[restart_history] Page {page_num}: {len(page_rows)} rows (offset {offset})")

        if len(page_rows) < PAGE_LIMIT:
            break  # last page
        offset += PAGE_LIMIT
        page_num += 1

    print(f"[restart_history] Total rows fetched: {len(all_rows)}")
    return all_rows


def _normalise_row(row):
    return {
        "dot_number": str(row.get("dot_number") or "").strip(),
        "docket_number": str(row.get("docket_number") or "").strip(),
        "sub_number": str(row.get("sub_number") or "").strip(),
        "mod_col_1": str(row.get("mod_col_1") or "").strip(),
        "original_action_desc": str(row.get("original_action_desc") or "").strip(),
        "orig_served_date": _clean_date(row.get("orig_served_date")),
        "disp_action_desc": str(row.get("disp_action_desc") or "").strip(),
        "disp_decided_date": _clean_date(row.get("disp_decided_date")),
        "disp_served_date": _clean_date(row.get("disp_served_date")),
    }


def _end_date(row):
    """A row's authority-ended date: disp_decided_date, falling back to
    disp_served_date when the former is blank."""
    return row["disp_decided_date"] or row["disp_served_date"]


def _row_relevant_year(row):
    """Returns the row's most relevant year for the MIN_HISTORY_YEAR trim,
    or None if the row has no usable date at all.

    FIX #1: previously this trim looked only at orig_served_date, which
    silently dropped rows where an OLD authority (granted long before
    MIN_HISTORY_YEAR) was revoked/disposed RECENTLY — exactly the rows
    needed to detect a same-year restart (e.g. granted 1996, revoked
    2023, reinstated 2023: the 1996 row's disposition IS the 2023 pause).
    We now keep a row if ANY of its dates — orig OR disposition — is
    within range, and only drop a row when NONE of its dates qualify (or
    it has no dates at all).
    """
    end_date = _end_date(row)
    for date_str in (end_date, row["orig_served_date"]):
        year_str = date_str[:4]
        if year_str.isdigit():
            return int(year_str)
    return None


def _is_real_disposition(row):
    end_date = _end_date(row)
    if not end_date:
        return False
    desc = _normalise(row["disp_action_desc"])
    return not any(kw in desc for kw in DISPOSITION_EXCLUDE_KEYWORDS)


def _is_restart_start(row):
    desc = _normalise(row["original_action_desc"])
    return any(kw in desc for kw in RESTART_KEYWORDS)


def group_by_docket(rows):
    """Groups normalised rows by (dot_number, docket_number) — the same
    authority, tracked across its modifications/re-grants — sorted by
    orig_served_date ascending.

    NOTE: rows with a blank orig_served_date sort first (empty string).
    That's harmless for restart-detection here since such rows only ever
    matter via their disposition date (see _is_real_disposition), not
    their position as a "restart start" — a blank-orig row can still
    become `pending_end`, but _is_restart_start only fires on rows that
    HAVE a real orig_served_date to compare chronologically.
    """
    groups = {}
    for row in rows:
        if not row["dot_number"] or not row["docket_number"]:
            continue
        key = (row["dot_number"], row["docket_number"])
        groups.setdefault(key, []).append(row)
    for key in groups:
        groups[key].sort(key=lambda r: r["orig_served_date"])
    return groups


def detect_restarts(docket_groups):
    """Walks each (dot_number, docket_number)'s sorted rows. Whenever a
    row has a real disposition (its authority ended) and a LATER row for
    the same docket starts again afterwards, that's a restart."""
    results = []

    for (dot_number, docket_number), rows in docket_groups.items():
        pending_end = None  # most recent unmatched disposition row

        for row in rows:
            if pending_end is not None and _is_restart_start(row) and row["orig_served_date"]:
                if row["orig_served_date"] > _end_date(pending_end):
                    results.append({
                        "dot_number": dot_number,
                        "docket_number": docket_number,
                        "paused_date": _end_date(pending_end),
                        "paused_reason": pending_end["disp_action_desc"],
                        "restarted_date": row["orig_served_date"],
                        "restarted_reason": row["original_action_desc"],
                    })
                    pending_end = None  # matched — reset for further cycles

            if _is_real_disposition(row):
                pending_end = row

    return results


def dedupe_restarts(rows):
    seen = set()
    out = []
    for row in rows:
        key = (row["dot_number"], row["docket_number"], row["paused_date"], row["restarted_date"])
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def fetch_and_parse_restarts(year):
    """Public entry point: fetches the full AuthHist All-With-History
    dataset, detects every restart across all history, then filters down
    to restarts whose restarted_date falls in `year`.

    Returns (restart_rows, source_row_count). Each restart row has:
    usdot, docket, paused_date, paused_reason, restarted_date,
    restarted_reason — matching the shape app.py's restart_worker expects.
    """
    print(f"[restart_history] Fetching full AuthHist history (target year: {year})")
    raw_rows = fetch_authhist_history_rows()
    normalised = [_normalise_row(r) for r in raw_rows]

    # Drop records with NO date (orig OR disposition) in-range — in Python,
    # since the server-side $where filter on these fields doesn't work
    # (see fetch_authhist_history_rows above). See _row_relevant_year() and
    # fix #1 in the module docstring: this used to check orig_served_date
    # only, which silently dropped old-grant/recently-revoked rows that
    # are exactly the ones needed to detect a same-year restart.
    kept = []
    for r in normalised:
        relevant_year = _row_relevant_year(r)
        if relevant_year is not None and relevant_year >= MIN_HISTORY_YEAR:
            kept.append(r)
    normalised = kept

    grouped = group_by_docket(normalised)
    all_restarts = detect_restarts(grouped)
    deduped = dedupe_restarts(all_restarts)

    year_str = str(year)
    in_year = [r for r in deduped if r["restarted_date"][:4] == year_str]

    # Map to the field names app.py's restart_worker/CSV already expect.
    final_rows = [{
        "usdot": r["dot_number"],
        "docket": r["docket_number"],
        "paused_date": r["paused_date"],
        "paused_reason": r["paused_reason"],
        "restarted_date": r["restarted_date"],
        "restarted_reason": r["restarted_reason"],
    } for r in in_year]

    print(f"[restart_history] Total restarts detected (all years): {len(deduped)}")
    print(f"[restart_history] Restarts in {year}: {len(final_rows)}")
    return final_rows, len(raw_rows)
