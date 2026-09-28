"""Restarted Carriers — FMCSA AuthHist "All With History" integration.

Source: FMCSA "AuthHist - All With History" dataset on Socrata
(https://catalog.data.gov/dataset/authhist-all-with-history), dataset id
9mw4-x3tu.

CONFIRMED SCHEMA (from a live 400-error response, and re-confirmed against
a live sample fetch): this dataset has ONLY these columns —

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

Dates come back from Socrata as plain "MM/DD/YYYY" strings, ZERO-PADDED
(confirmed via a live sample fetch — e.g. "06/24/2005", not "6/24/2005"),
with no time component. That zero-padding is what makes month-scoped
`like` filtering reliable (see Phase 1 below) — a server-side "$where
date >= '...'" filter never worked here because these columns aren't a
real Socrata date type, so ">=" was doing a plain STRING comparison
against an ISO literal that never matches MM/DD/YYYY.

RESTART DEFINITION: for the same (dot_number, docket_number), one row's
authority record ENDS (has a disp_decided_date/disp_served_date), and a
LATER row for that same docket STARTS again afterwards (orig_served_date
after that end date). That later start is the "restart".

--------------------------------------------------------------------------
Two-phase targeted fetch (instead of dumping the entire dataset):

The dataset covers ~90 years of FMCSA authority history across every
carrier that has ever existed — easily several million rows. Fetching it
all into memory at once was enough to exhaust RAM on a small server,
causing the host to OOM-kill and restart the process mid-fetch (seen as
a burst of 502s, followed by the in-memory job state being wiped so the
UI showed 0 results even though the search "completed").

Fix: only fetch what's needed for the requested period.

  Phase 1 — fetch ONLY rows where original_action_desc looks like a
  grant/reinstatement AND orig_served_date falls in the requested
  year (optionally narrowed to a single month too), via SoQL `like` on
  these plain-text columns.

  Phase 2 — take the distinct docket_numbers from Phase 1 and fetch each
  one's FULL history (batched via `docket_number in (...)`, ~100 at a
  time) so we have enough context to tell whether that grant/
  reinstatement was preceded by a real prior disposition (i.e. is
  actually a restart, not a brand-new carrier's first authority).

MONTH SCOPING (mandatory): fetch_and_parse_restarts(year, month) requires
a `month` (1-12) — full-year searches are no longer supported, since a
full-year candidate set (and the resulting Phase 2 docket count) was
still large enough to make a search slow. Phase 1 is always narrowed to
the given month (e.g. "03/%/2023"), and the final filter checks both
year and month.

Also carried over from the previous bugfix pass:
  - "DISCONTINUED REVOCATION" is in DISPOSITION_EXCLUDE_KEYWORDS — it
    means a revocation NOTICE was cancelled/withdrawn, i.e. the authority
    never actually stopped, so it must not be treated as a real "pause"
    end date (it used to corrupt the pending-pause tracking).

KNOWN REMAINING SIMPLIFICATION: grouping is still by
(dot_number, docket_number) only, ignoring sub_number/mod_col_1. Distinct
authority "categories" under the same docket (e.g. COMMON vs CONTRACT vs
BROKER) get treated as one continuous chronological chain. FMCSA's own
category labels for the same docket are inconsistent across decades, so
splitting groups by mod_col_1 as-is would likely create MORE false splits
than it fixes. Leaving this alone until there's a reliable way to
normalise mod_col_1 across eras.
--------------------------------------------------------------------------
"""
import re
import time

import requests

AUTHHIST_HISTORY_API = "https://data.transportation.gov/resource/9mw4-x3tu.json"

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 50000  # Socrata max per page; paged with $offset

# How many docket_numbers to pack into one Phase-2 "in (...)" query.
DOCKET_BATCH_SIZE = 100

# Small pause between paginated/batched requests — there's no Socrata
# app token configured here, so unauthenticated requests are subject to
# tighter throttling; this keeps us polite and reduces 429s.
REQUEST_PACING_SECONDS = 0.2

# Retry policy for transient errors (429/5xx/network).
MAX_RETRIES = 4
RETRY_BACKOFF_BASE_SECONDS = 2

# Keywords marking a disposition as the authority genuinely ending (as
# opposed to e.g. a clerical/administrative disposition that isn't really
# a pause, or a revocation notice that was itself cancelled). Kept
# permissive — any row with a disposition date at all is treated as
# "ended"; these keywords only exclude a few disposition types that
# shouldn't count as a real pause.
DISPOSITION_EXCLUDE_KEYWORDS = [
    "DISMISS",
    "WITHDRAWN BY APPLICANT PRE-GRANT",
    # This means the revocation NOTICE was cancelled/withdrawn — the
    # authority never actually stopped, so it must not be treated as a
    # real "pause" end date.
    "DISCONTINUED REVOCATION",
]

# Keywords marking a later record's start as a genuine (re)start, as
# opposed to some other administrative original-action type. Also used
# (as a SoQL `like '%...%'` filter) to build the Phase-1 candidate query.
RESTART_KEYWORDS = ["GRANT", "REINSTAT"]

_MDY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _normalise(value):
    return str(value or "").strip().upper()


def _soql_escape(value):
    """Escapes a value for safe interpolation inside a SoQL string
    literal ('...'): SoQL uses '' to escape a literal single quote."""
    return str(value).replace("'", "''")


def _clean_date(value):
    """Format-flexible date normaliser -> 'YYYY-MM-DD' or ''. Handles ISO
    timestamps/dates, compact YYYYMMDD, and MM/DD/YYYY (confirmed via a
    live sample fetch to be this dataset's actual, zero-padded format)."""
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


def _get_with_retries(params):
    """GET against the AuthHist endpoint with retry/backoff on
    throttling or transient server errors. Raises ValueError on a
    non-retryable error or once retries are exhausted."""
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(AUTHHIST_HISTORY_API, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_error = f"network error: {e}"
        else:
            if response.status_code < 400:
                return response.json()
            if response.status_code in (429, 500, 502, 503, 504):
                last_error = f"{response.status_code}: {response.text[:200]}"
            else:
                raise ValueError(
                    f"AuthHist API returned {response.status_code}: {response.text[:500]}"
                )

        if attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"[restart_history] Request failed ({last_error}); retrying in {wait}s "
                  f"(attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)

    raise ValueError(f"AuthHist API request failed after {MAX_RETRIES} retries: {last_error}")


def _fetch_all_pages(where_clause):
    """Pages through every row matching `where_clause` (a SoQL $where
    string). Used for both the Phase-1 candidate query and each Phase-2
    docket-history batch — both are expected to return far fewer rows
    than the full dataset, so this stays bounded in memory."""
    all_rows = []
    offset = 0
    page_num = 0
    while True:
        params = {
            "$limit": PAGE_LIMIT,
            "$offset": offset,
            "$where": where_clause,
            "$order": "dot_number ASC, docket_number ASC",
        }
        page_rows = _get_with_retries(params)
        if not isinstance(page_rows, list):
            raise ValueError(f"Unexpected AuthHist response type: {type(page_rows).__name__}")

        all_rows.extend(page_rows)

        if len(page_rows) < PAGE_LIMIT:
            break  # last page
        offset += PAGE_LIMIT
        page_num += 1
        time.sleep(REQUEST_PACING_SECONDS)

    return all_rows


def fetch_candidate_restart_rows(year, month=None):
    """Phase 1: fetch only rows that look like a grant/reinstatement
    served in `year` (optionally narrowed to a single `month`, 1-12).

    Both conditions are filtered server-side — these are plain text
    columns, so we use `like`, matching the same contains-style logic as
    _is_restart_start() below (not a strict prefix match), and a
    date-suffix match on orig_served_date (reliable since the format is
    a fixed-width, zero-padded 'MM/DD/YYYY').
    """
    keyword_clauses = " OR ".join(
        f"upper(original_action_desc) like '%{_soql_escape(kw)}%'"
        for kw in RESTART_KEYWORDS
    )
    if month:
        date_pattern = f"{int(month):02d}/%/{int(year)}"
        period_label = f"{int(year)}-{int(month):02d}"
    else:
        date_pattern = f"%/{int(year)}"
        period_label = str(year)

    where_clause = f"({keyword_clauses}) AND orig_served_date like '{date_pattern}'"
    print(f"[restart_history] Phase 1: fetching {period_label} grant/reinstatement candidates")
    rows = _fetch_all_pages(where_clause)
    print(f"[restart_history] Phase 1: {len(rows)} candidate rows")
    return rows


def fetch_docket_histories(docket_numbers):
    """Phase 2: fetch the FULL history (every row, any year) for each
    docket_number in `docket_numbers`, batched to keep each query and
    response a manageable size."""
    docket_numbers = sorted(set(d for d in docket_numbers if d))
    if not docket_numbers:
        return []

    all_rows = []
    total_batches = (len(docket_numbers) + DOCKET_BATCH_SIZE - 1) // DOCKET_BATCH_SIZE
    print(f"[restart_history] Phase 2: fetching full history for {len(docket_numbers)} "
          f"dockets in {total_batches} batch(es)")

    for i in range(0, len(docket_numbers), DOCKET_BATCH_SIZE):
        batch = docket_numbers[i:i + DOCKET_BATCH_SIZE]
        quoted = ",".join(f"'{_soql_escape(d)}'" for d in batch)
        where_clause = f"docket_number in ({quoted})"
        batch_rows = _fetch_all_pages(where_clause)
        all_rows.extend(batch_rows)
        time.sleep(REQUEST_PACING_SECONDS)

    print(f"[restart_history] Phase 2: {len(all_rows)} history rows fetched")
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
    become `pending_end`, but _is_restart_start is only checked against
    rows that HAVE a real orig_served_date to compare chronologically.
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
            if pending_end is not None and row["orig_served_date"] and _is_restart_start(row):
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


def fetch_and_parse_restarts(year, month=None):
    """Public entry point: two-phase targeted fetch (see module docstring
    for why), optionally scoped to a single `month` (1-12) as well as
    `year`, then detects every restart among the fetched dockets and
    filters down to restarts whose restarted_date falls in that period.

    Returns (restart_rows, source_row_count). Each restart row has:
    usdot, docket, paused_date, paused_reason, restarted_date,
    restarted_reason — matching the shape app.py's restart_worker expects.
    """
    candidate_rows_raw = fetch_candidate_restart_rows(year, month)
    candidate_normalised = [_normalise_row(r) for r in candidate_rows_raw]
    docket_numbers = [r["docket_number"] for r in candidate_normalised]

    history_rows_raw = fetch_docket_histories(docket_numbers)
    normalised = [_normalise_row(r) for r in history_rows_raw]

    grouped = group_by_docket(normalised)
    all_restarts = detect_restarts(grouped)
    deduped = dedupe_restarts(all_restarts)

    year_str = str(year)
    month_str = f"{int(month):02d}" if month else None

    def _in_period(r):
        d = r["restarted_date"]
        if d[:4] != year_str:
            return False
        if month_str is not None and d[5:7] != month_str:
            return False
        return True

    in_period = [r for r in deduped if _in_period(r)]

    # Map to the field names app.py's restart_worker/CSV already expect.
    final_rows = [{
        "usdot": r["dot_number"],
        "docket": r["docket_number"],
        "paused_date": r["paused_date"],
        "paused_reason": r["paused_reason"],
        "restarted_date": r["restarted_date"],
        "restarted_reason": r["restarted_reason"],
    } for r in in_period]

    source_row_count = len(candidate_rows_raw) + len(history_rows_raw)

    period_label = f"{year}-{month_str}" if month_str else str(year)
    print(f"[restart_history] Total restarts detected: {len(deduped)}")
    print(f"[restart_history] Restarts in {period_label}: {len(final_rows)}")
    return final_rows, source_row_count
