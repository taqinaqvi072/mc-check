"""Restarted Carriers — hybrid FMCSA AuthHist integration.

TWO DATA SOURCES (FMCSA froze the old one on 05/14/2026):

  1. LEGACY  "AuthHist - All With History"  (9mw4-x3tu)
     Frozen: "last refreshed on 05/14/2026 and will no longer be updated".
     Row = one authority record carrying BOTH its start
     (original_action_desc + orig_served_date, MM/DD/YYYY) and its end
     (disp_action_desc + disp_decided_date). Used for periods up to and
     including LEGACY_LAST_PERIOD (April 2026).

  2. MOTUS   "Motus AuthHist - All With History"  (yu5v-wbh6)
     Live. Row = one STATUS CHANGE:
       docket_number, usdot_number, op_auth_type,
       op_auth_status (Active / Pending / Inactive / Withdrawn),
       reason, status_change_date (text, YYYYMMDD).
     Used for May 2026 onwards.
     If this dataset id turns out to be wrong, change MOTUS_API only —
     the schema above is what the code expects (same as dm5j-zc6c).

ROUTER: fetch_and_parse_restarts(year, month=None) picks the source by
period, so app.py needs no changes (see the small optional CSV patch at
the bottom of this file's notes in the chat).

MOTUS RESTART DEFINITION: for the same (usdot, docket, op_auth_type),
an Inactive row (a real pause) is followed LATER by an Active row whose
reason contains GRANT or REINSTAT. That Active row is the restart.
  - Withdrawn / Pending are never pauses.
  - Inactive rows with reason "Administrative status adjustment" or
    "Administrative Correction" are ignored (bookkeeping, not a real pause).
  - "Initial Status" + Inactive IS counted as a pause (the carrier was
    already inactive when the Motus dataset started). Its date is whatever
    the dataset says and may not be the true original pause date.
  - If several Inactive rows come in a row (e.g. suspension then
    revocation) the FIRST one is kept as the pause start.

KNOWN LIMITATION: the Motus dataset starts around the Motus migration
(mid-2026), so it does not hold decades of history like the legacy one.
A restart is only detected if its pause row is in the Motus dataset
(including "Initial Status" rows).

Output rows (both sources): usdot, docket, paused_date, paused_reason,
restarted_date, restarted_reason, source. Motus rows also carry
authority_type. Dates are YYYY-MM-DD.
"""
import re
import time

import requests

LEGACY_API = "https://data.transportation.gov/resource/9mw4-x3tu.json"
MOTUS_API = "https://data.transportation.gov/resource/yu5v-wbh6.json"
AUTHHIST_HISTORY_API = LEGACY_API  # backwards-compatible alias

# Last (year, month) served from the legacy dataset. Anything after this
# comes from Motus.
LEGACY_LAST_PERIOD = (2026, 4)
MOTUS_FIRST_MONTH_OF_LAST_LEGACY_YEAR = LEGACY_LAST_PERIOD[1] + 1  # May

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 50000
DOCKET_BATCH_SIZE = 100
REQUEST_PACING_SECONDS = 0.2
MAX_RETRIES = 4
RETRY_BACKOFF_BASE_SECONDS = 2

LEGACY_ORDER = "dot_number ASC, docket_number ASC, sub_number ASC, orig_served_date ASC"
MOTUS_ORDER = "usdot_number ASC, docket_number ASC, status_change_date ASC, op_auth_type ASC"

DISPOSITION_EXCLUDE_KEYWORDS = [
    "DISMISS",
    "WITHDRAWN BY APPLICANT PRE-GRANT",
    "DISCONTINUED REVOCATION",
]
RESTART_KEYWORDS = ["GRANT", "REINSTAT"]

# Motus: Inactive rows with these reasons are bookkeeping, not real pauses.
MOTUS_PAUSE_EXCLUDE_REASONS = [
    "ADMINISTRATIVE STATUS ADJUSTMENT",
    "ADMINISTRATIVE CORRECTION",
]

_MDY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _normalise(value):
    return str(value or "").strip().upper()


def _soql_escape(value):
    return str(value).replace("'", "''")


def _clean_date(value):
    """-> 'YYYY-MM-DD' or ''. Handles ISO, compact YYYYMMDD (Motus),
    and MM/DD/YYYY (legacy)."""
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
        return value
    return value


def _get_with_retries(params, api_url):
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(api_url, params=params, timeout=REQUEST_TIMEOUT)
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


def _fetch_all_pages(where_clause, api_url, order):
    all_rows = []
    offset = 0
    while True:
        params = {
            "$limit": PAGE_LIMIT,
            "$offset": offset,
            "$where": where_clause,
            "$order": order,
        }
        page_rows = _get_with_retries(params, api_url)
        if not isinstance(page_rows, list):
            raise ValueError(f"Unexpected AuthHist response type: {type(page_rows).__name__}")
        all_rows.extend(page_rows)
        if len(page_rows) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        time.sleep(REQUEST_PACING_SECONDS)
    return all_rows


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


# ---------------------------------------------------------------------------
# LEGACY source (9mw4-x3tu) — logic unchanged from before
# ---------------------------------------------------------------------------
def fetch_candidate_restart_rows(year, month=None):
    keyword_clauses = " OR ".join(
        f"upper(original_action_desc) like '%{_soql_escape(kw)}%'"
        for kw in RESTART_KEYWORDS
    )
    if month:
        date_pattern = f"{int(month):02d}/%/{int(year)}"
    else:
        date_pattern = f"%/{int(year)}"

    where_clause = f"({keyword_clauses}) AND orig_served_date like '{date_pattern}'"
    print(f"[restart_history] LEGACY phase 1: {year}-{month or 'all'} candidates")
    rows = _fetch_all_pages(where_clause, LEGACY_API, LEGACY_ORDER)
    print(f"[restart_history] LEGACY phase 1: {len(rows)} candidate rows")
    return rows


def fetch_docket_histories(docket_numbers):
    docket_numbers = sorted(set(d for d in docket_numbers if d))
    if not docket_numbers:
        return []

    all_rows = []
    total_batches = (len(docket_numbers) + DOCKET_BATCH_SIZE - 1) // DOCKET_BATCH_SIZE
    print(f"[restart_history] LEGACY phase 2: {len(docket_numbers)} dockets, {total_batches} batch(es)")

    for i in range(0, len(docket_numbers), DOCKET_BATCH_SIZE):
        batch = docket_numbers[i:i + DOCKET_BATCH_SIZE]
        quoted = ",".join(f"'{_soql_escape(d)}'" for d in batch)
        all_rows.extend(_fetch_all_pages(f"docket_number in ({quoted})", LEGACY_API, LEGACY_ORDER))
        time.sleep(REQUEST_PACING_SECONDS)

    print(f"[restart_history] LEGACY phase 2: {len(all_rows)} history rows")
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
    return row["disp_decided_date"] or row["disp_served_date"]


def _is_real_disposition(row):
    if not _end_date(row):
        return False
    desc = _normalise(row["disp_action_desc"])
    return not any(kw in desc for kw in DISPOSITION_EXCLUDE_KEYWORDS)


def _is_restart_start(row):
    desc = _normalise(row["original_action_desc"])
    return any(kw in desc for kw in RESTART_KEYWORDS)


def group_by_docket(rows):
    groups = {}
    for row in rows:
        if not row["dot_number"] or not row["docket_number"]:
            continue
        groups.setdefault((row["dot_number"], row["docket_number"]), []).append(row)
    for key in groups:
        groups[key].sort(key=lambda r: r["orig_served_date"])
    return groups


def detect_restarts(docket_groups):
    results = []
    for (dot_number, docket_number), rows in docket_groups.items():
        pending_end = None
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
                    pending_end = None
            if _is_real_disposition(row):
                pending_end = row
    return results


def _fetch_legacy_restarts(year, month):
    """Legacy source, month-scoped (or whole-year if month is None)."""
    candidate_rows_raw = fetch_candidate_restart_rows(year, month)
    docket_numbers = [_normalise_row(r)["docket_number"] for r in candidate_rows_raw]

    history_rows_raw = fetch_docket_histories(docket_numbers)
    grouped = group_by_docket([_normalise_row(r) for r in history_rows_raw])
    deduped = dedupe_restarts(detect_restarts(grouped))

    year_str = str(year)
    month_str = f"{int(month):02d}" if month else None

    def _in_period(r):
        d = r["restarted_date"]
        if d[:4] != year_str:
            return False
        return month_str is None or d[5:7] == month_str

    final_rows = [{
        "usdot": r["dot_number"],
        "docket": r["docket_number"],
        "paused_date": r["paused_date"],
        "paused_reason": r["paused_reason"],
        "restarted_date": r["restarted_date"],
        "restarted_reason": r["restarted_reason"],
        "source": "Legacy AuthHist",
    } for r in deduped if _in_period(r)]

    print(f"[restart_history] LEGACY restarts in {year}-{month_str or 'all'}: {len(final_rows)}")
    return final_rows, len(candidate_rows_raw) + len(history_rows_raw)


# ---------------------------------------------------------------------------
# MOTUS source (yu5v-wbh6)
# ---------------------------------------------------------------------------
def _normalise_motus_row(row):
    return {
        "usdot": str(row.get("usdot_number") or "").strip(),
        "docket": str(row.get("docket_number") or "").strip(),
        "op_auth_type": str(row.get("op_auth_type") or "").strip(),
        "status": str(row.get("op_auth_status") or "").strip(),
        "reason": str(row.get("reason") or "").strip(),
        "date": _clean_date(row.get("status_change_date")),
    }


def _motus_is_pause(row):
    if not row["date"] or _normalise(row["status"]) != "INACTIVE":
        return False
    reason = _normalise(row["reason"])
    return not any(ex in reason for ex in MOTUS_PAUSE_EXCLUDE_REASONS)


def _motus_is_restart_start(row):
    if not row["date"] or _normalise(row["status"]) != "ACTIVE":
        return False
    reason = _normalise(row["reason"])
    return any(kw in reason for kw in RESTART_KEYWORDS)


def fetch_motus_candidates(year, month=None):
    """Phase 1: Active rows granted/reinstated in the period. Dates are
    plain YYYYMMDD text, so a prefix `like` is reliable."""
    keyword_clauses = " OR ".join(
        f"upper(reason) like '%{_soql_escape(kw)}%'" for kw in RESTART_KEYWORDS
    )
    prefix = f"{int(year)}{int(month):02d}%" if month else f"{int(year)}%"
    where_clause = (
        f"op_auth_status = 'Active' AND ({keyword_clauses}) "
        f"AND status_change_date like '{prefix}'"
    )
    print(f"[restart_history] MOTUS phase 1: {year}-{month or 'all'} candidates")
    rows = _fetch_all_pages(where_clause, MOTUS_API, MOTUS_ORDER)
    print(f"[restart_history] MOTUS phase 1: {len(rows)} candidate rows")
    return rows


def fetch_motus_histories(usdot_numbers):
    """Phase 2: every status-change row for each candidate carrier."""
    usdot_numbers = sorted(set(u for u in usdot_numbers if u))
    if not usdot_numbers:
        return []

    all_rows = []
    total_batches = (len(usdot_numbers) + DOCKET_BATCH_SIZE - 1) // DOCKET_BATCH_SIZE
    print(f"[restart_history] MOTUS phase 2: {len(usdot_numbers)} carriers, {total_batches} batch(es)")

    for i in range(0, len(usdot_numbers), DOCKET_BATCH_SIZE):
        batch = usdot_numbers[i:i + DOCKET_BATCH_SIZE]
        quoted = ",".join(f"'{_soql_escape(u)}'" for u in batch)
        all_rows.extend(_fetch_all_pages(f"usdot_number in ({quoted})", MOTUS_API, MOTUS_ORDER))
        time.sleep(REQUEST_PACING_SECONDS)

    print(f"[restart_history] MOTUS phase 2: {len(all_rows)} history rows")
    return all_rows


def group_motus(rows):
    groups = {}
    for row in rows:
        if not row["usdot"] or not row["docket"]:
            continue
        groups.setdefault((row["usdot"], row["docket"], row["op_auth_type"]), []).append(row)
    for key in groups:
        groups[key].sort(key=lambda r: r["date"])
    return groups


def detect_motus_restarts(groups):
    results = []
    for (usdot, docket, op_type), rows in groups.items():
        pending = None  # first unmatched real pause
        for row in rows:
            if pending is not None and _motus_is_restart_start(row) and row["date"] > pending["date"]:
                results.append({
                    "dot_number": usdot,
                    "docket_number": docket,
                    "paused_date": pending["date"],
                    "paused_reason": pending["reason"],
                    "restarted_date": row["date"],
                    "restarted_reason": row["reason"],
                    "authority_type": op_type,
                })
                pending = None
            if pending is None and _motus_is_pause(row):
                pending = row
    return results


def _fetch_motus_restarts(year, month=None):
    candidates_raw = fetch_motus_candidates(year, month)
    usdots = [_normalise_motus_row(r)["usdot"] for r in candidates_raw]

    history_raw = fetch_motus_histories(usdots)
    grouped = group_motus([_normalise_motus_row(r) for r in history_raw])
    deduped = dedupe_restarts(detect_motus_restarts(grouped))

    year_str = str(year)
    month_str = f"{int(month):02d}" if month else None

    def _in_period(r):
        d = r["restarted_date"]
        if d[:4] != year_str:
            return False
        return month_str is None or d[5:7] == month_str

    final_rows = [{
        "usdot": r["dot_number"],
        "docket": r["docket_number"],
        "paused_date": r["paused_date"],
        "paused_reason": r["paused_reason"],
        "restarted_date": r["restarted_date"],
        "restarted_reason": r["restarted_reason"],
        "authority_type": r.get("authority_type", ""),
        "source": "Motus AuthHist",
    } for r in deduped if _in_period(r)]

    print(f"[restart_history] MOTUS restarts in {year}-{month_str or 'all'}: {len(final_rows)}")
    return final_rows, len(candidates_raw) + len(history_raw)


# ---------------------------------------------------------------------------
# Public router
# ---------------------------------------------------------------------------
def fetch_and_parse_restarts(year, month=None):
    """Picks the data source by period:
        <= LEGACY_LAST_PERIOD (Apr 2026)  -> legacy 9mw4-x3tu
        >  LEGACY_LAST_PERIOD             -> Motus yu5v-wbh6
    A whole-year search that straddles the cutoff (2026, month=None)
    runs legacy for Jan-Apr and Motus for May-Dec and merges them.

    Returns (restart_rows, source_row_count) — same shape as before.
    """
    year = int(year)
    month = int(month) if month else None
    legacy_year, legacy_month = LEGACY_LAST_PERIOD

    if month:
        if (year, month) <= LEGACY_LAST_PERIOD:
            return _fetch_legacy_restarts(year, month)
        return _fetch_motus_restarts(year, month)

    # Whole-year searches
    if year < legacy_year:
        return _fetch_legacy_restarts(year, None)
    if year > legacy_year:
        return _fetch_motus_restarts(year, None)

    # year == legacy_year: split at the cutoff, month by month so each
    # query stays small.
    all_rows, total_source = [], 0
    for m in range(1, 13):
        if m <= legacy_month:
            rows, n = _fetch_legacy_restarts(year, m)
        else:
            rows, n = _fetch_motus_restarts(year, m)
        all_rows.extend(rows)
        total_source += n
    return all_rows, total_source
