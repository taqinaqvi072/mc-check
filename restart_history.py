"""Restarted Carriers — hybrid FMCSA AuthHist integration (v2, data-accuracy fixes).

TWO DATA SOURCES (FMCSA froze the old one on 05/14/2026):

  1. LEGACY  "AuthHist - All With History"  (9mw4-x3tu)
     Frozen 05/14/2026. Row = one authority record carrying BOTH its start
     (original_action_desc + orig_served_date) and its end
     (disp_action_desc + disp_decided_date). Used up to and including
     LEGACY_LAST_PERIOD (May 2026, partial: 1-14 May only).

  2. MOTUS   "Motus AuthHist - All With History"  (yu5v-wbh6)
     Live. Row = one STATUS CHANGE (status_change_date is text YYYYMMDD).
     Used from May 2026 onwards (May is queried from BOTH sources and merged).

WHAT CHANGED IN v2 (why the client saw "fake / empty" data):
  * Brokers and freight forwarders are no longer treated as carrier
    pauses/restarts (they used to show up in the list).
  * Motus: a restart is dropped if the carrier was paused AGAIN after it
    (so the list shows carriers that are really running now).
  * USDOT / docket numbers are cleaned (legacy has '00085526', 'MC000647').
    Legacy rows with USDOT '00000000' now keep an empty usdot, so app.py can
    look them up by MC number instead.
  * May 2026 is read from both sources (legacy stops 14 May, Motus starts
    ~9 June, so there is still an unavoidable gap 15 May - 9 Jun).

Output rows (both sources): usdot, docket, paused_date, paused_reason,
restarted_date, restarted_reason, source (+ authority_type for Motus).
Dates are YYYY-MM-DD.
"""
import re
import time
from datetime import date, datetime, timedelta

import requests

LEGACY_API = "https://data.transportation.gov/resource/9mw4-x3tu.json"
MOTUS_API = "https://data.transportation.gov/resource/yu5v-wbh6.json"
AUTHHIST_HISTORY_API = LEGACY_API  # backwards-compatible alias

# Last (year, month) served from legacy. That month is ALSO queried from Motus
# and merged (legacy froze mid-month).
LEGACY_LAST_PERIOD = (2026, 5)

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

# Authority kinds that are NOT carriers.
NON_CARRIER_KEYWORDS = ["BROKER", "FORWARDER"]

# Motus: Inactive rows with these reasons are bookkeeping, not real pauses.
MOTUS_PAUSE_EXCLUDE_REASONS = [
    "ADMINISTRATIVE STATUS ADJUSTMENT",
    "ADMINISTRATIVE CORRECTION",
]

_MDY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_DOCKET_RE = re.compile(r"^([A-Za-z]{1,3})0*(\d+)$")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _normalise(value):
    return str(value or "").strip().upper()


def _soql_escape(value):
    return str(value).replace("'", "''")


def _clean_usdot(value):
    """'00085526' -> '85526'; '00000000' -> ''."""
    return str(value or "").strip().lstrip("0")


def _clean_docket(value):
    """'MC000647' -> 'MC647'."""
    value = str(value or "").strip()
    m = _DOCKET_RE.match(value)
    return f"{m.group(1).upper()}{m.group(2)}" if m else value


def _is_non_carrier(text):
    t = _normalise(text)
    return any(k in t for k in NON_CARRIER_KEYWORDS)


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


def _in_period(restarted_date, year, month):
    if restarted_date[:4] != str(year):
        return False
    return month is None or restarted_date[5:7] == f"{int(month):02d}"


# ---------------------------------------------------------------------------
# LEGACY source (9mw4-x3tu)
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
    if _is_non_carrier(row["mod_col_1"]):
        return False
    if not _end_date(row):
        return False
    desc = _normalise(row["disp_action_desc"])
    return not any(kw in desc for kw in DISPOSITION_EXCLUDE_KEYWORDS)


def _is_restart_start(row):
    if _is_non_carrier(row["mod_col_1"]):
        return False
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
    candidate_rows_raw = fetch_candidate_restart_rows(year, month)
    docket_numbers = [_normalise_row(r)["docket_number"] for r in candidate_rows_raw]

    history_rows_raw = fetch_docket_histories(docket_numbers)
    grouped = group_by_docket([_normalise_row(r) for r in history_rows_raw])
    deduped = dedupe_restarts(detect_restarts(grouped))

    final_rows = [{
        "usdot": _clean_usdot(r["dot_number"]),
        "docket": _clean_docket(r["docket_number"]),
        "paused_date": r["paused_date"],
        "paused_reason": r["paused_reason"],
        "restarted_date": r["restarted_date"],
        "restarted_reason": r["restarted_reason"],
        "source": "Legacy AuthHist",
    } for r in deduped if _in_period(r["restarted_date"], year, month)]

    print(f"[restart_history] LEGACY restarts in {year}-{month or 'all'}: {len(final_rows)}")
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


def fetch_motus_candidates(year, month=None, date_from=None, date_to=None):
    """Phase 1: carrier Active rows granted/reinstated in the period.
    date_from/date_to ('YYYY-MM-DD') narrow it to an exact day range."""
    keyword_clauses = " OR ".join(
        f"upper(reason) like '%{_soql_escape(kw)}%'" for kw in RESTART_KEYWORDS
    )
    if date_from and date_to:
        date_clause = (f"status_change_date between '{date_from.replace('-', '')}' "
                       f"and '{date_to.replace('-', '')}'")
    else:
        prefix = f"{int(year)}{int(month):02d}%" if month else f"{int(year)}%"
        date_clause = f"status_change_date like '{prefix}'"
    where_clause = (
        f"op_auth_status = 'Active' AND ({keyword_clauses}) "
        f"AND upper(op_auth_type) not like '%BROKER%' "
        f"AND upper(op_auth_type) not like '%FORWARDER%' "
        f"AND {date_clause}"
    )
    print(f"[restart_history] MOTUS phase 1: {year}-{month or 'all'} candidates")
    rows = _fetch_all_pages(where_clause, MOTUS_API, MOTUS_ORDER)
    print(f"[restart_history] MOTUS phase 1: {len(rows)} candidate rows")
    return rows


def fetch_motus_histories(usdot_numbers):
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
        if _is_non_carrier(row["op_auth_type"]):
            continue  # broker / freight-forwarder authority, not a carrier
        groups.setdefault((row["usdot"], row["docket"], row["op_auth_type"]), []).append(row)
    for key in groups:
        groups[key].sort(key=lambda r: r["date"])
    return groups


def detect_motus_restarts(groups):
    results = []
    for (usdot, docket, op_type), rows in groups.items():
        pending = None       # first unmatched real pause
        last_idx = None      # index in `local` of the latest restart
        local = []
        for row in rows:
            if pending is not None and _motus_is_restart_start(row) and row["date"] > pending["date"]:
                local.append({
                    "dot_number": usdot,
                    "docket_number": docket,
                    "paused_date": pending["date"],
                    "paused_reason": pending["reason"],
                    "restarted_date": row["date"],
                    "restarted_reason": row["reason"],
                    "authority_type": op_type,
                    "_paused_again": False,
                })
                last_idx = len(local) - 1
                pending = None
            if _motus_is_pause(row):
                # paused again after a restart -> that restart is no longer live
                if last_idx is not None and row["date"] > local[last_idx]["restarted_date"]:
                    local[last_idx]["_paused_again"] = True
                    last_idx = None
                if pending is None:
                    pending = row
        results.extend(r for r in local if not r["_paused_again"])
    for r in results:
        r.pop("_paused_again", None)
    return results


def _fetch_motus_restarts(year, month=None, date_from=None, date_to=None):
    candidates_raw = fetch_motus_candidates(year, month, date_from, date_to)
    usdots = [_normalise_motus_row(r)["usdot"] for r in candidates_raw]

    history_raw = fetch_motus_histories(usdots)
    grouped = group_motus([_normalise_motus_row(r) for r in history_raw])
    deduped = dedupe_restarts(detect_motus_restarts(grouped))

    final_rows = [{
        "usdot": _clean_usdot(r["dot_number"]),
        "docket": _clean_docket(r["docket_number"]),
        "paused_date": r["paused_date"],
        "paused_reason": r["paused_reason"],
        "restarted_date": r["restarted_date"],
        "restarted_reason": r["restarted_reason"],
        "authority_type": r.get("authority_type", ""),
        "source": "Motus AuthHist",
    } for r in deduped if _in_period(r["restarted_date"], year, month)]

    print(f"[restart_history] MOTUS restarts in {year}-{month or 'all'}: {len(final_rows)}")
    return final_rows, len(candidates_raw) + len(history_raw)


# ---------------------------------------------------------------------------
# Public router
# ---------------------------------------------------------------------------
def _merge_unique(rows):
    seen, out = set(), []
    for r in rows:
        key = (r["usdot"], r["docket"], r["restarted_date"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _fetch_month(year, month, date_from=None, date_to=None):
    """One calendar month. date_from/date_to (inside that month) narrow the
    Motus query; legacy is filtered by the caller."""
    period = (year, month)
    if period < LEGACY_LAST_PERIOD:
        return _fetch_legacy_restarts(year, month)
    if period == LEGACY_LAST_PERIOD:  # overlap month: legacy froze mid-month
        l_rows, l_n = _fetch_legacy_restarts(year, month)
        m_rows, m_n = _fetch_motus_restarts(year, month, date_from, date_to)
        return _merge_unique(l_rows + m_rows), l_n + m_n
    return _fetch_motus_restarts(year, month, date_from, date_to)


def fetch_and_parse_restarts(from_date, to_date):
    """Restarts whose restart date is between from_date and to_date
    (inclusive, 'YYYY-MM-DD'). Walks month by month, picks the source per
    month, then filters to the exact day range.
    Returns (restart_rows, source_row_count)."""
    d1 = datetime.strptime(from_date, "%Y-%m-%d").date()
    d2 = datetime.strptime(to_date, "%Y-%m-%d").date()

    all_rows, total_source = [], 0
    y, m = d1.year, d1.month
    while (y, m) <= (d2.year, d2.month):
        month_start = date(y, m, 1)
        month_end = date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1)
        seg_from = max(d1, month_start).isoformat()
        seg_to = min(d2, month_end).isoformat()
        rows, n = _fetch_month(y, m, seg_from, seg_to)
        all_rows.extend(r for r in rows if from_date <= r["restarted_date"] <= to_date)
        total_source += n
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return all_rows, total_source
