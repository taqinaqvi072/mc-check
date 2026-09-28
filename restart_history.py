"""Restarted Carriers — FMCSA AuthHist integration (MOTUS-era rewrite).

WHY THIS WAS REWRITTEN
----------------------
FMCSA migrated its registration system to MOTUS. The old "AuthHist - All
With History" dataset (9mw4-x3tu) is now a *Legacy Archive* and stops at
~05/14/2026, so anything after 15 May 2026 never showed up.

SOURCES
-------
  MODERN (primary, live):  "Motus AuthHist - All With History"  yu5v-wbh6
      One row per authority status-change event. Each Operating Authority
      has its own docket number in the modern schema.
  LEGACY (context only):   "AuthHist - All With History"         9mw4-x3tu
      Frozen archive. Merged in so a restart in e.g. June 2026 can still be
      matched to a pause that happened BEFORE the MOTUS cut-over.
      Toggle with INCLUDE_LEGACY_HISTORY.

SCHEMA ADAPTIVITY
-----------------
The exact MOTUS column names / date format are detected at runtime from a
live sample (see get_modern_schema). If a required column can't be found,
a ValueError listing the REAL columns is raised, so it is a one-line fix in
_COLUMN_CANDIDATES rather than a silent empty result.

Run `python restart_history.py` to print the detected schema, distinct
status/reason values and per-month row counts for 2026. Use that to tune
END_KEYWORDS / START_KEYWORDS below if needed.

EVENT MODEL
-----------
Both datasets are converted into a flat list of events per docket:
    kind="end"   -> authority stopped (revoked/suspended/terminated/inactive)
    kind="start" -> authority (re)started (granted/reinstated/active)
A RESTART = an "end" event followed by a strictly LATER "start" event on the
same docket. Only restarts whose restarted_date falls in the requested
year/month are returned.

Two-phase fetch (memory-safe, same idea as before):
  Phase 1 - modern rows in the requested year/month that look like a start.
  Phase 2 - full history for just those dockets (modern + legacy), batched.
"""
import re
import time

import requests

MODERN_API = "https://data.transportation.gov/resource/yu5v-wbh6.json"
LEGACY_API = "https://data.transportation.gov/resource/9mw4-x3tu.json"

INCLUDE_LEGACY_HISTORY = True

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 50000
DOCKET_BATCH_SIZE = 100
REQUEST_PACING_SECONDS = 0.2
MAX_RETRIES = 4
RETRY_BACKOFF_BASE_SECONDS = 2

# --- Classification -------------------------------------------------------
# Checked in this order: EXCLUDE -> END -> START. END is checked before START
# on purpose ("INACTIVE" contains "ACTIVE").
EXCLUDE_KEYWORDS = [
    "DISMISS",
    "WITHDRAWN BY APPLICANT PRE-GRANT",
    "DISCONTINUED REVOCATION",  # revocation NOTICE cancelled: authority never stopped
    "RESCIND",
]
END_KEYWORDS = ["REVOK", "SUSPEND", "TERMINAT", "INACTIVE", "CANCEL", "LAPSE", "EXPIRE", "NOT AUTHORIZED"]
START_KEYWORDS = ["GRANT", "REINSTAT", "ACTIVE"]

# Legacy dataset: keywords for a row's *start* (original_action_desc).
LEGACY_START_KEYWORDS = ["GRANT", "REINSTAT"]

# Candidate MOTUS column names per role (first match wins, lowercase).
_COLUMN_CANDIDATES = {
    "docket": ["docket_number", "docket", "docket_no"],
    "dot": ["usdot_number", "dot_number", "usdot", "dot_no"],
    "date": [
        "op_auth_stat_change_date", "op_auth_status_change_date",
        "status_change_date", "change_date", "stat_change_date",
    ],
    "status": ["op_auth_status", "op_auth_stat", "status", "authority_status"],
    "reason": ["reason", "status_reason", "op_auth_stat_reason", "change_reason"],
    "type": ["op_auth_type", "category", "authority_type"],
}

_MDY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_schema_cache = {}


# --- Helpers --------------------------------------------------------------
def _normalise(value):
    return str(value or "").strip().upper()


def _soql_escape(value):
    return str(value).replace("'", "''")


def _docket_key(docket):
    """MC-123456 / MC123456 / mc 123456 -> MC123456 (used to join the
    modern and legacy datasets, whose docket formatting may differ)."""
    return re.sub(r"[^A-Z0-9]", "", _normalise(docket))


def _clean_date(value):
    """Any of: ISO timestamp/date, YYYYMMDD, MM/DD/YYYY -> 'YYYY-MM-DD' or ''."""
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
    if len(value) >= 10 and value[4] == "-":
        return value[:10]
    return value


def _get_with_retries(api, params):
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(api, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_error = f"network error: {e}"
        else:
            if response.status_code < 400:
                return response.json()
            if response.status_code in (429, 500, 502, 503, 504):
                last_error = f"{response.status_code}: {response.text[:200]}"
            else:
                raise ValueError(f"API returned {response.status_code}: {response.text[:500]}")

        if attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"[restart_history] Request failed ({last_error}); retrying in {wait}s "
                  f"(attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)

    raise ValueError(f"API request failed after {MAX_RETRIES} retries: {last_error}")


def _fetch_all_pages(api, where_clause, order):
    all_rows = []
    offset = 0
    while True:
        params = {
            "$limit": PAGE_LIMIT,
            "$offset": offset,
            "$where": where_clause,
            "$order": order,
        }
        page_rows = _get_with_retries(api, params)
        if not isinstance(page_rows, list):
            raise ValueError(f"Unexpected response type: {type(page_rows).__name__}")
        all_rows.extend(page_rows)
        if len(page_rows) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        time.sleep(REQUEST_PACING_SECONDS)
    return all_rows


# --- Modern schema detection ---------------------------------------------
def get_modern_schema():
    """Detects real column names + date format from a live sample.
    Returns {"cols": {role: column}, "date_mode": "mdy"|"iso"}."""
    if _schema_cache:
        return _schema_cache

    rows = _get_with_retries(MODERN_API, {"$limit": 200})
    if not rows:
        raise ValueError("Modern AuthHist dataset (yu5v-wbh6) returned no rows.")

    keys = set()
    for r in rows:
        keys.update(r.keys())

    cols = {}
    for role, candidates in _COLUMN_CANDIDATES.items():
        for c in candidates:
            if c in keys:
                cols[role] = c
                break

    if "date" not in cols:
        guess = sorted(k for k in keys if "date" in k and "chang" in k)
        if guess:
            cols["date"] = guess[0]

    missing = [r for r in ("docket", "date") if r not in cols]
    if "status" not in cols and "reason" not in cols:
        missing.append("status/reason")
    if missing:
        raise ValueError(
            f"Could not map MOTUS columns for: {missing}. Actual columns: {sorted(keys)}. "
            f"Add the right names to _COLUMN_CANDIDATES."
        )

    sample_date = next((str(r.get(cols["date"])) for r in rows if r.get(cols["date"])), "")
    date_mode = "mdy" if _MDY_RE.match(sample_date.strip()) else "iso"

    _schema_cache.update({"cols": cols, "date_mode": date_mode})
    print(f"[restart_history] Modern schema detected: {cols} (date format: {date_mode}, "
          f"sample={sample_date!r})")
    return _schema_cache


def _period_where(date_col, date_mode, year, month=None):
    year = int(year)
    if date_mode == "mdy":
        pattern = f"{int(month):02d}/%/{year}" if month else f"%/{year}"
        return f"{date_col} like '{pattern}'"

    if month:
        month = int(month)
        start = f"{year}-{month:02d}-01"
        end = f"{year + 1}-01-01" if month == 12 else f"{year}-{month + 1:02d}-01"
    else:
        start, end = f"{year}-01-01", f"{year + 1}-01-01"
    return f"{date_col} >= '{start}' AND {date_col} < '{end}'"


# --- Classification -------------------------------------------------------
def _classify(text):
    text = _normalise(text)
    if not text:
        return None
    if any(kw in text for kw in EXCLUDE_KEYWORDS):
        return None
    if any(kw in text for kw in END_KEYWORDS):
        return "end"
    if any(kw in text for kw in START_KEYWORDS):
        return "start"
    return None


def _modern_event(row, schema):
    cols = schema["cols"]
    status = str(row.get(cols.get("status", ""), "") or "").strip()
    reason = str(row.get(cols.get("reason", ""), "") or "").strip()
    desc = " / ".join(p for p in (status, reason) if p)
    kind = _classify(desc)
    date = _clean_date(row.get(cols["date"]))
    docket = str(row.get(cols["docket"]) or "").strip()
    if not kind or not date or not docket:
        return None
    return {
        "docket": docket,
        "dot": str(row.get(cols.get("dot", ""), "") or "").strip(),
        "date": date,
        "kind": kind,
        "desc": desc,
    }


def _legacy_events(row):
    """One legacy row = one start event + (optionally) one end event."""
    docket = str(row.get("docket_number") or "").strip()
    dot = str(row.get("dot_number") or "").strip()
    if not docket:
        return []
    events = []

    orig_desc = str(row.get("original_action_desc") or "").strip()
    orig_date = _clean_date(row.get("orig_served_date"))
    if orig_date and any(kw in _normalise(orig_desc) for kw in LEGACY_START_KEYWORDS):
        events.append({"docket": docket, "dot": dot, "date": orig_date,
                       "kind": "start", "desc": orig_desc})

    disp_desc = str(row.get("disp_action_desc") or "").strip()
    disp_date = _clean_date(row.get("disp_decided_date")) or _clean_date(row.get("disp_served_date"))
    if disp_date and not any(kw in _normalise(disp_desc) for kw in EXCLUDE_KEYWORDS):
        events.append({"docket": docket, "dot": dot, "date": disp_date,
                       "kind": "end", "desc": disp_desc})
    return events


# --- Fetching -------------------------------------------------------------
def fetch_candidate_start_rows(year, month=None):
    """Phase 1: modern rows in the period that look like a (re)start."""
    schema = get_modern_schema()
    cols = schema["cols"]

    text_cols = [cols[r] for r in ("status", "reason") if r in cols]
    keyword_clauses = " OR ".join(
        f"upper({c}) like '%{_soql_escape(kw)}%'"
        for c in text_cols for kw in START_KEYWORDS
    )
    date_clause = _period_where(cols["date"], schema["date_mode"], year, month)
    where_clause = f"({keyword_clauses}) AND {date_clause}"

    label = f"{year}-{int(month):02d}" if month else str(year)
    print(f"[restart_history] Phase 1: fetching {label} start candidates (MOTUS)")
    rows = _fetch_all_pages(MODERN_API, where_clause, f"{cols['docket']} ASC, {cols['date']} ASC")
    print(f"[restart_history] Phase 1: {len(rows)} candidate rows")
    return rows


def _batched(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_modern_histories(dockets):
    schema = get_modern_schema()
    cols = schema["cols"]
    dockets = sorted(set(d for d in dockets if d))
    all_rows = []
    total = (len(dockets) + DOCKET_BATCH_SIZE - 1) // DOCKET_BATCH_SIZE
    print(f"[restart_history] Phase 2 (MOTUS): {len(dockets)} dockets in {total} batch(es)")
    for batch in _batched(dockets, DOCKET_BATCH_SIZE):
        quoted = ",".join(f"'{_soql_escape(d)}'" for d in batch)
        where_clause = f"{cols['docket']} in ({quoted})"
        all_rows.extend(_fetch_all_pages(
            MODERN_API, where_clause, f"{cols['docket']} ASC, {cols['date']} ASC"))
        time.sleep(REQUEST_PACING_SECONDS)
    print(f"[restart_history] Phase 2 (MOTUS): {len(all_rows)} rows")
    return all_rows


def _legacy_docket_variants(docket):
    """Legacy docket formatting may differ from MOTUS (e.g. MC-123456 vs
    MC123456) — query a few plausible spellings."""
    raw = str(docket).strip()
    stripped = _docket_key(raw)
    variants = {raw, stripped}
    m = re.match(r"^([A-Z]{2})(\d+)$", stripped)
    if m:
        variants.add(f"{m.group(1)}-{m.group(2)}")
    return variants


def fetch_legacy_histories(dockets):
    variants = set()
    for d in set(dockets):
        if d:
            variants |= _legacy_docket_variants(d)
    variants = sorted(variants)
    all_rows = []
    total = (len(variants) + DOCKET_BATCH_SIZE - 1) // DOCKET_BATCH_SIZE
    print(f"[restart_history] Phase 2 (legacy): {len(variants)} docket spellings in {total} batch(es)")
    for batch in _batched(variants, DOCKET_BATCH_SIZE):
        quoted = ",".join(f"'{_soql_escape(d)}'" for d in batch)
        where_clause = f"docket_number in ({quoted})"
        all_rows.extend(_fetch_all_pages(
            LEGACY_API, where_clause, "docket_number ASC, sub_number ASC, orig_served_date ASC"))
        time.sleep(REQUEST_PACING_SECONDS)
    print(f"[restart_history] Phase 2 (legacy): {len(all_rows)} rows")
    return all_rows


# --- Restart detection ----------------------------------------------------
def group_events(events):
    groups = {}
    for e in events:
        groups.setdefault(_docket_key(e["docket"]), []).append(e)
    for key in groups:
        # same-day: "end" sorts before "start"
        groups[key].sort(key=lambda e: (e["date"], 0 if e["kind"] == "end" else 1))
    return groups


def detect_restarts(groups):
    results = []
    for docket_key, events in groups.items():
        pending_end = None
        dot = next((e["dot"] for e in reversed(events) if e["dot"]), "")
        docket_display = events[-1]["docket"]

        for e in events:
            if e["kind"] == "end":
                pending_end = e
            elif pending_end is not None and e["date"] > pending_end["date"]:
                results.append({
                    "dot_number": dot,
                    "docket_number": docket_display,
                    "paused_date": pending_end["date"],
                    "paused_reason": pending_end["desc"],
                    "restarted_date": e["date"],
                    "restarted_reason": e["desc"],
                })
                pending_end = None
    return results


def dedupe_restarts(rows):
    seen, out = set(), []
    for r in rows:
        key = (_docket_key(r["docket_number"]), r["paused_date"], r["restarted_date"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# --- Public entry point ---------------------------------------------------
def fetch_and_parse_restarts(year, month=None):
    """Returns (restart_rows, source_row_count). Each row has: usdot,
    docket, paused_date, paused_reason, restarted_date, restarted_reason —
    the shape app.py's restart_worker expects. Signature unchanged."""
    schema = get_modern_schema()

    candidate_raw = fetch_candidate_start_rows(year, month)
    cols = schema["cols"]
    dockets = [str(r.get(cols["docket"]) or "").strip() for r in candidate_raw]

    modern_raw = fetch_modern_histories(dockets)
    events = [ev for ev in (_modern_event(r, schema) for r in modern_raw) if ev]

    legacy_raw = []
    if INCLUDE_LEGACY_HISTORY and dockets:
        try:
            legacy_raw = fetch_legacy_histories(dockets)
        except ValueError as e:
            # Legacy is only extra context; don't fail the whole search.
            print(f"[restart_history] Legacy history skipped: {e}")
        for r in legacy_raw:
            events.extend(_legacy_events(r))

    all_restarts = dedupe_restarts(detect_restarts(group_events(events)))

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
    } for r in all_restarts if _in_period(r)]

    source_row_count = len(candidate_raw) + len(modern_raw) + len(legacy_raw)
    label = f"{year}-{month_str}" if month_str else year_str
    print(f"[restart_history] Total restarts detected: {len(all_restarts)}")
    print(f"[restart_history] Restarts in {label}: {len(final_rows)}")
    return final_rows, source_row_count


# --- Diagnostics ----------------------------------------------------------
def debug_report(year=2026):
    """Prints detected schema, distinct status/reason values and per-month
    row counts. Run: python restart_history.py"""
    schema = get_modern_schema()
    cols = schema["cols"]

    for role in ("status", "reason", "type"):
        if role not in cols:
            continue
        rows = _get_with_retries(MODERN_API, {
            "$select": f"{cols[role]}, count(*)",
            "$group": cols[role],
            "$order": "count DESC",
            "$limit": 60,
        })
        print(f"\n--- distinct {role} ({cols[role]}) ---")
        for r in rows:
            print(f"{r.get(cols[role])!r:45} {r.get('count')}")

    print(f"\n--- rows per month in {year} ---")
    for m in range(1, 13):
        where = _period_where(cols["date"], schema["date_mode"], year, m)
        r = _get_with_retries(MODERN_API, {"$select": "count(*)", "$where": where})
        print(f"{year}-{m:02d}: {r[0].get('count') if r else '?'}")


if __name__ == "__main__":
    debug_report()
