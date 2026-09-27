"""Restarted Carriers — FMCSA AuthHist "All With History" integration.

Source: FMCSA "AuthHist - All With History" dataset on Socrata
(https://catalog.data.gov/dataset/authhist-all-with-history), dataset id
9mw4-x3tu. Unlike motus.py's dataset (dm5j-zc6c / yu5v-wbh6), which is a
DAILY DIFFERENCE feed (only last ~24h), this one keeps the FULL history of
every authority action for every carrier/broker/freight forwarder — so it
can answer "who paused their authority in 2023/2024 and later restarted".

RESTART DEFINITION: a carrier "restarted" if it has 2+ authority action
records for the same USDOT/docket where an earlier record's final action
(revoked / terminated / withdrawn / inactive) is followed, after a gap, by
a later record's original action (granted / reinstated / active).

FIELD NAMES: the exact machine field names on this dataset have NOT been
confirmed against a live response yet (same caveat as motus.py). This
module prints the raw Socrata response's field names + a row sample on
every run so any mismatch shows up immediately in your host's logs
instead of silently returning 0 restarts. Adjust the *_FIELDS lists below
once you've checked a real run's logs.
"""
import requests

AUTHHIST_HISTORY_API = "https://data.transportation.gov/resource/9mw4-x3tu.json"

RESTART_INCLUDE_CATEGORIES = [
    "MOTOR CARRIER OF PROPERTY",
    "MOTOR CARRIER OF PASSENGERS",
]

# Keywords that mark a record as the carrier's authority being PAUSED
# (final/terminating action on that authority record).
PAUSE_KEYWORDS = ["REVOK", "TERM", "WITHDRAW", "INACTIVE", "SUSPEND", "CANCEL"]

# Keywords that mark a record as the carrier's authority being (RE)STARTED
# (original/granting action on a later authority record).
RESTART_KEYWORDS = ["GRANT", "REINSTAT", "ACTIVE"]

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 50000  # Socrata max per page; we page through with $offset


def _normalise(value):
    return str(value or "").strip().upper()


def _category_allowed(authority_type, include_categories):
    authority_type = _normalise(authority_type)
    categories = [_normalise(c) for c in (include_categories or RESTART_INCLUDE_CATEGORIES)]
    return any(category in authority_type for category in categories)


def _year_bounds(year):
    return f"{year}-01-01T00:00:00", f"{year}-12-31T23:59:59"


def fetch_authhist_history_rows(year, include_categories=None):
    """Pages through every AuthHist "All With History" row whose
    status_change_date falls in `year`. Returns the raw Socrata rows
    (list of dicts) — NOT yet grouped or restart-detected."""
    start, end = _year_bounds(year)
    where = f"status_change_date >= '{start}' AND status_change_date <= '{end}'"

    all_rows = []
    offset = 0
    page_num = 0
    while True:
        params = {
            "$limit": PAGE_LIMIT,
            "$offset": offset,
            "$where": where,
            "$order": "usdot_number ASC, status_change_date ASC",
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
            sample = page_rows[0]
            print(f"[restart_history] Sample raw row keys: {list(sample.keys())}")
            print(f"[restart_history] Sample raw row: {sample}")

        all_rows.extend(page_rows)
        print(f"[restart_history] Page {page_num}: {len(page_rows)} rows (offset {offset})")

        if len(page_rows) < PAGE_LIMIT:
            break  # last page
        offset += PAGE_LIMIT
        page_num += 1

    print(f"[restart_history] Total rows fetched for {year}: {len(all_rows)}")
    return all_rows


def _normalise_row(row):
    """Maps a raw Socrata row to a stable shape. Field names here are
    best-effort (matching the daily-difference schema) — check the
    [restart_history] Sample raw row keys log line and adjust if the
    full-history dataset uses different names."""
    return {
        "usdot": str(row.get("usdot_number") or "").strip(),
        "docket": str(row.get("docket_number") or "").strip(),
        "category": str(row.get("op_auth_type") or "").strip(),
        "status": str(row.get("op_auth_status") or "").strip(),
        "reason": str(row.get("reason") or "").strip(),
        "status_change_date": _clean_date(row.get("status_change_date")),
    }


def _clean_date(value):
    value = str(value or "").strip()
    if "T" in value:
        return value.split("T")[0]
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def _is_pause_event(reason, status):
    text = _normalise(reason) + " " + _normalise(status)
    return any(kw in text for kw in PAUSE_KEYWORDS)


def _is_restart_event(reason, status):
    text = _normalise(reason) + " " + _normalise(status)
    return any(kw in text for kw in RESTART_KEYWORDS)


def group_by_usdot(rows):
    """Groups normalised rows by USDOT number, each group's events sorted
    by status_change_date ascending."""
    groups = {}
    for row in rows:
        if not row["usdot"]:
            continue
        groups.setdefault(row["usdot"], []).append(row)
    for usdot in groups:
        groups[usdot].sort(key=lambda r: r["status_change_date"])
    return groups


def detect_restarts(usdot_groups, include_categories=None):
    """Walks each USDOT's sorted event list looking for a PAUSE event
    followed later by a RESTART event. Returns one entry per detected
    restart pair: {usdot, docket, category, paused_date, paused_reason,
    restarted_date, restarted_reason}."""
    results = []

    for usdot, events in usdot_groups.items():
        pending_pause = None  # most recent unmatched pause event

        for event in events:
            if not _category_allowed(event["category"], include_categories):
                continue

            if _is_pause_event(event["reason"], event["status"]):
                pending_pause = event
                continue

            if _is_restart_event(event["reason"], event["status"]) and pending_pause:
                if event["status_change_date"] > pending_pause["status_change_date"]:
                    results.append({
                        "usdot": usdot,
                        "docket": event["docket"] or pending_pause["docket"],
                        "category": event["category"],
                        "paused_date": pending_pause["status_change_date"],
                        "paused_reason": pending_pause["reason"] or pending_pause["status"],
                        "restarted_date": event["status_change_date"],
                        "restarted_reason": event["reason"] or event["status"],
                    })
                pending_pause = None  # matched — reset in case of further cycles

    return results


def dedupe_restarts(rows):
    seen = set()
    out = []
    for row in rows:
        key = (row["usdot"], row["paused_date"], row["restarted_date"])
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def fetch_and_parse_restarts(year, include_categories=None):
    """Public entry point: fetches AuthHist All-With-History for `year`,
    normalises, groups by USDOT, and returns deduped restart pairs.

    Returns (restart_rows, source_row_count).
    """
    print(f"[restart_history] AuthHist All-With-History request for year {year}")
    raw_rows = fetch_authhist_history_rows(year, include_categories)
    normalised = [_normalise_row(r) for r in raw_rows]
    grouped = group_by_usdot(normalised)
    restarts = detect_restarts(grouped, include_categories)
    final_rows = dedupe_restarts(restarts)
    print(f"[restart_history] Detected {len(final_rows)} restart(s) for {year}")
    return final_rows, len(raw_rows)
