"""Motus AuthHist integration for MC Scout.

Source: FMCSA "Motus AuthHist" dataset on Socrata
(https://data.transportation.gov/Trucking-and-Motorcoaches/Motus-AuthHist/dm5j-zc6c)
-- tracks every authority STATUS CHANGE (grants, reinstatements, voluntary
suspensions, inactivations, withdrawals, revocations), not just brand-new
registrations.

To get carriers that are newly registered (freshly granted authority), we
require the row's `reason` (or status) to indicate a GRANT, excluding
reinstatements/suspensions/revocations/withdrawals.

SPEED NOTE: category + grant filtering is now done SERVER-SIDE in the
Socrata $where clause, and only the needed columns are requested via
$select. Previously every status change in the window was downloaded
(up to 50,000 rows) and filtered in Python, which made the "fetching"
stage slow. Python-side filtering is kept as a safety net.

Optional: set SOCRATA_APP_TOKEN (free from data.transportation.gov) to
avoid Socrata throttling.
"""
import os

import requests

MOTUS_AUTHHIST_API = "https://data.transportation.gov/resource/yu5v-wbh6.json"
MOTUS_INCLUDE_CATEGORIES = [
    "MOTOR CARRIER OF PROPERTY",
    "MOTOR CARRIER OF PASSENGERS",
]
GRANT_KEYWORDS = ["GRANT"]
EXCLUDE_KEYWORDS = ["REINSTAT", "REVOK", "SUSPEND", "WITHDRAW", "TERM", "CANCEL"]
REQUEST_TIMEOUT = 60
PAGE_SIZE = 10000
MAX_ROWS = 200000  # hard safety cap

SELECT_FIELDS = (
    "usdot_number, docket_number, op_auth_type, "
    "op_auth_status, reason, status_change_date"
)


def _normalise(value):
    return str(value or "").strip().upper()


def _esc(value):
    return str(value).replace("'", "''")


def _category_allowed(authority_type, include_categories):
    authority_type = _normalise(authority_type)
    categories = [_normalise(c) for c in (include_categories or MOTUS_INCLUDE_CATEGORIES)]
    return any(category in authority_type for category in categories)


def _is_new_grant(reason, status):
    reason_u = _normalise(reason)
    status_u = _normalise(status)
    if any(bad in reason_u for bad in EXCLUDE_KEYWORDS):
        return False
    if any(good in reason_u for good in GRANT_KEYWORDS):
        return True
    if any(good in status_u for good in GRANT_KEYWORDS):
        return True
    return False


def _clean_date(value):
    value = str(value or "").strip()
    if "T" in value:
        return value.split("T")[0]
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def _iso_start(date_str):
    return f"{date_str}T00:00:00"


def _iso_end(date_str):
    return f"{date_str}T23:59:59"


def _build_where(from_date, to_date, include_categories):
    categories = include_categories or MOTUS_INCLUDE_CATEGORIES

    date_clause = (
        f"status_change_date >= '{_iso_start(from_date)}' "
        f"AND status_change_date <= '{_iso_end(to_date)}'"
    )
    cat_clause = "(" + " OR ".join(
        f"upper(op_auth_type) like '%{_esc(_normalise(c))}%'" for c in categories
    ) + ")"
    grant_parts = [f"upper(reason) like '%{_esc(k)}%'" for k in GRANT_KEYWORDS]
    grant_parts += [f"upper(op_auth_status) like '%{_esc(k)}%'" for k in GRANT_KEYWORDS]
    grant_clause = "(" + " OR ".join(grant_parts) + ")"
    exclude_clause = "NOT (" + " OR ".join(
        f"upper(reason) like '%{_esc(k)}%'" for k in EXCLUDE_KEYWORDS
    ) + ")"

    return f"{date_clause} AND {cat_clause} AND {grant_clause} AND {exclude_clause}"


def fetch_authhist_rows(from_date, to_date, include_categories=None):
    """from_date/to_date: 'YYYY-MM-DD'. Filtering happens on the server;
    results are fetched page by page so nothing is silently truncated."""
    where = _build_where(from_date, to_date, include_categories)
    headers = {"Accept-Encoding": "gzip"}
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    all_rows = []
    offset = 0
    with requests.Session() as sess:
        while offset < MAX_ROWS:
            params = {
                "$select": SELECT_FIELDS,
                "$where": where,
                "$order": "status_change_date ASC, usdot_number ASC",
                "$limit": PAGE_SIZE,
                "$offset": offset,
            }
            response = sess.get(
                MOTUS_AUTHHIST_API, params=params, headers=headers, timeout=REQUEST_TIMEOUT
            )
            if response.status_code >= 400:
                raise ValueError(
                    f"Motus AuthHist API returned {response.status_code}: {response.text[:500]}"
                )
            data = response.json()
            if not isinstance(data, list):
                raise ValueError(
                    f"Unexpected Motus AuthHist response type: {type(data).__name__}"
                )
            all_rows.extend(data)
            if len(data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    return all_rows


def parse_authhist_rows(rows, include_categories=None):
    include_categories = include_categories or MOTUS_INCLUDE_CATEGORIES

    if rows:
        print(f"[motus] Sample raw row: {rows[0]}")

    parsed = []
    skipped_category = 0
    skipped_not_grant = 0

    # Safety net: server already filtered, but re-check in case a field
    # format differs from what the SoQL filter assumed.
    for row in rows:
        authority_type = str(row.get("op_auth_type") or "").strip()
        if not authority_type or not _category_allowed(authority_type, include_categories):
            skipped_category += 1
            continue

        usdot = str(row.get("usdot_number") or "").strip()
        if not usdot:
            continue

        status = str(row.get("op_auth_status") or "").strip()
        reason = str(row.get("reason") or "").strip()

        if not _is_new_grant(reason, status):
            skipped_not_grant += 1
            continue

        parsed.append({
            "usdot": usdot,
            "docket": str(row.get("docket_number") or "").strip(),
            "category": authority_type,
            "status": status,
            "reason": reason,
            "status_change_date": _clean_date(row.get("status_change_date")),
        })

    print(f"[motus] Skipped (wrong category): {skipped_category}")
    print(f"[motus] Skipped (not a new grant): {skipped_not_grant}")
    print(f"[motus] Kept as new-registration candidates: {len(parsed)}")

    return parsed


def dedupe_rows(rows):
    seen = set()
    output = []
    for row in rows:
        key = (
            row.get("usdot", ""), row.get("docket", ""), row.get("category", ""),
            row.get("status", ""), row.get("reason", ""), row.get("status_change_date", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def fetch_and_parse_range(from_date, to_date, include_categories=None):
    print(f"[motus] AuthHist request: {from_date} -> {to_date}")
    raw_rows = fetch_authhist_rows(from_date, to_date, include_categories)
    print(f"[motus] AuthHist rows after server-side filter: {len(raw_rows)}")
    parsed_rows = parse_authhist_rows(raw_rows, include_categories)
    final_rows = dedupe_rows(parsed_rows)
    print(f"[motus] Unique new-registration rows: {len(final_rows)}")
    return final_rows, len(raw_rows)
