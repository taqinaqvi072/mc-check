"""Motus AuthHist Daily Difference integration for MC Scout.

Source: FMCSA "Motus AuthHist" dataset on Socrata
(https://data.transportation.gov/Trucking-and-Motorcoaches/Motus-AuthHist/dm5j-zc6c)
-- tracks every authority STATUS CHANGE (grants, reinstatements, voluntary
suspensions, inactivations, withdrawals, revocations), not just brand-new
registrations.

IMPORTANT: filtering to Property/Passengers alone is NOT enough to get
"recently registered" carriers -- that gives every kind of status change
in the window. To get carriers that are newly registered (freshly granted
authority for the first time), we additionally require the row's `reason`
field to indicate a GRANT, excluding reinstatements/suspensions/
revocations/withdrawals.

CONFIRMED (checked live against the Socrata endpoint): usdot_number,
docket_number, op_auth_type, op_auth_status, reason, status_change_date
are exactly the field names FMCSA uses, and the $where date-range filter
below works correctly (status_change_date is a Socrata date-type field --
it just displays compactly as "YYYYMMDD" in JSON, but SoQL comparisons
against it still work). No changes were needed in this file.

Note: the live dataset also carries a reason value not originally
anticipated -- "Published to FMCSA Register" (status Pending). It is
correctly excluded by the existing GRANT_KEYWORDS/EXCLUDE_KEYWORDS logic
below since it doesn't contain "GRANT".
"""
import requests

MOTUS_AUTHHIST_API = "https://data.transportation.gov/resource/dm5j-zc6c.json"
MOTUS_INCLUDE_CATEGORIES = [
    "MOTOR CARRIER OF PROPERTY",
    "MOTOR CARRIER OF PASSENGERS",
]
# Reasons that indicate a carrier is genuinely NEW (first-time authority
# grant), as opposed to an existing carrier's authority changing state.
# Adjust this list once you've confirmed the real `reason` values from the
# debug logs below -- this is a best-effort keyword match, not a confirmed
# exact enum.
GRANT_KEYWORDS = ["GRANT"]
# Reasons that should NEVER count as "newly registered", even if a GRANT
# keyword happens to appear inside them (defensive -- e.g. some datasets
# phrase reinstatement as "REINSTATE AFTER GRANT" or similar).
EXCLUDE_KEYWORDS = ["REINSTAT", "REVOK", "SUSPEND", "WITHDRAW", "TERM", "CANCEL"]
REQUEST_TIMEOUT = 30


def _normalise(value):
    return str(value or "").strip().upper()


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
    # Some rows may carry the "new grant" signal on the status field
    # instead of reason (dataset naming isn't 100% confirmed yet).
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


def fetch_authhist_rows(from_date, to_date):
    """from_date/to_date: 'YYYY-MM-DD' strings."""
    params = {
        "$limit": 50000,
        "$where": (
            f"status_change_date >= '{_iso_start(from_date)}' "
            f"AND status_change_date <= '{_iso_end(to_date)}'"
        ),
        "$order": "status_change_date ASC, usdot_number ASC",
    }
    response = requests.get(MOTUS_AUTHHIST_API, params=params, timeout=REQUEST_TIMEOUT)

    if response.status_code >= 400:
        # Surface Socrata's actual error message (e.g. "no such column",
        # bad $where syntax) instead of a generic requests exception --
        # this is the fastest way to catch a wrong field name.
        raise ValueError(
            f"Motus AuthHist API returned {response.status_code}: {response.text[:500]}"
        )

    data = response.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected Motus AuthHist response type: {type(data).__name__}")
    return data


def parse_authhist_rows(rows, include_categories=None):
    include_categories = include_categories or MOTUS_INCLUDE_CATEGORIES

    # Debug: show what values actually come back, once per call, so field
    # names/format assumptions can be verified from your host's logs.
    if rows:
        sample = rows[0]
        print(f"[motus] Sample raw row keys: {list(sample.keys())}")
        print(f"[motus] Sample raw row: {sample}")
        distinct_statuses = sorted({str(r.get("op_auth_status", "")) for r in rows})[:20]
        distinct_reasons = sorted({str(r.get("reason", "")) for r in rows})[:20]
        print(f"[motus] Distinct op_auth_status values seen: {distinct_statuses}")
        print(f"[motus] Distinct reason values seen: {distinct_reasons}")

    parsed = []
    skipped_category = 0
    skipped_not_grant = 0

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
    print(f"[motus] AuthHist Daily Difference request: {from_date} -> {to_date}")
    raw_rows = fetch_authhist_rows(from_date, to_date)
    print(f"[motus] AuthHist source rows: {len(raw_rows)}")
    parsed_rows = parse_authhist_rows(raw_rows, include_categories)
    final_rows = dedupe_rows(parsed_rows)
    print(f"[motus] Unique new-registration rows: {len(final_rows)}")
    return final_rows, len(raw_rows)
