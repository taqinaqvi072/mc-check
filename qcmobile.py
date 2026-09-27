"""FMCSA QCMobile API — free, official JSON REST API for FMCSA registration
data (mobile.fmcsa.dot.gov). Used here ONLY to enrich Restarted Carrier
results with phone + city/state at ZERO Webshare proxy cost — this is a
completely separate, unthrottled-by-us JSON API, not SAFER HTML scraping.

SETUP (one-time, free):
  1. Create a Login.gov account, then a developer account at
     https://mobile.fmcsa.dot.gov/QCDevsite (Login.gov info page links from
     there).
  2. Once logged in: "My WebKeys" -> "Get a new WebKey" -> fill the short
     form (app name, non-commercial/commercial, description) -> Create.
  3. Set the resulting key as an environment variable: FMCSA_WEBKEY=...

CONFIRMED FIELDS (from FMCSA's own API Elements doc,
https://mobile.fmcsa.dot.gov/QCDevsite/docs/apiElements): phyStreet,
phyCity, phyState, phyZip, phyCountry, telephone. There is NO general
"power units" / truck-count field here for property (trucking) carriers —
only passenger-carrier vehicle counts (busVehicle, vanVehicle, etc.) are
exposed. So this module deliberately does NOT attempt to return power
units; that still only ever comes from SAFER (via the existing proxy scan
engine) or from a carrier the user already has saved.

RATE LIMITS: not formally published for single-carrier lookups (the
documented 50-result cap only applies to name searches). REQUEST_PACING_
SECONDS below keeps us polite regardless.
"""
import os
import re
import time

import requests

QCMOBILE_BASE = "https://mobile.fmcsa.dot.gov/qc/services/carriers"
FMCSA_WEBKEY = os.environ.get("FMCSA_WEBKEY")

REQUEST_TIMEOUT = 15
REQUEST_PACING_SECONDS = 0.3
MAX_RETRIES = 2
RETRY_BACKOFF_BASE_SECONDS = 2

_DOCKET_PREFIX_RE = re.compile(r"^[A-Za-z\-]+")


def _clean_docket_number(docket):
    """QCMobile's docket-number endpoint expects just the digits (e.g.
    "1515"), but our AuthHist data stores dockets with their type prefix
    (e.g. "MC030404"). Strip any leading letters/dashes."""
    return _DOCKET_PREFIX_RE.sub("", str(docket or "").strip())


def _extract_carrier(json_data):
    """QCMobile wraps the carrier record under "content", which comes
    back as either a single object or a one-item list depending on the
    endpoint/carrier. Handles both shapes defensively."""
    if not isinstance(json_data, dict):
        return None
    content = json_data.get("content")
    if isinstance(content, list):
        content = content[0] if content else None
    if isinstance(content, dict):
        # Some responses nest the carrier one level deeper under "carrier".
        return content.get("carrier", content)
    return None


def _get(url):
    if not FMCSA_WEBKEY:
        return None
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, params={"webKey": FMCSA_WEBKEY}, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_error = str(e)
        else:
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None
            if r.status_code in (429, 500, 502, 503, 504):
                last_error = f"{r.status_code}"
            else:
                # 404/etc — genuinely no record, not a transient error.
                return None

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_BASE_SECONDS * (2 ** attempt))

    print(f"[qcmobile] Request failed after retries: {last_error}")
    return None


def fetch_carrier_by_docket(docket_number):
    """Looks up a carrier by MC/docket number. Returns a normalised dict
    {phone, city, state, street} or None if not found / no webKey set /
    the request failed."""
    docket_digits = _clean_docket_number(docket_number)
    if not docket_digits:
        return None
    time.sleep(REQUEST_PACING_SECONDS)
    data = _get(f"{QCMOBILE_BASE}/docket-number/{docket_digits}")
    carrier = _extract_carrier(data)
    if not carrier:
        return None
    return {
        "phone": str(carrier.get("telephone") or "").strip(),
        "city": str(carrier.get("phyCity") or "").strip(),
        "state": str(carrier.get("phyState") or "").strip(),
        "street": str(carrier.get("phyStreet") or "").strip(),
    }


def fetch_carrier_by_dot(dot_number):
    """Same as fetch_carrier_by_docket, but by USDOT number — used as a
    fallback when a restart row has no docket number."""
    dot_number = str(dot_number or "").strip()
    if not dot_number:
        return None
    time.sleep(REQUEST_PACING_SECONDS)
    data = _get(f"{QCMOBILE_BASE}/{dot_number}")
    carrier = _extract_carrier(data)
    if not carrier:
        return None
    return {
        "phone": str(carrier.get("telephone") or "").strip(),
        "city": str(carrier.get("phyCity") or "").strip(),
        "state": str(carrier.get("phyState") or "").strip(),
        "street": str(carrier.get("phyStreet") or "").strip(),
    }
