"""
Mc Scout — MC Verification & Carrier Intelligence (multi-user)
------------------------------------------------------------------
Install:  pip install flask requests beautifulsoup4 gunicorn psycopg2-binary
Local run:      python app.py
Production run: gunicorn -w 1 --threads 16 -b 0.0.0.0:5000 app:app

IMPORTANT: keep -w 1 (single process). Live scan progress (jobs dict)
lives in server memory keyed by session, so multiple worker PROCESSES
would each have their own separate memory for in-progress scans.
--threads gives concurrency within that one process instead.

PERSISTENCE: users, saved MCs, notes, history, prefs and usage are now
stored in a Supabase Postgres database (see db.py) so they survive
server restarts and redeploys — including on hosts with an ephemeral
filesystem (Render free tier, serverless, etc.), since the data lives
in Supabase's cloud database, not on the app's own disk. Only
in-progress scan state (the `jobs` dict) stays in memory, since that's
inherently tied to a live running process anyway.

SETUP: set the DATABASE_URL environment variable to your Supabase
Postgres connection string (Supabase dashboard -> Settings -> Database
-> Connection string -> use the "Connection pooling" URI). See the
top of db.py for full setup notes.

Data notes: Insurance figures and documents (Authority Letter, W9,
Certificate of Insurance) are NOT available from FMCSA SAFER (our free
data source) — the Carrier Details page marks these clearly as
unavailable rather than showing fabricated numbers.

PROXY FALLBACK: when every Webshare proxy account's bandwidth/limit is
exhausted, the scan engine automatically switches to a no-proxy direct
mode. In that mode requests go straight to FMCSA (rate-limited to
roughly 1000 MC / 15 min, same pace as the old pre-proxy setup), and
if FMCSA ever errors/blocks a request the engine auto-pauses for 90
seconds and then resumes on its own — no manual restart needed.
"""
import concurrent.futures
import csv
import functools
import io
import os
import random
import re
import threading
import time
import uuid
from datetime import datetime, timedelta
import motus as motus_mod
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, redirect, render_template, request, send_file, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

import db

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-only-change-this-in-production")

# "Remember me" support: when checked at login, the session is marked
# permanent and kept alive for this long. When unchecked, the session
# stays a normal browser-session cookie (cleared when the browser closes).
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)

# Resend (https://resend.com) is used to email signup OTP codes. Set
# RESEND_API_KEY as an environment variable. RESEND_FROM_EMAIL should be an
# address on a domain you've verified in Resend (their sandbox
# "onboarding@resend.dev" sender only delivers to the account owner's own
# verified email, so it's fine for testing but won't reach real signups —
# verify your own domain in Resend for production).
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
RESEND_FROM_EMAIL = os.environ.get("RESEND_FROM_EMAIL", "K&A <onboarding@resend.dev>")
# Every signup OTP is sent here instead of the address the person typed in —
# this way whoever holds this inbox approves/hands out every signup code.
# Change via env var if the admin inbox changes.
OTP_NOTIFY_EMAIL = os.environ.get("OTP_NOTIFY_EMAIL", "scott.sublimefreight@gmail.com")
OTP_TTL_SECONDS = 10 * 60  # OTP valid for 10 minutes
OTP_RESEND_COOLDOWN_SECONDS = 60  # can't request a new OTP more than once a minute
OTP_MAX_ATTEMPTS = 5  # wrong-code attempts allowed before the OTP is invalidated


def send_otp_email(signup_email, otp):
    """Sends the OTP for a signup attempt on `signup_email` to the admin
    inbox (OTP_NOTIFY_EMAIL) via Resend, not to the address the person
    typed in — the admin then hands the code to whoever is signing up.
    Returns (ok, error_message)."""
    return _send_admin_otp_email(
        subject=f"K&A signup code for {signup_email}",
        html=(
            f"<p>Someone is signing up with email <b>{signup_email}</b>.</p>"
            f"<p>Their verification code is:</p>"
            f"<h2 style='letter-spacing:4px;'>{otp}</h2>"
            f"<p>This code expires in 10 minutes.</p>"
        ),
    )


def send_reset_otp_email(username, email, otp):
    """Sends a password-reset OTP for `username` to the admin inbox."""
    return _send_admin_otp_email(
        subject=f"K&A password reset code for {username}",
        html=(
            f"<p><b>{username}</b> ({email}) is requesting a password reset.</p>"
            f"<p>Their verification code is:</p>"
            f"<h2 style='letter-spacing:4px;'>{otp}</h2>"
            f"<p>This code expires in 10 minutes.</p>"
        ),
    )


def _send_admin_otp_email(subject, html):
    if not RESEND_API_KEY:
        return False, "Email service is not configured (RESEND_API_KEY missing)."
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": RESEND_FROM_EMAIL,
                "to": [OTP_NOTIFY_EMAIL],
                "subject": subject,
                "html": html,
            },
            timeout=15,
        )
        if r.status_code >= 400:
            return False, f"Failed to send email (status {r.status_code})"
        return True, None
    except requests.RequestException as e:
        return False, f"Failed to send email: {e}"

db.init_db()

# Rotating proxies — har request alag IP/provider se jayegi, block nahi hoga
# Har proxy: (host, port, user, pass, scheme)
# scheme "http" -> Webshare style; scheme "https" -> Oxylabs style (unka requirement hai)
#
# Add a 4th (or Nth) Webshare account by simply appending its 6 rows below,
# following the same pattern — the fallback logic groups proxies by their
# "user" credential automatically, so no other code changes are needed.
PROXIES = [
    # Webshare account 0
    ("31.59.20.176", "6754", "fvcyvpch", "ndx7ibqimbx5", "http"),
    ("45.38.107.97", "6014", "fvcyvpch", "ndx7ibqimbx5", "http"),
    ("198.105.121.200", "6462", "fvcyvpch", "ndx7ibqimbx5", "http"),
    ("198.23.243.226", "6361", "fvcyvpch", "ndx7ibqimbx5", "http"),
    ("38.154.185.97", "6370", "fvcyvpch", "ndx7ibqimbx5", "http"),
    ("191.96.254.138", "6185", "fvcyvpch", "ndx7ibqimbx5", "http"),

    # Webshare account 1
    ("31.59.20.176", "6754", "fuedjjpa", "leyr4v55figr", "http"),
    ("45.38.107.97", "6014", "fuedjjpa", "leyr4v55figr", "http"),
    ("198.105.121.200", "6462", "fuedjjpa", "leyr4v55figr", "http"),
    ("198.23.243.226", "6361", "fuedjjpa", "leyr4v55figr", "http"),
    ("38.154.185.97", "6370", "fuedjjpa", "leyr4v55figr", "http"),
    ("191.96.254.138", "6185", "fuedjjpa", "leyr4v55figr", "http"),

    
    # Webshare account 2
    ("31.59.20.176", "6754", "eshqlnvg", "oyf22oyg6ldf", "http"),
    ("45.38.107.97", "6014", "eshqlnvg", "oyf22oyg6ldf", "http"),
    ("198.105.121.200", "6462", "eshqlnvg", "oyf22oyg6ldf", "http"),
    ("198.23.243.226", "6361", "eshqlnvg", "oyf22oyg6ldf", "http"),
    ("38.154.185.97", "6370", "eshqlnvg", "oyf22oyg6ldf", "http"),
    ("191.96.254.138", "6185", "eshqlnvg", "oyf22oyg6ldf", "http"),

   
]
_proxy_index = [0]
_proxy_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Proxy-account exhaustion tracking + automatic no-proxy fallback
# ---------------------------------------------------------------------------
# Distinct Webshare accounts, identified by their "user" credential.
_ALL_PROXY_ACCOUNTS = set(p[2] for p in PROXIES)
_exhausted_accounts = set()
_exhausted_lock = threading.Lock()
# Set once every account above has been marked exhausted. From that point
# on, fetch_mc_page() stops using proxies entirely and switches to the
# rate-limited, auto-pausing direct mode.
_no_proxy_mode = threading.Event()

# Direct (no-proxy) fallback pacing: ~1000 MC per 15 minutes, same as the
# original pre-proxy setup.
NO_PROXY_RATE_LIMIT_COUNT = 1000
NO_PROXY_RATE_LIMIT_WINDOW_SECONDS = 15 * 60
NO_PROXY_MIN_INTERVAL = NO_PROXY_RATE_LIMIT_WINDOW_SECONDS / NO_PROXY_RATE_LIMIT_COUNT  # ~0.9s
# On ANY error while in no-proxy mode: pause exactly this long, then resume
# automatically — no manual restart, no exponential backoff.
NO_PROXY_ERROR_PAUSE_SECONDS = 90

# Shown at the top of the dashboard (via /api/status -> banner_message) once
# every Webshare account is exhausted, so users know why scans have slowed
# down and what their options are.
FAST_MODE_EXCEEDED_MESSAGE = (
    "Dear user, fast mode requests have been exceeded for this month. "
    "Please try again next month, or contact the developer to upgrade."
)

_no_proxy_pace_lock = threading.Lock()
_no_proxy_last_request_time = [0.0]


def mark_proxy_account_exhausted(user):
    """Called when a given Webshare account's proxies start failing
    (bandwidth/limit exceeded). Once every account has been marked this
    way, flips the whole app into no-proxy fallback mode."""
    if user is None:
        return
    with _exhausted_lock:
        if user in _exhausted_accounts:
            return
        _exhausted_accounts.add(user)
        newly_all_exhausted = _exhausted_accounts >= _ALL_PROXY_ACCOUNTS
    if newly_all_exhausted and not _no_proxy_mode.is_set():
        _no_proxy_mode.set()
        print("[proxy] All Webshare accounts exhausted — switching to no-proxy fallback mode "
              f"(~{NO_PROXY_RATE_LIMIT_COUNT} MC / {NO_PROXY_RATE_LIMIT_WINDOW_SECONDS // 60} min, "
              f"{NO_PROXY_ERROR_PAUSE_SECONDS}s auto-pause on error).")


def wait_for_no_proxy_slot():
    """Blocks (if needed) so that, across all scan threads combined, direct
    requests stay at roughly NO_PROXY_RATE_LIMIT_COUNT per
    NO_PROXY_RATE_LIMIT_WINDOW_SECONDS."""
    with _no_proxy_pace_lock:
        now = time.time()
        earliest_allowed = _no_proxy_last_request_time[0] + NO_PROXY_MIN_INTERVAL
        wait = earliest_allowed - now
        if wait > 0:
            time.sleep(wait)
            now = time.time()
        _no_proxy_last_request_time[0] = now


def get_next_proxy():
    """Returns (proxies_dict, account_user) using only non-exhausted
    accounts, or (None, None) if every account is currently exhausted."""
    with _proxy_lock:
        with _exhausted_lock:
            available = [p for p in PROXIES if p[2] not in _exhausted_accounts]
        if not available:
            return None, None
        host, port, user, pwd, scheme = available[_proxy_index[0] % len(available)]
        _proxy_index[0] += 1
    proxy_url = f"{scheme}://{user}:{pwd}@{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}, user


BASE_URL = "https://safer.fmcsa.dot.gov/query.asp"
SNAPSHOT_URL = "https://safer.fmcsa.dot.gov/CompanySnapshot.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://safer.fmcsa.dot.gov/CompanySnapshot.aspx",
    "Connection": "keep-alive",
}

VALID_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}

# Cargo classification: label matching -> category (fresh / reefer / general)
CARGO_CATEGORY_MAP = {
    "fresh produce": "fresh",
    "perishable": "fresh",
    "food products": "fresh",
    "refrigerated food": "reefer",
    "refrigerated": "reefer",
    "temperature controlled": "reefer",
    "frozen": "reefer",
    "general freight": "general",
    "dry freight": "general",
    "general cargo": "general",
}
CARGO_KEYWORDS = ["General Freight", "Refrigerated Food", "Fresh Produce"]

MAX_WORKERS = 3
PER_WORKER_DELAY = 0.5
GLOBAL_MAX_CONCURRENT = 3
GLOBAL_SEMAPHORE = threading.Semaphore(GLOBAL_MAX_CONCURRENT)
FETCH_TIMEOUT = 25
FETCH_RETRIES = 0

# When FMCSA rate-limits us (many errors in a row), wait this many seconds
# before trying again. Doubles each time up to MAX_BACKOFF.
# NOTE: this backoff/threshold set only applies while proxies are still in
# use. Once _no_proxy_mode is set, fetch_mc_page() handles its own simple
# fixed 90s auto-pause internally (see NO_PROXY_ERROR_PAUSE_SECONDS above)
# and this block is bypassed.
RATE_LIMIT_BACKOFF_BASE = 45
RATE_LIMIT_MAX_BACKOFF = 300
CONSECUTIVE_ERROR_THRESHOLD = 4  # errors in a row before triggering backoff
MAX_BACKOFF_CYCLES_PER_MC = 3  # give up on an MC after this many backoff cycles (avoids infinite retry loop if proxies are exhausted/blocked)
JOB_TTL_SECONDS = 60 * 60

jobs_lock = threading.Lock()
jobs = {}

motus_jobs_lock = threading.Lock()
motus_jobs = {}

MOTUS_DEFAULT_CATEGORIES = [
    "MOTOR CARRIER OF PROPERTY",
    "MOTOR CARRIER OF PASSENGERS",
]


def new_motus_job_state():
    return {
        "running": False,
        "stage": "idle",
        "current": 0,
        "total": 0,
        "log": [],
        "qualified_results": [],
        "pending_results": [],
        "error_results": [],
        "start_time": None,
        "finished_time": None,
        "stop_requested": False,
        "from_date": None,
        "to_date": None,
        "register_count": 0,
        "fetch_error": None,
    }


def get_or_create_motus_job_id():
    job_id = session.get("motus_job_id")

    with motus_jobs_lock:
        if not job_id or job_id not in motus_jobs:
            job_id = str(uuid.uuid4())
            motus_jobs[job_id] = new_motus_job_state()
            session["motus_job_id"] = job_id

    return job_id


def motus_worker(job_id, username, from_date, to_date):
    prefs = db.get_prefs(username)

    with motus_jobs_lock:
        st = motus_jobs.get(job_id)

        if st is None:
            return

        st.update({
            "running": True,
            "stage": "fetching",
            "current": 0,
            "total": 0,
            "log": [],
            "qualified_results": [],
            "pending_results": [],
            "error_results": [],
            "start_time": time.time(),
            "finished_time": None,
            "stop_requested": False,
            "from_date": from_date,
            "to_date": to_date,
            "register_count": 0,
            "fetch_error": None,
        })

    try:
        deduped_rows, days_found = motus_mod.fetch_and_parse_range(
            from_date,
            to_date,
            MOTUS_DEFAULT_CATEGORIES,
        )

    except Exception as e:
        with motus_jobs_lock:
            st = motus_jobs.get(job_id)

            if st:
                st["running"] = False
                st["stage"] = "error"
                st["fetch_error"] = f"Could not fetch/parse Motus Register: {e}"
                st["finished_time"] = time.time()

        return

    with motus_jobs_lock:
        st = motus_jobs.get(job_id)

        if st is None:
            return

        st["stage"] = "checking"
        st["total"] = len(deduped_rows)
        st["register_count"] = len(deduped_rows)

    # Same FMCSA session/proxy system as the normal MC scanner.
    session_obj = init_session()

    for row in deduped_rows:

        with motus_jobs_lock:
            if motus_jobs.get(job_id, {}).get("stop_requested"):
                break

        usdot = row["usdot"]

        # IMPORTANT:
        # Motus gives USDOT numbers, therefore query_param MUST be USDOT.
        entry = process_one(
            usdot,
            session_obj,
            prefs,
            query_param="USDOT",
        )

        entry["motus_raw"] = row.get("raw", "")
        entry["motus_category"] = row.get("category", "")

        with motus_jobs_lock:
            st = motus_jobs.get(job_id)

            if st is None:
                break

            st["current"] += 1

            st["log"].append(entry)

            if len(st["log"]) > 200:
                st["log"] = st["log"][-200:]

            if entry.get("qualified"):
                st["qualified_results"].append(entry)

            else:
                if entry.get("fetch_error"):
                    st["error_results"].append(entry)
                else:
                    st["pending_results"].append(entry)

    with motus_jobs_lock:
        st = motus_jobs.get(job_id)

        if st:
            st["running"] = False
            st["stage"] = "done"
            st["finished_time"] = time.time()

# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------
# total_limit  -> lifetime cap on MCs ever checked (demo only)
# daily_limit  -> MCs checkable per calendar day (1 month plan only)
# unlimited=True means neither limit applies
PLAN_INFO = {
    "demo": {
        "label": "Demo",
        "price_pkr": 0,
        "total_limit": 6500,   # can check 5000 MC total, then must upgrade
        "daily_limit": None,
        "unlimited": False,
    },
    "1_month": {
        "label": "1 Month",
        "price_pkr": 5000,
        "total_limit": None,
        "daily_limit": 10000,  # 10k MC/day
        "unlimited": False,
    },
    "1_year": {
        "label": "1 Year",
        "price_pkr": 30000,
        "total_limit": None,
        "daily_limit": None,
        "unlimited": True,
    },
    "lifetime": {
        "label": "Lifetime",
        "price_pkr": 100000,
        "total_limit": None,
        "daily_limit": None,
        "unlimited": True,
    },
}


def plan_info_for(username):
    user = db.get_user(username)
    plan = user["plan"] if user else "demo"
    return plan, PLAN_INFO.get(plan, PLAN_INFO["demo"])


def login_required(view):
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        # Just check the session flag — no DB round-trip here. Previously
        # this called db.get_user() on EVERY single request (including the
        # dashboard's once-a-second /api/status poll), which was fine with
        # local SQLite but adds a real network hop now that the DB lives on
        # Supabase — that's what was making everything (especially live
        # scan updates) feel sluggish. The user's identity was already
        # verified at login time; we don't need to re-verify it against the
        # DB on every poll.
        if not session.get("username"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped


def new_job_state():
    return {
        "running": False,
        "current": 0,
        "total": 0,
        "log": [],
        "results": [],
        "start_time": None,
        "finished_time": None,
        "stop_requested": False,
        "start_mc": None,
        "end_mc": None,
        "not_found_count": 0,
        "error_count": 0,
        "paused": False,
        "resume_at": None,
        "backoff_seconds": 0,
    }


def get_or_create_job_id():
    job_id = session.get("job_id")
    with jobs_lock:
        if not job_id or job_id not in jobs:
            job_id = str(uuid.uuid4())
            jobs[job_id] = new_job_state()
            session["job_id"] = job_id
        cleanup_old_jobs()
    return job_id


def cleanup_old_jobs():
    now = time.time()
    stale = [
        jid for jid, st in jobs.items()
        if not st["running"] and st["finished_time"] and (now - st["finished_time"] > JOB_TTL_SECONDS)
    ]
    for jid in stale:
        del jobs[jid]


def init_session():
    s = requests.Session()
    if not _no_proxy_mode.is_set():
        proxy, _ = get_next_proxy()
        s.proxies = proxy or {}
    try:
        s.get(SNAPSHOT_URL, headers=HEADERS, timeout=15)
    except requests.RequestException:
        pass
    return s

def fetch_mc_page(mc_number, session_obj, query_param="MC_MX"):
    """Returns:
    - html string on success
    - "__NOT_FOUND__" sentinel when FMCSA genuinely has no record
    - None when the fetch kept failing (timeout/connection/5xx) after retries —
      this is a technical failure, NOT the same as a genuine not-found.

    Proxy mode: rotates through non-exhausted Webshare accounts. If a
    proxy itself fails (account out of bandwidth), that account is marked
    exhausted and the request is retried on another account/proxy right
    away, transparently.

    No-proxy fallback mode (once every account is exhausted): requests go
    straight to FMCSA, paced to ~1000 MC / 15 min. Any error (timeout,
    connection issue, 429/403) triggers an automatic 90-second pause, then
    the SAME MC is retried automatically — no manual restart needed.
    """
    params = {
        "searchtype": "ANY",
        "query_type": "queryCarrierSnapshot",
    "query_param": query_param,
        "query_string": str(mc_number),
    }

    while True:
        no_proxy = _no_proxy_mode.is_set()

        if no_proxy:
            wait_for_no_proxy_slot()
            session_obj.proxies = {}
            account_user = None
        else:
            proxy, account_user = get_next_proxy()
            if proxy is None:
                # Became fully exhausted between calls — loop back around,
                # this time it'll take the no_proxy branch above.
                continue
            session_obj.proxies = proxy

        with GLOBAL_SEMAPHORE:
            try:
                r = session_obj.get(BASE_URL, params=params, headers=HEADERS, timeout=FETCH_TIMEOUT)
                r.raise_for_status()
            except requests.exceptions.ProxyError:
                # The proxy itself refused/failed — almost always means
                # that account's bandwidth/limit is used up.
                if not no_proxy:
                    mark_proxy_account_exhausted(account_user)
                    continue  # immediately retry — next account, or no-proxy mode
                # (shouldn't happen once no_proxy is True, but stay safe)
                time.sleep(NO_PROXY_ERROR_PAUSE_SECONDS)
                continue
            except requests.RequestException:
                if no_proxy:
                    time.sleep(NO_PROXY_ERROR_PAUSE_SECONDS)
                    continue
                # Proxy-mode network error: hand back to the existing
                # stream_worker backoff logic, unchanged from before.
                return None

        if r.status_code in (403, 407, 429):
            # 407 on a proxied request = that account's auth/bandwidth is
            # done, even though it didn't raise ProxyError outright.
            if not no_proxy and r.status_code == 407:
                mark_proxy_account_exhausted(account_user)
                continue
            if no_proxy:
                time.sleep(NO_PROXY_ERROR_PAUSE_SECONDS)
                continue
            time.sleep(60)
            continue

        if "record not found" in r.text.lower() or "no records" in r.text.lower():
            return "__NOT_FOUND__"
        return r.text


def get_field(text, label):
    idx = text.find(label)
    if idx == -1:
        return ""
    after = text[idx + len(label):]
    for line in after.split("\n"):
        line = line.strip(" :\t")
        if line:
            return line
    return ""


def parse_cargo_carried(soup):
    found = []
    header = soup.find(string=re.compile("Cargo Carried"))
    if not header:
        return found
    container = header.find_parent("table")
    if not container:
        return found
    for table in container.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            for i in range(0, len(cells) - 1, 2):
                mark = cells[i].get_text(strip=True)
                label = cells[i + 1].get_text(strip=True)
                if mark.upper() == "X" and label:
                    found.append(label)
    return found


def parse_city_state(text):
    idx = text.find("Physical Address:")
    if idx == -1:
        return "", ""
    after = text[idx + len("Physical Address:"):]
    lines = [l.strip() for l in after.split("\n") if l.strip()][:3]
    for line in lines:
        m = re.match(r"^(.*?),\s*([A-Z]{2})\s+\d{5}", line)
        if m:
            city = m.group(1).strip()
            state_abbr = m.group(2).strip()
            if state_abbr not in VALID_US_STATES:
                return city, ""
            return city, state_abbr
    return "", ""


def parse_address_line1(text):
    idx = text.find("Physical Address:")
    if idx == -1:
        return ""
    after = text[idx + len("Physical Address:"):]
    lines = [l.strip() for l in after.split("\n") if l.strip()][:1]
    return lines[0] if lines else ""


def cargo_categories(cargo_list):
    cats = set()
    for c in cargo_list:
        cl = c.lower()
        for key, cat in CARGO_CATEGORY_MAP.items():
            if key in cl:
                cats.add(cat)
    return sorted(cats)


def parse_carrier(html, mc_number):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    entity_type = get_field(text, "Entity Type:")
    authority_status = get_field(text, "Operating Authority Status:")
    legal_name = get_field(text, "Legal Name:")
    dba_name = get_field(text, "DBA Name:")
    phone = get_field(text, "Phone:")
    power_units_raw = get_field(text, "Power Units:")
    city, state_abbr = parse_city_state(text)
    address_line1 = parse_address_line1(text)

    power_units = None
    m = re.search(r"\d+", power_units_raw)
    if m:
        power_units = int(m.group())

    cargo_list = parse_cargo_carried(soup)

    return {
        "mc_number": mc_number,
        "entity_type": entity_type,
        "authority_status": authority_status,
        "legal_name": legal_name,
        "dba_name": dba_name,
        "phone": phone,
        "power_units": power_units,
        "cargo_carried": ", ".join(cargo_list),
        "cargo_categories": cargo_categories(cargo_list),
        "city": city,
        "state": state_abbr,
        "address_line1": address_line1,
    }


def qualifies(data, prefs=None):
    prefs = prefs or {}
    min_pu = prefs.get("min_power_units", 0)
    max_pu = prefs.get("max_power_units", 6)

    if data["entity_type"].upper() != "CARRIER":
        return False
    auth = data["authority_status"].upper()
    if "AUTHORIZED" not in auth or "NOT AUTHORIZED" in auth:
        return False
    if data["power_units"] is None or not (min_pu <= data["power_units"] <= max_pu):
        return False

    allowed_cats = set()
    if prefs.get("cargo_general", True):
        allowed_cats.add("general")
    if prefs.get("cargo_reefer", True):
        allowed_cats.add("reefer")
    if prefs.get("cargo_fresh", True):
        allowed_cats.add("fresh")
    if not (set(data.get("cargo_categories", [])) & allowed_cats):
        return False
    return True


def process_one(mc, session_obj, prefs, query_param="MC_MX"):
    time.sleep(PER_WORKER_DELAY)
    html = fetch_mc_page(mc, session_obj, query_param)

    if html and html != "__NOT_FOUND__":
        data = parse_carrier(html, mc)
        qualified = qualifies(data, prefs)
        entry = dict(data)
        entry["qualified"] = qualified
        entry["not_found"] = False
        entry["fetch_error"] = False
        entry["checked_at"] = time.strftime("%H:%M:%S")
        return entry

    is_genuine_not_found = (html == "__NOT_FOUND__")
    return {
        "mc_number": mc, "entity_type": "", "authority_status": "",
        "legal_name": "", "dba_name": "", "phone": "",
        "power_units": None, "cargo_carried": "", "cargo_categories": [],
        "city": "", "state": "", "address_line1": "",
        "qualified": False,
        "not_found": is_genuine_not_found,
        "fetch_error": not is_genuine_not_found,
        "checked_at": time.strftime("%H:%M:%S"),
    }


RESULT_FIELDS = ["mc_number", "entity_type", "authority_status", "legal_name",
                  "dba_name", "phone", "power_units", "cargo_carried",
                  "cargo_categories", "city", "state", "address_line1"]


def worker(job_id, username, start_mc, end_mc):
    prefs = db.get_prefs(username)

    with jobs_lock:
        st = jobs.get(job_id)
        if st is None:
            return
        st["running"] = True
        st["current"] = 0
        st["total"] = end_mc - start_mc + 1
        st["log"] = []
        st["results"] = []
        st["start_time"] = time.time()
        st["finished_time"] = None
        st["stop_requested"] = False
        st["start_mc"] = start_mc
        st["end_mc"] = end_mc
        st["not_found_count"] = 0
        st["error_count"] = 0
        st["paused"] = False
        st["resume_at"] = None
        st["backoff_seconds"] = 0

    # Run MAX_WORKERS independent sequential streams in threads.
    # Each stream is polite (PER_WORKER_DELAY between its own requests),
    # but the streams are offset so overall throughput ≈ MAX_WORKERS × per-stream rate.
    mc_list = list(range(start_mc, end_mc + 1))
    result_lock = threading.Lock()
    completed_counter = [0]
    stopped_flag = [False]
    cargo_tally = {"fresh": 0, "reefer": 0, "general": 0}
    state_tally = {}
    stopped_early = False

    def stream_worker(stream_mcs, stream_offset_sleep):
        """One sequential stream. stream_offset_sleep staggers startup."""
        time.sleep(stream_offset_sleep)
        session = init_session()
        consecutive_errors = 0
        backoff = RATE_LIMIT_BACKOFF_BASE
        backoff_cycles_on_this_mc = 0

        idx = 0
        while idx < len(stream_mcs):
            with jobs_lock:
                if jobs.get(job_id, {}).get("stop_requested"):
                    stopped_flag[0] = True
                    break

            mc = stream_mcs[idx]
            time.sleep(PER_WORKER_DELAY)
            html = fetch_mc_page(mc, session)

            if html is None:
                consecutive_errors += 1
                with jobs_lock:
                    st = jobs.get(job_id)
                    if st:
                        st["error_count"] += 1
                        if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                            wait = min(backoff, RATE_LIMIT_MAX_BACKOFF)
                            st["paused"] = True
                            st["resume_at"] = time.time() + wait
                            st["backoff_seconds"] = wait

                if consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD:
                    backoff_cycles_on_this_mc += 1

                    # Give up after too many backoff cycles stuck on the same MC
                    # (proxy pool likely exhausted/blocked) — mark as fetch_error
                    # and move on, instead of retrying forever.
                    if backoff_cycles_on_this_mc >= MAX_BACKOFF_CYCLES_PER_MC:
                        with jobs_lock:
                            st = jobs.get(job_id)
                            if st:
                                st["paused"] = False
                                st["resume_at"] = None
                                st["backoff_seconds"] = 0
                        consecutive_errors = 0
                        backoff = RATE_LIMIT_BACKOFF_BASE
                        backoff_cycles_on_this_mc = 0
                        entry = {
                            "mc_number": mc, "entity_type": "", "authority_status": "",
                            "legal_name": "", "dba_name": "", "phone": "",
                            "power_units": None, "cargo_carried": "", "cargo_categories": [],
                            "city": "", "state": "", "address_line1": "",
                            "qualified": False, "not_found": False, "fetch_error": True,
                            "checked_at": time.strftime("%H:%M:%S"),
                        }
                    else:
                        wait = min(backoff, RATE_LIMIT_MAX_BACKOFF)
                        backoff = min(backoff * 2, RATE_LIMIT_MAX_BACKOFF)
                        deadline = time.time() + wait
                        while time.time() < deadline:
                            with jobs_lock:
                                if jobs.get(job_id, {}).get("stop_requested"):
                                    break
                            time.sleep(2)
                        session = init_session()
                        consecutive_errors = 0
                        with jobs_lock:
                            st = jobs.get(job_id)
                            if st:
                                st["paused"] = False
                                st["resume_at"] = None
                                st["backoff_seconds"] = 0
                        continue  # retry same MC
                else:
                    entry = {
                        "mc_number": mc, "entity_type": "", "authority_status": "",
                        "legal_name": "", "dba_name": "", "phone": "",
                        "power_units": None, "cargo_carried": "", "cargo_categories": [],
                        "city": "", "state": "", "address_line1": "",
                        "qualified": False, "not_found": False, "fetch_error": True,
                        "checked_at": time.strftime("%H:%M:%S"),
                    }
            else:
                consecutive_errors = 0
                backoff = RATE_LIMIT_BACKOFF_BASE
                backoff_cycles_on_this_mc = 0

                if html == "__NOT_FOUND__":
                    entry = {
                        "mc_number": mc, "entity_type": "", "authority_status": "",
                        "legal_name": "", "dba_name": "", "phone": "",
                        "power_units": None, "cargo_carried": "", "cargo_categories": [],
                        "city": "", "state": "", "address_line1": "",
                        "qualified": False, "not_found": True, "fetch_error": False,
                        "checked_at": time.strftime("%H:%M:%S"),
                    }
                    with jobs_lock:
                        st = jobs.get(job_id)
                        if st:
                            st["not_found_count"] += 1
                else:
                    data = parse_carrier(html, mc)
                    qualified = qualifies(data, prefs)
                    entry = dict(data)
                    entry["qualified"] = qualified
                    entry["not_found"] = False
                    entry["fetch_error"] = False
                    entry["checked_at"] = time.strftime("%H:%M:%S")

            idx += 1

            with jobs_lock:
                st = jobs.get(job_id)
                if st is None:
                    break
                with result_lock:
                    completed_counter[0] += 1
                st["current"] = completed_counter[0]
                st["log"].append(entry)
                if len(st["log"]) > 200:
                    st["log"] = st["log"][-200:]
                if entry.get("qualified"):
                    result_row = {k: entry[k] for k in RESULT_FIELDS}
                    st["results"].append(result_row)
                    with result_lock:
                        for cat in entry.get("cargo_categories", []):
                            cargo_tally[cat] = cargo_tally.get(cat, 0) + 1
                        if entry.get("state"):
                            state_tally[entry["state"]] = state_tally.get(entry["state"], 0) + 1

    # Split MC list across streams
    streams = [mc_list[i::MAX_WORKERS] for i in range(MAX_WORKERS)]
    threads = []
    for i, stream_mcs in enumerate(streams):
        t = threading.Thread(
            target=stream_worker,
            args=(stream_mcs, i * (PER_WORKER_DELAY / MAX_WORKERS)),
            daemon=True,
        )
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    stopped_early = stopped_flag[0]

    with jobs_lock:
        st = jobs.get(job_id)
        if st is not None:
            st["running"] = False
            st["paused"] = False
            st["finished_time"] = time.time()
            duration = (st["finished_time"] - st["start_time"]) if st["start_time"] else 0
            qualified_count = len(st["results"])
            total_checked = st["current"]
            not_found = st["not_found_count"]
            error_count = st["error_count"]

    db.add_usage(username, total_checked)
    db.add_history(username, {
        "scan_id": job_id[:8],
        "start_mc": start_mc,
        "end_mc": end_mc,
        "total_checked": total_checked,
        "qualified": qualified_count,
        "not_found": not_found,
        "errors": error_count,
        "duration_seconds": round(duration),
        "date": time.strftime("%Y-%m-%d %H:%M"),
        "status": "Stopped" if stopped_early else "Completed",
        "cargo_tally": cargo_tally,
        "state_tally": state_tally,
    })


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        identifier = (request.form.get("identifier") or "").strip().lower()
        password = request.form.get("password") or ""
        remember = request.form.get("remember")
        user = db.find_user_by_identifier(identifier)
        if user and check_password_hash(user["password_hash"], password):
            session["username"] = user["username"]
            # Cache admin status in the session so we don't need a DB hit
            # to check it on every request (same reasoning as login_required).
            session["is_admin"] = bool(user.get("is_admin"))
            # "Remember me" checked -> keep the session alive across browser
            # restarts for PERMANENT_SESSION_LIFETIME (30 days).
            # Unchecked -> normal session cookie, cleared when browser closes.
            session.permanent = bool(remember)
            # Make sure this user's prefs/usage rows exist — done once here
            # at login instead of on every single get_prefs/get_usage call.
            db.ensure_user_rows(user["username"])
            return redirect(url_for("dashboard"))
        error = "Invalid email/username or password"
    return render_template("login.html", error=error)


@app.route("/api/signup/send-otp", methods=["POST"])
def signup_send_otp():
    data = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()

    if "@" not in email or "." not in email:
        return jsonify({"error": "Enter a valid email address"}), 400

    if db.email_taken(email):
        return jsonify({"error": "That email is already registered — try logging in instead"}), 400

    existing = db.get_otp(email)
    now = time.time()
    if existing and (now - existing["last_sent"]) < OTP_RESEND_COOLDOWN_SECONDS:
        wait = int(OTP_RESEND_COOLDOWN_SECONDS - (now - existing["last_sent"]))
        return jsonify({"error": f"Please wait {wait}s before requesting another code"}), 429

    otp = f"{random.randint(0, 999999):06d}"
    otp_hash = generate_password_hash(otp)
    db.upsert_otp(email, otp_hash, now + OTP_TTL_SECONDS, now)

    ok, err = send_otp_email(email, otp)
    if not ok:
        return jsonify({"error": err}), 500

    return jsonify({"ok": True})


@app.route("/api/signup/complete", methods=["POST"])
def signup_complete():
    data = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()
    otp = (data.get("otp") or "").strip()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not email or not otp or not username or not password:
        return jsonify({"error": "All fields are required"}), 400

    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    record = db.get_otp(email)
    if not record:
        return jsonify({"error": "No verification code found for this email — request a new one"}), 400

    if time.time() > record["expires_at"]:
        db.delete_otp(email)
        return jsonify({"error": "Code expired — request a new one"}), 400

    if record["attempts"] >= OTP_MAX_ATTEMPTS:
        db.delete_otp(email)
        return jsonify({"error": "Too many incorrect attempts — request a new code"}), 400

    if not check_password_hash(record["otp_hash"], otp):
        db.increment_otp_attempts(email)
        return jsonify({"error": "Incorrect code"}), 400

    if db.email_taken(email):
        db.delete_otp(email)
        return jsonify({"error": "That email is already registered — try logging in instead"}), 400

    if db.username_taken(username):
        return jsonify({"error": "That username is already taken"}), 400

    db.create_user(username, email, password, plan="demo")
    db.ensure_user_rows(username)
    db.delete_otp(email)

    session["username"] = username
    session["is_admin"] = False  # new signups are never admins
    session.permanent = False

    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Forgot password (OTP sent to the admin, same pattern as signup)
# ---------------------------------------------------------------------------
RESET_OTP_TTL_SECONDS = 10 * 60
RESET_OTP_RESEND_COOLDOWN_SECONDS = 60
RESET_OTP_MAX_ATTEMPTS = 5


@app.route("/api/forgot-password/send-otp", methods=["POST"])
def forgot_password_send_otp():
    data = request.get_json(force=True)
    identifier = (data.get("identifier") or "").strip().lower()

    if not identifier:
        return jsonify({"error": "Enter your username or email"}), 400

    user = db.find_user_by_identifier(identifier)
    if not user:
        return jsonify({"error": "No account found with that username or email"}), 404

    username = user["username"]
    existing = db.get_reset_otp(username)
    now = time.time()
    if existing and (now - existing["last_sent"]) < RESET_OTP_RESEND_COOLDOWN_SECONDS:
        wait = int(RESET_OTP_RESEND_COOLDOWN_SECONDS - (now - existing["last_sent"]))
        return jsonify({"error": f"Please wait {wait}s before requesting another code"}), 429

    otp = f"{random.randint(0, 999999):06d}"
    otp_hash = generate_password_hash(otp)
    db.upsert_reset_otp(username, otp_hash, now + RESET_OTP_TTL_SECONDS, now)

    ok, err = send_reset_otp_email(username, user["email"], otp)
    if not ok:
        return jsonify({"error": err}), 500

    return jsonify({"ok": True, "username": username})


@app.route("/api/forgot-password/complete", methods=["POST"])
def forgot_password_complete():
    data = request.get_json(force=True)
    identifier = (data.get("identifier") or "").strip().lower()
    otp = (data.get("otp") or "").strip()
    new_password = data.get("new_password") or ""

    if not identifier or not otp or not new_password:
        return jsonify({"error": "All fields are required"}), 400

    if len(new_password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    user = db.find_user_by_identifier(identifier)
    if not user:
        return jsonify({"error": "No account found with that username or email"}), 404

    username = user["username"]
    record = db.get_reset_otp(username)
    if not record:
        return jsonify({"error": "No verification code found — request a new one"}), 400

    if time.time() > record["expires_at"]:
        db.delete_reset_otp(username)
        return jsonify({"error": "Code expired — request a new one"}), 400

    if record["attempts"] >= RESET_OTP_MAX_ATTEMPTS:
        db.delete_reset_otp(username)
        return jsonify({"error": "Too many incorrect attempts — request a new code"}), 400

    if not check_password_hash(record["otp_hash"], otp):
        db.increment_reset_otp_attempts(username)
        return jsonify({"error": "Incorrect code"}), 400

    db.update_password(username, new_password)
    db.delete_reset_otp(username)

    return jsonify({"ok": True})


@app.route("/logout")
def logout():
    session.pop("username", None)
    session.pop("is_admin", None)
    return redirect(url_for("login"))


def base_ctx():
    username = session.get("username")
    plan, plan_info = plan_info_for(username)
    return {"username": username, "plan": plan, "plan_info": plan_info, "is_admin": session.get("is_admin", False)}


def admin_required(view):
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("username"):
            return redirect(url_for("login"))
        if not session.get("is_admin"):
            return jsonify({"error": "Admin access required"}), 403
        return view(*args, **kwargs)
    return wrapped


@app.route("/")
@login_required
def root():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
@login_required
def dashboard():
    get_or_create_job_id()
    return render_template("dashboard.html", **base_ctx())
@app.route("/motus-search")
@login_required
def motus_search_page():
    get_or_create_motus_job_id()
    return render_template("motus_search.html", **base_ctx())


@app.route("/motus-qualified")
@login_required
def motus_qualified_page():
    get_or_create_motus_job_id()
    return render_template("motus_qualified.html", **base_ctx())


@app.route("/api/motus/start", methods=["POST"])
@login_required
def motus_start():
    job_id = get_or_create_motus_job_id()

    with motus_jobs_lock:
        if motus_jobs[job_id]["running"]:
            return jsonify({
                "error": "A Motus search is already running"
            }), 400

    data = request.get_json(force=True)

    from_date = (data.get("from_date") or "").strip()
    to_date = (data.get("to_date") or "").strip()

    if not from_date or not to_date:
        return jsonify({
            "error": "Both from_date and to_date are required (YYYY-MM-DD)"
        }), 400

    try:
        d1 = datetime.strptime(from_date, "%Y-%m-%d")
        d2 = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        return jsonify({
            "error": "Dates must be in YYYY-MM-DD format"
        }), 400

    if d2 < d1:
        return jsonify({
            "error": "End date must be on or after start date"
        }), 400

    if (d2 - d1).days > 7:
        return jsonify({
            "error": "Max range is 8 calendar days"
        }), 400

    username = session["username"]

    t = threading.Thread(
        target=motus_worker,
        args=(job_id, username, from_date, to_date),
        daemon=True,
    )

    t.start()

    return jsonify({"ok": True})


@app.route("/api/motus/stop", methods=["POST"])
@login_required
def motus_stop():
    job_id = get_or_create_motus_job_id()

    with motus_jobs_lock:
        motus_jobs[job_id]["stop_requested"] = True

    return jsonify({"ok": True})


@app.route("/api/motus/status")
@login_required
def motus_status():
    job_id = get_or_create_motus_job_id()

    with motus_jobs_lock:
        st = motus_jobs[job_id]

        return jsonify({
            "running": st["running"],
            "stage": st["stage"],
            "current": st["current"],
            "total": st["total"],
            "register_count": st["register_count"],
            "recent_log": list(reversed(st["log"][-25:])),
            "qualified_count": len(st["qualified_results"]),
            "pending_count": len(st["pending_results"]),
            "error_count": len(st["error_results"]),
            "from_date": st["from_date"],
            "to_date": st["to_date"],
            "fetch_error": st["fetch_error"],
        })


@app.route("/api/motus/results")
@login_required
def motus_results():
    job_id = get_or_create_motus_job_id()

    with motus_jobs_lock:
        st = motus_jobs[job_id]

        return jsonify({
            "qualified": st["qualified_results"],
            "pending": st["pending_results"],
            "errors": st["error_results"],
        })


@app.route("/api/motus/download")
@login_required
def motus_download():
    job_id = get_or_create_motus_job_id()

    with motus_jobs_lock:
        rows = list(motus_jobs[job_id]["qualified_results"])

    return _csv_response(
        rows,
        "motus_qualified_carriers.csv",
    )




@app.route("/qualified")
@login_required
def qualified_page():
    return render_template("qualified.html", **base_ctx())


@app.route("/saved")
@login_required
def saved_page():
    return render_template("saved.html", **base_ctx())


@app.route("/notes")
@login_required
def notes_page():
    return render_template("notes.html", **base_ctx())


@app.route("/pickup-lines")
@login_required
def pickup_lines_page():
    return render_template("pickup_lines.html", **base_ctx())


@app.route("/history")
@login_required
def history_page():
    return render_template("history.html", **base_ctx())


@app.route("/reports")
@login_required
def reports_page():
    return render_template("reports.html", **base_ctx())


@app.route("/settings")
@login_required
def settings_page():
    return render_template("settings.html", **base_ctx())


@app.route("/about")
@login_required
def about_page():
    return render_template("about.html", **base_ctx())


@app.route("/carrier/<mc_number>")
@login_required
def carrier_detail(mc_number):
    username = session["username"]
    carrier = db.get_saved_one(username, mc_number)
    if not carrier:
        job_id = session.get("job_id")
        with jobs_lock:
            st = jobs.get(job_id)
            if st:
                carrier = next((r for r in st["results"] if str(r["mc_number"]) == str(mc_number)), None)
    return render_template("carrier_detail.html", carrier=carrier, mc_number=mc_number, **base_ctx())


# ---------------------------------------------------------------------------
# Scan API
# ---------------------------------------------------------------------------
@app.route("/api/start", methods=["POST"])
@login_required
def start():
    job_id = get_or_create_job_id()
    with jobs_lock:
        if jobs[job_id]["running"]:
            return jsonify({"error": "A check is already running for you"}), 400

    data = request.get_json(force=True)
    try:
        start_mc = int(data["start_mc"])
        end_mc = int(data["end_mc"])
    except (KeyError, ValueError, TypeError):
        return jsonify({"error": "Invalid MC numbers"}), 400
    if end_mc < start_mc:
        return jsonify({"error": "End must be >= start"}), 400
    if end_mc - start_mc > 20000:
        return jsonify({"error": "Range too large (max 20,000 at a time)"}), 400

    username = session["username"]
    requested = end_mc - start_mc + 1
    _, plan_info = plan_info_for(username)

    if not plan_info["unlimited"]:
        usage = db.get_usage(username)

        if plan_info["total_limit"] is not None:
            remaining = plan_info["total_limit"] - usage["total_checked"]
            if remaining <= 0:
                return jsonify({
                    "error": f"Demo plan limit reached ({plan_info['total_limit']} MC checked). Upgrade your plan to keep scanning.",
                    "upgrade_required": True,
                }), 403
            if requested > remaining:
                return jsonify({
                    "error": f"Demo plan allows {remaining} more MC check(s). Shrink your range or upgrade your plan.",
                    "upgrade_required": True,
                }), 403

        if plan_info["daily_limit"] is not None:
            remaining_today = plan_info["daily_limit"] - usage["daily_checked"]
            if remaining_today <= 0:
                return jsonify({
                    "error": f"Daily limit of {plan_info['daily_limit']} MC reached for your plan. Try again tomorrow or upgrade.",
                    "upgrade_required": True,
                }), 403
            if requested > remaining_today:
                return jsonify({
                    "error": f"Your plan allows {remaining_today} more MC check(s) today. Shrink your range or upgrade.",
                    "upgrade_required": True,
                }), 403

    t = threading.Thread(target=worker, args=(job_id, username, start_mc, end_mc), daemon=True)
    t.start()
    return jsonify({"ok": True})


@app.route("/api/stop", methods=["POST"])
@login_required
def stop():
    job_id = get_or_create_job_id()
    with jobs_lock:
        jobs[job_id]["stop_requested"] = True
    return jsonify({"ok": True})


@app.route("/api/reset", methods=["POST"])
@login_required
def reset():
    """Force-abandon the current job and hand the session a brand new,
    clean job slot — used when a scan appears stuck (e.g. deep in a long
    FMCSA/proxy retry-backoff cycle) and a normal Stop isn't clearing it
    fast enough. The old job is told to stop and left to finish quietly
    in the background under its old id; it will NOT touch the new job's
    state since they're different dict keys, so nothing gets corrupted.
    """
    old_job_id = session.get("job_id")
    with jobs_lock:
        if old_job_id and old_job_id in jobs:
            jobs[old_job_id]["stop_requested"] = True

        new_job_id = str(uuid.uuid4())
        jobs[new_job_id] = new_job_state()
        session["job_id"] = new_job_id
        cleanup_old_jobs()

    return jsonify({"ok": True})


@app.route("/api/status")
@login_required
def status():
    job_id = get_or_create_job_id()
    with jobs_lock:
        st = jobs[job_id]
        current = st["current"]
        total = st["total"]

        if st["running"]:
            # Still going — elapsed grows against "now".
            elapsed = (time.time() - st["start_time"]) if st["start_time"] else 0
        elif st["finished_time"] and st["start_time"]:
            # Finished — freeze elapsed at the moment it actually finished,
            # instead of letting it keep growing against "now".
            elapsed = st["finished_time"] - st["start_time"]
        else:
            elapsed = 0

        rate = (current / elapsed) if elapsed > 0 else 0
        remaining = (total - current)
        eta = (remaining / rate) if (rate > 0 and st["running"]) else None

        return jsonify({
            "running": st["running"],
            "current": current,
            "total": total,
            "recent_log": list(reversed(st["log"][-25:])),
            "results_count": len(st["results"]),
            "not_found_count": st["not_found_count"],
            "error_count": st["error_count"],
            "paused": st.get("paused", False),
            "resume_at": st.get("resume_at"),
            "backoff_seconds": st.get("backoff_seconds", 0),
            "no_proxy_mode": _no_proxy_mode.is_set(),
            "banner_message": FAST_MODE_EXCEEDED_MESSAGE if _no_proxy_mode.is_set() else None,
            "elapsed_seconds": elapsed,
            "eta_seconds": eta,
            "rate_per_min": rate * 60 if rate else 0,
            "start_mc": st["start_mc"],
            "end_mc": st["end_mc"],
        })


@app.route("/api/results")
@login_required
def results():
    job_id = get_or_create_job_id()
    with jobs_lock:
        return jsonify(jobs[job_id]["results"])


@app.route("/api/download")
@login_required
def download():
    job_id = get_or_create_job_id()
    with jobs_lock:
        results_data = list(jobs[job_id]["results"])
    return _csv_response(results_data, "qualified_carriers.csv")


def _csv_response(rows, filename):
    output = io.StringIO()
    fieldnames = ["mc_number", "entity_type", "authority_status", "legal_name",
                  "dba_name", "phone", "power_units", "cargo_carried", "city", "state"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    mem = io.BytesIO(output.getvalue().encode("utf-8"))
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=filename)


# ---------------------------------------------------------------------------
# Saved MCs API
# ---------------------------------------------------------------------------
@app.route("/api/saved", methods=["GET"])
@login_required
def get_saved():
    return jsonify(db.get_saved(session["username"]))


@app.route("/api/saved", methods=["POST"])
@login_required
def add_saved():
    carrier = request.get_json(force=True)
    if not carrier.get("mc_number"):
        return jsonify({"error": "mc_number required"}), 400
    db.add_saved(session["username"], carrier)
    return jsonify({"ok": True})


@app.route("/api/saved/<mc_number>", methods=["DELETE"])
@login_required
def remove_saved(mc_number):
    db.remove_saved(session["username"], mc_number)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Notes API
# ---------------------------------------------------------------------------
@app.route("/api/notes", methods=["GET"])
@login_required
def get_notes():
    return jsonify(db.get_notes(session["username"]))


@app.route("/api/notes", methods=["POST"])
@login_required
def add_note():
    data = request.get_json(force=True)
    note = {
        "id": str(uuid.uuid4())[:8],
        "mc_number": data.get("mc_number", ""),
        "title": data.get("title", ""),
        "body": data.get("body", ""),
        "tags": data.get("tags", ""),
        "priority": data.get("priority", "Normal"),
        "reminder_date": data.get("reminder_date", ""),
        "created": time.strftime("%Y-%m-%d %H:%M"),
    }
    db.add_note(session["username"], note)
    return jsonify(note)


@app.route("/api/notes/<note_id>", methods=["DELETE"])
@login_required
def delete_note(note_id):
    db.delete_note(session["username"], note_id)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# History API
# ---------------------------------------------------------------------------
@app.route("/api/history", methods=["GET"])
@login_required
def get_history():
    return jsonify(db.get_history(session["username"]))


@app.route("/api/history/<scan_id>", methods=["DELETE"])
@login_required
def delete_history(scan_id):
    db.delete_history(session["username"], scan_id)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Settings / prefs API
# ---------------------------------------------------------------------------
@app.route("/api/settings", methods=["GET"])
@login_required
def get_settings():
    return jsonify(db.get_prefs(session["username"]))


@app.route("/api/settings", methods=["POST"])
@login_required
def save_settings():
    data = request.get_json(force=True)
    db.save_prefs(session["username"], data)
    return jsonify({"ok": True})


@app.route("/api/usage")
@login_required
def get_usage():
    username = session["username"]
    plan, plan_info = plan_info_for(username)
    usage = db.get_usage(username)
    return jsonify({
        "plan": plan,
        "plan_label": plan_info["label"],
        "price_pkr": plan_info["price_pkr"],
        "unlimited": plan_info["unlimited"],
        "total_limit": plan_info["total_limit"],
        "total_checked": usage["total_checked"],
        "daily_limit": plan_info["daily_limit"],
        "daily_checked": usage["daily_checked"],
    })


# ---------------------------------------------------------------------------
# Account (username / email / password) — used by the Settings page
# ---------------------------------------------------------------------------
@app.route("/api/account", methods=["POST"])
@login_required
def update_account():
    username = session["username"]
    user = db.get_user(username)
    data = request.get_json(force=True)

    current_password = data.get("current_password") or ""
    new_username = (data.get("new_username") or "").strip()
    new_email = (data.get("new_email") or "").strip().lower()
    new_password = data.get("new_password") or ""

    if not new_username and not new_email and not new_password:
        return jsonify({"error": "Nothing to update"}), 400

    # Current password is only required when changing the password itself.
    # Username/email changes don't need it.
    if new_password:
        if not current_password or not check_password_hash(user["password_hash"], current_password):
            return jsonify({"error": "Current password is incorrect"}), 403
        if len(new_password) < 6:
            return jsonify({"error": "New password must be at least 6 characters"}), 400
        db.update_password(username, new_password)

    if new_email:
        if "@" not in new_email or "." not in new_email:
            return jsonify({"error": "Enter a valid email"}), 400
        if db.email_taken(new_email, exclude_username=username):
            return jsonify({"error": "That email is already in use"}), 400
        db.update_email(username, new_email)

    if new_username and new_username != username:
        if db.username_taken(new_username, exclude_username=username):
            return jsonify({"error": "That username is already taken"}), 400
        db.rename_username(username, new_username)
        session["username"] = new_username
        username = new_username

    return jsonify({"ok": True, "username": username})


# ---------------------------------------------------------------------------
# Admin (visible only to accounts with is_admin=True, e.g. ka@gmail.com)
# ---------------------------------------------------------------------------
@app.route("/api/admin/users", methods=["GET"])
@admin_required
def admin_list_users():
    users = db.list_all_users()
    for u in users:
        u["created"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(u["created"]))
    return jsonify(users)


@app.route("/api/admin/users/<username>/plan", methods=["POST"])
@admin_required
def admin_change_plan(username):
    data = request.get_json(force=True)
    plan = data.get("plan")
    if plan not in PLAN_INFO:
        return jsonify({"error": "Invalid plan"}), 400
    if not db.get_user(username):
        return jsonify({"error": "User not found"}), 404
    db.set_user_plan(username, plan)
    return jsonify({"ok": True})


@app.route("/api/admin/users/<username>", methods=["DELETE"])
@admin_required
def admin_delete_user(username):
    if username == session.get("username"):
        return jsonify({"error": "You can't delete your own account from here"}), 400
    if not db.get_user(username):
        return jsonify({"error": "User not found"}), 404
    db.delete_user(username)
    return jsonify({"ok": True})


@app.route("/errors")
@login_required
def errors_page():
    return render_template("errors.html", **base_ctx())


@app.route("/api/errors")
@login_required
def get_errors():
    job_id = session.get("job_id")
    with jobs_lock:
        st = jobs.get(job_id) if job_id else None
        if not st:
            return jsonify([])
        error_entries = [e for e in st.get("log", []) if e.get("fetch_error")]
        return jsonify(error_entries)


if __name__ == "__main__":
    app.run(debug=False, port=5000, threaded=True)
