"""

db.py — Postgres (Supabase) persistence layer for MC Scout / K&A.

Migrated from SQLite to Postgres so data survives redeploys on platforms
(like Render's free tier, or serverless hosts) that have an ephemeral
filesystem. Supabase's free Postgres tier is permanent (no auto-expiry),
so this removes the need to pay for a persistent disk.

SETUP:
  1. pip install psycopg2-binary
  2. Create a free project at https://supabase.com
  3. In your Supabase project: Settings -> Database -> Connection string
     (use the "Connection pooling" URI, mode = Transaction, port 6543 —
     works better with short-lived connections than the direct 5432 one).
  4. Set it as an environment variable on your host:
         DATABASE_URL=postgresql://postgres:<password>@<host>:6543/postgres
  5. That's it — init_db() below creates all tables automatically on
     first run, and re-running it is always safe (CREATE TABLE IF NOT EXISTS).

Every public function name/signature below is unchanged from the old
SQLite version, so app.py does not need any changes.
"""
import json
import os
import threading
import time

import psycopg2
import psycopg2.extras
from werkzeug.security import generate_password_hash

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    # Defensive: trim accidental whitespace/newlines that can sneak in when
    # copy-pasting the connection string into a host's env var UI — a
    # trailing "\n" turns "postgres" into "postgres\n" and Postgres will
    # report that as "database postgres\n does not exist".
    DATABASE_URL = DATABASE_URL.strip()

_local = threading.local()


def get_conn():
    """One connection per thread (Flask's threaded=True spins up one
    worker thread per request), same pattern as the old SQLite version."""
    if not hasattr(_local, "conn") or _local.conn.closed:
        if not DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL is not set. Add your Supabase Postgres "
                "connection string as the DATABASE_URL environment variable."
            )
        _local.conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        _local.conn.autocommit = False
    return _local.conn


def init_db():
    conn = get_conn()

    # Kill any lingering "idle in transaction" sessions on this database
    # before doing schema work. If a previous deploy crashed mid-transaction
    # (e.g. Render killed the process before commit), that stale connection
    # can hold a lock forever and block DDL statements (ALTER TABLE etc.) on
    # every subsequent deploy until it times out. This clears that safely —
    # it only ever affects OTHER idle-in-transaction sessions, never active
    # queries, and never this connection itself.
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT pg_terminate_backend(pid)
                   FROM pg_stat_activity
                   WHERE datname = current_database()
                     AND pid <> pg_backend_pid()
                     AND state = 'idle in transaction'
                     AND state_change < now() - interval '30 seconds'"""
            )
        conn.commit()
    except Exception:
        # Not critical — if this fails (e.g. insufficient privilege on some
        # hosts), just carry on with schema setup as normal.
        conn.rollback()

    # Each table gets its own short statement — if one has to wait on a
    # lock, only that one line is at risk of timing out, not the entire
    # 80-line block, and every successfully-created table commits right
    # away instead of everything staying open in one long transaction.
    table_statements = [
        """CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            plan TEXT NOT NULL DEFAULT 'demo',
            created DOUBLE PRECISION NOT NULL,
            is_admin BOOLEAN NOT NULL DEFAULT FALSE
        )""",
        """CREATE TABLE IF NOT EXISTS saved_mcs (
            username TEXT NOT NULL,
            mc_number TEXT NOT NULL,
            data TEXT NOT NULL,
            saved_date TEXT NOT NULL,
            PRIMARY KEY (username, mc_number)
        )""",
        """CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            mc_number TEXT,
            title TEXT,
            body TEXT,
            tags TEXT,
            priority TEXT,
            reminder_date TEXT,
            created TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS history (
            scan_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            start_mc INTEGER,
            end_mc INTEGER,
            total_checked INTEGER,
            qualified INTEGER,
            not_found INTEGER,
            errors INTEGER,
            duration_seconds INTEGER,
            date TEXT,
            status TEXT,
            cargo_tally TEXT,
            state_tally TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS prefs (
            username TEXT PRIMARY KEY,
            min_power_units INTEGER DEFAULT 0,
            max_power_units INTEGER DEFAULT 6,
            cargo_general INTEGER DEFAULT 1,
            cargo_reefer INTEGER DEFAULT 1,
            cargo_fresh INTEGER DEFAULT 1
        )""",
        """CREATE TABLE IF NOT EXISTS usage (
            username TEXT PRIMARY KEY,
            total_checked INTEGER DEFAULT 0,
            daily_checked INTEGER DEFAULT 0,
            daily_date TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS signup_otps (
            email TEXT PRIMARY KEY,
            otp_hash TEXT NOT NULL,
            expires_at DOUBLE PRECISION NOT NULL,
            attempts INTEGER DEFAULT 0,
            last_sent DOUBLE PRECISION NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS reset_otps (
            username TEXT PRIMARY KEY,
            otp_hash TEXT NOT NULL,
            expires_at DOUBLE PRECISION NOT NULL,
            attempts INTEGER DEFAULT 0,
            last_sent DOUBLE PRECISION NOT NULL
        )""",
    ]

    for stmt in table_statements:
        with conn.cursor() as cur:
            cur.execute(stmt)
        conn.commit()

    # ALTER TABLE needs an exclusive lock on `users`, which is the one
    # statement most likely to hang if something else is still touching
    # that table. Give it its own short lock_timeout so a stuck deploy
    # fails in a few seconds with a clear log line instead of hanging
    # until Postgres's (much longer) statement_timeout kills it — and if
    # it does fail, don't crash the whole app over a column that, on any
    # database created after this feature shipped, already exists anyway.
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '5s'")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db.init_db] Skipped is_admin column migration (will retry next start): {e}")

    _seed_default_users()
    # Make sure the K&A admin account is always marked admin, even on a
    # database that already existed before is_admin was introduced.
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET is_admin = TRUE WHERE email = %s", ("ka@gmail.com",))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db.init_db] Skipped admin-flag backfill (will retry next start): {e}")


def _seed_default_users():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM users")
        if cur.fetchone()["c"] > 0:
            return
        defaults = [
            ("Demo", "demo@gmail.com", generate_password_hash("demo@123"), "demo", False),
           ("Demo", "demo2@gmail.com", generate_password_hash("demo@1234"), "demo", False),

            ("Scott", "ka@gmail.com", generate_password_hash("sublime@2026"), "lifetime", True),
        ]
        for username, email, pw_hash, plan, is_admin in defaults:
            cur.execute(
                "INSERT INTO users (username, email, password_hash, plan, created, is_admin) VALUES (%s, %s, %s, %s, %s, %s)",
                (username, email, pw_hash, plan, time.time(), is_admin),
            )
    conn.commit()


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------
def find_user_by_identifier(identifier):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM users WHERE lower(username) = %s OR lower(email) = %s",
            (identifier.lower(), identifier.lower()),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_user(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    return dict(row) if row else None


def create_user(username, email, password, plan="demo"):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (username, email, password_hash, plan, created) VALUES (%s, %s, %s, %s, %s)",
            (username, email, generate_password_hash(password), plan, time.time()),
        )
    conn.commit()


def set_user_plan(username, plan):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET plan = %s WHERE username = %s", (plan, username))
    conn.commit()


def email_taken(email, exclude_username=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT username FROM users WHERE lower(email) = %s", (email.lower(),))
        row = cur.fetchone()
    return bool(row) and row["username"] != exclude_username


def username_taken(new_username, exclude_username=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT username FROM users WHERE lower(username) = %s", (new_username.lower(),))
        row = cur.fetchone()
    return bool(row) and row["username"] != exclude_username


def update_email(username, new_email):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET email = %s WHERE username = %s", (new_email, username))
    conn.commit()


def update_password(username, new_password):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET password_hash = %s WHERE username = %s",
            (generate_password_hash(new_password), username),
        )
    conn.commit()


def rename_username(old_username, new_username):
    """Renames a user and moves all their related rows (saved MCs, notes,
    history, prefs, usage) over to the new username, since username is the
    primary key / foreign key used everywhere else in this schema."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET username = %s WHERE username = %s", (new_username, old_username))
        cur.execute("UPDATE saved_mcs SET username = %s WHERE username = %s", (new_username, old_username))
        cur.execute("UPDATE notes SET username = %s WHERE username = %s", (new_username, old_username))
        cur.execute("UPDATE history SET username = %s WHERE username = %s", (new_username, old_username))
        cur.execute("UPDATE prefs SET username = %s WHERE username = %s", (new_username, old_username))
        cur.execute("UPDATE usage SET username = %s WHERE username = %s", (new_username, old_username))
    conn.commit()


def ensure_user_rows(username):
    """Make sure prefs/usage rows exist for a user (first-touch init)."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO prefs (username) VALUES (%s) ON CONFLICT (username) DO NOTHING", (username,)
        )
        cur.execute(
            "INSERT INTO usage (username, daily_date) VALUES (%s, %s) ON CONFLICT (username) DO NOTHING",
            (username, time.strftime("%Y-%m-%d")),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------
def is_admin(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT is_admin FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    return bool(row and row["is_admin"])


def list_all_users():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT username, email, plan, created, is_admin FROM users ORDER BY created DESC"
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def delete_user(username):
    """Deletes a user and every row tied to them across the schema."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM saved_mcs WHERE username = %s", (username,))
        cur.execute("DELETE FROM notes WHERE username = %s", (username,))
        cur.execute("DELETE FROM history WHERE username = %s", (username,))
        cur.execute("DELETE FROM prefs WHERE username = %s", (username,))
        cur.execute("DELETE FROM usage WHERE username = %s", (username,))
        cur.execute("DELETE FROM users WHERE username = %s", (username,))
    conn.commit()


# ---------------------------------------------------------------------------
# Saved MCs
# ---------------------------------------------------------------------------
def get_saved(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT mc_number, data, saved_date FROM saved_mcs WHERE username = %s", (username,)
        )
        rows = cur.fetchall()
    out = []
    for r in rows:
        d = json.loads(r["data"])
        d["saved_date"] = r["saved_date"]
        out.append(d)
    return out


def add_saved(username, carrier):
    mc = str(carrier.get("mc_number"))
    saved_date = time.strftime("%Y-%m-%d")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO saved_mcs (username, mc_number, data, saved_date) VALUES (%s, %s, %s, %s)
               ON CONFLICT (username, mc_number) DO UPDATE SET data = EXCLUDED.data, saved_date = EXCLUDED.saved_date""",
            (username, mc, json.dumps(carrier), saved_date),
        )
    conn.commit()


def remove_saved(username, mc_number):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM saved_mcs WHERE username = %s AND mc_number = %s", (username, str(mc_number))
        )
    conn.commit()


def get_saved_one(username, mc_number):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT data FROM saved_mcs WHERE username = %s AND mc_number = %s",
            (username, str(mc_number)),
        )
        row = cur.fetchone()
    return json.loads(row["data"]) if row else None


# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------
def get_notes(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM notes WHERE username = %s ORDER BY created DESC", (username,)
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def add_note(username, note):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO notes (id, username, mc_number, title, body, tags, priority, reminder_date, created)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                note["id"], username, note.get("mc_number", ""), note.get("title", ""),
                note.get("body", ""), note.get("tags", ""), note.get("priority", "Normal"),
                note.get("reminder_date", ""), note["created"],
            ),
        )
    conn.commit()


def delete_note(username, note_id):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM notes WHERE username = %s AND id = %s", (username, note_id))
    conn.commit()


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------
def get_history(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM history WHERE username = %s ORDER BY date DESC LIMIT 100", (username,)
        )
        rows = cur.fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["cargo_tally"] = json.loads(d["cargo_tally"] or "{}")
        d["state_tally"] = json.loads(d["state_tally"] or "{}")
        out.append(d)
    return out


def add_history(username, entry):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO history (scan_id, username, start_mc, end_mc, total_checked, qualified,
               not_found, errors, duration_seconds, date, status, cargo_tally, state_tally)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                entry["scan_id"], username, entry["start_mc"], entry["end_mc"],
                entry["total_checked"], entry["qualified"], entry["not_found"], entry["errors"],
                entry["duration_seconds"], entry["date"], entry["status"],
                json.dumps(entry.get("cargo_tally", {})), json.dumps(entry.get("state_tally", {})),
            ),
        )
        # keep only latest 100 rows per user
        cur.execute(
            """DELETE FROM history WHERE username = %s AND scan_id NOT IN (
                   SELECT scan_id FROM history WHERE username = %s ORDER BY date DESC LIMIT 100
               )""",
            (username, username),
        )
    conn.commit()


def delete_history(username, scan_id):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM history WHERE username = %s AND scan_id = %s", (username, scan_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Prefs
# ---------------------------------------------------------------------------
def get_prefs(username):
    # ensure_user_rows() is no longer called here on every read — it's
    # called once at login instead (see the /login route in app.py). The
    # prefs/usage rows only need to exist once per user, ever; re-checking
    # on every single call was two unnecessary round-trips per request.
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM prefs WHERE username = %s", (username,))
        row = cur.fetchone()
    d = dict(row)
    d.pop("username")
    d["cargo_general"] = bool(d["cargo_general"])
    d["cargo_reefer"] = bool(d["cargo_reefer"])
    d["cargo_fresh"] = bool(d["cargo_fresh"])
    return d


def save_prefs(username, data):
    conn = get_conn()
    current = get_prefs(username)
    for key in ["min_power_units", "max_power_units", "cargo_general", "cargo_reefer", "cargo_fresh"]:
        if key in data:
            current[key] = data[key]
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE prefs SET min_power_units=%s, max_power_units=%s, cargo_general=%s,
               cargo_reefer=%s, cargo_fresh=%s WHERE username=%s""",
            (
                current["min_power_units"], current["max_power_units"],
                int(bool(current["cargo_general"])), int(bool(current["cargo_reefer"])),
                int(bool(current["cargo_fresh"])), username,
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
def get_usage(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM usage WHERE username = %s", (username,))
        row = cur.fetchone()
    usage = dict(row)
    today = time.strftime("%Y-%m-%d")
    if usage["daily_date"] != today:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE usage SET daily_checked = 0, daily_date = %s WHERE username = %s",
                (today, username),
            )
        conn.commit()
        usage["daily_checked"] = 0
        usage["daily_date"] = today
    return usage


def add_usage(username, count):
    usage = get_usage(username)  # ensures daily reset done first
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE usage SET total_checked = total_checked + %s, daily_checked = daily_checked + %s WHERE username = %s",
            (count, count, username),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Signup OTP (email verification for new signups, sent via Resend)
# ---------------------------------------------------------------------------
def upsert_otp(email, otp_hash, expires_at, last_sent):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO signup_otps (email, otp_hash, expires_at, attempts, last_sent)
               VALUES (%s, %s, %s, 0, %s)
               ON CONFLICT (email) DO UPDATE SET
                   otp_hash = EXCLUDED.otp_hash,
                   expires_at = EXCLUDED.expires_at,
                   attempts = 0,
                   last_sent = EXCLUDED.last_sent""",
            (email, otp_hash, expires_at, last_sent),
        )
    conn.commit()


def get_otp(email):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM signup_otps WHERE email = %s", (email,))
        row = cur.fetchone()
    return dict(row) if row else None


def increment_otp_attempts(email):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE signup_otps SET attempts = attempts + 1 WHERE email = %s", (email,)
        )
    conn.commit()


def delete_otp(email):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM signup_otps WHERE email = %s", (email,))
    conn.commit()


# ---------------------------------------------------------------------------
# Password reset OTP (sent to the admin, same pattern as signup)
# ---------------------------------------------------------------------------
def upsert_reset_otp(username, otp_hash, expires_at, last_sent):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO reset_otps (username, otp_hash, expires_at, attempts, last_sent)
               VALUES (%s, %s, %s, 0, %s)
               ON CONFLICT (username) DO UPDATE SET
                   otp_hash = EXCLUDED.otp_hash,
                   expires_at = EXCLUDED.expires_at,
                   attempts = 0,
                   last_sent = EXCLUDED.last_sent""",
            (username, otp_hash, expires_at, last_sent),
        )
    conn.commit()


def get_reset_otp(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM reset_otps WHERE username = %s", (username,))
        row = cur.fetchone()
    return dict(row) if row else None


def increment_reset_otp_attempts(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE reset_otps SET attempts = attempts + 1 WHERE username = %s", (username,)
        )
    conn.commit()


def delete_reset_otp(username):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM reset_otps WHERE username = %s", (username,))
    conn.commit()
