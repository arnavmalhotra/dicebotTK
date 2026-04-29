"""
SQLite database layer for DiceBot.
Single file: ~/.dicebot-ticketkings/dicebot.db
Thread-safe with WAL mode for concurrent reads.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import threading
from datetime import datetime, UTC

DB_DIR = os.path.join(os.path.expanduser("~"), ".dicebot-ticketkings")
DB_PATH = os.path.join(DB_DIR, "dicebot.db")
SESSION_MAX_AGE = 6 * 24 * 3600  # 6 days
SESSION_WARN_AGE = 5 * 24 * 3600  # 5 days — "expiring soon"

_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables if they don't exist."""
    with _lock:
        conn = _connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT NOT NULL UNIQUE,
                email TEXT DEFAULT '',
                card_number TEXT DEFAULT '',
                card_exp_month TEXT DEFAULT '',
                card_exp_year TEXT DEFAULT '',
                card_cvv TEXT DEFAULT '',
                billing_name TEXT DEFAULT '',
                billing_email TEXT DEFAULT '',
                billing_phone TEXT DEFAULT '',
                billing_postal TEXT DEFAULT '',
                billing_country TEXT DEFAULT 'US',
                proxy TEXT DEFAULT '',
                aycd_key TEXT DEFAULT '',
                aycd_email TEXT DEFAULT '',
                imap_email TEXT DEFAULT '',
                imap_password TEXT DEFAULT '',
                imap_host TEXT DEFAULT '',
                group_id INTEGER REFERENCES groups(id) ON DELETE SET NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sessions (
                account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                bearer_token TEXT NOT NULL,
                device_id TEXT DEFAULT '',
                saved_at REAL NOT NULL,
                phone TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                event_url TEXT DEFAULT '',
                min_price REAL,
                max_price REAL,
                presale_code TEXT DEFAULT '',
                ticket_tier TEXT DEFAULT '',
                quantity INTEGER DEFAULT 1,
                mode TEXT DEFAULT 'auto',
                scheduled_at TEXT DEFAULT '',
                scheduled_tz TEXT DEFAULT '',
                status TEXT DEFAULT 'idle',
                session_id TEXT DEFAULT '',
                last_error TEXT DEFAULT '',
                ephemeral INTEGER DEFAULT 0,
                updated_at REAL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS inventory_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_key TEXT NOT NULL UNIQUE,
                purchase_id TEXT DEFAULT '',
                account_id INTEGER REFERENCES accounts(id) ON DELETE SET NULL,
                account_name TEXT DEFAULT '',
                account_phone TEXT DEFAULT '',
                event_url TEXT DEFAULT '',
                event_name TEXT DEFAULT '',
                event_date TEXT DEFAULT '',
                event_venue TEXT DEFAULT '',
                ticket_type_id TEXT DEFAULT '',
                ticket_name TEXT DEFAULT '',
                ticket_currency TEXT DEFAULT 'USD',
                ticket_price REAL DEFAULT 0,
                quantity INTEGER DEFAULT 1,
                total_price REAL DEFAULT 0,
                purchase_status TEXT DEFAULT 'purchased',
                purchased_at TEXT DEFAULT (datetime('now'))
            );

            INSERT OR IGNORE INTO groups (name) VALUES ('Default');
        """)
        # Idempotent migrations for DBs created before these columns existed.
        for col in ("imap_email", "imap_password", "imap_host", "aycd_email"):
            try:
                conn.execute(f"ALTER TABLE accounts ADD COLUMN {col} TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # column already exists
        for col in ("scheduled_at", "scheduled_tz"):
            try:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass
        try:
            conn.execute("ALTER TABLE tasks ADD COLUMN ephemeral INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()


# ── Groups ─────────────────────────────────────────────────────────────────

def get_groups() -> list[dict]:
    conn = _connect()
    rows = conn.execute("SELECT * FROM groups ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_group(name: str) -> int:
    with _lock:
        conn = _connect()
        cur = conn.execute("INSERT INTO groups (name) VALUES (?)", (name.strip(),))
        conn.commit()
        gid = cur.lastrowid
        conn.close()
        return gid


def delete_group(group_id: int) -> None:
    with _lock:
        conn = _connect()
        conn.execute("UPDATE accounts SET group_id = NULL WHERE group_id = ?", (group_id,))
        conn.execute("DELETE FROM groups WHERE id = ?", (group_id,))
        conn.commit()
        conn.close()


def rename_group(group_id: int, name: str) -> None:
    with _lock:
        conn = _connect()
        conn.execute("UPDATE groups SET name = ? WHERE id = ?", (name.strip(), group_id))
        conn.commit()
        conn.close()


# ── Accounts ──────────────────────────────────────────────────────────────

def get_accounts(group_id: int | None = None) -> list[dict]:
    conn = _connect()
    if group_id is not None:
        rows = conn.execute("""
            SELECT a.*, g.name as group_name,
                   s.bearer_token, s.saved_at as session_saved_at
            FROM accounts a
            LEFT JOIN groups g ON a.group_id = g.id
            LEFT JOIN sessions s ON s.account_id = a.id
            WHERE a.group_id = ?
            ORDER BY a.name
        """, (group_id,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT a.*, g.name as group_name,
                   s.bearer_token, s.saved_at as session_saved_at
            FROM accounts a
            LEFT JOIN groups g ON a.group_id = g.id
            LEFT JOIN sessions s ON s.account_id = a.id
            ORDER BY a.name
        """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_account(account_id: int) -> dict | None:
    conn = _connect()
    row = conn.execute("""
        SELECT a.*, g.name as group_name,
               s.bearer_token, s.saved_at as session_saved_at
        FROM accounts a
        LEFT JOIN groups g ON a.group_id = g.id
        LEFT JOIN sessions s ON s.account_id = a.id
        WHERE a.id = ?
    """, (account_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def add_account(
    name: str, phone: str, email: str = "",
    card_number: str = "", card_exp_month: str = "", card_exp_year: str = "", card_cvv: str = "",
    billing_name: str = "", billing_email: str = "", billing_phone: str = "",
    billing_postal: str = "", billing_country: str = "US",
    proxy: str = "", aycd_key: str = "", aycd_email: str = "",
    imap_email: str = "", imap_password: str = "", imap_host: str = "",
    group_id: int | None = None,
) -> int:
    with _lock:
        conn = _connect()
        cur = conn.execute("""
            INSERT OR REPLACE INTO accounts
            (name, phone, email, card_number, card_exp_month, card_exp_year, card_cvv,
             billing_name, billing_email, billing_phone, billing_postal, billing_country,
             proxy, aycd_key, aycd_email, imap_email, imap_password, imap_host, group_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, phone, email, card_number, card_exp_month, card_exp_year, card_cvv,
              billing_name, billing_email, billing_phone, billing_postal, billing_country,
              proxy, aycd_key, aycd_email, imap_email, imap_password, imap_host, group_id))
        conn.commit()
        aid = cur.lastrowid
        conn.close()
        return aid


def update_account(account_id: int, **fields) -> None:
    """Update account fields by id."""
    if not fields: return
    allowed = {"name","phone","email","card_number","card_exp_month","card_exp_year","card_cvv",
                "billing_name","billing_email","billing_phone","billing_postal","billing_country",
                "proxy","aycd_key","aycd_email","imap_email","imap_password","imap_host","group_id"}
    sets = [f"{k} = ?" for k in fields if k in allowed]
    vals = [fields[k] for k in fields if k in allowed]
    if not sets: return
    vals.append(account_id)
    with _lock:
        conn = _connect()
        conn.execute(f"UPDATE accounts SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
        conn.close()


def delete_account(account_id: int) -> None:
    with _lock:
        conn = _connect()
        conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        conn.commit()
        conn.close()


def assign_group(account_ids: list[int], group_id: int | None) -> None:
    with _lock:
        conn = _connect()
        conn.executemany(
            "UPDATE accounts SET group_id = ? WHERE id = ?",
            [(group_id, aid) for aid in account_ids]
        )
        conn.commit()
        conn.close()


_COUNTRY_MAP = {
    "united states": "US", "usa": "US", "canada": "CA",
    "united kingdom": "GB", "uk": "GB", "germany": "DE",
    "france": "FR", "netherlands": "NL", "australia": "AU",
    "spain": "ES", "italy": "IT", "india": "IN", "brazil": "BR",
    "mexico": "MX", "ireland": "IE", "switzerland": "CH",
}


def _col(row, *keys):
    for k in keys:
        if k in row and row[k]:
            return str(row[k]).strip()
    return ""


def _country(val):
    v = val.strip().lower()
    return _COUNTRY_MAP.get(v, val.strip().upper() if len(val.strip()) == 2 else val.strip()) or "US"


def _account_exists_by_phone(phone: str) -> bool:
    conn = _connect()
    row = conn.execute("SELECT 1 FROM accounts WHERE phone = ? LIMIT 1", (phone,)).fetchone()
    conn.close()
    return row is not None


def get_account_by_email(email: str) -> dict | None:
    """Lookup an account by email (case-insensitive). Returns None if not found."""
    if not email:
        return None
    conn = _connect()
    row = conn.execute("""
        SELECT a.*, s.saved_at as session_saved_at
        FROM accounts a
        LEFT JOIN sessions s ON s.account_id = a.id
        WHERE lower(a.email) = lower(?)
        LIMIT 1
    """, (email.strip(),)).fetchone()
    conn.close()
    return dict(row) if row else None


def _import_rows(rows: list[dict], group_id: int | None = None) -> dict:
    """Import a list of dicts (from CSV or XLSX).

    Returns {'count': N, 'created': C, 'updated': U, 'skipped': S, 'log': [...]}.
    Each log entry is {'row': int, 'outcome': 'created'|'updated'|'skipped', 'phone': str, 'email': str, 'reason': str}.
    """
    log: list[dict] = []
    created = updated = skipped = 0

    if not rows:
        return {"count": 0, "created": 0, "updated": 0, "skipped": 0, "log": [
            {"row": 0, "outcome": "skipped", "phone": "", "email": "", "reason": "file had no data rows"}
        ]}

    for idx, row in enumerate(rows, start=1):
        cleaned = {k.strip().lower().replace(" ", "").replace("_", ""): str(v).strip() for k, v in row.items() if v is not None and str(v).strip() != ""}
        phone = _col(cleaned, "phone", "phonenumber")
        email = _col(cleaned, "email", "diceemail", "billingemail")

        if not phone:
            skipped += 1
            log.append({
                "row": idx, "outcome": "skipped", "phone": "", "email": email,
                "reason": f"no phone column (saw headers: {', '.join(sorted(cleaned.keys())) or 'none'})"
            })
            continue

        try:
            was_existing = _account_exists_by_phone(phone)
            add_account(
                name=_col(cleaned, "profilename", "profile", "account") or (email.split("@")[0] if email else "") or phone,
                phone=phone, email=email,
                card_number=_col(cleaned, "cardnumber", "card"),
                card_exp_month=_col(cleaned, "cardexpmonth", "expmonth"),
                card_exp_year=_col(cleaned, "cardexpyear", "expyear"),
                card_cvv=_col(cleaned, "cvv", "cvc"),
                billing_name=_col(cleaned, "billingname", "name", "fullname"),
                billing_email=_col(cleaned, "billingemail") or email,
                billing_phone=_col(cleaned, "billingphone") or phone,
                billing_postal=_col(
                    cleaned,
                    "billingpostal",
                    "billingpostalcode",
                    "billingzip",
                    "postal",
                    "zip",
                    "zipcode",
                    "postalcode",
                ),
                billing_country=_country(_col(cleaned, "country", "billingcountry") or "US"),
                proxy=_col(cleaned, "proxy"),
                aycd_key=_col(cleaned, "aycdkey", "aycd", "aycdapikey"),
                aycd_email=_col(cleaned, "aycdemail", "aycdmail", "aycdlookupemail"),
                imap_email=_col(cleaned, "imapemail", "imapuser", "imapusername", "imaplogin"),
                imap_password=_col(cleaned, "imappassword", "imappass", "imapapppassword", "imaptoken"),
                imap_host=_col(cleaned, "imaphost", "imapserver"),
                group_id=group_id,
            )
        except Exception as exc:
            skipped += 1
            log.append({
                "row": idx, "outcome": "skipped", "phone": phone, "email": email,
                "reason": f"db error: {exc}"
            })
            continue

        if was_existing:
            updated += 1
            log.append({"row": idx, "outcome": "updated", "phone": phone, "email": email, "reason": "existing phone — row replaced"})
        else:
            created += 1
            log.append({"row": idx, "outcome": "created", "phone": phone, "email": email, "reason": ""})

    return {"count": created + updated, "created": created, "updated": updated, "skipped": skipped, "log": log}


def import_file(file_path: str, group_id: int | None = None) -> dict:
    """Import accounts from CSV or XLSX. Returns {count, created, updated, skipped, log}."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".xlsx", ".xls"):
        rows = _read_xlsx_rows(file_path)
    else:
        rows = _read_csv_rows(file_path)
    return _import_rows(rows, group_id)


def _read_csv_rows(csv_path: str) -> list[dict]:
    import csv as csv_mod
    # Excel on Windows saves "CSV (Comma delimited)" as cp1252, not UTF-8.
    # latin-1 is byte-identity so the final fallback always succeeds.
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with open(csv_path, newline="", encoding=encoding) as f:
                reader = csv_mod.DictReader(f)
                return [row for row in reader]
        except UnicodeDecodeError:
            continue
    return []


def _read_xlsx_rows(xlsx_path: str) -> list[dict]:
    from openpyxl import load_workbook
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h or "").strip() for h in next(rows_iter)]
    rows = []
    for values in rows_iter:
        row = {}
        for i, val in enumerate(values):
            if i < len(headers) and val is not None:
                row[headers[i]] = str(val).strip()
        if any(row.values()):
            rows.append(row)
    wb.close()
    return rows


# Keep old names as aliases
import_csv = import_file
_import_csv = _read_csv_rows
_import_xlsx = _read_xlsx_rows


# ── Task CSV import ───────────────────────────────────────────────────────

def _num(v):
    if v in ("", None):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def import_tasks_file(file_path: str) -> dict:
    """Import tasks from CSV/XLSX.

    Expected columns: email, eventURL, min_price, max_price, qty, code.
    For each row: look up the account by email, require a warm session, then create a task.
    Returns {'count', 'created', 'skipped', 'log': [{row, outcome, email, event_url, reason}]}.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".xlsx", ".xls"):
        rows = _read_xlsx_rows(file_path)
    else:
        rows = _read_csv_rows(file_path)

    log: list[dict] = []
    created = skipped = 0

    if not rows:
        return {"count": 0, "created": 0, "skipped": 0, "log": [
            {"row": 0, "outcome": "skipped", "email": "", "event_url": "", "reason": "file had no data rows"}
        ]}

    for idx, row in enumerate(rows, start=1):
        cleaned = {k.strip().lower().replace(" ", "").replace("_", ""): str(v).strip() for k, v in row.items() if v is not None and str(v).strip() != ""}

        email = _col(cleaned, "email", "diceemail", "accountemail")
        event_url = _col(cleaned, "eventurl", "event", "url")

        if not email:
            skipped += 1
            log.append({"row": idx, "outcome": "skipped", "email": "", "event_url": event_url, "reason": "row has no email"})
            continue
        if not event_url:
            skipped += 1
            log.append({"row": idx, "outcome": "skipped", "email": email, "event_url": "", "reason": "row has no eventURL"})
            continue

        account = get_account_by_email(email)
        if not account:
            skipped += 1
            log.append({"row": idx, "outcome": "skipped", "email": email, "event_url": event_url, "reason": "no account matches this email"})
            continue

        saved_at = account.get("session_saved_at")
        if not saved_at or (time.time() - saved_at) >= SESSION_MAX_AGE:
            skipped += 1
            reason = "account has no session — run auth farm first" if not saved_at else "session expired — re-run auth"
            log.append({"row": idx, "outcome": "skipped", "email": email, "event_url": event_url, "reason": reason})
            continue

        try:
            qty_raw = _col(cleaned, "qty", "quantity")
            qty = int(qty_raw) if qty_raw else 1
        except ValueError:
            qty = 1

        try:
            create_task(
                account_id=int(account["id"]),
                event_url=event_url,
                min_price=_num(_col(cleaned, "minprice", "min")),
                max_price=_num(_col(cleaned, "maxprice", "max")),
                presale_code=_col(cleaned, "code", "presalecode", "accesscode"),
                ticket_tier=_col(cleaned, "tier", "tickettier"),
                quantity=qty,
                mode=_col(cleaned, "mode") or "auto",
            )
        except Exception as exc:
            skipped += 1
            log.append({"row": idx, "outcome": "skipped", "email": email, "event_url": event_url, "reason": f"db error: {exc}"})
            continue

        created += 1
        log.append({"row": idx, "outcome": "created", "email": email, "event_url": event_url, "reason": ""})

    return {"count": created, "created": created, "skipped": skipped, "log": log}


# ── Sessions ──────────────────────────────────────────────────────────────

def get_session(account_id: int) -> dict | None:
    conn = _connect()
    row = conn.execute("SELECT * FROM sessions WHERE account_id = ?", (account_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def save_session(account_id: int, bearer_token: str, phone: str = "", device_id: str = "") -> None:
    with _lock:
        conn = _connect()
        conn.execute("""
            INSERT OR REPLACE INTO sessions (account_id, bearer_token, device_id, saved_at, phone)
            VALUES (?, ?, ?, ?, ?)
        """, (account_id, bearer_token, device_id, time.time(), phone))
        conn.commit()
        conn.close()


def session_status(saved_at: float | None) -> str:
    """Returns 'active', 'expiring', 'expired', or 'none'."""
    if saved_at is None:
        return "none"
    age = time.time() - saved_at
    if age > SESSION_MAX_AGE:
        return "expired"
    if age > SESSION_WARN_AGE:
        return "expiring"
    return "active"


def session_expires_in(saved_at: float | None) -> str:
    """Human-readable time until session expires."""
    if saved_at is None:
        return "—"
    remaining = SESSION_MAX_AGE - (time.time() - saved_at)
    if remaining <= 0:
        return "expired"
    days = int(remaining // 86400)
    hours = int((remaining % 86400) // 3600)
    if days > 0:
        return f"{days}d {hours}h"
    return f"{hours}h"


def get_accounts_needing_auth() -> list[dict]:
    """Get accounts with no session or expired/expiring session."""
    conn = _connect()
    rows = conn.execute("""
        SELECT a.*, g.name as group_name,
               s.bearer_token, s.saved_at as session_saved_at
        FROM accounts a
        LEFT JOIN groups g ON a.group_id = g.id
        LEFT JOIN sessions s ON s.account_id = a.id
        WHERE s.account_id IS NULL
           OR (? - s.saved_at) > ?
        ORDER BY s.saved_at ASC NULLS FIRST
    """, (time.time(), SESSION_WARN_AGE)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_accounts_with_valid_session(group_id: int | None = None) -> list[dict]:
    """Get accounts with active sessions, optionally filtered by group."""
    conn = _connect()
    if group_id is not None:
        rows = conn.execute("""
            SELECT a.*, g.name as group_name,
                   s.bearer_token, s.saved_at as session_saved_at
            FROM accounts a
            LEFT JOIN groups g ON a.group_id = g.id
            INNER JOIN sessions s ON s.account_id = a.id
            WHERE a.group_id = ?
              AND (? - s.saved_at) < ?
            ORDER BY a.name
        """, (group_id, time.time(), SESSION_MAX_AGE)).fetchall()
    else:
        rows = conn.execute("""
            SELECT a.*, g.name as group_name,
                   s.bearer_token, s.saved_at as session_saved_at
            FROM accounts a
            LEFT JOIN groups g ON a.group_id = g.id
            INNER JOIN sessions s ON s.account_id = a.id
            WHERE (? - s.saved_at) < ?
            ORDER BY a.name
        """, (time.time(), SESSION_MAX_AGE)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Tasks ─────────────────────────────────────────────────────────────────

_TASK_FIELDS = {"event_url","min_price","max_price","presale_code","ticket_tier","quantity","mode","scheduled_at","scheduled_tz","ephemeral"}


def get_tasks() -> list[dict]:
    """Persistent (non-ephemeral) tasks only — what the Tasks page shows."""
    conn = _connect()
    rows = conn.execute("""
        SELECT t.*, a.phone as account_phone, a.email as account_email,
               a.group_id as account_group_id, g.name as group_name,
               s.saved_at as session_saved_at
        FROM tasks t
        INNER JOIN accounts a ON a.id = t.account_id
        LEFT JOIN groups g ON a.group_id = g.id
        LEFT JOIN sessions s ON s.account_id = a.id
        WHERE COALESCE(t.ephemeral, 0) = 0
        ORDER BY t.id DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def purge_ephemeral_tasks() -> int:
    """Delete every ephemeral task row. Call on worker boot so stale quick-runs
    from a prior crash don't linger."""
    with _lock:
        conn = _connect()
        cur = conn.execute("DELETE FROM tasks WHERE COALESCE(ephemeral, 0) = 1")
        conn.commit()
        n = cur.rowcount
        conn.close()
        return n


def get_task(task_id: int) -> dict | None:
    conn = _connect()
    row = conn.execute("""
        SELECT t.*, a.phone as account_phone, a.email as account_email
        FROM tasks t
        INNER JOIN accounts a ON a.id = t.account_id
        WHERE t.id = ?
    """, (task_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_task(account_id: int, **fields) -> int:
    clean = {k: fields.get(k) for k in _TASK_FIELDS}
    with _lock:
        conn = _connect()
        cur = conn.execute("""
            INSERT INTO tasks
            (account_id, event_url, min_price, max_price, presale_code, ticket_tier,
             quantity, mode, scheduled_at, scheduled_tz, ephemeral, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            account_id,
            clean.get("event_url") or "",
            clean.get("min_price"),
            clean.get("max_price"),
            clean.get("presale_code") or "",
            clean.get("ticket_tier") or "",
            int(clean.get("quantity") or 1),
            clean.get("mode") or "auto",
            clean.get("scheduled_at") or "",
            clean.get("scheduled_tz") or "",
            1 if clean.get("ephemeral") else 0,
            time.time(),
        ))
        conn.commit()
        tid = cur.lastrowid
        conn.close()
        return tid


def update_task(task_id: int, **fields) -> None:
    sets, vals = [], []
    for k, v in fields.items():
        if k in _TASK_FIELDS:
            sets.append(f"{k} = ?"); vals.append(v)
    if not sets:
        return
    sets.append("updated_at = ?"); vals.append(time.time())
    vals.append(task_id)
    with _lock:
        conn = _connect()
        conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
        conn.close()


def delete_task(task_id: int) -> None:
    with _lock:
        conn = _connect()
        conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        conn.commit()
        conn.close()


def set_task_status(task_id: int, status: str, session_id: str = "", last_error: str = "") -> None:
    with _lock:
        conn = _connect()
        conn.execute(
            "UPDATE tasks SET status = ?, session_id = ?, last_error = ?, updated_at = ? WHERE id = ?",
            (status, session_id, last_error, time.time(), task_id),
        )
        conn.commit()
        conn.close()


def account_has_valid_session(account_id: int) -> bool:
    conn = _connect()
    row = conn.execute(
        "SELECT saved_at FROM sessions WHERE account_id = ?", (account_id,)
    ).fetchone()
    conn.close()
    if not row:
        return False
    return (time.time() - row["saved_at"]) < SESSION_MAX_AGE


def clear_proxies_without_valid_session() -> int:
    """Drop stored proxies on accounts whose session is missing or expired.
    Lets borrowed pool proxies be reclaimed before each farm run."""
    now = time.time()
    with _lock:
        conn = _connect()
        cur = conn.execute("""
            UPDATE accounts
            SET proxy = ''
            WHERE COALESCE(proxy, '') <> ''
              AND NOT EXISTS (
                SELECT 1 FROM sessions s
                WHERE s.account_id = accounts.id
                  AND (? - s.saved_at) < ?
              )
        """, (now, SESSION_MAX_AGE))
        changed = cur.rowcount or 0
        conn.commit()
        conn.close()
        return changed


# ── Stats ─────────────────────────────────────────────────────────────────

def get_stats() -> dict:
    """Get overall account/session stats."""
    conn = _connect()
    total = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
    with_session = conn.execute("""
        SELECT COUNT(*) FROM sessions WHERE (? - saved_at) < ?
    """, (time.time(), SESSION_MAX_AGE)).fetchone()[0]
    expiring = conn.execute("""
        SELECT COUNT(*) FROM sessions
        WHERE (? - saved_at) BETWEEN ? AND ?
    """, (time.time(), SESSION_WARN_AGE, SESSION_MAX_AGE)).fetchone()[0]
    groups = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
    conn.close()
    return {
        "total_accounts": total,
        "active_sessions": with_session,
        "expiring_sessions": expiring,
        "no_session": total - with_session,
        "groups": groups,
    }


# ── Inventory ─────────────────────────────────────────────────────────────

def get_inventory_items() -> list[dict]:
    conn = _connect()
    rows = conn.execute("""
        SELECT * FROM inventory_items ORDER BY purchased_at DESC, id DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_inventory_item(item_id: int) -> None:
    with _lock:
        conn = _connect()
        conn.execute("DELETE FROM inventory_items WHERE id = ?", (item_id,))
        conn.commit()
        conn.close()


def record_inventory_purchase(
    record_key: str,
    purchase_id: str = "",
    account_id: int | None = None,
    account_name: str = "",
    account_phone: str = "",
    event_url: str = "",
    event_name: str = "",
    event_date: str = "",
    event_venue: str = "",
    ticket_type_id: str = "",
    ticket_name: str = "",
    ticket_currency: str = "USD",
    ticket_price: float | None = None,
    quantity: int = 1,
    total_price: float | None = None,
    purchase_status: str = "purchased",
    purchased_at: str | None = None,
) -> None:
    qty = max(1, int(quantity or 1))
    unit_price = float(ticket_price or 0)
    total = float(total_price) if total_price is not None else (unit_price * qty)
    stamp = purchased_at or datetime.now(UTC).isoformat().replace("+00:00", "Z")

    with _lock:
        conn = _connect()
        conn.execute("""
            INSERT INTO inventory_items (
                record_key, purchase_id, account_id, account_name, account_phone,
                event_url, event_name, event_date, event_venue,
                ticket_type_id, ticket_name, ticket_currency, ticket_price,
                quantity, total_price, purchase_status, purchased_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_key) DO UPDATE SET
                purchase_id = excluded.purchase_id,
                account_id = excluded.account_id,
                account_name = excluded.account_name,
                account_phone = excluded.account_phone,
                event_url = excluded.event_url,
                event_name = excluded.event_name,
                event_date = excluded.event_date,
                event_venue = excluded.event_venue,
                ticket_type_id = excluded.ticket_type_id,
                ticket_name = excluded.ticket_name,
                ticket_currency = excluded.ticket_currency,
                ticket_price = excluded.ticket_price,
                quantity = excluded.quantity,
                total_price = excluded.total_price,
                purchase_status = excluded.purchase_status,
                purchased_at = excluded.purchased_at
        """, (
            record_key.strip(),
            (purchase_id or "").strip(),
            int(account_id) if account_id else None,
            (account_name or "").strip(),
            (account_phone or "").strip(),
            (event_url or "").strip(),
            (event_name or "").strip(),
            (event_date or "").strip(),
            (event_venue or "").strip(),
            (ticket_type_id or "").strip(),
            (ticket_name or "").strip(),
            (ticket_currency or "USD").strip().upper(),
            unit_price,
            qty,
            total,
            (purchase_status or "purchased").strip(),
            stamp,
        ))
        conn.commit()
        conn.close()
