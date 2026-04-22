#!/usr/bin/env python3
"""Persistent JSON-RPC worker spawned by the Electron main process.

Protocol (JSONL over stdin/stdout):
    Request:  {"id": <int>, "method": "<ns.method>", "params": {...}}
    Reply:    {"id": <int>, "ok": true, "data": ...}
              {"id": <int>, "ok": false, "error": "..."}
    Event:    {"type": "log"|"status"|"progress"|"cart_update"|"auth_update"|"task_update",
               "session_id": "...", ...}

Long-running methods (auth.login_one, cart.run, auth.farm, task.run) spawn a
background thread and return {"ok": true, "data": {"session_id": "..."}}
immediately. The thread emits events tagged with that session id and finishes
with a "done" event.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import sys
import threading
import time
import traceback
import uuid
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

multiprocessing.freeze_support()

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import db

_SESSIONS_LOCK = threading.Lock()
_SESSIONS: dict[str, dict] = {}
_STDOUT_LOCK = threading.Lock()


def _ts() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def emit(payload: dict) -> None:
    line = json.dumps(payload, default=str)
    with _STDOUT_LOCK:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def _reply(msg_id, ok: bool, data=None, error: str | None = None) -> None:
    out: dict = {"id": msg_id, "ok": ok}
    if ok:
        out["data"] = data
    else:
        out["error"] = error or "Unknown error"
    emit(out)


def _event(session_id: str, **fields) -> None:
    emit({"session_id": session_id, "timestamp": _ts(), **fields})


def _session_dir() -> str:
    home = os.path.expanduser("~")
    path = os.path.join(home, ".dicebot-ticketkings", "sessions")
    os.makedirs(path, exist_ok=True)
    return path


# ── DB methods ──────────────────────────────────────────────────────────────

def m_db_init(_):
    db.init_db()
    return {"ok": True}


def m_db_get_groups(_):
    return db.get_groups()


def m_db_create_group(params):
    gid = db.create_group(params["name"])
    return {"id": gid}


def m_db_delete_group(params):
    db.delete_group(int(params["group_id"]))
    return {"ok": True}


def m_db_rename_group(params):
    db.rename_group(int(params["group_id"]), params["name"])
    return {"ok": True}


_FIELD_ALIASES = {
    "exp_month": "card_exp_month",
    "exp_year": "card_exp_year",
    "cvc": "card_cvv",
}


def _normalize_account_fields(params: dict) -> dict:
    out = {}
    for k, v in params.items():
        out[_FIELD_ALIASES.get(k, k)] = v
    return out


def _enrich(row: dict) -> dict:
    saved = row.get("session_saved_at")
    row["session_status"] = db.session_status(saved)
    row["session_expires_in"] = db.session_expires_in(saved)
    return row


def m_db_get_accounts(params):
    gid = params.get("group_id")
    rows = db.get_accounts(int(gid) if gid is not None else None)
    return [_enrich(r) for r in rows]


def m_db_get_account(params):
    row = db.get_account(int(params["account_id"]))
    return _enrich(row) if row else None


def m_db_add_account(params):
    fields = _normalize_account_fields(params)
    fields.setdefault("name", fields.get("email") or fields.get("phone") or "unnamed")
    return {"id": db.add_account(**fields)}


def m_db_update_account(params):
    aid = int(params.pop("account_id"))
    db.update_account(aid, **_normalize_account_fields(params))
    return {"ok": True}


def m_db_delete_account(params):
    db.delete_account(int(params["account_id"]))
    return {"ok": True}


def m_db_assign_group(params):
    gid = params.get("group_id")
    db.assign_group([int(x) for x in params["account_ids"]], int(gid) if gid is not None else None)
    return {"ok": True}


def m_db_import_file(params):
    gid = params.get("group_id")
    result = db.import_file(params["file_path"], int(gid) if gid is not None else None)
    # result is already {count, created, updated, skipped, log}
    return result


def m_db_import_tasks_file(params):
    return db.import_tasks_file(params["file_path"])


def m_db_get_inventory(_):
    return db.get_inventory_items()


def m_db_delete_inventory_item(params):
    db.delete_inventory_item(int(params["item_id"]))
    return {"ok": True}


def m_db_get_stats(_):
    return db.get_stats()


def m_db_get_accounts_needing_auth(_):
    return db.get_accounts_needing_auth()


def m_db_get_accounts_with_valid_session(params):
    gid = params.get("group_id")
    return db.get_accounts_with_valid_session(int(gid) if gid is not None else None)


def m_db_get_session(params):
    return db.get_session(int(params["account_id"]))


# ── Tasks ──────────────────────────────────────────────────────────────────

def _enrich_task(row: dict) -> dict:
    saved = row.get("session_saved_at")
    row["session_status"] = db.session_status(saved)
    return row


def m_db_get_tasks(_):
    return [_enrich_task(r) for r in db.get_tasks()]


def m_db_get_task(params):
    row = db.get_task(int(params["task_id"]))
    return _enrich_task(row) if row else None


def _task_fields(params: dict) -> dict:
    def _num(v):
        if v in ("", None):
            return None
        try:
            return float(v)
        except Exception:
            return None
    return {
        "event_url": (params.get("event_url") or "").strip(),
        "min_price": _num(params.get("min_price")),
        "max_price": _num(params.get("max_price")),
        "presale_code": (params.get("presale_code") or "").strip(),
        "ticket_tier": (params.get("ticket_tier") or "").strip(),
        "quantity": int(params.get("quantity") or 1),
        "mode": (params.get("mode") or "auto").strip() or "auto",
        "scheduled_at": (params.get("scheduled_at") or "").strip(),
        "scheduled_tz": (params.get("scheduled_tz") or "").strip(),
    }


def m_db_create_task(params):
    aid = int(params["account_id"])
    tid = db.create_task(aid, **_task_fields(params))
    return {"id": tid}


def m_db_update_task(params):
    tid = int(params["task_id"])
    db.update_task(tid, **_task_fields(params))
    return {"ok": True}


def m_db_delete_task(params):
    db.delete_task(int(params["task_id"]))
    return {"ok": True}


# ── Background-task helpers ────────────────────────────────────────────────

def _start_session(kind: str, runner, params) -> str:
    sid = uuid.uuid4().hex[:12]
    stop_evt = threading.Event()
    approve_evt = threading.Event()
    otp_holder = {"code": None}
    driver_holder: dict = {"driver": None}

    with _SESSIONS_LOCK:
        _SESSIONS[sid] = {
            "kind": kind,
            "stop": stop_evt,
            "approve": approve_evt,
            "otp": otp_holder,
            "driver_holder": driver_holder,
            "meta": {},
        }

    def _wrap():
        try:
            runner(sid, params, stop_evt, otp_holder, driver_holder, approve_evt)
        except Exception as exc:
            _event(sid, type="log", level="error", message=f"Worker thread crashed: {exc}")
            _event(sid, type="done", ok=False, error=str(exc))
        finally:
            with _SESSIONS_LOCK:
                _SESSIONS.pop(sid, None)

    threading.Thread(target=_wrap, daemon=True).start()
    return sid


def _log_fn_for(sid: str):
    def log(msg: str, level: str = "info"):
        _event(sid, type="log", level=level, message=msg)
    return log


def _parse_scheduled_ts(scheduled_at: str, scheduled_tz: str) -> float | None:
    """Parse "YYYY-MM-DDTHH:MM[:SS]" + IANA tz → unix seconds, or None."""
    s = (scheduled_at or "").strip()
    if not s:
        return None
    # datetime-local inputs give "YYYY-MM-DDTHH:MM" with no seconds.
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            naive = datetime.strptime(s, fmt)
            break
        except ValueError:
            naive = None
    if naive is None:
        return None
    tz = scheduled_tz.strip() or "UTC"
    try:
        zone = ZoneInfo(tz)
    except Exception:
        zone = ZoneInfo("UTC")
    return naive.replace(tzinfo=zone).timestamp()


def _wait_and_poll_for_drop(client, target_ts: float, stop_evt: threading.Event, log) -> bool:
    """Block until a reserve_token appears, anchored around target_ts.

    reserve_token = eligibility to cart, not a ticket hold. First POST to
    /purchases wins, so we fire as soon as the API will accept one.

    - Interruptible sleep until T-3s (don't hammer the API early).
    - From T-3s: polls fetch_ticket_types every ~80ms and fires the moment
      a reserve_token appears (before, at, or after T=0).
    - After T=0: keeps polling for up to POST_DROP_GRACE seconds if tiers
      haven't been published yet.

    Returns True if a reserve_token was obtained, False otherwise.
    """
    PRE_DROP_INTERVAL = 0.08
    POST_DROP_INTERVAL = 0.08
    POST_DROP_GRACE = 30.0

    # ── 1. Interruptible sleep until T-3s ────────────────────────────────
    while True:
        remaining = target_ts - time.time()
        if remaining <= 3.0:
            break
        if stop_evt.wait(timeout=min(remaining - 3.0, 1.0)):
            return False

    log(f"T-3s — polling for reserve_token; will fire as soon as one appears.")

    # ── 2. Tight poll loop. Fire early if tiers publish before T=0;
    #       keep going through T=0 and into the post-drop grace window. ─
    deadline = target_ts + POST_DROP_GRACE
    interval = PRE_DROP_INTERVAL
    announced_t0 = False
    while time.time() < deadline:
        if stop_evt.is_set():
            return False
        try:
            client.fetch_ticket_types(authenticated=True)
        except Exception as exc:
            log(f"Poll error: {exc}", "warn")
        if getattr(client, "reserveToken", None):
            delta = time.time() - target_ts
            if delta < 0:
                log(f"Reserve token obtained T{delta:.2f}s (before drop) — firing.")
            else:
                log(f"Reserve token obtained T+{delta:.2f}s (after drop) — firing.")
            return True
        if not announced_t0 and time.time() >= target_ts:
            log("T-0 — no reserve_token yet, continuing to poll.")
            announced_t0 = True
            interval = POST_DROP_INTERVAL
        time.sleep(interval)

    return False


_COUNTRY_PREFIXES = {
    "44": "gb", "49": "de", "33": "fr", "31": "nl", "61": "au", "91": "in",
    "34": "es", "39": "it", "353": "ie", "52": "mx", "55": "br",
}


def _split_phone(phone: str) -> tuple[str, str]:
    """Return (country_iso, local_digits) — dice.fm expects local digits only."""
    import re
    digits = re.sub(r"\D", "", phone or "")
    if (phone or "").startswith("+"):
        for prefix in sorted(_COUNTRY_PREFIXES, key=len, reverse=True):
            if digits.startswith(prefix) and len(digits) > len(prefix) + 5:
                return _COUNTRY_PREFIXES[prefix], digits[len(prefix):]
        if digits.startswith("1") and len(digits) == 11:
            return "us", digits[1:]
    return "us", digits


# ── Auth: login single / farm ──────────────────────────────────────────────

def _run_login_one(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    import auth_harvester

    account = params["account"]
    log = _log_fn_for(sid)
    _event(sid, type="status", status="starting")
    country_iso, local_digits = _split_phone(account.get("phone") or "")

    result = auth_harvester.login_single_account(
        phone=local_digits,
        country_iso=account.get("country_iso") or country_iso,
        email=(account.get("aycd_email") or account.get("email")),
        proxy=account.get("proxy"),
        session_dir=_session_dir(),
        aycd_key=account.get("aycd_key"),
        imap_email=account.get("imap_email"),
        imap_password=account.get("imap_password"),
        imap_host=account.get("imap_host") or "imap.gmail.com",
        imap_recipient=account.get("email") or "",
        log_fn=log,
        on_driver=lambda d: driver_holder.update(driver=d),
    )

    if result.get("ok") and result.get("bearer_token"):
        try:
            db.save_session(int(account["id"]), result["bearer_token"], phone=account["phone"])
        except Exception as exc:
            log(f"DB save failed: {exc}", "error")
        _event(sid, type="auth_update", account_id=account["id"], status="ok")
        _event(sid, type="done", ok=True)
    else:
        _event(sid, type="auth_update", account_id=account["id"], status="fail", error=result.get("error"))
        _event(sid, type="done", ok=False, error=result.get("error") or "Login failed")


def m_auth_login_one(params):
    sid = _start_session("auth_one", _run_login_one, params)
    return {"session_id": sid}


def _run_auth_farm(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    import auth_harvester
    from concurrent.futures import ThreadPoolExecutor

    accounts = params.get("accounts") or []
    try:
        concurrency = max(1, int(params.get("concurrency") or 1))
    except (TypeError, ValueError):
        concurrency = 1
    concurrency = min(concurrency, max(1, len(accounts)))

    log = _log_fn_for(sid)
    _event(sid, type="status", status="starting", total=len(accounts), concurrency=concurrency)
    log(f"Farm starting — {len(accounts)} accounts, concurrency={concurrency}")

    # Promote driver_holder to a multi-driver shape so parallel workers can
    # all be interrupted by a single m_session_stop. We keep the legacy
    # "driver" key working too by leaving it unset here.
    driver_holder.setdefault("drivers", set())
    driver_holder.setdefault("lock", threading.Lock())

    def _register_driver(d):
        if d is None:
            return
        with driver_holder["lock"]:
            driver_holder["drivers"].add(d)

    def _unregister_driver(d):
        if d is None:
            return
        with driver_holder["lock"]:
            driver_holder["drivers"].discard(d)

    completed = {"n": 0}
    completed_lock = threading.Lock()

    def _run_one(idx: int, account: dict):
        if stop_evt.is_set():
            return
        phone = account.get("phone") or ""
        prefix = f"[{idx}/{len(accounts)}] {phone}"
        log(f"{prefix} starting")
        country_iso, local_digits = _split_phone(phone)
        my_driver = {"d": None}

        def _capture_driver(d):
            my_driver["d"] = d
            _register_driver(d)

        def _wlog(msg, level="info"):
            log(f"{prefix}: {msg}", level)

        try:
            result = auth_harvester.login_single_account(
                phone=local_digits,
                country_iso=account.get("country_iso") or country_iso,
                email=(account.get("aycd_email") or account.get("email")),
                proxy=account.get("proxy"),
                session_dir=_session_dir(),
                aycd_key=account.get("aycd_key"),
                imap_email=account.get("imap_email"),
                imap_password=account.get("imap_password"),
                imap_host=account.get("imap_host") or "imap.gmail.com",
                imap_recipient=account.get("email") or "",
                log_fn=_wlog,
                on_driver=_capture_driver,
            )
            if result.get("ok") and result.get("bearer_token"):
                try:
                    db.save_session(int(account["id"]), result["bearer_token"], phone=phone)
                except Exception as exc:
                    _wlog(f"DB save failed: {exc}", "error")
                _event(sid, type="auth_update", account_id=account["id"], status="ok")
            else:
                _event(sid, type="auth_update", account_id=account["id"], status="fail",
                       error=result.get("error"))
        except Exception as exc:
            _wlog(f"error: {exc}", "error")
            _event(sid, type="auth_update", account_id=account["id"], status="fail", error=str(exc))
        finally:
            _unregister_driver(my_driver["d"])
            with completed_lock:
                completed["n"] += 1
                n = completed["n"]
            _event(sid, type="progress", done=n, total=len(accounts))

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(_run_one, i, acct) for i, acct in enumerate(accounts, start=1)]
        for f in futures:
            try:
                f.result()
            except Exception as exc:
                log(f"Worker crashed: {exc}", "error")

    if stop_evt.is_set():
        log("Farm stopped by user.", "warning")
        _event(sid, type="done", ok=False, error="stopped")
    else:
        log(f"Farm finished — {completed['n']}/{len(accounts)} accounts processed.")
        _event(sid, type="done", ok=True)


def m_auth_farm(params):
    sid = _start_session("auth_farm", _run_auth_farm, params)
    return {"session_id": sid}


# ── Cart / purchase ────────────────────────────────────────────────────────

def _run_cart_inner(
    sid: str,
    account: dict,
    event_url: str,
    params: dict,
    stop_evt: threading.Event,
    driver_holder: dict,
    approve_evt: threading.Event,
    task_id: int | None = None,
) -> tuple[bool, str]:
    """Runs the cart flow. Returns (ok, error). Emits cart_update events.
    Does NOT emit 'done' — caller is responsible for that."""
    import dice_requests

    log = _log_fn_for(sid)

    def emit_update(**fields):
        payload = {"account_id": account.get("id"), **fields}
        if task_id is not None:
            payload["task_id"] = task_id
        _event(sid, type="cart_update", **payload)

    emit_update(status="starting", event_url=event_url)

    client = dice_requests.DiceFM(
        phone=account["phone"],
        email=account.get("email") or "",
        event_url=event_url,
        capsolver_key=params.get("capsolver_key"),
        twocaptcha_key=params.get("twocaptcha_key"),
        proxy_string=account.get("proxy"),
        code=params.get("presale_code") or "",
        target_min_price=params.get("target_min_price") or params.get("min_price"),
        target_max_price=params.get("target_max_price") or params.get("max_price"),
        ticket_tier=params.get("ticket_tier"),
        session_dir=_session_dir(),
        log_fn=log,
    )

    if not client.load_session():
        return False, "No valid session — run auth first"

    client.visit_event_page()
    if stop_evt.is_set():
        return False, "stopped"

    # Probe the event once so we know if it's locked, and claim the access
    # code before any timing-critical work. Claims are per-account-per-event
    # and persist server-side, so a single upfront claim is enough.
    probe = client.fetch_ticket_types(authenticated=True)
    if probe is None:
        return False, "Failed to fetch event"
    presale_code = (params.get("presale_code") or "").strip()
    if client.eventIsLocked:
        if not presale_code:
            return False, "Event is locked — add an access code to this task"
        log("Event is locked — claiming access code.")
        resp = client.claim_code()
        if not resp or resp.status_code != 200:
            return False, "Access code claim failed"
        # Refresh state after claim so the poller starts from a known-good place
        client.fetch_ticket_types(authenticated=True)

    target_ts = _parse_scheduled_ts(params.get("scheduled_at") or "", params.get("scheduled_tz") or "")
    if client.reserveToken:
        # Probe already got an eligible reserve_token — fire now regardless
        # of whether this task was scheduled for later.
        log("Reserve token already held from probe — firing immediately.")
    elif target_ts is not None and target_ts - time.time() > 3.0:
        log(f"Scheduled drop at {params.get('scheduled_at')} {params.get('scheduled_tz')} "
            f"(T-{target_ts - time.time():.1f}s) — will fire as soon as a reserve_token is available.")
        emit_update(status="armed", scheduled_at=params.get("scheduled_at"),
                    scheduled_tz=params.get("scheduled_tz"))
        got_token = _wait_and_poll_for_drop(client, target_ts, stop_evt, log)
        if stop_evt.is_set():
            return False, "stopped"
        if not got_token:
            return False, "No tickets available after drop"
    else:
        # Immediate flow — probe already set reserveToken on success. If the
        # event was locked we just refreshed after the claim; either way a
        # non-empty reserveToken means we have a ticket to cart.
        if not client.reserveToken:
            client.fetch_ticket_types(authenticated=True)
        if not client.reserveToken:
            return False, "No tickets available"

    if stop_evt.is_set():
        return False, "stopped"

    mode = params.get("mode", "auto")

    if mode == "manual":
        emit_update(
            status="reserved",
            ticket_name=client.ticketName,
            ticket_price=client.ticketPrice,
            ticket_currency=client.ticketCurrency,
            event_name=client.eventName,
            ttl=300,
        )
        _event(sid, type="await_approval")
        if not approve_evt.wait(timeout=300):
            return False, "Approval timeout — ticket released"
        if stop_evt.is_set():
            return False, "stopped"

    emit_update(
        status="purchasing",
        ticket_name=client.ticketName,
        ticket_price=client.ticketPrice,
        ticket_currency=client.ticketCurrency,
        event_name=client.eventName,
    )

    ok = client.run_purchase_flow(
        quantity=int(params.get("quantity") or 1),
        card_number=account.get("card_number") or "",
        exp_month=str(account.get("card_exp_month") or account.get("exp_month") or ""),
        exp_year=str(account.get("card_exp_year") or account.get("exp_year") or ""),
        cvc=str(account.get("card_cvv") or account.get("cvc") or ""),
        billing_name=account.get("billing_name") or "",
        billing_email=account.get("billing_email") or account.get("email") or "",
        billing_phone=account.get("billing_phone") or account.get("phone") or "",
        billing_postal_code=account.get("billing_postal") or "",
        billing_country=account.get("billing_country") or "US",
    )

    if ok:
        quantity = int(client.purchaseQuantity or params.get("quantity") or 1)
        ticket_price = float(client.ticketPrice or 0)
        total_price = ticket_price * quantity
        try:
            db.record_inventory_purchase(
                record_key=(f"purchase:{client.purchaseId}" if client.purchaseId else f"session:{sid}"),
                purchase_id=client.purchaseId or "",
                account_id=int(account.get("id") or 0) or None,
                account_name=account.get("name") or "",
                account_phone=account.get("phone") or "",
                event_url=client.eventUrl or event_url,
                event_name=client.eventName or "",
                event_date=client.eventDate or "",
                event_venue=client.eventVenue or "",
                ticket_type_id=str(client.ticketTypeId or ""),
                ticket_name=client.ticketName or "",
                ticket_currency=client.ticketCurrency or "USD",
                ticket_price=ticket_price,
                quantity=quantity,
                total_price=total_price,
                purchase_status="purchased",
                purchased_at=_ts(),
            )
            _event(sid, type="inventory_update")
        except Exception as exc:
            log(f"Inventory record failed: {exc}", "warning")
        emit_update(
            status="purchased",
            purchase_id=client.purchaseId,
            quantity=quantity,
            total_price=total_price,
            event_name=client.eventName,
            ticket_name=client.ticketName,
            ticket_price=client.ticketPrice,
            ticket_currency=client.ticketCurrency,
        )
        return True, ""
    emit_update(status="purchase_failed")
    return False, "Purchase failed — see logs"


def m_cart_run(params):
    """Quick one-off purchase. Creates an ephemeral task row and runs it via
    the same _run_task path as persistent tasks. The row is deleted on
    terminal status so it never appears on the Tasks page."""
    account = params.get("account") or {}
    account_id = int(account.get("id") or 0)
    if not db.account_has_valid_session(account_id):
        raise RuntimeError("Account has no valid session — run Auth Farm first")

    event_url = (params.get("event_url") or "").strip()
    if not event_url:
        raise RuntimeError("event_url is required")

    tid = db.create_task(
        account_id=account_id,
        event_url=event_url,
        min_price=params.get("target_min_price") or params.get("min_price"),
        max_price=params.get("target_max_price") or params.get("max_price"),
        presale_code=params.get("presale_code") or "",
        ticket_tier=params.get("ticket_tier") or "",
        quantity=params.get("quantity") or 1,
        mode=params.get("mode") or "auto",
        ephemeral=True,
    )
    task = db.get_task(tid)

    sid = _start_session("task", _run_task, {
        "task": task,
        "account": db.get_account(account_id) or account,
        "capsolver_key": params.get("capsolver_key"),
        "twocaptcha_key": params.get("twocaptcha_key"),
    })
    return {"session_id": sid, "task_id": tid}


# ── Task runner ────────────────────────────────────────────────────────────

def _run_task(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    task = params["task"]
    account = params["account"]
    task_id = int(task["id"])
    log = _log_fn_for(sid)

    db.set_task_status(task_id, "running", session_id=sid)
    _event(sid, type="task_update", task_id=task_id, status="running")

    merged = {
        "capsolver_key": params.get("capsolver_key"),
        "twocaptcha_key": params.get("twocaptcha_key"),
        "presale_code": task.get("presale_code") or "",
        "target_min_price": task.get("min_price"),
        "target_max_price": task.get("max_price"),
        "ticket_tier": task.get("ticket_tier"),
        "quantity": task.get("quantity") or 1,
        "mode": task.get("mode") or "auto",
        "scheduled_at": task.get("scheduled_at") or "",
        "scheduled_tz": task.get("scheduled_tz") or "",
    }

    ok, err = False, ""
    try:
        ok, err = _run_cart_inner(
            sid, account, task.get("event_url") or "", merged,
            stop_evt, driver_holder, approve_evt, task_id=task_id,
        )
    except Exception as exc:
        err = str(exc)
        log(f"Task run failed: {exc}", "error")
    finally:
        if stop_evt.is_set() and not ok:
            status = "stopped"
        elif ok:
            status = "done"
        else:
            status = "failed"
        ephemeral = bool(task.get("ephemeral"))
        if ephemeral:
            # Quick-runs vanish once they terminate — their state lived
            # only to unify the runner. The dashboard card still shows
            # the final status because the event goes out below.
            db.delete_task(task_id)
        else:
            db.set_task_status(task_id, status, session_id="", last_error=err)
        _event(sid, type="task_update", task_id=task_id, status=status, error=err or None, ephemeral=ephemeral)
        _event(sid, type="done", ok=ok, error=err or None)


def m_task_run(params):
    tid = int(params["task_id"])
    task = db.get_task(tid)
    if not task:
        raise RuntimeError("Task not found")
    if not task.get("event_url"):
        raise RuntimeError("Task has no event URL — edit before starting")
    account = db.get_account(int(task["account_id"]))
    if not account:
        raise RuntimeError("Account missing for task")
    if not db.account_has_valid_session(int(account["id"])):
        raise RuntimeError("Account has no valid session — run Auth Farm first")

    sid = _start_session("task", _run_task, {
        "task": task,
        "account": account,
        "capsolver_key": params.get("capsolver_key"),
        "twocaptcha_key": params.get("twocaptcha_key"),
    })
    return {"session_id": sid, "task_id": tid}


def m_task_stop(params):
    tid = int(params["task_id"])
    task = db.get_task(tid)
    if not task:
        return {"ok": False, "error": "task not found"}
    sid = task.get("session_id") or ""
    if sid:
        with _SESSIONS_LOCK:
            sess = _SESSIONS.get(sid)
        if sess:
            sess["stop"].set()
            driver = (sess.get("driver_holder") or {}).get("driver")
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
    db.set_task_status(tid, "stopped", session_id="", last_error="")
    return {"ok": True}


# ── Session control ────────────────────────────────────────────────────────

def m_session_stop(params):
    sid = params["session_id"]
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(sid)
    if not sess:
        return {"ok": False, "error": "session not found"}
    sess["stop"].set()
    dh = sess.get("driver_holder") or {}
    # Legacy single-driver shape (login_one, cart, task runners).
    d = dh.get("driver")
    if d is not None:
        try:
            d.quit()
        except Exception:
            pass
    # Multi-driver shape (parallel auth farm).
    drivers = dh.get("drivers")
    if drivers:
        lock = dh.get("lock")
        if lock is not None:
            with lock:
                to_quit = list(drivers)
        else:
            to_quit = list(drivers)
        for drv in to_quit:
            try:
                drv.quit()
            except Exception:
                pass
    return {"ok": True}


def m_session_approve(params):
    sid = params["session_id"]
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(sid)
    if not sess:
        return {"ok": False, "error": "session not found"}
    sess["approve"].set()
    return {"ok": True}


def m_session_set_otp(params):
    sid = params["session_id"]
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(sid)
    if not sess:
        return {"ok": False, "error": "session not found"}
    sess["otp"]["code"] = params.get("code")
    return {"ok": True}


# ── Dispatch ───────────────────────────────────────────────────────────────

METHODS = {
    "db.init": m_db_init,
    "db.get_groups": m_db_get_groups,
    "db.create_group": m_db_create_group,
    "db.delete_group": m_db_delete_group,
    "db.rename_group": m_db_rename_group,
    "db.get_accounts": m_db_get_accounts,
    "db.get_account": m_db_get_account,
    "db.add_account": m_db_add_account,
    "db.update_account": m_db_update_account,
    "db.delete_account": m_db_delete_account,
    "db.assign_group": m_db_assign_group,
    "db.import_file": m_db_import_file,
    "db.import_tasks_file": m_db_import_tasks_file,
    "db.get_inventory": m_db_get_inventory,
    "db.delete_inventory_item": m_db_delete_inventory_item,
    "db.get_stats": m_db_get_stats,
    "db.get_accounts_needing_auth": m_db_get_accounts_needing_auth,
    "db.get_accounts_with_valid_session": m_db_get_accounts_with_valid_session,
    "db.get_session": m_db_get_session,
    "db.get_tasks": m_db_get_tasks,
    "db.get_task": m_db_get_task,
    "db.create_task": m_db_create_task,
    "db.update_task": m_db_update_task,
    "db.delete_task": m_db_delete_task,
    "auth.login_one": m_auth_login_one,
    "auth.farm": m_auth_farm,
    "cart.run": m_cart_run,
    "task.run": m_task_run,
    "task.stop": m_task_stop,
    "session.stop": m_session_stop,
    "session.approve": m_session_approve,
    "session.set_otp": m_session_set_otp,
}


def handle(raw: str) -> None:
    try:
        msg = json.loads(raw)
    except Exception:
        return
    msg_id = msg.get("id")
    method = msg.get("method")
    params = msg.get("params") or {}
    fn = METHODS.get(method)
    if not fn:
        _reply(msg_id, False, error=f"unknown method: {method}")
        return
    try:
        result = fn(params)
        _reply(msg_id, True, data=result)
    except Exception as exc:
        tb = traceback.format_exc()
        _reply(msg_id, False, error=f"{exc}\n{tb}")


def main() -> int:
    try:
        db.init_db()
    except Exception as exc:
        emit({"type": "log", "level": "error", "message": f"DB init failed: {exc}"})
    try:
        purged = db.purge_ephemeral_tasks()
        if purged:
            emit({"type": "log", "level": "info", "message": f"Cleared {purged} stale quick-run task(s) from prior session."})
    except Exception as exc:
        emit({"type": "log", "level": "error", "message": f"Ephemeral cleanup failed: {exc}"})

    emit({"type": "ready", "timestamp": _ts()})

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        handle(line)

    return 0


if __name__ == "__main__":
    sys.exit(main())
