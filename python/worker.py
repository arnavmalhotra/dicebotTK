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
import traceback
import uuid
from datetime import UTC, datetime

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
    count = db.import_file(params["file_path"], int(gid) if gid is not None else None)
    return {"count": count}


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
        email=account.get("email"),
        proxy=account.get("proxy"),
        session_dir=_session_dir(),
        aycd_key=account.get("aycd_key"),
        imap_email=account.get("imap_email"),
        imap_password=account.get("imap_password"),
        imap_host=account.get("imap_host") or "imap.gmail.com",
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

    accounts = params.get("accounts") or []
    log = _log_fn_for(sid)
    _event(sid, type="status", status="starting", total=len(accounts))

    for idx, account in enumerate(accounts, start=1):
        if stop_evt.is_set():
            log("Stop requested. Ending farm.", "warning")
            break
        log(f"[{idx}/{len(accounts)}] {account['phone']}")
        country_iso, local_digits = _split_phone(account.get("phone") or "")
        try:
            result = auth_harvester.login_single_account(
                phone=local_digits,
                country_iso=account.get("country_iso") or country_iso,
                email=account.get("email"),
                proxy=account.get("proxy"),
                session_dir=_session_dir(),
                aycd_key=account.get("aycd_key"),
                imap_email=account.get("imap_email"),
                imap_password=account.get("imap_password"),
                imap_host=account.get("imap_host") or "imap.gmail.com",
                log_fn=log,
                on_driver=lambda d: driver_holder.update(driver=d),
            )
            driver_holder["driver"] = None
            if result.get("ok") and result.get("bearer_token"):
                db.save_session(int(account["id"]), result["bearer_token"], phone=account["phone"])
                _event(sid, type="auth_update", account_id=account["id"], status="ok")
            else:
                _event(sid, type="auth_update", account_id=account["id"], status="fail", error=result.get("error"))
        except Exception as exc:
            log(f"{account['phone']}: {exc}", "error")
            _event(sid, type="auth_update", account_id=account["id"], status="fail", error=str(exc))

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

    tickets = client.fetch_ticket_types(authenticated=True)
    if not tickets or not client.reserveToken:
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
        emit_update(status="purchased", purchase_id=client.purchaseId)
        return True, ""
    emit_update(status="purchase_failed")
    return False, "Purchase failed — see logs"


def _run_cart(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    account = params["account"]
    event_url = params["event_url"]
    try:
        ok, err = _run_cart_inner(sid, account, event_url, params, stop_evt, driver_holder, approve_evt)
        _event(sid, type="done", ok=ok, error=err or None)
    except Exception as exc:
        _event(sid, type="log", level="error", message=f"Cart run failed: {exc}")
        _event(sid, type="done", ok=False, error=str(exc))


def m_cart_run(params):
    account = params.get("account") or {}
    if not db.account_has_valid_session(int(account.get("id") or 0)):
        raise RuntimeError("Account has no valid session — run Auth Farm first")
    sid = _start_session("cart", _run_cart, params)
    return {"session_id": sid}


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
        db.set_task_status(task_id, status, session_id="", last_error=err)
        _event(sid, type="task_update", task_id=task_id, status=status, error=err or None)
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
    driver = (sess.get("driver_holder") or {}).get("driver")
    if driver is not None:
        try:
            driver.quit()
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

    emit({"type": "ready", "timestamp": _ts()})

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        handle(line)

    return 0


if __name__ == "__main__":
    sys.exit(main())
