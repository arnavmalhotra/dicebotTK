#!/usr/bin/env python3
"""Persistent JSON-RPC worker spawned by the Electron main process.

Protocol (JSONL over stdin/stdout):
    Request:  {"id": <int>, "method": "<ns.method>", "params": {...}}
    Reply:    {"id": <int>, "ok": true, "data": ...}
              {"id": <int>, "ok": false, "error": "..."}
    Event:    {"type": "log"|"status"|"progress"|"cart_update"|"auth_update",
               "session_id": "...", ...}

Long-running methods (auth.login_one, cart.run, auth.farm)
spawn a background thread and return {"ok": true,
"data": {"session_id": "..."}} immediately. The thread emits events tagged
with that session id and finishes with a "done" event.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import queue
import random
import requests
import ssl
import sys
import threading
import time
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

multiprocessing.freeze_support()

# Add this dir to path so side-by-side modules import cleanly from both the
# source checkout and PyInstaller's bundled one-dir layout.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import certifi
except ImportError:
    certifi = None


def _resolve_cert_bundle_path() -> str | None:
    candidates: list[Path] = []

    if certifi is not None:
        try:
            candidates.append(Path(certifi.where()))
        except Exception:
            pass

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        candidates.extend(
            [
                Path(meipass) / "certifi" / "cacert.pem",
                Path(meipass) / "_internal" / "certifi" / "cacert.pem",
            ]
        )

    for candidate in candidates:
        try:
            if candidate.exists():
                return str(candidate)
        except OSError:
            continue

    return None


def _configure_tls_trust() -> None:
    # macOS Python builds (and PyInstaller-frozen apps) don't read the system
    # keychain, so urllib.urlopen() fails with CERTIFICATE_VERIFY_FAILED when
    # undetected_chromedriver tries to fetch the driver release manifest.
    # Point OpenSSL at certifi's bundle so the patcher can resolve roots.
    cert_path = _resolve_cert_bundle_path()
    if not cert_path:
        return

    os.environ.setdefault("SSL_CERT_FILE", cert_path)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", cert_path)
    os.environ.setdefault("CURL_CA_BUNDLE", cert_path)

    def _create_verified_context(*args: Any, **kwargs: Any) -> ssl.SSLContext:
        if "cafile" not in kwargs or not kwargs["cafile"]:
            kwargs["cafile"] = cert_path
        return ssl.create_default_context(*args, **kwargs)

    ssl._create_default_https_context = _create_verified_context


_configure_tls_trust()

import db

_SESSIONS_LOCK = threading.Lock()
_SESSIONS: dict[str, dict] = {}
_STDOUT_LOCK = threading.Lock()

_APPROVAL_TTL_SECONDS = 300

# ── Telemetry (silent — end users never see this) ─────────────────────────
# These values are baked into the worker so every TK install posts to the
# same DiceBotWeb endpoint without per-user configuration. Override at
# runtime by setting TK_TELEMETRY_URL / TK_TELEMETRY_SECRET in the env (used
# during local dev only — the packaged build has no env override).
TK_TELEMETRY_URL = os.environ.get(
    "TK_TELEMETRY_URL",
    "https://tieroneonly.com/api/tk/telemetry",
)
TK_TELEMETRY_SECRET = os.environ.get(
    "TK_TELEMETRY_SECRET",
    "a5328ee83a3378ffb2316da716184e6bb8f62f1a2b6b89c6585c978c1f0d24bc",
)


def _post_telemetry_async(*, payload, log, device_id=None) -> None:
    """Fire-and-forget POST to DiceBotWeb's TK telemetry endpoint.

    Failures are logged at debug level only — purchases must never depend on
    telemetry succeeding. End users have no visibility into this.
    """
    url = TK_TELEMETRY_URL
    secret = TK_TELEMETRY_SECRET
    if not url:
        return

    if device_id:
        payload = {**payload, "device_id": device_id}

    def _do_post():
        try:
            req = requests.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "x-tk-telemetry-secret": str(secret or ""),
                    "User-Agent": "DiceBotTK/telemetry",
                },
                timeout=8,
            )
            if 200 <= req.status_code < 300:
                return
            log(f"Telemetry POST returned {req.status_code}.", "debug")
        except Exception:
            # Don't surface telemetry failures in the user-facing log.
            pass

    threading.Thread(target=_do_post, daemon=True).start()


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
    db.clear_proxies_without_valid_session()
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
    db.clear_proxies_without_valid_session()
    gid = params.get("group_id")
    rows = db.get_accounts(int(gid) if gid is not None else None)
    return [_enrich(r) for r in rows]


def m_db_get_account(params):
    db.clear_proxies_without_valid_session()
    row = db.get_account(int(params["account_id"]))
    return _enrich(row) if row else None


def m_db_add_account(params):
    fields = _normalize_account_fields(params)
    fields.setdefault("name", fields.get("email") or fields.get("phone") or "unnamed")
    fields["proxy"] = ""
    return {"id": db.add_account(**fields)}


def m_db_update_account(params):
    aid = int(params.pop("account_id"))
    fields = _normalize_account_fields(params)
    if "proxy" in fields and not db.account_has_valid_session(aid):
        fields["proxy"] = ""
    db.update_account(aid, **fields)
    return {"ok": True}


def m_db_delete_account(params):
    db.delete_account(int(params["account_id"]))
    return {"ok": True}


def m_db_assign_group(params):
    gid = params.get("group_id")
    db.assign_group([int(x) for x in params["account_ids"]], int(gid) if gid is not None else None)
    return {"ok": True}


# ── Payment cards (DiceBotNew-style: labeled cards assigned to accounts) ──

_CARD_PARAM_ALIASES = {
    "exp_month": "card_exp_month",
    "exp_year": "card_exp_year",
    "cvc": "card_cvv",
    "cvv": "card_cvv",
}


def _normalize_card_fields(params: dict) -> dict:
    out = {}
    for k, v in params.items():
        if k in {"card_id", "account_id"}:
            continue
        out[_CARD_PARAM_ALIASES.get(k, k)] = v
    return out


def m_db_get_payment_pools(_):
    return db.get_payment_pools()


def m_db_create_payment_pool(params):
    pid = db.create_payment_pool(params["name"])
    return {"id": pid}


def m_db_rename_payment_pool(params):
    db.rename_payment_pool(int(params["pool_id"]), params["name"])
    return {"ok": True}


def m_db_delete_payment_pool(params):
    db.delete_payment_pool(int(params["pool_id"]))
    return {"ok": True}


def m_db_get_payment_cards(_):
    return db.get_payment_cards()


def m_db_get_payment_card(params):
    return db.get_payment_card(int(params["card_id"]))


def m_db_add_payment_card(params):
    fields = _normalize_card_fields(params)
    cid = db.add_payment_card(**fields)
    return {"id": cid}


def m_db_update_payment_card(params):
    fields = _normalize_card_fields(params)
    db.update_payment_card(int(params["card_id"]), **fields)
    return {"ok": True}


def m_db_delete_payment_card(params):
    db.delete_payment_card(int(params["card_id"]))
    return {"ok": True}


def m_db_assign_card(params):
    db.assign_card(int(params["account_id"]), int(params["card_id"]))
    return {"ok": True}


def m_db_unassign_card(params):
    db.unassign_card(int(params["account_id"]), int(params["card_id"]))
    return {"ok": True}


def m_db_get_card_labels(_):
    return db.get_card_labels()


def m_db_get_assigned_cards_for_account(params):
    return db.get_assigned_cards_for_account(int(params["account_id"]))


def m_db_bulk_account_cards_by_label(params):
    ids = [int(x) for x in (params.get("account_ids") or [])]
    return db.bulk_account_cards_by_label(ids, params.get("label") or "")


def m_db_bulk_add_payment_cards(params):
    return db.bulk_add_payment_cards(params.get("rows") or [])


def m_db_get_code_pools(_):
    return db.get_code_pools()


def m_db_create_code_pool(params):
    pid = db.create_code_pool(params["name"])
    return {"id": pid}


def m_db_rename_code_pool(params):
    db.rename_code_pool(int(params["pool_id"]), params["name"])
    return {"ok": True}


def m_db_delete_code_pool(params):
    db.delete_code_pool(int(params["pool_id"]))
    return {"ok": True}


def m_db_get_code_pool_codes(params):
    return db.get_code_pool_codes(int(params["pool_id"]))


def m_db_add_code_pool_codes(params):
    return db.add_code_pool_codes(
        int(params["pool_id"]),
        params.get("codes") or [],
    )


def m_db_delete_code_pool_code(params):
    db.delete_code_pool_code(int(params["code_id"]))
    return {"ok": True}


def m_db_clear_code_pool(params):
    db.clear_code_pool(int(params["pool_id"]))
    return {"ok": True}


def m_db_draw_codes_from_pool(params):
    return db.draw_codes_from_pool(
        int(params["pool_id"]),
        int(params.get("count") or 0),
    )


# ── Proxy pools ───────────────────────────────────────────────────────────

def m_db_get_proxy_pools(_):
    return db.get_proxy_pools()


def m_db_create_proxy_pool(params):
    return {"id": db.create_proxy_pool(params["name"])}


def m_db_rename_proxy_pool(params):
    db.rename_proxy_pool(int(params["pool_id"]), params["name"])
    return {"ok": True}


def m_db_delete_proxy_pool(params):
    db.delete_proxy_pool(int(params["pool_id"]))
    return {"ok": True}


def m_db_get_proxy_pool_proxies(params):
    return db.get_proxy_pool_proxies(int(params["pool_id"]))


def m_db_add_proxy_pool_proxies(params):
    return db.add_proxy_pool_proxies(
        int(params["pool_id"]),
        list(params.get("proxies") or []),
    )


def m_db_delete_proxy_pool_proxy(params):
    db.delete_proxy_pool_proxy(int(params["proxy_id"]))
    return {"ok": True}


def m_db_clear_proxy_pool(params):
    db.clear_proxy_pool(int(params["pool_id"]))
    return {"ok": True}


def m_db_import_file(params):
    gid = params.get("group_id")
    count = db.import_file(params["file_path"], int(gid) if gid is not None else None)
    db.clear_proxies_without_valid_session()
    return {"count": count}


def m_db_get_stats(_):
    db.clear_proxies_without_valid_session()
    return db.get_stats()


def m_db_get_accounts_needing_auth(_):
    db.clear_proxies_without_valid_session()
    return db.get_accounts_needing_auth()


def m_db_get_accounts_with_valid_session(params):
    gid = params.get("group_id")
    return db.get_accounts_with_valid_session(int(gid) if gid is not None else None)


def m_db_get_session(params):
    return db.get_session(int(params["account_id"]))


def m_db_get_inventory_items(_):
    return db.get_inventory_items()


def m_db_delete_inventory_item(params):
    db.delete_inventory_item(int(params["item_id"]))
    return {"ok": True}


def _optional_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_price_rules(raw):
    rules = []
    for item in (raw or []):
        if not isinstance(item, dict):
            continue
        try:
            quantity = int(item.get("quantity"))
        except (TypeError, ValueError):
            quantity = 1
        if quantity <= 0:
            quantity = 1
        rule = {"quantity": quantity}
        min_price = _optional_float(item.get("min_price"))
        if min_price is not None and min_price > 0:
            rule["min_price"] = min_price
        max_price = _optional_float(item.get("max_price"))
        if max_price is not None and max_price > 0:
            rule["max_price"] = max_price
        rules.append(rule)
    return rules


# ── Background-task helpers ────────────────────────────────────────────────

def _start_session(kind: str, runner, params) -> str:
    sid = uuid.uuid4().hex[:12]
    stop_evt = threading.Event()
    approve_evt = threading.Event()
    # `code` is the legacy single-account slot used by _run_login_one.
    # `by_account` is keyed by account_id and used by _run_auth_farm so each
    # concurrent account can be given its own manual OTP independently.
    otp_holder = {"code": None, "by_account": {}}
    driver_holder: dict = {"driver": None, "drivers": [], "lock": threading.Lock()}

    with _SESSIONS_LOCK:
        _SESSIONS[sid] = {
            "kind": kind,
            "stop": stop_evt,
            "approve": approve_evt,
            "otp": otp_holder,
            "driver_holder": driver_holder,
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


def _register_driver(driver_holder: dict, driver) -> None:
    if driver is None:
        return
    lock = driver_holder.get("lock")
    if lock is None:
        driver_holder["driver"] = driver
        return
    with lock:
        drivers = driver_holder.setdefault("drivers", [])
        if all(existing is not driver for existing in drivers):
            drivers.append(driver)
        driver_holder["driver"] = driver


def _unregister_driver(driver_holder: dict, driver) -> None:
    if driver is None:
        return
    lock = driver_holder.get("lock")
    if lock is None:
        if driver_holder.get("driver") is driver:
            driver_holder["driver"] = None
        return
    with lock:
        drivers = [existing for existing in (driver_holder.get("drivers") or []) if existing is not driver]
        driver_holder["drivers"] = drivers
        if driver_holder.get("driver") is driver:
            driver_holder["driver"] = drivers[-1] if drivers else None


def _snapshot_drivers(driver_holder: dict) -> list:
    lock = driver_holder.get("lock")
    if lock is None:
        driver = driver_holder.get("driver")
        return [driver] if driver is not None else []
    with lock:
        drivers = list(driver_holder.get("drivers") or [])
        current = driver_holder.get("driver")
        if current is not None and all(existing is not current for existing in drivers):
            drivers.append(current)
        return drivers


def _log_fn_for(sid: str):
    def log(msg: str, level: str = "info"):
        _event(sid, type="log", level=level, message=msg)
    return log


def _parse_scheduled_ts(scheduled_at: str, scheduled_tz: str) -> float | None:
    """Parse "YYYY-MM-DDTHH:MM[:SS]" plus an IANA tz into unix seconds."""
    raw = (scheduled_at or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            naive = datetime.strptime(raw, fmt)
            break
        except ValueError:
            naive = None
    if naive is None:
        return None
    tz_name = (scheduled_tz or "").strip() or "UTC"
    try:
        zone = ZoneInfo(tz_name)
    except Exception:
        try:
            zone = ZoneInfo("UTC")
        except Exception:
            zone = UTC
    return naive.replace(tzinfo=zone).timestamp()


def _approval_config(params: dict) -> dict:
    return {
        "webhook_url": str(params.get("approval_webhook_url") or "").strip(),
        "poll_url": str(params.get("approval_poll_url") or "").strip(),
        "secret": str(params.get("approval_secret") or "").strip(),
        "poll_interval_seconds": max(1.0, min(15.0, float(params.get("approval_poll_interval_seconds") or 2))),
    }


def _approval_headers(secret: str) -> dict:
    headers = {
        "User-Agent": "DiceBotTK/approval-bridge",
        "Accept": "application/json",
    }
    if secret:
        headers["Authorization"] = f"Bearer {secret}"
    return headers


def _build_approval_payload(
    sid: str,
    account: dict,
    client,
    params: dict,
    approval_id: str,
    expires_at_iso: str,
    expires_in_seconds: int,
) -> dict:
    source = str(params.get("approval_source") or params.get("source") or "dashboard_cart")
    monitor_id = params.get("monitor_id")
    monitor_name = params.get("monitor_name")
    quantity = int(params.get("quantity") or 1)
    unit_price = float(client.ticketPrice or 0)

    return {
        "event": "checkout.approval_requested",
        "version": "2026-04-19.1",
        "delivery_id": str(uuid.uuid4()),
        "sent_at": _ts(),
        "approval": {
            "approval_id": approval_id,
            "session_id": sid,
            "status": "pending",
            "expires_at": expires_at_iso,
            "expires_in_seconds": expires_in_seconds,
        },
        "context": {
            "source": source,
            "monitor_id": int(monitor_id) if monitor_id not in (None, "") else None,
            "monitor_name": monitor_name or None,
            "scheduled_at": params.get("scheduled_at") or None,
            "scheduled_tz": params.get("scheduled_tz") or None,
        },
        "account": {
            "id": int(account.get("id") or 0) or None,
            "name": account.get("name") or "",
            "phone": account.get("phone") or "",
            "email": account.get("email") or "",
        },
        "event_data": {
            "url": client.eventUrl or params.get("event_url") or "",
            "name": client.eventName or "",
            "date": client.eventDate or "",
            "venue": client.eventVenue or "",
        },
        "ticket": {
            "ticket_type_id": str(client.ticketTypeId or params.get("ticket_type_id") or ""),
            "tier_name": client.ticketName or params.get("ticket_tier") or "",
            "unit_price": unit_price,
            "currency": client.ticketCurrency or "USD",
            "quantity": quantity,
            "estimated_subtotal": unit_price * quantity,
        },
    }


def _post_approval_request(config: dict, payload: dict, log) -> tuple[bool, str]:
    if not config["webhook_url"]:
        return False, "Approval webhook URL is not configured"
    try:
        resp = requests.post(
            config["webhook_url"],
            json=payload,
            headers={
                **_approval_headers(config["secret"]),
                "Content-Type": "application/json",
            },
            timeout=10,
        )
    except Exception as exc:
        return False, str(exc)
    if 200 <= resp.status_code < 300:
        return True, ""
    body = (resp.text or "").strip()
    if len(body) > 240:
        body = body[:240] + "…"
    return False, f"HTTP {resp.status_code}{f': {body}' if body else ''}"


def _poll_approval_status(config: dict, approval_id: str, sid: str, log) -> tuple[str, dict | None, str | None]:
    if not config["poll_url"]:
        return "pending", None, None
    try:
        resp = requests.get(
            config["poll_url"],
            params={"approval_id": approval_id, "session_id": sid},
            headers=_approval_headers(config["secret"]),
            timeout=10,
        )
    except Exception as exc:
        return "pending", None, str(exc)

    if resp.status_code == 404:
        return "pending", None, None
    if not (200 <= resp.status_code < 300):
        return "pending", None, f"HTTP {resp.status_code}"

    try:
        payload = resp.json() if resp.content else {}
    except Exception as exc:
        return "pending", None, f"Invalid JSON: {exc}"

    status = str(payload.get("status") or "pending").strip().lower()
    if status not in {"pending", "approved", "declined"}:
        status = "pending"
    return status, payload, None


def _wait_for_checkout_approval(
    sid: str,
    account: dict,
    client,
    params: dict,
    requested_quantity: int,
    final_quantity: int,
    quantity_warning: str,
    stop_evt: threading.Event,
    approve_evt: threading.Event,
    emit_update,
    log,
) -> tuple[bool, str | None]:
    config = _approval_config(params)
    remote_enabled = bool(config["webhook_url"] and config["poll_url"])
    approval_id = f"apr_{uuid.uuid4().hex[:26]}"
    expires_at = datetime.now(UTC) + timedelta(seconds=_APPROVAL_TTL_SECONDS)
    expires_at_iso = expires_at.isoformat().replace("+00:00", "Z")

    emit_update(
        status="reserved",
        ticket_name=client.ticketName,
        ticket_price=client.ticketPrice,
        ticket_currency=client.ticketCurrency,
        event_name=client.eventName,
        ttl=_APPROVAL_TTL_SECONDS,
        quantity=final_quantity,
        total_price=(float(client.ticketPrice or 0) * final_quantity),
        requested_quantity=requested_quantity,
        quantity_warning=quantity_warning,
        approval_id=approval_id,
        approval_channel="webhook" if remote_enabled else "local",
        approval_expires_at=expires_at_iso,
    )
    _event(sid, type="await_approval", approval_id=approval_id)

    if remote_enabled:
        payload = _build_approval_payload(
            sid=sid,
            account=account,
            client=client,
            params=params,
            approval_id=approval_id,
            expires_at_iso=expires_at_iso,
            expires_in_seconds=_APPROVAL_TTL_SECONDS,
        )
        sent_ok, send_error = _post_approval_request(config, payload, log)
        if sent_ok:
            log(f"Approval webhook sent ({approval_id}).")
        else:
            log(f"Approval webhook failed ({approval_id}): {send_error}", "warning")
            emit_update(
                status="reserved",
                approval_id=approval_id,
                approval_channel="webhook",
                approval_error=send_error,
            )

    deadline = time.time() + _APPROVAL_TTL_SECONDS
    next_poll_at = 0.0
    last_poll_error = None

    while not stop_evt.is_set():
        remaining = deadline - time.time()
        if remaining <= 0:
            break

        if approve_evt.wait(timeout=min(0.5, remaining)):
            if remote_enabled:
                log(f"Checkout approved locally while webhook approval was active ({approval_id}).")
            return True, None

        if remote_enabled and time.time() >= next_poll_at:
            next_poll_at = time.time() + config["poll_interval_seconds"]
            status, payload, poll_error = _poll_approval_status(config, approval_id, sid, log)
            if poll_error and poll_error != last_poll_error:
                last_poll_error = poll_error
                log(f"Approval poll issue ({approval_id}): {poll_error}", "warning")
            if status == "approved":
                actor = ((payload or {}).get("actor") or {}).get("name") or ((payload or {}).get("actor") or {}).get("id") or "remote reviewer"
                log(f"Webhook approval received from {actor} ({approval_id}).")
                return True, None
            if status == "declined":
                actor = ((payload or {}).get("actor") or {}).get("name") or ((payload or {}).get("actor") or {}).get("id") or "remote reviewer"
                note = (payload or {}).get("note") or ""
                emit_update(status="declined", approval_id=approval_id)
                return False, f"Webhook approval declined by {actor}{f' — {note}' if note else ''}"

    if stop_evt.is_set():
        return False, "stopped"
    return False, "Approval timeout — ticket released"


def _cart_response_error(resp) -> str:
    if resp is None:
        return "no response"
    try:
        payload = resp.json() if resp.content else {}
        body = (
            payload.get("message")
            or payload.get("error")
            or payload.get("detail")
            or ""
        )
    except Exception:
        body = (resp.text or "").strip()
    if len(body) > 240:
        body = body[:240] + "…"
    return f"HTTP {resp.status_code}{f': {body}' if body else ''}"


def _claim_cart_access_code(client, log) -> tuple[bool, str]:
    if not getattr(client, "code", ""):
        return False, "no access code provided"
    if getattr(client, "accessCodeClaimed", False):
        return True, ""
    try:
        resp = client.claim_code()
    except Exception as exc:
        return False, str(exc)
    if resp is not None and 200 <= resp.status_code < 300:
        return True, ""
    return False, _cart_response_error(resp)


_PRE_DROP_FIRE_WINDOW = 240.0  # default fire-window length when enabled


def _wait_and_poll_for_drop(
    client,
    target_ts: float,
    stop_evt: threading.Event,
    log,
    fire_window_seconds: float = _PRE_DROP_FIRE_WINDOW,
) -> bool:
    """Wait until a reserve token can be fired.

    Behavior: hold (do not fire) until we are within `fire_window_seconds`
    of the scheduled drop. Once inside the window, fire as soon as a fresh
    reserve_token is available — including a re-fetch right at fire time so
    we avoid stale tokens that Dice may have invalidated during the wait.

    With `fire_window_seconds=0`, behavior is strict: hold until exactly the
    scheduled drop instant, then refresh and fire.
    """
    poll_interval = 0.08
    pre_window_poll_interval = 1.0
    post_drop_grace = 30.0
    code_claim_interval = 0.5
    last_code_claim = 0.0
    fire_window_seconds = max(0.0, float(fire_window_seconds or 0.0))
    fire_window_start = target_ts - fire_window_seconds

    def maybe_claim_code() -> bool:
        nonlocal last_code_claim
        if not getattr(client, "code", "") or getattr(client, "accessCodeClaimed", False):
            return True
        now = time.time()
        if now < target_ts:
            return False
        if now - last_code_claim < code_claim_interval:
            return False
        last_code_claim = now
        ok, error = _claim_cart_access_code(client, log)
        if ok:
            log("Access code claimed; polling unlocked ticket tiers.")
        else:
            log(f"Access code claim not accepted yet: {error}", "warning")
        return ok

    # Phase 1: before fire window opens. Hold any token, poll lightly for one
    # if missing (so we're armed when the window opens).
    held_logged = False
    while time.time() < fire_window_start:
        if stop_evt.is_set():
            return False
        if client.reserveToken:
            if not held_logged:
                if fire_window_seconds > 0:
                    log(
                        f"reserve_token held — waiting for fire window "
                        f"(opens at T-{fire_window_seconds:.0f}s, "
                        f"in {fire_window_start - time.time():.0f}s)."
                    )
                else:
                    log(
                        f"reserve_token held — waiting for scheduled drop "
                        f"(in {target_ts - time.time():.0f}s)."
                    )
                held_logged = True
            remaining = fire_window_start - time.time()
            if remaining <= 0:
                break
            if stop_evt.wait(timeout=min(remaining, pre_window_poll_interval)):
                return False
            continue
        try:
            client.fetch_ticket_types(authenticated=True)
            if client.reserveToken and not held_logged:
                log(
                    f"reserve_token acquired early — holding for fire window "
                    f"(opens in {max(0.0, fire_window_start - time.time()):.0f}s)."
                )
                held_logged = True
        except Exception as exc:
            log(f"Pre-window poll error: {exc}", "warning")
        if stop_evt.wait(timeout=pre_window_poll_interval):
            return False

    # Phase 2: inside the fire window — fire the moment we have a fresh token.
    log(
        f"Fire window open (T-{max(0.0, target_ts - time.time()):.0f}s) — "
        "refreshing tier state and firing as soon as a reserve_token is available."
    )
    deadline = target_ts + post_drop_grace
    while time.time() < deadline:
        if stop_evt.is_set():
            return False
        try:
            if getattr(client, "code", "") and time.time() >= target_ts and not maybe_claim_code():
                if stop_evt.wait(timeout=poll_interval):
                    return False
                continue
            client.fetch_ticket_types(authenticated=True)
            if client.reserveToken:
                log("Fresh reserve_token — firing.")
                return True
        except Exception as exc:
            log(f"Fire-window poll error: {exc}", "warning")
        if stop_evt.wait(timeout=poll_interval):
            return False

    return False


def _split_phone(phone: str) -> tuple[str, str]:
    """Return (country_iso, local_digits) — dice.fm expects local digits only.

    Every number is assumed to be US. Strips a leading "1" for the
    11-digit US format (e.g. 14155551234 → 4155551234).
    """
    import re
    raw_phone = str(phone or "").strip()
    if re.fullmatch(r"\+?\d+\.0+", raw_phone):
        raw_phone = raw_phone.split(".", 1)[0]
    digits = re.sub(r"\D", "", raw_phone)
    if digits.startswith("1") and len(digits) == 11:
        return "us", digits[1:]
    return "us", digits


# ── Auth: login single / farm ──────────────────────────────────────────────

def _run_login_one(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    import auth_harvester

    account = params["account"]
    keep_open = bool(params.get("keep_open"))
    manual_phone = bool(params.get("manual_phone"))
    log = _log_fn_for(sid)
    _event(sid, type="status", status="starting", manual=manual_phone)
    country_iso, local_digits = _split_phone(account.get("phone") or "")
    local_driver_ref = {"driver": None}

    def _otp_getter():
        # Prefer per-account slot, fall back to the legacy global slot.
        aid = account.get("id")
        if aid is not None:
            try:
                pa = otp_holder.get("by_account", {})
                cached = pa.get(int(aid))
                if cached:
                    pa[int(aid)] = None
                    return cached
            except (TypeError, ValueError):
                pass
        code = otp_holder.get("code")
        if code:
            otp_holder["code"] = None
        return code

    def _otp_notifier(**kwargs):
        _event(sid, type="await_otp", account_id=account.get("id"), **kwargs)

    result = auth_harvester.login_single_account(
        phone=local_digits,
        country_iso=account.get("country_iso") or country_iso,
        email=account.get("email"),
        proxy=account.get("proxy"),
        session_dir=_session_dir(),
        aycd_key=account.get("aycd_key"),
        imap_email=account.get("imap_email") or account.get("email"),
        imap_password=account.get("imap_password"),
        imap_host="imap.gmail.com",
        session_phone=account.get("phone") or local_digits,
        log_fn=log,
        on_driver=lambda d: (
            local_driver_ref.update(driver=d),
            _register_driver(driver_holder, d),
        ),
        keep_open_on_success=keep_open,
        manual_phone=manual_phone,
        manual_otp_getter=_otp_getter,
        manual_otp_notifier=_otp_notifier,
    )
    if not keep_open:
        _unregister_driver(driver_holder, local_driver_ref.get("driver"))

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


def _require_license() -> None:
    # No-op in TK — licensing is not enforced. Call sites kept to mirror CLI
    # for low-friction future syncs.
    pass


def m_auth_login_one(params):
    _require_license()
    sid = _start_session("auth_one", _run_login_one, params)
    return {"session_id": sid}


def m_auth_open_profile(params):
    _require_license()
    sid = _start_session("auth_open_profile", _run_login_one, {**params, "keep_open": True})
    return {"session_id": sid}


def m_auth_manual_login_one(params):
    _require_license()
    sid = _start_session("auth_manual_one", _run_login_one, {**params, "manual_phone": True})
    return {"session_id": sid}


def _run_auth_state_refresh(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    import dice_requests

    accounts = list(params.get("accounts") or [])
    total = len(accounts)
    log = _log_fn_for(sid)

    if not total:
        log("No cached sessions to validate.")
        _event(sid, type="done", ok=True, total=0, valid=0, revoked=0, skipped=0)
        return

    log(f"Refreshing auth state for {total} cached session{'s' if total != 1 else ''}.")
    valid = 0
    revoked = 0
    skipped = 0

    for idx, account in enumerate(accounts, start=1):
        if stop_evt.is_set():
            _event(
                sid,
                type="done",
                ok=False,
                error="stopped",
                total=total,
                valid=valid,
                revoked=revoked,
                skipped=skipped,
            )
            return

        account_id = int(account.get("id") or 0)
        phone_label = str(account.get("phone") or account.get("email") or f"Account {idx}")
        _event(
            sid,
            type="auth_state_update",
            account_id=account_id,
            status="checking",
            checked=idx - 1,
            total=total,
            valid=valid,
            revoked=revoked,
            skipped=skipped,
        )

        def account_log(msg: str, level: str = "info", prefix=f"[{idx}/{total}] {phone_label}: "):
            log(prefix + msg, level)

        client = dice_requests.DiceFM(
            phone=account.get("phone") or "",
            email=account.get("email") or "",
            event_url="https://dice.fm/event/refresh-auth-state",
            proxy_string=account.get("proxy"),
            session_dir=_session_dir(),
            log_fn=account_log,
        )

        if _load_cart_session(client, account, account_log):
            valid += 1
            log(f"[{idx}/{total}] {phone_label}: session valid.")
            _event(
                sid,
                type="auth_state_update",
                account_id=account_id,
                status="ok",
                checked=idx,
                total=total,
                valid=valid,
                revoked=revoked,
                skipped=skipped,
            )
            continue

        remaining = db.get_session(account_id) if account_id else None
        if remaining is None:
            revoked += 1
            log(f"[{idx}/{total}] {phone_label}: session revoked or expired; re-auth required.", "warning")
            status = "invalid"
        else:
            skipped += 1
            log(f"[{idx}/{total}] {phone_label}: could not verify live session; cached state left unchanged.", "warning")
            status = "skipped"

        _event(
            sid,
            type="auth_state_update",
            account_id=account_id,
            status=status,
            checked=idx,
            total=total,
            valid=valid,
            revoked=revoked,
            skipped=skipped,
        )

    _event(
        sid,
        type="done",
        ok=True,
        total=total,
        valid=valid,
        revoked=revoked,
        skipped=skipped,
    )


def m_auth_refresh_state(params):
    _require_license()
    group_id = params.get("group_id")
    accounts = db.get_accounts_with_valid_session(int(group_id) if group_id is not None else None)
    sid = _start_session("auth_refresh_state", _run_auth_state_refresh, {**params, "accounts": accounts})
    return {"session_id": sid, "total": len(accounts)}


def _run_auth_farm(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    import auth_harvester

    accounts = list(params.get("accounts") or [])
    log = _log_fn_for(sid)
    requested_concurrency = max(1, int(params.get("concurrency") or 1))
    concurrency = min(requested_concurrency, max(len(accounts), 1))
    max_passes = int(params.get("max_passes") or 0)  # 0 = loop forever until all ok or stopped
    pass_backoff_seconds = float(params.get("pass_backoff_seconds") or 15.0)
    _event(sid, type="status", status="starting", total=len(accounts), concurrency=concurrency)

    if not accounts:
        _event(sid, type="done", ok=True)
        return

    cleared = db.clear_proxies_without_valid_session()
    if cleared:
        log(f"Cleared {cleared} stale proxy assignment{'s' if cleared != 1 else ''} from unauthenticated account{'s' if cleared != 1 else ''}.")

    auth_proxy_pool = [str(x).strip() for x in (params.get("auth_proxy_pool") or []) if str(x).strip()]
    auth_proxy_pool = list(dict.fromkeys(auth_proxy_pool))
    proxy_lock = threading.Lock()
    proxy_cursor = 0
    proxies_in_flight: set[str] = set()
    proxies_assigned_this_run: set[str] = set()
    proxies_retained_by_valid_sessions = {
        str(row.get("proxy") or "").strip()
        for row in db.get_accounts(None)
        if str(row.get("proxy") or "").strip()
        and db.session_status(row.get("session_saved_at")) in {"active", "expiring"}
    }
    if auth_proxy_pool:
        retained = len(proxies_retained_by_valid_sessions)
        suffix = f"; {retained} already retained by authed accounts" if retained else ""
        log(f"Auth proxy pool loaded — {len(auth_proxy_pool)} available{suffix}.")

    def borrow_auth_proxy() -> str | None:
        nonlocal proxy_cursor
        if not auth_proxy_pool:
            return None
        with proxy_lock:
            locked = proxies_retained_by_valid_sessions | proxies_assigned_this_run | proxies_in_flight
            pool_len = len(auth_proxy_pool)
            for offset in range(pool_len):
                idx = (proxy_cursor + offset) % pool_len
                candidate = auth_proxy_pool[idx]
                if candidate in locked:
                    continue
                proxy_cursor = (idx + 1) % pool_len
                proxies_in_flight.add(candidate)
                return candidate

            candidate = auth_proxy_pool[proxy_cursor % pool_len]
            proxy_cursor = (proxy_cursor + 1) % pool_len
            proxies_in_flight.add(candidate)
            log("Auth proxy pool is fully allocated; reusing a pool proxy for this attempt.", "warning")
            return candidate

    def finish_auth_proxy(proxy: str | None, *, success: bool) -> None:
        if not proxy:
            return
        with proxy_lock:
            proxies_in_flight.discard(proxy)
            if success:
                proxies_assigned_this_run.add(proxy)

    total = len(accounts)
    success_ids: set[int] = set()
    failure_errors: dict[int, str] = {}

    def process_account(worker_idx: int, item_idx: int, account: dict, pass_num: int, pass_total: int) -> bool:
        if stop_evt.is_set():
            return False
        phone_label = str(account.get("phone") or "")
        log(f"[pass {pass_num} · W{worker_idx} · {item_idx}/{pass_total}] {phone_label}")
        country_iso, local_digits = _split_phone(account.get("phone") or "")
        local_driver_ref = {"driver": None}
        account_id = int(account["id"])
        # Drop any stale manual OTP the user may have submitted on a prior
        # pass — Dice OTPs are one-time-use and reusing a stale code wastes
        # a "Send code" attempt and risks rate-limiting the phone number.
        try:
            otp_holder.get("by_account", {}).pop(account_id, None)
        except AttributeError:
            pass
        account_has_valid_auth = db.account_has_valid_session(account_id)
        retained_proxy = str(account.get("proxy") or "").strip() if account_has_valid_auth else ""
        borrowed_proxy = None if account_has_valid_auth else borrow_auth_proxy()
        auth_proxy = retained_proxy or borrowed_proxy
        proxy_for_event = auth_proxy or ""
        proxy_origin = "retained" if retained_proxy else ("borrowed" if borrowed_proxy else "none")
        started_at = time.time()
        _event(
            sid, type="auth_update", account_id=account["id"], status="running",
            phone=phone_label, email=account.get("email") or "",
            proxy=proxy_for_event, proxy_origin=proxy_origin, started_at=started_at,
        )
        if borrowed_proxy:
            log(f"{phone_label}: borrowed auth proxy from pool.")
        elif retained_proxy:
            log(f"{phone_label}: retaining existing proxy from active session.")
        def _farm_otp_getter():
            try:
                pa = otp_holder.get("by_account", {})
                cached = pa.get(account_id)
                if cached:
                    pa[account_id] = None
                    return cached
            except (TypeError, AttributeError):
                pass
            return None

        def _farm_otp_notifier(**kwargs):
            _event(sid, type="await_otp", account_id=account_id, **kwargs)

        try:
            result = auth_harvester.login_single_account(
                phone=local_digits,
                country_iso=account.get("country_iso") or country_iso,
                email=account.get("email"),
                proxy=auth_proxy,
                session_dir=_session_dir(),
                aycd_key=account.get("aycd_key"),
                imap_email=account.get("imap_email") or account.get("email"),
                imap_password=account.get("imap_password"),
                imap_host="imap.gmail.com",
                session_phone=account.get("phone") or local_digits,
                log_fn=log,
                on_driver=lambda d: (
                    local_driver_ref.update(driver=d),
                    _register_driver(driver_holder, d),
                ),
                manual_otp_getter=_farm_otp_getter,
                manual_otp_notifier=_farm_otp_notifier,
            )
            if result.get("ok") and result.get("bearer_token"):
                db.save_session(account_id, result["bearer_token"], phone=account["phone"])
                if borrowed_proxy:
                    db.update_account(account_id, proxy=borrowed_proxy)
                    account["proxy"] = borrowed_proxy
                    log(f"{phone_label}: assigned auth proxy to account.")
                _event(
                    sid, type="auth_update", account_id=account_id, status="ok",
                    phone=phone_label, email=account.get("email") or "",
                    proxy=proxy_for_event, proxy_origin=proxy_origin,
                    started_at=started_at, finished_at=time.time(),
                    duration_s=round(time.time() - started_at, 2),
                )
                finish_auth_proxy(borrowed_proxy, success=True)
                return True
            error = result.get("error") or "unknown error"
            failure_errors[account_id] = error
            _event(
                sid, type="auth_update", account_id=account_id, status="fail", error=error,
                phone=phone_label, email=account.get("email") or "",
                proxy=proxy_for_event, proxy_origin=proxy_origin,
                started_at=started_at, finished_at=time.time(),
                duration_s=round(time.time() - started_at, 2),
            )
            finish_auth_proxy(borrowed_proxy, success=False)
            return False
        except Exception as exc:
            log(f"{phone_label}: {exc}", "error")
            failure_errors[account_id] = str(exc)
            _event(
                sid, type="auth_update", account_id=account_id, status="fail", error=str(exc),
                phone=phone_label, email=account.get("email") or "",
                proxy=proxy_for_event, proxy_origin=proxy_origin,
                started_at=started_at, finished_at=time.time(),
                duration_s=round(time.time() - started_at, 2),
            )
            finish_auth_proxy(borrowed_proxy, success=False)
            return False
        finally:
            _unregister_driver(driver_holder, local_driver_ref.get("driver"))

    pass_num = 0
    pending = list(accounts)
    while pending and not stop_evt.is_set():
        pass_num += 1
        pass_total = len(pending)
        log(f"── Auth pass {pass_num} — {pass_total} pending account{'s' if pass_total != 1 else ''}, concurrency {concurrency}")
        _event(
            sid,
            type="auth_pass",
            pass_number=pass_num,
            pending=pass_total,
            total=total,
            succeeded=len(success_ids),
        )

        work_q: queue.Queue[tuple[int, dict]] = queue.Queue()
        for idx, account in enumerate(pending, start=1):
            work_q.put((idx, account))

        pass_successes: set[int] = set()
        pass_successes_lock = threading.Lock()

        def worker_loop(worker_idx: int) -> None:
            while not stop_evt.is_set():
                try:
                    item_idx, account = work_q.get_nowait()
                except queue.Empty:
                    return
                try:
                    ok = process_account(worker_idx, item_idx, account, pass_num, pass_total)
                    if ok:
                        with pass_successes_lock:
                            pass_successes.add(int(account["id"]))
                finally:
                    work_q.task_done()

        threads = [
            threading.Thread(target=worker_loop, args=(worker_idx,), daemon=True)
            for worker_idx in range(1, concurrency + 1)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        success_ids.update(pass_successes)
        pending = [a for a in pending if int(a["id"]) not in success_ids]

        log(
            f"── Pass {pass_num} complete — {len(pass_successes)} authed, "
            f"{len(pending)} remaining (total {len(success_ids)}/{total})"
        )

        if not pending:
            break
        if stop_evt.is_set():
            break
        if max_passes and pass_num >= max_passes:
            log(f"Reached max_passes={max_passes}; stopping with {len(pending)} still pending.", "warning")
            break

        # Brief cool-down before next pass so rate limiting eases.
        log(f"Waiting {int(pass_backoff_seconds)}s before next pass…")
        if stop_evt.wait(timeout=pass_backoff_seconds):
            break

    summary_ok = not pending
    _event(
        sid,
        type="done",
        ok=summary_ok,
        total=total,
        succeeded=len(success_ids),
        failed=len(pending),
        passes=pass_num,
    )


def m_auth_farm(params):
    _require_license()
    sid = _start_session("auth_farm", _run_auth_farm, params)
    return {"session_id": sid}


# ── Cart / purchase ────────────────────────────────────────────────────────

def _load_cart_session(client, account: dict, log) -> bool:
    if client.load_session():
        return True

    account_id = int(account.get("id") or 0)
    if not account_id:
        return False

    session = db.get_session(account_id)
    if not session:
        return False

    saved_at = float(session.get("saved_at") or 0)
    if not saved_at or (time.time() - saved_at) >= db.SESSION_MAX_AGE:
        return False

    token = str(session.get("bearer_token") or "").strip()
    if not token:
        return False

    log("Session file missing or stale; retrying with the DB-backed token.")
    client.bearerToken = token
    device_id = str(session.get("device_id") or "").strip()
    if device_id:
        client.deviceId = device_id

    try:
        resp = client._api_get("/users/me", api_ts="2024-03-25")
    except Exception as exc:
        log(f"DB-backed session validation failed: {exc}", "warning")
        client.bearerToken = None
        return False

    if resp.status_code != 200:
        if resp.status_code in (401, 403):
            db.delete_session(account_id)
            log("DB-backed session was expired/revoked; cleared cached session.", "warning")
        else:
            log(f"DB-backed session validation returned {resp.status_code}.", "warning")
        client.bearerToken = None
        return False

    try:
        client.save_session()
    except Exception as exc:
        log(f"Could not sync session file from DB token: {exc}", "warning")
    return True

def _run_cart_inner(
    sid: str,
    account: dict,
    event_url: str,
    params: dict,
    stop_evt: threading.Event,
    driver_holder: dict,
    approve_evt: threading.Event,
):
    import dice_requests

    log = _log_fn_for(sid)

    def emit_update(**fields):
        _event(sid, type="cart_update", account_id=account.get("id"), **fields)

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
        ticket_type_id=params.get("ticket_type_id"),
        tier_strategy=params.get("tier_strategy"),
        tier_keywords=params.get("tier_keywords"),
        allowed_tier_ids=params.get("allowed_tier_ids"),
        price_rules=_clean_price_rules(params.get("price_rules")),
        session_dir=_session_dir(),
        log_fn=log,
    )

    if not _load_cart_session(client, account, log):
        return False, "No valid session — run auth first"

    client.visit_event_page()
    if stop_evt.is_set():
        return False, "stopped"
    if not client.eventId:
        return False, f"Could not extract event ID from event URL/page: {client.eventUrl or event_url}"

    presale_code = (params.get("presale_code") or "").strip()
    target_ts = _parse_scheduled_ts(params.get("scheduled_at") or "", params.get("scheduled_tz") or "")
    scheduled_future = target_ts is not None and target_ts > time.time()
    access_code_error = ""
    if presale_code:
        if scheduled_future:
            log(f"Access code '{presale_code}' assigned to this profile — will claim exactly when the sale goes live.")
        else:
            log(f"Access code '{presale_code}' assigned to this profile — claiming now.")
            code_ok, access_code_error = _claim_cart_access_code(client, log)
            if code_ok:
                log(f"Access code '{presale_code}' accepted.")
            else:
                return False, f"Access code claim failed: {access_code_error}"

    probe = client.fetch_ticket_types(authenticated=True)
    if probe is None:
        return False, "Failed to fetch event"

    if client.eventIsLocked:
        if not presale_code:
            return False, "Event is locked — add an access code to this cart"
        if not getattr(client, "accessCodeClaimed", False):
            if scheduled_future:
                log(f"Event is locked; access code '{presale_code}' claim is deferred until the scheduled sale time.")
            else:
                log(f"Event is locked — claiming access code '{presale_code}'.")
                code_ok, access_code_error = _claim_cart_access_code(client, log)
                if code_ok:
                    client.fetch_ticket_types(authenticated=True)
                else:
                    return False, f"Access code claim failed: {access_code_error}"

    # TK fires strictly on drop — no pre-drop fire window. Ignore any
    # incoming pre_drop_fire_window_enabled / fire_mode params.
    fire_window_seconds = 0.0

    has_probe_token = client.reserveToken and not (
        presale_code and scheduled_future and not getattr(client, "accessCodeClaimed", False)
    )
    in_fire_window = scheduled_future and (target_ts - time.time()) <= fire_window_seconds
    if has_probe_token and (not scheduled_future or in_fire_window):
        if scheduled_future and fire_window_seconds > 0:
            log(
                f"Reserve token held from probe and inside {fire_window_seconds:.0f}s fire window "
                f"(T-{target_ts - time.time():.1f}s) — firing now."
            )
        elif scheduled_future:
            log(
                f"Reserve token held from probe at scheduled drop "
                f"(T-{target_ts - time.time():.1f}s) — firing now."
            )
        else:
            log("Reserve token already held from probe — firing immediately.")
    elif scheduled_future:
        if has_probe_token and fire_window_seconds > 0:
            log(
                f"Reserve token already held from probe — holding until {fire_window_seconds:.0f}s "
                f"before drop at {params.get('scheduled_at')} {params.get('scheduled_tz')} "
                f"(T-{target_ts - time.time():.1f}s)."
            )
        elif has_probe_token:
            log(
                f"Reserve token already held from probe — holding until scheduled drop "
                f"at {params.get('scheduled_at')} {params.get('scheduled_tz')} "
                f"(T-{target_ts - time.time():.1f}s)."
            )
        elif fire_window_seconds > 0:
            log(
                f"Scheduled drop at {params.get('scheduled_at')} {params.get('scheduled_tz')} "
                f"(T-{target_ts - time.time():.1f}s) — will fire as soon as a reserve_token is "
                f"available within {fire_window_seconds:.0f}s of drop."
            )
        else:
            log(
                f"Scheduled drop at {params.get('scheduled_at')} {params.get('scheduled_tz')} "
                f"(T-{target_ts - time.time():.1f}s) — will fire at the scheduled instant only."
            )
        emit_update(
            status="armed",
            scheduled_at=params.get("scheduled_at"),
            scheduled_tz=params.get("scheduled_tz"),
        )
        got_token = _wait_and_poll_for_drop(
            client, target_ts, stop_evt, log, fire_window_seconds=fire_window_seconds,
        )
        if stop_evt.is_set():
            return False, "stopped"
        if not got_token:
            if presale_code and not getattr(client, "accessCodeClaimed", False):
                error = getattr(client, "lastAccessCodeError", None) or access_code_error
                if error:
                    return False, f"No tickets available after drop; access code claim failed: {error}"
            return False, "No tickets available after drop"
    else:
        if not client.reserveToken:
            if presale_code and not getattr(client, "accessCodeClaimed", False):
                log(f"Retrying access code '{presale_code}' before final ticket check.")
                code_ok, access_code_error = _claim_cart_access_code(client, log)
                if code_ok:
                    log(f"Access code '{presale_code}' accepted.")
            client.fetch_ticket_types(authenticated=True)
        if not client.reserveToken:
            if presale_code and not getattr(client, "accessCodeClaimed", False) and access_code_error:
                return False, f"No tickets available; access code claim failed: {access_code_error}"
            return False, "No tickets available"

    if stop_evt.is_set():
        return False, "stopped"

    rule_quantity = getattr(client, "selected_quantity", None)
    requested_quantity = max(1, int(rule_quantity if rule_quantity else (params.get("quantity") or 1)))
    allowed_quantity = max(1, int(getattr(client, "ticketMaxPerOrder", 0) or requested_quantity))
    final_quantity = min(requested_quantity, allowed_quantity)
    quantity_warning = ""
    if requested_quantity > allowed_quantity:
        quantity_warning = (
            f"Requested {requested_quantity} ticket(s), but {client.ticketName or 'this tier'} "
            f"allows max {allowed_quantity}. Using {allowed_quantity}."
        )
        log(quantity_warning)

    # TK has auto-checkout disabled — every cart waits for approval, regardless
    # of what the params say. Webhook approval still works alongside in-app.
    mode = "manual"
    approval_cfg = _approval_config(params)
    approval_required = True
    if approval_required:
        approved, approval_error = _wait_for_checkout_approval(
            sid=sid,
            account=account,
            client=client,
            params={**params, "quantity": final_quantity},
            requested_quantity=requested_quantity,
            final_quantity=final_quantity,
            quantity_warning=quantity_warning,
            stop_evt=stop_evt,
            approve_evt=approve_evt,
            emit_update=emit_update,
            log=log,
        )
        if not approved:
            return False, approval_error or "Approval declined"

    emit_update(
        status="purchasing",
        ticket_name=client.ticketName,
        ticket_price=client.ticketPrice,
        ticket_currency=client.ticketCurrency,
        event_name=client.eventName,
        quantity=final_quantity,
        total_price=(float(client.ticketPrice or 0) * final_quantity),
        requested_quantity=requested_quantity,
        quantity_warning=quantity_warning,
    )

    ok = client.run_purchase_flow(
        quantity=final_quantity,
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
        except Exception as exc:
            log(f"Inventory record failed (local DB): {exc}", "error")

        _post_telemetry_async(
            device_id=params.get("telemetry_device_id") or "",
            log=log,
            payload={
                "platform": "dice",
                "event_name": client.eventName or "",
                "venue": client.eventVenue or "",
                "event_date": client.eventDate or "",
                "profile_name": account.get("name") or account.get("phone") or "",
                "account_phone": account.get("phone") or "",
                "quantity": quantity,
                "ticket_family": client.ticketName or "",
                "ticket_currency": client.ticketCurrency or "USD",
                "ticket_price": ticket_price,
                "total_price": total_price,
                "event_url": client.eventUrl or event_url,
                "purchase_id": client.purchaseId or "",
                "status": "acquired",
            },
        )

        emit_update(
            status="purchased",
            purchase_id=client.purchaseId,
            quantity=quantity,
            total_price=total_price,
            event_name=client.eventName,
            ticket_name=client.ticketName,
            ticket_price=client.ticketPrice,
            ticket_currency=client.ticketCurrency,
            requested_quantity=requested_quantity,
            quantity_warning=quantity_warning,
        )
        return True, ""

    emit_update(status="purchase_failed")
    return False, "Purchase failed — see logs"


def _run_cart(sid: str, params: dict, stop_evt: threading.Event, otp_holder: dict, driver_holder: dict, approve_evt: threading.Event):
    account = params["account"]
    event_url = params["event_url"]
    log = _log_fn_for(sid)
    try:
        ok, err = _run_cart_inner(sid, account, event_url, params, stop_evt, driver_holder, approve_evt)
        _event(sid, type="done", ok=ok, error=err or None)
    except Exception as exc:
        log(f"Cart run failed: {exc}", "error")
        _event(sid, type="done", ok=False, error=str(exc))


def m_cart_run(params):
    _require_license()
    account = params.get("account") or {}
    if not db.account_has_valid_session(int(account.get("id") or 0)):
        raise RuntimeError("Account has no valid session — run Auth Farm first")
    sid = _start_session("cart", _run_cart, params)
    return {"session_id": sid}

def m_event_preview(params):
    _require_license()
    event_url = str(params.get("event_url") or "").strip()
    if not event_url:
        raise RuntimeError("event_url is required")
    import venue_monitor
    return venue_monitor.fetch_event_preview(event_url)


# ── Session control ────────────────────────────────────────────────────────

def m_session_stop(params):
    sid = params["session_id"]
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(sid)
    if not sess:
        return {"ok": False, "error": "session not found"}
    sess["stop"].set()
    for driver in _snapshot_drivers(sess.get("driver_holder") or {}):
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
    code = params.get("code")
    account_id = params.get("account_id")
    if account_id is not None:
        try:
            sess["otp"]["by_account"][int(account_id)] = code
        except (TypeError, ValueError):
            sess["otp"]["code"] = code
    else:
        sess["otp"]["code"] = code
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
    "db.get_payment_pools": m_db_get_payment_pools,
    "db.create_payment_pool": m_db_create_payment_pool,
    "db.rename_payment_pool": m_db_rename_payment_pool,
    "db.delete_payment_pool": m_db_delete_payment_pool,
    "db.get_payment_cards": m_db_get_payment_cards,
    "db.get_payment_card": m_db_get_payment_card,
    "db.add_payment_card": m_db_add_payment_card,
    "db.update_payment_card": m_db_update_payment_card,
    "db.delete_payment_card": m_db_delete_payment_card,
    "db.assign_card": m_db_assign_card,
    "db.unassign_card": m_db_unassign_card,
    "db.get_card_labels": m_db_get_card_labels,
    "db.get_assigned_cards_for_account": m_db_get_assigned_cards_for_account,
    "db.bulk_account_cards_by_label": m_db_bulk_account_cards_by_label,
    "db.bulk_add_payment_cards": m_db_bulk_add_payment_cards,
    "db.get_code_pools": m_db_get_code_pools,
    "db.create_code_pool": m_db_create_code_pool,
    "db.rename_code_pool": m_db_rename_code_pool,
    "db.delete_code_pool": m_db_delete_code_pool,
    "db.get_code_pool_codes": m_db_get_code_pool_codes,
    "db.add_code_pool_codes": m_db_add_code_pool_codes,
    "db.delete_code_pool_code": m_db_delete_code_pool_code,
    "db.clear_code_pool": m_db_clear_code_pool,
    "db.get_proxy_pools": m_db_get_proxy_pools,
    "db.create_proxy_pool": m_db_create_proxy_pool,
    "db.rename_proxy_pool": m_db_rename_proxy_pool,
    "db.delete_proxy_pool": m_db_delete_proxy_pool,
    "db.get_proxy_pool_proxies": m_db_get_proxy_pool_proxies,
    "db.add_proxy_pool_proxies": m_db_add_proxy_pool_proxies,
    "db.delete_proxy_pool_proxy": m_db_delete_proxy_pool_proxy,
    "db.clear_proxy_pool": m_db_clear_proxy_pool,
    "db.draw_codes_from_pool": m_db_draw_codes_from_pool,
    "db.import_file": m_db_import_file,
    "db.get_stats": m_db_get_stats,
    "db.get_accounts_needing_auth": m_db_get_accounts_needing_auth,
    "db.get_accounts_with_valid_session": m_db_get_accounts_with_valid_session,
    "db.get_session": m_db_get_session,
    "db.get_inventory_items": m_db_get_inventory_items,
    "db.delete_inventory_item": m_db_delete_inventory_item,
    "auth.login_one": m_auth_login_one,
    "auth.open_profile": m_auth_open_profile,
    "auth.manual_login_one": m_auth_manual_login_one,
    "auth.refresh_state": m_auth_refresh_state,
    "auth.farm": m_auth_farm,
    "cart.run": m_cart_run,
    "event.preview": m_event_preview,
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
