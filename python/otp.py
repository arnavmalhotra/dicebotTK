"""OTP retrieval methods: IMAP and AYCD Inbox."""

from __future__ import annotations

import re
import threading
import time
from datetime import datetime, timedelta


# ── Per-inbox IMAP concurrency gate ────────────────────────────────────────
# Most IMAP providers cap concurrent sessions per account (Gmail ~15). We cap
# lower to leave headroom. Accounts that share an inbox serialize through the
# same semaphore; accounts on distinct inboxes don't contend.
_IMAP_MAX_CONCURRENT = 10
_IMAP_SEM_REGISTRY: dict[tuple[str, str], threading.Semaphore] = {}
_IMAP_SEM_LOCK = threading.Lock()


def _imap_semaphore(host: str, email: str) -> threading.Semaphore:
    key = (host.lower().strip(), email.lower().strip())
    with _IMAP_SEM_LOCK:
        sem = _IMAP_SEM_REGISTRY.get(key)
        if sem is None:
            sem = threading.Semaphore(_IMAP_MAX_CONCURRENT)
            _IMAP_SEM_REGISTRY[key] = sem
        return sem


def _imap_escape(value: str) -> str:
    """Escape double quotes for IMAP literal strings."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def fetch_otp_imap(
    imap_email: str,
    imap_password: str,
    imap_host: str = "imap.gmail.com",
    imap_port: int = 993,
    timeout_seconds: int = 120,
    poll_interval: int = 5,
    recipient_email: str | None = None,
    log_fn=None,
) -> str | None:
    """Poll an IMAP inbox for the latest Dice OTP email and return the 4-digit code.

    If recipient_email is set (and differs from imap_email) the IMAP search is
    restricted to messages with that To: header. This lets many accounts share
    a single forwarded inbox without stealing each other's OTPs.
    """
    import imaplib
    import email as email_lib
    from email.header import decode_header
    from email.utils import parsedate_to_datetime

    log = log_fn or (lambda msg, level="info": print(f"[{level}] {msg}"))
    start_time = time.time()

    to_filter = None
    if recipient_email and recipient_email.strip() and recipient_email.strip().lower() != imap_email.strip().lower():
        to_filter = recipient_email.strip()

    sem = _imap_semaphore(imap_host, imap_email)
    sem.acquire()
    try:
        log(f"Connecting to IMAP ({imap_host}:{imap_port}) as {imap_email}"
            + (f" (filter TO={to_filter})" if to_filter else "") + "...")
        try:
            mail = imaplib.IMAP4_SSL(imap_host, imap_port)
            mail.login(imap_email, imap_password)
        except Exception as exc:
            log(f"IMAP login failed: {exc}", "error")
            return None

        # Build the IMAP SEARCH criteria. Parts are space-joined inside parens.
        parts = ['FROM "noreply@dice.fm"', 'SUBJECT "Login code"']
        if to_filter:
            parts.append(f'TO "{_imap_escape(to_filter)}"')
        search_arg = "(" + " ".join(parts) + ")"

        try:
            deadline = start_time + timeout_seconds
            while time.time() < deadline:
                try:
                    mail.select("INBOX")
                    status, msg_ids = mail.search(None, search_arg)
                    if status != "OK" or not msg_ids or not msg_ids[0]:
                        time.sleep(poll_interval)
                        continue

                    # Walk newest → oldest so we pick the freshest matching mail.
                    id_list = msg_ids[0].split()
                    picked = None
                    for mid in reversed(id_list):
                        status, msg_data = mail.fetch(mid, "(RFC822)")
                        if status != "OK" or not msg_data or not msg_data[0]:
                            continue
                        raw = msg_data[0][1]
                        msg = email_lib.message_from_bytes(raw)

                        # Verify To: header matches when a filter was requested.
                        # IMAP SEARCH TO can be fuzzy on some servers; re-check.
                        if to_filter:
                            to_hdr = (msg.get("To", "") or "").lower()
                            cc_hdr = (msg.get("Cc", "") or "").lower()
                            delivered_to = (msg.get("Delivered-To", "") or "").lower()
                            if (to_filter.lower() not in to_hdr
                                and to_filter.lower() not in cc_hdr
                                and to_filter.lower() not in delivered_to):
                                continue

                        # Skip old mail that predates this fetch attempt.
                        date_str = msg.get("Date", "")
                        if date_str:
                            try:
                                email_ts = parsedate_to_datetime(date_str).timestamp()
                                if email_ts < start_time - 60:
                                    continue
                            except Exception:
                                pass

                        subject = ""
                        for part, charset in decode_header(msg.get("Subject", "")):
                            if isinstance(part, bytes):
                                subject += part.decode(charset or "utf-8", errors="replace")
                            else:
                                subject += part

                        m = re.search(r"Login code:\s*(\d{4})", subject)
                        if not m:
                            continue
                        picked = (mid, m.group(1))
                        break

                    if picked is None:
                        time.sleep(poll_interval)
                        continue

                    mid, otp_code = picked
                    log(f"OTP code found: {otp_code}")
                    try:
                        mail.store(mid, "+FLAGS", "\\Deleted")
                        mail.expunge()
                    except Exception:
                        pass
                    return otp_code

                except Exception as exc:
                    log(f"IMAP poll error: {exc}", "warning")
                    time.sleep(poll_interval)

            log("Timed out waiting for Dice OTP email.", "warning")
            return None

        finally:
            try:
                mail.logout()
            except Exception:
                pass
    finally:
        sem.release()


def fetch_otp_aycd(
    api_key: str,
    email: str,
    timeout_seconds: int = 120,
    log_fn=None,
) -> str | None:
    """Use the AYCD Inbox API to fetch the Dice OTP code from email."""
    log = log_fn or (lambda msg, level="info": print(f"[{level}] {msg}"))

    try:
        import inbox
    except ImportError:
        log("aycd-inbox-api-client package is not installed.", "error")
        return None

    log(f"Initializing AYCD Inbox API for {email}...")
    try:
        inbox.init_mail_task_service_with_api_key(api_key)
        service = inbox.get_mail_task_service()

        task = service.new_mail_task(
            email=email,
            user_template_id="",
            received_at=datetime.now() - timedelta(seconds=30),
            mail_filters=[
                inbox.MailFilter(
                    target=inbox.MAIL_FILTER_TARGET_SUBJECT,
                    comparator=inbox.MAIL_FILTER_COMPARATOR_INCLUDES,
                    value="Login code",
                )
            ],
            mail_elements=[
                inbox.MailElement(
                    name="otpCode",
                    target=inbox.MAIL_ELEMENT_TARGET_SUBJECT,
                    regex=r"\d{4}",
                )
            ],
            timeout=timedelta(seconds=timeout_seconds),
        )

        log("AYCD task created. Waiting for OTP email...")
        response = service.send_and_receive_mail_task(task)
        log(f"AYCD task completed — status: {response.status}")

        if response.status == "error":
            err_msg = ""
            if response.results and isinstance(response.results, dict):
                err_msg = response.results.get("errorMessage", "")
            log(f"AYCD task error: {err_msg or 'unknown error'}", "error")
            return None

        if response.results and isinstance(response.results, dict):
            otp_code = response.results.get("otpCode", "")
            if otp_code:
                log(f"OTP code found via AYCD: {otp_code}")
                return str(otp_code)

        log("AYCD task completed but no OTP code found.", "warning")
        return None

    except Exception as exc:
        log(f"AYCD Inbox API error: {exc}", "error")
        return None
