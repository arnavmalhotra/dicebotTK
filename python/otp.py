"""OTP retrieval methods: IMAP and AYCD Inbox."""

from __future__ import annotations

import html
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
_OTP_PATTERNS = [
    re.compile(r"login code[:\s-]*(\d{4,6})", re.I),
    re.compile(r"dice[^0-9]{0,40}(\d{4,6})", re.I),
    re.compile(r"\b(\d{4,6})\b"),
]
_RECEIVED_FOR_PATTERN = re.compile(r"for\s+<?([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})>?", re.I)


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


def _decode_mime_header(value: str) -> str:
    from email.header import decode_header

    decoded = ""
    for part, charset in decode_header(value or ""):
        if isinstance(part, bytes):
            decoded += part.decode(charset or "utf-8", errors="replace")
        else:
            decoded += part
    return decoded


def _extract_message_text(msg) -> str:
    chunks: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            disposition = str(part.get("Content-Disposition") or "").lower()
            if "attachment" in disposition:
                continue
            try:
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset() or "utf-8"
                text = payload.decode(charset, errors="replace") if payload else ""
            except Exception:
                text = ""
            if not text:
                continue
            if part.get_content_type() == "text/html":
                text = re.sub(r"<[^>]+>", " ", html.unescape(text))
            chunks.append(text)
    else:
        try:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace") if payload else ""
        except Exception:
            text = ""
        if text:
            if msg.get_content_type() == "text/html":
                text = re.sub(r"<[^>]+>", " ", html.unescape(text))
            chunks.append(text)
    return "\n".join(chunks)


def _extract_otp_code(subject: str, message_text: str) -> str | None:
    haystacks = [subject or "", message_text or ""]
    for haystack in haystacks:
        if not haystack:
            continue
        for pattern in _OTP_PATTERNS:
            match = pattern.search(haystack)
            if match:
                return match.group(1)
    return None


def _extract_header_recipients(msg) -> set[str]:
    from email.utils import getaddresses

    recipients: set[str] = set()
    recipient_headers = [
        msg.get("To", ""),
        msg.get("Delivered-To", ""),
        msg.get("X-Forwarded-To", ""),
        msg.get("X-Original-To", ""),
        msg.get("Envelope-To", ""),
        msg.get("X-Envelope-To", ""),
        msg.get("Original-Recipient", ""),
        msg.get("Resent-To", ""),
        msg.get("Cc", ""),
    ]
    for _name, addr in getaddresses(recipient_headers):
        clean = (addr or "").strip().lower()
        if clean:
            recipients.add(clean)
    for received in msg.get_all("Received", []):
        for match in _RECEIVED_FOR_PATTERN.findall(received or ""):
            clean = (match or "").strip().lower()
            if clean:
                recipients.add(clean)
    return recipients


def _sender_matches(msg) -> bool:
    sender_blob = " ".join([
        _decode_mime_header(msg.get("From", "")),
        _decode_mime_header(msg.get("Reply-To", "")),
        _decode_mime_header(msg.get("Return-Path", "")),
    ]).lower()
    if "noreply@dice.fm" in sender_blob:
        return True
    return "dice.fm" in sender_blob


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

    If recipient_email is set, recipient headers are checked locally after fetch.
    This lets many accounts share a single forwarded inbox without stealing each
    other's OTPs, even when IMAP server-side TO filtering is unreliable.
    """
    import imaplib
    import email as email_lib
    from email.utils import parsedate_to_datetime

    log = log_fn or (lambda msg, level="info": print(f"[{level}] {msg}"))
    start_time = time.time()

    recipient_filter = str(recipient_email or "").strip().lower()

    sem = _imap_semaphore(imap_host, imap_email)
    sem.acquire()
    try:
        log(f"Connecting to IMAP ({imap_host}:{imap_port}) as {imap_email}"
            + (f" (recipient={recipient_filter})" if recipient_filter else "") + "...")
        try:
            mail = imaplib.IMAP4_SSL(imap_host, imap_port)
            mail.login(imap_email, imap_password)
        except Exception as exc:
            log(f"IMAP login failed: {exc}", "error")
            return None

        try:
            deadline = start_time + timeout_seconds
            recipient_mismatch_logged = False
            dice_candidate_logged = False
            while time.time() < deadline:
                try:
                    mail.select("INBOX")
                    status, msg_ids = mail.search(None, "ALL")
                    if status != "OK" or not msg_ids or not msg_ids[0]:
                        time.sleep(poll_interval)
                        continue

                    id_list = msg_ids[0].split()
                    dice_candidates = 0

                    for mid in reversed(id_list[-60:]):
                        status, msg_data = mail.fetch(mid, "(RFC822)")
                        if status != "OK" or not msg_data or not msg_data[0]:
                            continue
                        raw = msg_data[0][1]
                        msg = email_lib.message_from_bytes(raw)
                        if not _sender_matches(msg):
                            continue

                        dice_candidates += 1

                        if recipient_filter:
                            recipients = _extract_header_recipients(msg)
                            if recipient_filter not in recipients:
                                if not recipient_mismatch_logged:
                                    preview = ", ".join(sorted(recipients)) if recipients else "none"
                                    log(
                                        f"Recent DICE email found, but its recipient headers did not include {recipient_filter} "
                                        f"(saw: {preview}).",
                                        "warning",
                                    )
                                    recipient_mismatch_logged = True
                                continue

                        date_str = msg.get("Date", "")
                        if date_str:
                            try:
                                email_ts = parsedate_to_datetime(date_str).timestamp()
                                if email_ts < start_time - 60:
                                    continue
                            except Exception:
                                pass

                        subject = _decode_mime_header(msg.get("Subject", ""))
                        message_text = _extract_message_text(msg)
                        otp_code = _extract_otp_code(subject, message_text)
                        if not otp_code:
                            continue

                        log(f"OTP code found: {otp_code}")
                        try:
                            mail.store(mid, "+FLAGS", "\\Deleted")
                            mail.expunge()
                        except Exception:
                            pass
                        return otp_code

                    if dice_candidates and not dice_candidate_logged:
                        log(f"Found {dice_candidates} recent DICE email(s) in the inbox. Waiting for a matching OTP...")
                        dice_candidate_logged = True

                    time.sleep(poll_interval)

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
