"""OTP retrieval methods: IMAP and AYCD Inbox."""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta


def fetch_otp_imap(
    imap_email: str,
    imap_password: str,
    imap_host: str = "imap.gmail.com",
    imap_port: int = 993,
    timeout_seconds: int = 120,
    poll_interval: int = 5,
    log_fn=None,
) -> str | None:
    """Poll an IMAP inbox for the latest Dice OTP email and return the 4-digit code."""
    import imaplib
    import email as email_lib
    from email.header import decode_header

    log = log_fn or (lambda msg, level="info": print(f"[{level}] {msg}"))
    start_time = time.time()

    log(f"Connecting to IMAP ({imap_host}:{imap_port}) as {imap_email}...")
    try:
        mail = imaplib.IMAP4_SSL(imap_host, imap_port)
        mail.login(imap_email, imap_password)
    except Exception as exc:
        log(f"IMAP login failed: {exc}", "error")
        return None

    log("IMAP connected. Polling for Dice OTP email...")

    try:
        deadline = start_time + timeout_seconds
        while time.time() < deadline:
            try:
                mail.select("INBOX")
                status, msg_ids = mail.search(None, '(FROM "noreply@dice.fm" SUBJECT "Login code")')
                if status != "OK" or not msg_ids or not msg_ids[0]:
                    time.sleep(poll_interval)
                    continue

                id_list = msg_ids[0].split()
                latest_id = id_list[-1]

                status, msg_data = mail.fetch(latest_id, "(RFC822)")
                if status != "OK" or not msg_data or not msg_data[0]:
                    time.sleep(poll_interval)
                    continue

                raw_email = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw_email)

                subject = ""
                raw_subject = msg.get("Subject", "")
                decoded_parts = decode_header(raw_subject)
                for part, charset in decoded_parts:
                    if isinstance(part, bytes):
                        subject += part.decode(charset or "utf-8", errors="replace")
                    else:
                        subject += part

                code_match = re.search(r"Login code:\s*(\d{4})", subject)
                if not code_match:
                    time.sleep(poll_interval)
                    continue

                # Check that the email is recent
                date_str = msg.get("Date", "")
                try:
                    from email.utils import parsedate_to_datetime
                    email_dt = parsedate_to_datetime(date_str)
                    email_ts = email_dt.timestamp()
                    if email_ts < start_time - 60:
                        time.sleep(poll_interval)
                        continue
                except Exception:
                    pass

                otp_code = code_match.group(1)
                log(f"OTP code found: {otp_code}")

                try:
                    mail.store(latest_id, "+FLAGS", "\\Deleted")
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
