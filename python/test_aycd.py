"""Manual AYCD OTP sanity check.

Run from repo root with the project venv:

    .venv/bin/python python/test_aycd.py --email you@gmail.com --key aycd_xxx

Then, in a browser, go to dice.fm/login and submit your phone number so Dice
sends the login-code email to <email>. This script polls AYCD and prints
whether it detected the OTP.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from otp import fetch_otp_aycd


def _log(msg: str, level: str = "info") -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.environ.get("AYCD_EMAIL"),
                    help="Inbox email Dice will send the OTP to")
    ap.add_argument("--key", default=os.environ.get("AYCD_KEY"),
                    help="AYCD Inbox API key")
    ap.add_argument("--timeout", type=int, default=180,
                    help="Seconds to wait for the OTP (default 180)")
    args = ap.parse_args()

    if not args.email or not args.key:
        ap.error("both --email and --key are required (or AYCD_EMAIL / AYCD_KEY env vars)")

    _log(f"Starting AYCD poll for {args.email} (timeout={args.timeout}s).")
    _log("Now go submit your phone number on dice.fm/login.")

    start = time.time()
    code = fetch_otp_aycd(
        api_key=args.key,
        email=args.email,
        timeout_seconds=args.timeout,
        log_fn=_log,
    )
    elapsed = time.time() - start

    if code:
        _log(f"SUCCESS — OTP detected: {code} (in {elapsed:.1f}s)")
        return 0
    _log(f"FAIL — no OTP returned after {elapsed:.1f}s", "error")
    return 1


if __name__ == "__main__":
    sys.exit(main())
