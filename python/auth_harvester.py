"""
Auth Harvester — Chrome-based Dice login copied from DiceBotCustom.

Uses undetected_chromedriver with local proxy forwarder for authenticated proxies.
"""

from __future__ import annotations

import base64
import http.server
import json
import os
import re
import select
import socket
import socketserver
import threading
import time
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from otp import fetch_otp_imap, fetch_otp_aycd

SESSION_MAX_AGE = 6 * 24 * 3600  # 6 days


# ── Session management ─────────────────────────────────────────────────────

def has_valid_session(session_dir: str, phone: str) -> bool:
    safe_phone = phone.replace("+", "").lstrip("0")
    path = os.path.join(session_dir, f"dice_session_{safe_phone}.json")
    if not os.path.exists(path):
        return False
    try:
        with open(path) as f:
            data = json.load(f)
        if time.time() - data.get("saved_at", 0) > SESSION_MAX_AGE:
            return False
        return bool(data.get("bearer_token"))
    except Exception:
        return False


def save_session(session_dir: str, phone: str, bearer_token: str, device_id: str = "", log_fn=None) -> str:
    safe_phone = phone.replace("+", "").lstrip("0")
    path = os.path.join(session_dir, f"dice_session_{safe_phone}.json")
    os.makedirs(session_dir, exist_ok=True)
    data = {
        "bearer_token": bearer_token,
        "device_id": device_id,
        "saved_at": int(time.time()),
        "saved_at_human": datetime.now(UTC).isoformat(),
        "phone": phone,
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    if log_fn:
        log_fn(f"Session saved: {path}")
        log_fn(f"  token: {bearer_token[:40]}...")
    return path


# ── Local proxy forwarder (from DiceBotCustom) ────────────────────────────

class _LocalProxyForwarder:
    """Local HTTP proxy that forwards to upstream with auth. Chrome connects
    to localhost (no auth popup), this proxy adds Proxy-Authorization."""

    def __init__(self, upstream_host, upstream_port, username, password, upstream_scheme="http"):
        self._upstream = (upstream_host, int(upstream_port))
        self._scheme = upstream_scheme
        self._auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.port = None

        forwarder = self

        class ProxyHandler(http.server.BaseHTTPRequestHandler):
            def do_CONNECT(self):
                try:
                    upstream_sock = socket.create_connection(forwarder._upstream, timeout=15)
                    connect_req = (
                        f"CONNECT {self.path} HTTP/1.1\r\n"
                        f"Host: {self.path}\r\n"
                        f"Proxy-Authorization: Basic {forwarder._auth}\r\n\r\n"
                    )
                    upstream_sock.sendall(connect_req.encode())
                    response = b""
                    while b"\r\n\r\n" not in response:
                        chunk = upstream_sock.recv(4096)
                        if not chunk:
                            break
                        response += chunk
                    status_line = response.split(b"\r\n")[0].decode()
                    if "200" not in status_line:
                        self.send_error(502, f"Upstream rejected: {status_line}")
                        upstream_sock.close()
                        return
                    self.send_response(200, "Connection Established")
                    self.end_headers()
                    self.connection.setblocking(False)
                    upstream_sock.setblocking(False)
                    sockets = [self.connection, upstream_sock]
                    while True:
                        readable, _, exceptional = select.select(sockets, [], sockets, 30)
                        if exceptional:
                            break
                        done = False
                        for sock in readable:
                            other = upstream_sock if sock is self.connection else self.connection
                            try:
                                data = sock.recv(65536)
                                if not data:
                                    done = True
                                    break
                                other.sendall(data)
                            except (ConnectionError, OSError):
                                done = True
                                break
                        if done:
                            break
                    upstream_sock.close()
                except Exception:
                    try:
                        self.send_error(502, "Bad Gateway")
                    except Exception:
                        pass

            def _forward_request(self):
                try:
                    upstream_sock = socket.create_connection(forwarder._upstream, timeout=15)
                    headers = f"{self.command} {self.path} HTTP/1.1\r\n"
                    headers += f"Proxy-Authorization: Basic {forwarder._auth}\r\n"
                    for key, value in self.headers.items():
                        headers += f"{key}: {value}\r\n"
                    headers += "\r\n"
                    upstream_sock.sendall(headers.encode())
                    content_length = int(self.headers.get("Content-Length", 0))
                    if content_length > 0:
                        upstream_sock.sendall(self.rfile.read(content_length))
                    while True:
                        data = upstream_sock.recv(65536)
                        if not data:
                            break
                        self.wfile.write(data)
                    upstream_sock.close()
                except Exception:
                    try:
                        self.send_error(502, "Bad Gateway")
                    except Exception:
                        pass

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = do_OPTIONS = do_PATCH = lambda s: s._forward_request()

            def log_message(self, format, *args):
                pass

        self._server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), ProxyHandler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._server:
            self._server.shutdown()


_forwarders: list[_LocalProxyForwarder] = []


def parse_proxy_string(proxy: str) -> dict | None:
    if not proxy:
        return None
    raw = proxy.strip()
    scheme = "http"
    for prefix in ("socks5://", "socks4://", "https://", "http://"):
        if raw.lower().startswith(prefix):
            scheme = prefix.rstrip(":/")
            raw = raw[len(prefix):]
            break
    if "@" in raw:
        creds, hostport = raw.rsplit("@", 1)
        parts = hostport.split(":")
        cred_parts = creds.split(":", 1)
        return {"scheme": scheme, "host": parts[0], "port": parts[1] if len(parts) > 1 else "80",
                "username": cred_parts[0], "password": cred_parts[1] if len(cred_parts) > 1 else ""}
    sp = raw.split(":")
    if len(sp) == 4:
        return {"scheme": scheme, "host": sp[0], "port": sp[1], "username": sp[2], "password": sp[3]}
    if len(sp) == 2:
        return {"scheme": scheme, "host": sp[0], "port": sp[1], "username": "", "password": ""}
    return None


# ── Chrome driver creation (from DiceBotCustom) ───────────────────────────

def create_driver(proxy: str | None = None, profile_path: str | None = None, log_fn=None):
    log = log_fn or (lambda msg, level="info": print(f"[chrome] {msg}"))
    options = uc.ChromeOptions()
    # Open minimized so it doesn't steal focus — browser runs in background
    options.add_argument("--window-position=-2000,-2000")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-popup-blocking")
    # Prevent stealing focus
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-background-networking")
    options.page_load_strategy = "eager"

    proxy_info = parse_proxy_string(proxy)
    if proxy_info:
        if proxy_info["username"] and proxy_info["password"]:
            forwarder = _LocalProxyForwarder(
                proxy_info["host"], proxy_info["port"],
                proxy_info["username"], proxy_info["password"],
                proxy_info["scheme"]
            )
            _forwarders.append(forwarder)
            options.add_argument(f"--proxy-server=http://127.0.0.1:{forwarder.port}")
            log(f"Proxy: {proxy_info['host']}:{proxy_info['port']} via local forwarder :{forwarder.port}")
        else:
            options.add_argument(f"--proxy-server={proxy_info['scheme']}://{proxy_info['host']}:{proxy_info['port']}")
            log(f"Proxy: {proxy_info['host']}:{proxy_info['port']}")

    if profile_path:
        Path(profile_path).mkdir(parents=True, exist_ok=True)
        options.add_argument(f"--user-data-dir={profile_path}")

    driver = uc.Chrome(options=options, use_subprocess=True)

    # Remove automation traces
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                window.chrome = {runtime: {}};
            """
        })
    except Exception:
        pass

    return driver


# ── Login attempt (from DiceBotCustom) ─────────────────────────────────────

def _dice_login_attempt(driver, country_iso: str, phone_number: str, log) -> str:
    """Single login attempt. Returns 'otp', 'retry', or 'fail'."""

    # Dismiss cookie banner
    try:
        driver.execute_script("""
            const selectors = ['button[data-testid="accept-all"]', 'button[id*="accept"]', 'button[class*="accept"]'];
            for (const sel of selectors) { const btn = document.querySelector(sel); if (btn) { btn.click(); return; } }
            const btns = [...document.querySelectorAll('button')];
            const match = btns.find(b => /allow all|accept all|accept cookies/i.test(b.textContent));
            if (match) match.click();
        """)
        time.sleep(1)
    except Exception:
        pass

    # Select country (skip if US — Dice defaults to US)
    if country_iso != "us":
        log(f"Selecting country: {country_iso.upper()}")
        try:
            country_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'button[class*="SelectedButton"]'))
            )
            driver.execute_script("arguments[0].click();", country_btn)
            time.sleep(1.5)
            country_item = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, f'//div[contains(@class, "ListItem")][.//div[@iso="{country_iso}"]]'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", country_item)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", country_item)
            time.sleep(1)
        except Exception as exc:
            log(f"Country selection may have failed: {exc}", "warning")

    # Enter phone number (JS to avoid element-not-interactable)
    log("Entering phone number...")
    try:
        phone_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="tel"]'))
        )
        driver.execute_script("""
            const el = arguments[0];
            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            setter.call(el, '');
            el.dispatchEvent(new Event('input', { bubbles: true }));
            setter.call(el, arguments[1]);
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        """, phone_input, phone_number)
        time.sleep(1)
    except Exception as exc:
        log(f"Failed to enter phone: {exc}", "error")
        return "fail"

    # Submit form
    log("Submitting...")
    try:
        submit_btn = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
        driver.execute_script("arguments[0].click();", submit_btn)
    except Exception as exc:
        log(f"Failed to submit: {exc}", "error")
        return "fail"

    log("Waiting for OTP input...")
    time.sleep(5)

    # Check for OTP input
    try:
        otp_els = driver.find_elements(By.CSS_SELECTOR, 'input[autocomplete="one-time-code"]')
        if otp_els:
            log("OTP input appeared!")
            return "otp"
    except Exception:
        pass

    # Check for error
    try:
        page_text = driver.find_element(By.TAG_NAME, "body").text or ""
        if "unauthori" in page_text.lower() or "error" in page_text.lower():
            log("Error response (captcha failure). Will retry.", "warning")
            return "retry"
    except Exception:
        pass

    # Wait longer
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[autocomplete="one-time-code"]'))
        )
        log("OTP input appeared!")
        return "otp"
    except Exception:
        log("OTP input did not appear. Will retry.", "warning")
        return "retry"


# ── Single account login ──────────────────────────────────────────────────

def login_single_account(
    phone: str,
    country_iso: str,
    email: str | None,
    proxy: str | None,
    session_dir: str,
    aycd_key: str | None = None,
    imap_email: str | None = None,
    imap_password: str | None = None,
    imap_host: str = "imap.gmail.com",
    imap_recipient: str | None = None,
    log_fn=None,
    on_driver=None,
) -> dict:
    """Login a single account via Chrome. Returns {ok, bearer_token?, error?}."""
    log = log_fn or (lambda msg, level="info": print(f"[{phone}] {msg}"))
    driver = None

    try:
        log("Launching Chrome...")
        driver = create_driver(proxy=proxy, log_fn=log)
        if on_driver:
            try: on_driver(driver)
            except Exception: pass

        log("Navigating to dice.fm/login...")
        driver.get("https://dice.fm/login")

        for _ in range(15):
            time.sleep(1)
            try:
                if "dice.fm" in (driver.current_url or ""):
                    break
            except Exception:
                pass
        time.sleep(2)

        # Check if already logged in
        try:
            logout_els = driver.find_elements(By.XPATH, '//div[@role="button" and contains(text(), "Log out")]')
            if logout_els and any(el.is_displayed() for el in logout_els):
                log("Already logged in!")
                token = _extract_token(driver)
                if token:
                    save_session(session_dir, phone, token, log_fn=log)
                    return {"ok": True, "bearer_token": token}
        except Exception:
            pass

        # Login attempts
        MAX_ATTEMPTS = 3
        otp_input_found = False
        for attempt in range(1, MAX_ATTEMPTS + 1):
            if attempt > 1:
                log(f"Retry {attempt}/{MAX_ATTEMPTS}...")
                time.sleep(10)
                driver.get("https://dice.fm/login")
                time.sleep(4)

            result = _dice_login_attempt(driver, country_iso, phone, log)
            if result == "otp":
                otp_input_found = True
                break
            elif result == "fail":
                break

        if not otp_input_found:
            return {"ok": False, "error": "OTP input did not appear"}

        # Fetch OTP
        otp_code = None
        if aycd_key and email:
            log(f"Fetching OTP via AYCD ({email})...")
            otp_code = fetch_otp_aycd(api_key=aycd_key, email=email, log_fn=log)
        if not otp_code and imap_email and imap_password:
            log(f"Fetching OTP via IMAP ({imap_email})...")
            # imap_recipient is passed explicitly by the caller (the Dice
            # account email — what the OTP was sent to). Fall back to `email`
            # only if not provided.
            otp_code = fetch_otp_imap(
                imap_email=imap_email,
                imap_password=imap_password,
                imap_host=imap_host,
                recipient_email=imap_recipient if imap_recipient is not None else email,
                log_fn=log,
            )

        if not otp_code:
            return {"ok": False, "error": "Could not retrieve OTP code"}

        # Enter OTP
        log(f"Entering OTP: {otp_code}")
        try:
            code_input = driver.find_element(By.CSS_SELECTOR, 'input[autocomplete="one-time-code"]')
            driver.execute_script("""
                const el = arguments[0];
                const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                setter.call(el, arguments[1]);
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            """, code_input, otp_code)
        except Exception as exc:
            return {"ok": False, "error": f"Failed to enter OTP: {exc}"}

        time.sleep(5)

        # Wait for login to complete
        log("Waiting for login to complete...")
        for _ in range(30):
            try:
                if "/login" not in (driver.current_url or ""):
                    break
                logout_els = driver.find_elements(By.XPATH, '//div[contains(text(), "Log out")]')
                if logout_els:
                    break
            except Exception:
                pass
            time.sleep(2)

        # Extract token
        token = _extract_token(driver)
        if token:
            log(f"Login successful!")
            save_session(session_dir, phone, token, log_fn=log)
            return {"ok": True, "bearer_token": token}
        else:
            return {"ok": False, "error": "Login completed but no bearer token found"}

    except Exception as exc:
        log(f"Error: {exc}", "error")
        return {"ok": False, "error": str(exc)}
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _extract_token(driver) -> str | None:
    """Extract bearer token from cookies or localStorage."""
    try:
        for cookie in driver.get_cookies():
            if cookie["name"] == "DICE_API_TOKEN_NEW":
                return cookie["value"]
    except Exception:
        pass
    try:
        token = driver.execute_script("return localStorage.getItem('DICE_API_TOKEN_NEW');")
        if token:
            return token
    except Exception:
        pass
    try:
        token = driver.execute_script("""
            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                const val = localStorage.getItem(key);
                if (val && val.startsWith('eyJ') && val.length > 100) return val;
            }
            return null;
        """)
        if token:
            return token
    except Exception:
        pass
    return None
