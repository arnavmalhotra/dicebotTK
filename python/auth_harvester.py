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
import shutil
import socket
import socketserver
import subprocess
import sys
import threading
import time
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from otp import fetch_otp_imap, fetch_otp_aycd

SESSION_MAX_AGE = 6 * 24 * 3600  # 6 days


# ── Session management ─────────────────────────────────────────────────────

def _session_phone_key(phone: str) -> str:
    digits = re.sub(r"\D", "", str(phone or ""))
    return digits.lstrip("0")

def has_valid_session(session_dir: str, phone: str) -> bool:
    safe_phone = _session_phone_key(phone)
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
    safe_phone = _session_phone_key(phone)
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
_chrome_launch_lock = threading.Lock()
_chrome_detect_lock = threading.Lock()
_chrome_binary_cache_loaded = False
_chrome_binary_cache: str | None = None
_chrome_major_cache_loaded = False
_chrome_major_cache: int | None = None


def _extract_chrome_major(version_text: str) -> int | None:
    match = re.search(r"\b(?:Google Chrome|Chrome|Chromium)\s+(\d+)\.", version_text or "")
    if match:
        return int(match.group(1))
    match = re.search(r"\b(\d{2,3})\.\d+\.\d+\.\d+\b", version_text or "")
    return int(match.group(1)) if match else None


def _chrome_binary_candidates() -> list[str]:
    candidates: list[str] = []
    for env_name in ("DICEBOT_CHROME_BINARY", "CHROME_BINARY", "GOOGLE_CHROME_BIN"):
        value = os.environ.get(env_name)
        if value:
            candidates.append(value)

    if sys.platform == "darwin":
        candidates.extend([
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            str(Path.home() / "Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        ])
    elif sys.platform == "win32":
        for base in ("PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"):
            root = os.environ.get(base)
            if root:
                candidates.append(os.path.join(root, "Google", "Chrome", "Application", "chrome.exe"))
    else:
        candidates.extend([
            "google-chrome",
            "google-chrome-stable",
            "chromium",
            "chromium-browser",
        ])

    # Keep order but remove duplicates.
    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate and candidate not in seen:
            unique.append(candidate)
            seen.add(candidate)
    return unique


def _find_chrome_binary() -> str | None:
    global _chrome_binary_cache_loaded, _chrome_binary_cache
    with _chrome_detect_lock:
        if _chrome_binary_cache_loaded:
            return _chrome_binary_cache

        found_path = None
        for candidate in _chrome_binary_candidates():
            path = candidate if os.path.isabs(candidate) else shutil.which(candidate)
            if path and os.path.exists(path):
                found_path = path
                break

        _chrome_binary_cache = found_path
        _chrome_binary_cache_loaded = True
        return _chrome_binary_cache


def _windows_chrome_registry_version() -> str | None:
    if sys.platform != "win32":
        return None
    try:
        import winreg
    except Exception:
        return None

    registry_paths = [
        (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\WOW6432Node\Google\Chrome\BLBeacon"),
    ]
    for hive, key_path in registry_paths:
        try:
            with winreg.OpenKey(hive, key_path) as key:
                value, _ = winreg.QueryValueEx(key, "version")
                if value:
                    return str(value)
        except OSError:
            continue
        except Exception:
            continue
    return None


def _mac_chrome_plist_version(binary: str | None) -> str | None:
    if sys.platform != "darwin" or not binary:
        return None
    try:
        import plistlib
        plist_path = Path(binary).parents[1] / "Info.plist"
        with open(plist_path, "rb") as f:
            data = plistlib.load(f)
        version = data.get("CFBundleShortVersionString") or data.get("CFBundleVersion")
        return str(version) if version else None
    except Exception:
        return None


def _set_chrome_major_cache(major: int | None) -> None:
    global _chrome_major_cache_loaded, _chrome_major_cache
    with _chrome_detect_lock:
        _chrome_major_cache = major
        _chrome_major_cache_loaded = True


def _detect_chrome_major(log) -> int | None:
    global _chrome_major_cache_loaded, _chrome_major_cache
    with _chrome_detect_lock:
        if _chrome_major_cache_loaded:
            return _chrome_major_cache

    detected_major: int | None = None

    for env_name in ("DICEBOT_CHROME_VERSION_MAIN", "CHROME_VERSION_MAIN"):
        raw = os.environ.get(env_name)
        if raw:
            try:
                detected_major = int(str(raw).strip().split(".", 1)[0])
                log(f"Using Chrome major {detected_major} from {env_name}")
                _set_chrome_major_cache(detected_major)
                return detected_major
            except ValueError:
                log(f"Ignoring invalid {env_name}={raw!r}", "warning")

    registry_version = _windows_chrome_registry_version()
    if registry_version:
        detected_major = _extract_chrome_major(registry_version)
        if detected_major:
            log(f"Detected Chrome {registry_version} from Windows registry; using ChromeDriver major {detected_major}")
            _set_chrome_major_cache(detected_major)
            return detected_major

    binary = _find_chrome_binary()
    if not binary:
        log("Could not locate Chrome binary; letting undetected_chromedriver auto-detect.", "warning")
        _set_chrome_major_cache(None)
        return None

    plist_version = _mac_chrome_plist_version(binary)
    if plist_version:
        detected_major = _extract_chrome_major(plist_version)
        if detected_major:
            log(f"Detected Chrome {plist_version} from app metadata; using ChromeDriver major {detected_major}")
            _set_chrome_major_cache(detected_major)
            return detected_major

    try:
        result = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            timeout=2 if sys.platform == "win32" else 5,
            check=False,
        )
        version_text = (result.stdout or result.stderr or "").strip()
        detected_major = _extract_chrome_major(version_text)
        if detected_major:
            log(f"Detected Chrome {version_text}; using ChromeDriver major {detected_major}")
            _set_chrome_major_cache(detected_major)
            return detected_major
        log(f"Could not parse Chrome version from: {version_text!r}", "warning")
    except Exception as exc:
        log(f"Could not detect Chrome version: {exc!r}", "warning")

    _set_chrome_major_cache(None)
    return None


def _chrome_major_from_exception(exc: Exception) -> int | None:
    text = str(exc)
    match = re.search(r"Current browser version is\s+(\d+)\.", text)
    if match:
        return int(match.group(1))
    return None


def _is_chromedriver_version_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "only supports chrome version" in text
        or "current browser version is" in text
        or "session not created" in text and "chromedriver" in text and "chrome version" in text
    )


def _uc_driver_cache_candidates() -> list[Path]:
    if sys.platform == "darwin":
        return [Path.home() / "Library" / "Application Support" / "undetected_chromedriver" / "undetected_chromedriver"]
    if sys.platform == "win32":
        roots = [os.environ.get("APPDATA"), os.environ.get("LOCALAPPDATA")]
        return [
            Path(root) / "undetected_chromedriver" / "undetected_chromedriver.exe"
            for root in roots
            if root
        ]
    return [Path.home() / ".local" / "share" / "undetected_chromedriver" / "undetected_chromedriver"]


def _clear_uc_driver_cache(log) -> None:
    for path in _uc_driver_cache_candidates():
        try:
            if path.exists() and path.is_file():
                path.unlink()
                log(f"Removed stale ChromeDriver cache: {path}", "warning")
        except Exception as exc:
            log(f"Could not remove ChromeDriver cache {path}: {exc!r}", "warning")


def _cached_uc_driver_major(log) -> int | None:
    for path in _uc_driver_cache_candidates():
        if not (path.exists() and path.is_file()):
            continue
        try:
            result = subprocess.run(
                [str(path), "--version"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except Exception as exc:
            log(f"Could not probe cached ChromeDriver {path}: {exc!r}", "warning")
            return None
        version_text = (result.stdout or result.stderr or "").strip()
        match = re.search(r"ChromeDriver\s+(\d+)\.", version_text)
        if match:
            return int(match.group(1))
        log(f"Could not parse cached ChromeDriver version from: {version_text!r}", "warning")
        return None
    return None


def _strip_macos_quarantine(log) -> None:
    """Best-effort: remove com.apple.quarantine from cached UC drivers.

    Recent macOS releases tag freshly downloaded binaries with this xattr,
    which then refuse to execute via Selenium with opaque errors. Stripping
    is a no-op when the attribute is absent, so it's safe to call routinely.
    """
    if sys.platform != "darwin":
        return
    for path in _uc_driver_cache_candidates():
        if not (path.exists() and path.is_file()):
            continue
        try:
            result = subprocess.run(
                ["xattr", "-d", "com.apple.quarantine", str(path)],
                capture_output=True,
                text=True,
                timeout=3,
                check=False,
            )
            if result.returncode == 0:
                log(f"Stripped com.apple.quarantine from {path}", "warning")
        except Exception as exc:
            log(f"Could not strip quarantine xattr from {path}: {exc!r}", "warning")


def _ensure_uc_driver_matches_chrome(chrome_major: int | None, log) -> None:
    """Wipe the cached UC driver if its major doesn't match installed Chrome.

    UC's own version_main hint isn't always honored when a cached driver
    already exists, and the post-failure recovery only fires for specific
    UC error strings. Checking up front catches drift after a Chrome
    auto-update without relying on UC's exception wording.
    """
    if not chrome_major:
        return
    cached_major = _cached_uc_driver_major(log)
    if cached_major is None or cached_major == chrome_major:
        return
    log(
        f"Cached ChromeDriver is major {cached_major} but Chrome is major {chrome_major}; "
        "clearing cache so a matching driver is downloaded.",
        "warning",
    )
    _clear_uc_driver_cache(log)


def _chrome_launch_error_message(exc: Exception, attempted_major: int | None) -> str:
    text = str(exc)
    current_major = _chrome_major_from_exception(exc)
    supported_match = re.search(r"only supports Chrome version\s+(\d+)", text)
    supported_major = int(supported_match.group(1)) if supported_match else None

    details: list[str] = ["Chrome could not start."]
    if supported_major and current_major:
        details.append(
            f"ChromeDriver supports major {supported_major}, but this device has Chrome major {current_major}."
        )
    elif attempted_major:
        details.append(f"DiceBot tried ChromeDriver major {attempted_major}.")

    details.append(
        "Update Google Chrome, or set DICEBOT_CHROME_BINARY to the Chrome executable and "
        "DICEBOT_CHROME_VERSION_MAIN to its major version if this device uses a custom install."
    )
    if text:
        first_line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
        if first_line:
            details.append(f"Underlying error: {first_line}")
    return " ".join(details)


def _stop_forwarders(forwarders: list[_LocalProxyForwarder]) -> None:
    for forwarder in forwarders:
        try:
            forwarder.stop()
        except Exception:
            pass


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

def create_driver(
    proxy: str | None = None,
    profile_path: str | None = None,
    log_fn=None,
    background: bool = True,
):
    log = log_fn or (lambda msg, level="info": print(f"[chrome] {msg}"))

    def build_options() -> tuple[uc.ChromeOptions, list[_LocalProxyForwarder]]:
        options = uc.ChromeOptions()
        local_forwarders: list[_LocalProxyForwarder] = []
        chrome_binary = _find_chrome_binary()
        if chrome_binary:
            options.binary_location = chrome_binary

        if background:
            if sys.platform == "win32":
                # Off-screen positioning can make Chrome appear not to launch on Windows.
                options.add_argument("--start-minimized")
                options.add_argument("--window-size=1200,800")
            else:
                # Keep auth farm windows out of the way on macOS/Linux.
                options.add_argument("--window-position=-2000,-2000")
                options.add_argument("--window-size=1200,800")
        else:
            options.add_argument("--window-position=80,80")
            options.add_argument("--window-size=1280,900")
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
                local_forwarders.append(forwarder)
                options.add_argument(f"--proxy-server=http://127.0.0.1:{forwarder.port}")
                log(f"Proxy: {proxy_info['host']}:{proxy_info['port']} via local forwarder :{forwarder.port}")
            else:
                options.add_argument(f"--proxy-server={proxy_info['scheme']}://{proxy_info['host']}:{proxy_info['port']}")
                log(f"Proxy: {proxy_info['host']}:{proxy_info['port']}")

        if profile_path:
            Path(profile_path).mkdir(parents=True, exist_ok=True)
            options.add_argument(f"--user-data-dir={profile_path}")

        return options, local_forwarders

    def launch(version_main: int | None):
        options, local_forwarders = build_options()
        kwargs: dict[str, Any] = {"options": options, "use_subprocess": True}
        if version_main:
            kwargs["version_main"] = version_main
        try:
            # undetected_chromedriver patches/downloads a shared driver binary.
            # Serializing launch avoids cache races when auth farm starts workers.
            with _chrome_launch_lock:
                driver = uc.Chrome(**kwargs)
            _forwarders.extend(local_forwarders)
            return driver
        except Exception:
            _stop_forwarders(local_forwarders)
            raise

    chrome_major = _detect_chrome_major(log)
    _ensure_uc_driver_matches_chrome(chrome_major, log)
    _strip_macos_quarantine(log)

    # Escalating recovery between attempts: strip quarantine, then wipe
    # cache + strip again. Each attempt logs the raw UC exception so
    # remote debugging isn't blocked by our wrapper message.
    recoveries: list[tuple[str, Any]] = [
        ("strip quarantine xattr from cached driver",
         lambda: _strip_macos_quarantine(log)),
        ("wipe cached ChromeDriver and strip xattr from re-download",
         lambda: (_clear_uc_driver_cache(log), _strip_macos_quarantine(log))),
    ]

    driver = None
    last_exc: Exception | None = None
    attempt_major = chrome_major
    for attempt in range(len(recoveries) + 1):
        try:
            driver = launch(attempt_major)
            break
        except Exception as exc:
            last_exc = exc
            log(f"Chrome launch attempt {attempt + 1} failed: {exc!r}", "warning")
            mismatch_major = _chrome_major_from_exception(exc)
            if mismatch_major:
                attempt_major = mismatch_major
                _set_chrome_major_cache(mismatch_major)
            if attempt >= len(recoveries):
                break
            label, action = recoveries[attempt]
            log(f"Recovery: {label}.", "warning")
            try:
                action()
            except Exception as rec_exc:
                log(f"Recovery action failed: {rec_exc!r}", "warning")
            time.sleep(0.8)

    if driver is None:
        assert last_exc is not None
        raise RuntimeError(_chrome_launch_error_message(last_exc, attempt_major)) from last_exc

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

_PHONE_INPUT_SELECTORS = [
    'input[type="tel"]',
    'input[autocomplete="tel"]',
    'input[autocomplete="tel-national"]',
    'input[name*="phone" i]',
    'input[id*="phone" i]',
    'input[placeholder*="phone" i]',
    'input[aria-label*="phone" i]',
]


def _find_visible_element(driver, selectors: list[str], timeout: float = 10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for selector in selectors:
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, selector):
                    if el.is_displayed():
                        return el
            except Exception:
                continue
        time.sleep(0.35)
    raise TimeoutException(f"No visible element found for selectors: {selectors}")


def _open_login_prompt_if_needed(driver, log) -> None:
    try:
        _find_visible_element(driver, _PHONE_INPUT_SELECTORS, timeout=1.0)
        return
    except Exception:
        pass

    cta_selectors = [
        '//button[contains(normalize-space(.), "Log in") or contains(normalize-space(.), "Sign up")]',
        '//a[contains(normalize-space(.), "Log in") or contains(normalize-space(.), "Sign up")]',
    ]
    for xpath in cta_selectors:
        try:
            ctas = driver.find_elements(By.XPATH, xpath)
            visible_cta = next((el for el in ctas if el.is_displayed()), None)
            if not visible_cta:
                continue
            log("Opening login prompt...")
            driver.execute_script("arguments[0].click();", visible_cta)
            time.sleep(1.5)
            _find_visible_element(driver, _PHONE_INPUT_SELECTORS, timeout=8.0)
            return
        except Exception:
            continue


def _fill_phone_input(driver, phone_input, phone_number: str) -> bool:
    clean_value = str(phone_number or "").strip()
    if not clean_value:
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", phone_input)
    except Exception:
        pass

    try:
        phone_input.click()
        phone_input.send_keys(Keys.CONTROL, "a")
        phone_input.send_keys(Keys.BACKSPACE)
        phone_input.send_keys(clean_value)
        typed = (phone_input.get_attribute("value") or "").strip()
        if typed:
            return True
    except Exception:
        pass

    try:
        driver.execute_script("""
            const el = arguments[0];
            const value = arguments[1];
            el.focus();
            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            setter.call(el, '');
            el.dispatchEvent(new Event('input', { bubbles: true }));
            setter.call(el, value);
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
            el.dispatchEvent(new Event('blur', { bubbles: true }));
        """, phone_input, clean_value)
        typed = (phone_input.get_attribute("value") or "").strip()
        return bool(typed)
    except Exception:
        return False


def _dice_login_attempt(driver, country_iso: str, phone_number: str, log) -> str:
    """Single login attempt. Returns 'otp', 'retry', or 'fail'.

    'fail' is reserved for hopeless cases — element/submit/fill errors are
    retry-able because they are almost always caused by slow network, captcha
    loading, or intermittent element visibility.
    """

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

    try:
        _open_login_prompt_if_needed(driver, log)
    except Exception as exc:
        log(f"Login prompt may not have opened cleanly: {exc!r}", "warning")

    # Select country explicitly — Dice defaults based on IP, which won't always match the account.
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
        phone_input = _find_visible_element(driver, _PHONE_INPUT_SELECTORS, timeout=15.0)
        if not _fill_phone_input(driver, phone_input, phone_number):
            raise RuntimeError("Unable to populate phone field")
        time.sleep(1)
    except Exception as exc:
        log(f"Failed to enter phone (will retry): {exc!r}", "warning")
        return "retry"

    # Submit form
    log("Submitting...")
    try:
        submit_btn = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
        driver.execute_script("arguments[0].click();", submit_btn)
    except Exception as exc:
        log(f"Failed to submit (will retry): {exc}", "warning")
        return "retry"

    log("Waiting for OTP input...")

    # Poll up to 30s for OTP input while keeping an eye on captcha failures.
    deadline = time.time() + 30.0
    saw_error_text = False
    while time.time() < deadline:
        try:
            otp_els = driver.find_elements(By.CSS_SELECTOR, 'input[autocomplete="one-time-code"]')
            if any(el.is_displayed() for el in otp_els):
                log("OTP input appeared!")
                return "otp"
        except Exception:
            pass

        if not saw_error_text:
            try:
                page_text = driver.find_element(By.TAG_NAME, "body").text or ""
                lower = page_text.lower()
                if "unauthori" in lower or "try again" in lower or "captcha" in lower:
                    log("Captcha/unauthorized response detected. Will retry.", "warning")
                    saw_error_text = True
            except Exception:
                pass

        if saw_error_text:
            return "retry"

        time.sleep(1.0)

    log("OTP input did not appear within 30s. Will retry.", "warning")
    return "retry"


def _wait_for_manual_otp_screen(driver, log, timeout_seconds: float) -> bool:
    """Wait for OTP input to appear while the user enters phone/submits manually."""
    deadline = time.time() + timeout_seconds
    last_remind = 0.0
    while time.time() < deadline:
        try:
            otp_els = driver.find_elements(By.CSS_SELECTOR, 'input[autocomplete="one-time-code"]')
            if any(el.is_displayed() for el in otp_els):
                log("OTP screen reached — fetching code automatically.")
                return True
        except Exception:
            pass
        # Periodic reminder so the user knows we're still waiting.
        now = time.time()
        if now - last_remind > 30:
            remaining = int(deadline - now)
            log(f"Waiting for you to submit the phone and reach the OTP screen… ({remaining}s left)")
            last_remind = now
        time.sleep(1.0)
    return False


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
    session_phone: str | None = None,
    log_fn=None,
    on_driver=None,
    keep_open_on_success: bool = False,
    manual_phone: bool = False,
    manual_timeout_seconds: float = 600.0,
    max_attempts: int = 5,
    manual_otp_getter=None,
    manual_otp_notifier=None,
    manual_otp_timeout_seconds: float = 300.0,
) -> dict:
    """Login a single account via Chrome. Returns {ok, bearer_token?, error?}.

    When ``manual_phone`` is True the browser opens visibly and the function
    waits for the user to enter the phone number and submit the form
    themselves. Once the OTP screen appears, the OTP is fetched and entered
    automatically just like the fully-automated path.
    """
    log = log_fn or (lambda msg, level="info": print(f"[{phone}] {msg}"))
    session_phone = (session_phone or phone or "").strip()
    driver = None
    should_close_driver = True
    foreground = bool(keep_open_on_success or manual_phone)

    try:
        log("Launching Chrome...")
        driver = create_driver(
            proxy=proxy,
            log_fn=log,
            background=not foreground,
        )
        if on_driver:
            try: on_driver(driver)
            except Exception: pass

        log("Navigating to dice.fm/login...")
        try:
            driver.get("https://dice.fm/login")
        except Exception as exc:
            log(f"Initial navigation failed: {exc!r}", "warning")

        loaded = False
        for _ in range(25):
            time.sleep(1)
            try:
                if "dice.fm" in (driver.current_url or ""):
                    loaded = True
                    break
            except Exception:
                pass
        if not loaded:
            log("Page didn't reach dice.fm — attempting one more navigation.", "warning")
            try:
                driver.get("https://dice.fm/login")
                time.sleep(5)
            except Exception as exc:
                return {"ok": False, "error": f"Could not load dice.fm (proxy issue?): {exc}"}
        time.sleep(2)

        # Check if already logged in
        try:
            logout_els = driver.find_elements(By.XPATH, '//div[@role="button" and contains(text(), "Log out")]')
            if logout_els and any(el.is_displayed() for el in logout_els):
                log("Already logged in!")
                token = _extract_token(driver)
                if token:
                    save_session(session_dir, session_phone, token, log_fn=log)
                    if keep_open_on_success:
                        should_close_driver = False
                        log("Leaving Chrome open for account review.")
                    return {"ok": True, "bearer_token": token}
        except Exception:
            pass

        otp_input_found = False

        if manual_phone:
            banner = (
                "\n"
                "┌─────────────────────────────────────────────────────────────┐\n"
                f"│  MANUAL AUTH — enter this phone in the Chrome window:       │\n"
                f"│    {phone:<57}│\n"
                f"│  Country: {country_iso.upper():<50}│\n"
                "│  Then press Continue/Submit. OTP will be filled for you.    │\n"
                "└─────────────────────────────────────────────────────────────┘"
            )
            for line in banner.splitlines():
                log(line)
            otp_input_found = _wait_for_manual_otp_screen(driver, log, manual_timeout_seconds)
            if not otp_input_found:
                return {"ok": False, "error": "Manual auth timed out waiting for OTP screen"}
        else:
            # Login attempts — every non-'otp' result is a retry, we keep trying
            # the full number of attempts before giving up.
            last_result = None
            for attempt in range(1, max_attempts + 1):
                if attempt > 1:
                    log(f"Retry {attempt}/{max_attempts}...")
                    time.sleep(8)
                    try:
                        driver.get("https://dice.fm/login")
                    except Exception as exc:
                        log(f"Re-navigation failed: {exc!r}", "warning")
                    time.sleep(4)

                last_result = _dice_login_attempt(driver, country_iso, phone, log)
                if last_result == "otp":
                    otp_input_found = True
                    break

            if not otp_input_found:
                return {"ok": False, "error": f"OTP input did not appear after {max_attempts} attempts"}

        # Fetch OTP
        otp_code = None
        if aycd_key and email:
            log(f"Fetching OTP via AYCD ({email})...")
            otp_code = fetch_otp_aycd(api_key=aycd_key, email=email, log_fn=log)
        imap_target_email = (imap_email or email or "").strip()
        if not otp_code and imap_target_email and imap_password:
            log(f"Fetching OTP via IMAP ({imap_target_email})...")
            otp_code = fetch_otp_imap(
                imap_email=imap_target_email,
                imap_password=imap_password,
                imap_host=imap_host,
                imap_recipient=email,
                log_fn=log,
            )

        if not otp_code and manual_otp_getter is not None:
            reason = (
                "AYCD/IMAP did not return an OTP"
                if (aycd_key or (imap_target_email and imap_password))
                else "No OTP source configured"
            )
            log(f"{reason} — waiting for you to enter the code manually.", "warning")
            if manual_otp_notifier is not None:
                try:
                    manual_otp_notifier(
                        status="waiting",
                        reason=reason,
                        timeout_seconds=manual_otp_timeout_seconds,
                    )
                except Exception:
                    pass
            deadline = time.time() + float(manual_otp_timeout_seconds)
            last_remind = 0.0
            while time.time() < deadline:
                try:
                    candidate = manual_otp_getter()
                except Exception:
                    candidate = None
                if candidate:
                    otp_code = str(candidate).strip()
                    log("Manual OTP received.")
                    break
                now = time.time()
                if now - last_remind > 30:
                    remaining = int(deadline - now)
                    log(f"Waiting for manual OTP code… ({remaining}s left)")
                    last_remind = now
                time.sleep(1.0)
            if manual_otp_notifier is not None:
                try:
                    manual_otp_notifier(status="received" if otp_code else "timeout")
                except Exception:
                    pass

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
            save_session(session_dir, session_phone, token, log_fn=log)
            if keep_open_on_success:
                should_close_driver = False
                log("Leaving Chrome open for account review.")
            return {"ok": True, "bearer_token": token}
        else:
            return {"ok": False, "error": "Login completed but no bearer token found"}

    except Exception as exc:
        log(f"Error: {exc}", "error")
        return {"ok": False, "error": str(exc)}
    finally:
        if driver and should_close_driver:
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
