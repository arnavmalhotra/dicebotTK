"""
Dice.fm requests-based booking engine.

Replaces the Selenium-based Dice flow with direct HTTP requests using curl_cffi
for TLS fingerprint matching. Handles authentication, ticket reservation, and
Stripe payment entirely via API calls — no browser needed.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import time
import uuid
from base64 import b64encode
from urllib.parse import quote, urlencode, urlsplit

import requests as plain_requests
from curl_cffi.requests import Session as CffiSession


class DiceFM:
    """Dice.fm API client for requests-based ticket purchasing."""

    # reCAPTCHA context loaded from recaptcha_context.json (from CapSolver extension)
    _RECAPTCHA_ANCHOR = None
    _RECAPTCHA_RELOAD = None

    @staticmethod
    def normalize_event_url(event_url: str) -> tuple[str, str]:
        raw = re.sub(r"\s+", "", str(event_url or "").strip()).strip(" ,")
        if not raw:
            raise ValueError("Event URL is required")

        if re.fullmatch(r"[a-f0-9]{24}", raw, flags=re.I):
            return raw.lower(), f"https://dice.fm/event/{raw.lower()}"

        if "/" not in raw and "." not in raw:
            slug = raw.strip("/")
            return slug, f"https://dice.fm/event/{slug}"

        if raw.startswith("//"):
            raw = "https:" + raw
        elif not re.match(r"^[a-z][a-z0-9+.-]*://", raw, flags=re.I):
            raw = "https://" + raw

        parsed = urlsplit(raw)
        parts = [part for part in parsed.path.split("/") if part]
        slug = ""
        for idx, part in enumerate(parts):
            if part in ("event", "events") and idx + 1 < len(parts):
                slug = parts[idx + 1]
                break
        if not slug and parts and parts[-1] not in ("event", "events"):
            slug = parts[-1]
        slug = slug.strip("/")

        if not slug:
            raise ValueError(f"Invalid DICE event URL: {event_url}")

        return slug, f"https://dice.fm/event/{slug}"

    @classmethod
    def _load_recaptcha_context(cls):
        if cls._RECAPTCHA_ANCHOR is not None:
            return
        ctx_path = os.path.join(os.path.dirname(__file__), "recaptcha_context.json")
        if os.path.exists(ctx_path):
            with open(ctx_path) as f:
                ctx = json.load(f)
            cls._RECAPTCHA_ANCHOR = ctx.get("anchor") or None
            cls._RECAPTCHA_RELOAD = ctx.get("reload") or None

    def __init__(
        self,
        phone: str,
        email: str,
        event_url: str,
        capsolver_key: str | None = None,
        twocaptcha_key: str | None = None,
        proxy_string: str | None = None,
        code: str = "",
        target_min_price: float | None = None,
        target_max_price: float | None = None,
        ticket_tier: str | None = None,
        ticket_type_id: str | None = None,
        tier_strategy: str | None = None,
        tier_keywords: str | list | None = None,
        allowed_tier_ids: list | tuple | set | None = None,
        price_rules: list[dict] | None = None,
        session_dir: str | None = None,
        log_fn=None,
    ):
        self._log_fn = log_fn or (lambda msg, level="info": print(f"[{level}] {msg}"))
        self._load_recaptcha_context()

        # Account info
        self.email = email
        self.phone = phone
        self.capsolver_key = capsolver_key
        self.twocaptcha_key = twocaptcha_key
        self.captchafun_key: str | None = None  # set externally if needed
        self.code = (code or "").strip()
        self.target_min_price = target_min_price
        self.target_max_price = target_max_price
        self.ticket_tier = ticket_tier
        self.ticketTypePreferenceId = str(ticket_type_id).strip() if ticket_type_id else None
        normalized_strategy = str(tier_strategy or "").strip().lower().replace("-", "_")
        if normalized_strategy not in {"cheapest", "most_expensive"}:
            normalized_strategy = "cheapest"
        self.tier_strategy = normalized_strategy
        self.positive_keywords, self.negative_keywords = self._parse_tier_keywords(tier_keywords)
        self.allowed_tier_ids = {
            str(x).strip() for x in (allowed_tier_ids or []) if str(x).strip()
        }
        self.price_rules = []
        for item in (price_rules or []):
            if not isinstance(item, dict):
                continue
            try:
                qty = int(item.get("quantity"))
            except (TypeError, ValueError):
                qty = 0
            # 0 = sentinel meaning "use the tier's max_per_order"; resolved
            # in fetch_ticket_types once the chosen tier is known.
            if qty < 0:
                qty = 0
            rule = {"quantity": qty}
            raw_min = item.get("min_price")
            if raw_min not in (None, ""):
                try:
                    val = float(raw_min)
                    if val > 0:
                        rule["min_price"] = val
                except (TypeError, ValueError):
                    pass
            raw_max = item.get("max_price")
            if raw_max not in (None, ""):
                try:
                    val = float(raw_max)
                    if val > 0:
                        rule["max_price"] = val
                except (TypeError, ValueError):
                    pass
            self.price_rules.append(rule)
        self.selected_quantity: int | None = None

        # Browser fingerprint — Chrome 146 on Windows 10
        self.userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        self.secCH = '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"'
        self.fullList = '"Chromium";v="146.0.7680.178", "Not-A.Brand";v="24.0.0.0", "Google Chrome";v="146.0.7680.178"'
        self.acceptLang = "en-US,en;q=0.9"
        self.secCHArch = '"x86"'
        self.secCHBitness = '"64"'
        self.secCHPlatformVersion = '"10.0.0"'
        self.secCHModel = '""'
        self.secCHWow64 = "?0"
        self.secCHFormFactors = '"Desktop"'
        self.screenResolution = "3440x1440"

        # Dice.fm hosts
        self.frontendHost = "dice.fm"
        self.apiHost = "api.dice.fm"

        # reCAPTCHA + hCaptcha site keys
        self.recaptchaSiteKey = "6LdiBf8lAAAAAPuauvCRygX-wgKjPuJeCD0yQQf7"
        self.hcaptchaSiteKey = "463b917e-e264-403f-ad34-34af0ee10294"

        # Stripe identifiers
        self.stripeJsId = "ee551065d9efd1e9e68cc0198252c8c56b4a1ce7"
        self.stripeObjId = "sobj-" + "".join(random.choices("0123456789abcdef", k=6))
        self.stripeJsVersion = "6f8494a281"
        self.stripeGuid = str(uuid.uuid4())
        self.stripeMuid = str(uuid.uuid4())
        self.stripeSid = str(uuid.uuid4())

        # Parse event slug from URL (strip whitespace that copy-paste can introduce)
        self.eventSlug, self.eventUrl = self.normalize_event_url(event_url)

        # State variables (populated during flow)
        self.bearerToken: str | None = None
        self.eventId: str | None = None
        self.ticketTypeId: str | None = None
        self.reserveToken: str | None = None
        self.stripeClientSecret: str | None = None
        self.stripePublishableKey: str | None = None
        self.purchaseId: str | None = None
        self.piSecret: str | None = None
        self.customerSessionSecret: str | None = None
        self.elementsSessionId: str | None = None
        self.elementsSessionConfigId: str | None = None
        self.ticketName: str | None = None
        self.ticketPrice: float | None = None
        self.ticketCurrency: str | None = None
        self.ticketStatus: str | None = None
        self.ticketMaxPerOrder: int | None = None
        self.excludedTierIds: set[str] = set()
        self.purchaseTTL: int = 30
        self.purchaseQuantity: int | None = None
        self.cardLast4: str | None = None
        self.eventName: str | None = None
        self.eventDate: str | None = None
        self.eventVenue: str | None = None
        self.eventIsLocked: bool | None = None
        self.accessCodeClaimed = False
        self.lastAccessCodeError: str | None = None
        self.minPrice: float | None = None
        self.maxPrice: float | None = None
        self.flowStartTime: float | None = None
        self.lastCaptchaProvider: str | None = None  # "capsolver" or "2captcha"
        self.deviceId: str = self._generate_device_id()
        self.allTicketTypes: list = []

        # Proxy — stored raw for CapSolver, formatted for curl_cffi
        self._raw_proxy = proxy_string.strip() if proxy_string else None

        # HTTP session — use Chrome TLS fingerprint but clear default headers
        # so our custom headers are the only ones sent (no dual fingerprint).
        self.session = CffiSession(impersonate="chrome")
        self.session.headers = {}  # clear impersonate's default headers
        self.session.max_redirects = 10
        if self._raw_proxy:
            formatted = self.format_proxy(self._raw_proxy)
            self.session.proxies = {"http": formatted, "https": formatted}

        # Session persistence directory
        self._session_dir = session_dir

    # ── Logging ────────────────────────────────────────────────────────────

    def info(self, msg: str) -> None:
        self._log_fn(msg, "info")

    def warn(self, msg: str) -> None:
        self._log_fn(msg, "warning")

    def error(self, msg: str) -> None:
        self._log_fn(msg, "error")

    # ── Static helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _api_timestamp() -> str:
        return "2024-04-15"

    @staticmethod
    def format_proxy(px: str) -> str:
        """Format proxy for curl_cffi: http://user:pass@host:port"""
        px = px.strip()
        if px.startswith("http://") or px.startswith("https://") or px.startswith("socks"):
            return px
        sp = px.split(":")
        if len(sp) == 4:
            return "http://" + sp[2] + ":" + sp[3] + "@" + sp[0] + ":" + sp[1]
        elif len(sp) == 2:
            return "http://" + sp[0] + ":" + sp[1]
        return "http://" + px

    @staticmethod
    def _parse_proxy(px: str) -> dict | None:
        """Parse proxy into CapSolver format: {type, host, port, username, password}."""
        if not px:
            return None
        raw = px.strip()
        proxy_type = "http"
        for prefix in ("socks5://", "socks4://", "https://", "http://"):
            if raw.lower().startswith(prefix):
                proxy_type = prefix.rstrip(":/")
                raw = raw[len(prefix):]
                break
        # Handle user:pass@host:port format
        if "@" in raw:
            creds, hostport = raw.rsplit("@", 1)
            parts = hostport.split(":")
            cred_parts = creds.split(":", 1)
            return {
                "proxyType": proxy_type,
                "proxyAddress": parts[0],
                "proxyPort": int(parts[1]) if len(parts) > 1 else 80,
                "proxyLogin": cred_parts[0],
                "proxyPassword": cred_parts[1] if len(cred_parts) > 1 else "",
            }
        # host:port:user:pass format
        sp = raw.split(":")
        if len(sp) == 4:
            return {
                "proxyType": proxy_type,
                "proxyAddress": sp[0],
                "proxyPort": int(sp[1]),
                "proxyLogin": sp[2],
                "proxyPassword": sp[3],
            }
        if len(sp) == 2:
            return {
                "proxyType": proxy_type,
                "proxyAddress": sp[0],
                "proxyPort": int(sp[1]),
                "proxyLogin": "",
                "proxyPassword": "",
            }
        return None

    # ── Fingerprint generators ─────────────────────────────────────────────

    def _generate_device_id(self) -> str:
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"
        return "".join(random.choices(chars, k=21))

    def _generate_canvas_fingerprint(self) -> str:
        return "".join(random.choices("0123456789abcdef", k=64))

    def _generate_webgl_hash(self) -> str:
        raw = self.userAgent + self.screenResolution + str(random.randint(0, 99999))
        return hashlib.md5(raw.encode()).hexdigest()

    def _generate_stripe_fingerprint_id(self) -> str:
        return uuid.uuid4().hex

    def _generate_integrity_url(self) -> str:
        raw = b64encode(os.urandom(32)).decode().rstrip("=").replace("+", "-").replace("/", "_")
        return "https://" + self.frontendHost + "/" + raw

    def _fingerprint_headers(self) -> dict:
        return {
            "sec-ch-ua-full-version-list": self.fullList,
            "sec-ch-ua-arch": self.secCHArch,
            "sec-ch-ua-bitness": self.secCHBitness,
            "sec-ch-ua-platform-version": self.secCHPlatformVersion,
            "sec-ch-ua-model": self.secCHModel,
            "sec-ch-ua-wow64": self.secCHWow64,
            "sec-ch-ua-form-factors": self.secCHFormFactors,
        }

    # ── API helpers ────────────────────────────────────────────────────────

    def _api_get(self, path: str, extra_headers: dict | None = None, api_ts: str | None = None, auth: bool = True):
        url = "https://" + self.apiHost + path
        headers = {
            "sec-ch-ua-platform": '"Windows"',
            "Accept-Language": "en-US",
            "sec-ch-ua": self.secCH,
            "x-api-timestamp": api_ts or "2024-03-25",
            "sec-ch-ua-mobile": "?0",
            "x-client-timezone": "Europe/Berlin",
            "User-Agent": self.userAgent,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://" + self.frontendHost,
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://" + self.frontendHost + "/",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "priority": "u=1, i",
        }
        if auth and self.bearerToken:
            headers["Authorization"] = "Bearer " + self.bearerToken
        if extra_headers:
            headers.update(extra_headers)
        return self.session.get(url, headers=headers)

    def _api_post(self, path: str, body: dict, extra_headers: dict | None = None, api_ts: str | None = None, auth: bool = True):
        url = "https://" + self.apiHost + path
        headers = {
            "sec-ch-ua-platform": '"Windows"',
            "Accept-Language": "en-US",
            "sec-ch-ua": self.secCH,
            "x-api-timestamp": api_ts or "2024-03-25",
            "sec-ch-ua-mobile": "?0",
            "x-client-timezone": "Europe/Berlin",
            "User-Agent": self.userAgent,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://" + self.frontendHost,
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://" + self.frontendHost + "/",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "priority": "u=1, i",
        }
        if auth and self.bearerToken:
            headers["Authorization"] = "Bearer " + self.bearerToken
        if extra_headers:
            headers.update(extra_headers)
        return self.session.post(url, headers=headers, json=body)

    def _api_put(self, path: str, body: dict, extra_headers: dict | None = None, api_ts: str | None = None, auth: bool = True):
        url = "https://" + self.apiHost + path
        headers = {
            "sec-ch-ua-platform": '"Windows"',
            "Accept-Language": "en-US",
            "sec-ch-ua": self.secCH,
            "x-api-timestamp": api_ts or "2024-03-25",
            "sec-ch-ua-mobile": "?0",
            "x-client-timezone": "Europe/Berlin",
            "User-Agent": self.userAgent,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://" + self.frontendHost,
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://" + self.frontendHost + "/",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "priority": "u=1, i",
        }
        if auth and self.bearerToken:
            headers["Authorization"] = "Bearer " + self.bearerToken
        if extra_headers:
            headers.update(extra_headers)
        return self.session.put(url, headers=headers, json=body)

    # ── Session persistence ────────────────────────────────────────────────

    def _session_file_path(self) -> str | None:
        if not self._session_dir:
            return None
        # Match auth_harvester.save_session's format: strip + and leading zeros
        # (no country-code stripping). Previously this method stripped country
        # codes, which caused a filename mismatch with what Auth Farm wrote,
        # making cart runs report "No valid session" even when a session
        # existed on disk under the full-digit name.
        digits = self.phone.replace("+", "").lstrip("0")
        return os.path.join(self._session_dir, f"dice_session_{digits}.json")

    def _legacy_session_file_paths(self) -> list[str]:
        """Return legacy country-code-stripped paths, for reading sessions
        written by older builds of dice_requests before the format unified."""
        if not self._session_dir:
            return []
        digits = self.phone.replace("+", "")
        candidates: list[str] = []
        prefixes = ["44", "49", "33", "31", "61", "34", "39", "91", "81", "82", "86",
                    "55", "52", "90", "353", "351", "32", "41", "43", "46", "47", "45",
                    "48", "420", "36", "7"]
        for p in sorted(prefixes, key=len, reverse=True):
            if digits.startswith(p) and len(digits) > len(p) + 5:
                candidates.append(os.path.join(self._session_dir, f"dice_session_{digits[len(p):]}.json"))
                break
        else:
            if digits.startswith("1") and len(digits) == 11:
                candidates.append(os.path.join(self._session_dir, f"dice_session_{digits[1:]}.json"))
        return candidates

    def save_session(self) -> None:
        if not self.bearerToken:
            return
        path = self._session_file_path()
        if not path:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = {
            "bearer_token": self.bearerToken,
            "device_id": self.deviceId,
            "saved_at": int(time.time()),
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        self.info("Session saved for " + self.phone)

    def load_session(self) -> bool:
        path = self._session_file_path()
        if not path:
            return False
        if not os.path.exists(path):
            for legacy in self._legacy_session_file_paths():
                if os.path.exists(legacy):
                    self.info(f"Reading legacy session file at {legacy}")
                    path = legacy
                    break
            else:
                return False
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            return False
        saved_at = data.get("saved_at", 0)
        if time.time() - saved_at > 604800:  # 7 days
            self.info("Saved session expired, re-login needed")
            return False
        self.bearerToken = data.get("bearer_token")
        self.deviceId = data.get("device_id", self.deviceId)
        self.info("Found saved session, validating...")
        try:
            resp = self._api_get("/users/me", api_ts="2024-03-25")
            if resp.status_code == 200:
                self.info("Session token valid!")
                return True
            else:
                self.info(f"Session token invalid ({resp.status_code}), re-login needed")
                self.bearerToken = None
                return False
        except Exception:
            self.info("Session validation failed, re-login needed")
            self.bearerToken = None
            return False

    def clear_session(self) -> None:
        path = self._session_file_path()
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
        self.info("Session cleared for " + self.phone)

    # ── Step 1: Visit event page ───────────────────────────────────────────

    def visit_event_page(self) -> None:
        self.info("Step 1: Visiting event page...")
        url = self.eventUrl
        self.info("URL: " + url)
        headers = {
            "sec-ch-ua": self.secCH,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "User-Agent": self.userAgent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": self.acceptLang,
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Accept-Encoding": "gzip, deflate, br, zstd",
        }
        resp = self.session.get(url, headers=headers)

        match = re.search(r"/events/([a-f0-9]{24})", resp.text)
        if match:
            self.eventId = match.group(1)
            self.info("Event ID: " + self.eventId)
        else:
            self.warn("Could not extract Event ID from page")

        m = re.search(r"<title>([^<]+)</title>", resp.text)
        if m:
            self.eventName = m.group(1).split("|")[0].strip()
            self.info("Event Name: " + self.eventName)
        m = re.search(r'"startDate"\s*:\s*"([^"]+)"', resp.text)
        if m:
            self.eventDate = m.group(1)
            self.info("Event Date: " + self.eventDate)
        m = re.search(r'"name"\s*:\s*"([^"]+)".*?"address"', resp.text)
        if m:
            self.eventVenue = m.group(1)
            self.info("Venue: " + self.eventVenue)

    # ── Fetch ticket types ─────────────────────────────────────────────────

    @staticmethod
    def _parse_tier_keywords(raw) -> tuple[list[str], list[str]]:
        """Split a keyword spec into (positive, negative) token lists.

        Accepts a string ("stage vip -parking, -resale") or a list of strings.
        Tokens prefixed with '-' or '!' are negative; everything else is
        positive. Tokens are normalized lowercase, whitespace-collapsed.
        """
        positive: list[str] = []
        negative: list[str] = []
        if raw is None:
            return positive, negative
        if isinstance(raw, (list, tuple, set)):
            tokens = []
            for item in raw:
                tokens.extend(str(item or "").replace(",", " ").split())
        else:
            tokens = str(raw or "").replace(",", " ").split()
        for tok in tokens:
            t = tok.strip()
            if not t:
                continue
            if t.startswith(("-", "!")):
                term = t[1:].strip().lower()
                if term:
                    negative.append(term)
            else:
                term = t.strip().lower()
                if term:
                    positive.append(term)
        return positive, negative

    def _candidate_text_blob(self, candidate: dict) -> str:
        tt = candidate.get("tt", {}) or {}
        parts = [str(candidate.get("name") or "")]
        for key in ("description", "price_tier_name", "subtitle"):
            val = tt.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(val)
        return " ".join(parts).lower()

    def _apply_tier_filters(self, candidates: list[dict]) -> list[dict]:
        """Filter candidates by positive/negative keywords and min/max price.

        Logs each rejection so it's traceable in cart logs.
        """
        allowed_ids = self.allowed_tier_ids or set()
        # Explicit tier picks override keyword matching: when the user prefetched
        # and selected specific tier ids, ignore the keyword inputs entirely so
        # there's no surprise "selected tier was filtered out by a stale keyword."
        if allowed_ids:
            positive: list[str] = []
            negative: list[str] = []
        else:
            positive = list(self.positive_keywords or [])
            negative = list(self.negative_keywords or [])
        min_price = self.target_min_price
        max_price = self.target_max_price
        if not (positive or negative or allowed_ids or min_price is not None or max_price is not None):
            return candidates

        kept: list[dict] = []
        for c in candidates:
            tid = str((c.get("tt") or {}).get("id") or "")
            if allowed_ids and tid not in allowed_ids:
                self.info(f"  filtered: '{c['name']}' (id {tid}) not in selected tiers")
                continue
            blob = self._candidate_text_blob(c)
            price = float(c.get("price") or 0)
            hit_neg = next((n for n in negative if n in blob), None)
            if hit_neg is not None:
                self.info(f"  filtered: '{c['name']}' matches negative keyword '{hit_neg}'")
                continue
            if positive and not any(p in blob for p in positive):
                self.info(f"  filtered: '{c['name']}' does not match any positive keyword ({', '.join(positive)})")
                continue
            if min_price is not None and price < float(min_price):
                self.info(f"  filtered: '{c['name']}' priced ${price:.2f} below min ${float(min_price):.2f}")
                continue
            if max_price is not None and price > float(max_price):
                self.info(f"  filtered: '{c['name']}' priced ${price:.2f} above max ${float(max_price):.2f}")
                continue
            kept.append(c)
        if not kept:
            self.warn("All tiers were filtered out by tier filters (selection / keywords / min / max).")
        return kept

    def fetch_ticket_types(self, authenticated: bool = False) -> dict | None:
        if not self.eventId:
            self.warn("Event ID not set, skipping ticket_types")
            return None
        # Reset selection so first-match logic works on re-fetch
        self.reserveToken = None
        self.ticketTypeId = None
        self.ticketName = None
        self.ticketPrice = None
        self.ticketMaxPerOrder = None
        label = "authenticated" if authenticated else "unauthenticated"
        self.info(f"Fetching ticket_types ({label})...")
        resp = self._api_get(
            f"/events/{self.eventId}/ticket_types",
            api_ts="2024-04-15",
            auth=authenticated,
        )
        data = resp.json()
        ticket_types = data.get("ticket_types", [])
        self.allTicketTypes = ticket_types
        self.info(f"Found {len(ticket_types)} ticket type(s)")
        self.eventName = data.get("name", self.eventName)
        self.eventIsLocked = data.get("is_locked", False)

        # Parse all ticket types first
        excluded = {str(x) for x in (self.excludedTierIds or set())}
        candidates = []
        for tt in ticket_types:
            name = tt.get("name", "?")
            tid = tt.get("id", "?")
            status = tt.get("status", "?")

            raw_price = tt.get("price", 0)
            if isinstance(raw_price, dict):
                price = float(raw_price.get("amount", 0) or 0) / 100.0
                currency = raw_price.get("currency", "USD")
            else:
                try:
                    price = float(raw_price or 0)
                except (TypeError, ValueError):
                    price = 0.0
                currency = tt.get("currency", "USD")

            rt = tt.get("reserve_token")
            has_rt = "YES" if rt else "no"
            max_per = tt.get("limits", {}).get("max_increments", "?")
            excluded_marker = " | EXCLUDED (prior failure)" if str(tid) in excluded else ""
            self.info(f"  Ticket: {name} | ID: {tid} | Status: {status} | ${price:.2f} {currency} | max: {max_per} | reserve_token: {has_rt}{excluded_marker}")

            if rt and str(tid) not in excluded:
                candidates.append({"tt": tt, "name": name, "price": price, "currency": currency,
                                    "status": status, "rt": rt})

        if not candidates:
            return data

        # Apply task-level filters (keywords + min/max price) to narrow the
        # candidate pool before strategy / fuzzy / preference picks one.
        candidates = self._apply_tier_filters(candidates)
        if not candidates:
            return data

        def _fuzzy_matches(candidate, tier_name):
            from difflib import SequenceMatcher
            tier_lower = tier_name.lower().strip()
            if not tier_lower:
                return 0.0
            tt = candidate.get("tt", {}) or {}
            name_lower = candidate["name"].lower()
            extras = []
            for key in ("description", "price_tier_name", "subtitle"):
                val = tt.get(key)
                if isinstance(val, str) and val.strip():
                    extras.append(val.lower())
            blob = " ".join([name_lower, *extras])
            if tier_lower in blob:
                return 1.0
            return SequenceMatcher(None, tier_lower, name_lower).ratio()

        selected = None
        if self.ticketTypePreferenceId:
            for candidate in candidates:
                if str(candidate["tt"].get("id") or "") == self.ticketTypePreferenceId:
                    selected = candidate
                    self.info(f"  -> Exact ticket type match: {selected['name']} ({self.ticketTypePreferenceId})")
                    break
            else:
                self.warn(f"No ticket type matches '{self.ticketTypePreferenceId}'")
                return data
        elif self.ticket_tier:
            scored = [(_fuzzy_matches(c, self.ticket_tier), c) for c in candidates]
            scored.sort(key=lambda x: (-x[0], x[1]["price"]))
            if scored and scored[0][0] > 0.3:
                selected = scored[0][1]
                self.info(f"  -> Fuzzy match for '{self.ticket_tier}': {selected['name']} (score: {scored[0][0]:.2f})")
            else:
                self.warn(f"No ticket tier matches '{self.ticket_tier}'")
                return data
        else:
            if self.tier_strategy == "most_expensive":
                selected = max(candidates, key=lambda c: c["price"])
                self.info(f"  -> Strategy 'most_expensive': picked {selected['name']} (${selected['price']:.2f})")
            else:
                selected = min(candidates, key=lambda c: c["price"])
                self.info(f"  -> Strategy 'cheapest': picked {selected['name']} (${selected['price']:.2f})")

        def _describe_rule(r):
            min_p = r.get("min_price")
            max_p = r.get("max_price")
            if min_p is not None and max_p is not None:
                return f"{r.get('quantity', 1)}@${min_p:.2f}-${max_p:.2f}"
            if max_p is not None:
                return f"{r.get('quantity', 1)}@<=${max_p:.2f}"
            if min_p is not None:
                return f"{r.get('quantity', 1)}@>=${min_p:.2f}"
            return f"{r.get('quantity', 1)}@any"

        def _rule_matches(r, price):
            min_p = r.get("min_price")
            max_p = r.get("max_price")
            if min_p is not None and price < float(min_p):
                return False
            if max_p is not None and price > float(max_p):
                return False
            return True

        if self.price_rules:
            matched_rule = None
            for idx, rule in enumerate(self.price_rules, start=1):
                if _rule_matches(rule, selected["price"]):
                    matched_rule = (idx, rule)
                    break
            if matched_rule is None:
                rules_desc = ", ".join(_describe_rule(r) for r in self.price_rules)
                self.warn(
                    f"{selected['name']} priced at ${selected['price']:.2f}; no rule allowed this price "
                    f"(rules: {rules_desc})"
                )
                return data
            idx, rule = matched_rule
            tier_max = int(selected["tt"].get("limits", {}).get("max_increments") or 1)
            rule_qty = int(rule.get("quantity") or 0)
            # 0 = "use tier max"; otherwise honour the rule but cap at tier max.
            if rule_qty <= 0:
                self.selected_quantity = tier_max
                qty_label = f"max ({tier_max})"
            else:
                self.selected_quantity = min(rule_qty, tier_max)
                qty_label = str(self.selected_quantity)
            self.info(
                f"  -> Rule #{idx} matched: buy {qty_label} ({_describe_rule(rule)}) "
                f"at price ${selected['price']:.2f}"
            )

        self.reserveToken = selected["rt"]
        self.ticketTypeId = selected["tt"].get("id")
        self.ticketName = selected["name"]
        self.ticketPrice = selected["price"]
        self.ticketCurrency = selected["currency"]
        self.ticketStatus = selected["status"]
        self.ticketMaxPerOrder = int(selected["tt"].get("limits", {}).get("max_increments") or 1)
        self.info(f"  -> SELECTED: {self.ticketName} (${self.ticketPrice:.2f})")
        return data

    # ── Steps 2-4: Login flow ──────────────────────────────────────────────

    def visit_login_page(self) -> None:
        self.info("Step 2: Visiting login page...")
        headers = {
            "sec-ch-ua": self.secCH,
            "sec-ch-ua-platform": '"Windows"',
            "User-Agent": self.userAgent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": self.acceptLang,
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://" + self.frontendHost + "/event/" + self.eventSlug,
            "Accept-Encoding": "gzip, deflate, br, zstd",
            **self._fingerprint_headers(),
        }
        self.session.get("https://" + self.frontendHost + "/login", headers=headers)

        # Simulate cookies that page JavaScript would normally set.
        # Dice's API may check for these as proof the request came from a real session.
        if not self.session.cookies.get("rl_anonymous_id"):
            anon_id = str(uuid.uuid4())
            # RudderStack stores values as "RudderEncrypt:U2FsdGVk..." but the raw ID also works
            self.session.cookies.set("rl_anonymous_id", anon_id, domain=".dice.fm")
            self.session.cookies.set("rl_user_id", "", domain=".dice.fm")
            self.session.cookies.set("rl_group_id", "", domain=".dice.fm")
            self.session.cookies.set("rl_trait", "", domain=".dice.fm")
            self.session.cookies.set("rl_group_trait", "", domain=".dice.fm")
            self.session.cookies.set("rl_page_init_referrer", "", domain=".dice.fm")
            self.session.cookies.set("rl_page_init_referring_domain", "", domain=".dice.fm")
            self.session.cookies.set("__stripe_mid", str(uuid.uuid4()), domain=".dice.fm")
            self.session.cookies.set("__stripe_sid", str(uuid.uuid4()), domain=".dice.fm")

    def solve_cloudflare_challenge(self) -> bool:
        """Solve Cloudflare challenge to get cf_clearance cookie. Requires proxy + CapSolver key."""
        if not self.capsolver_key:
            self.warn("No CapSolver key — skipping CF challenge")
            return False
        proxy_str = self._capsolver_proxy_string()
        if not proxy_str:
            self.warn("No proxy — CF challenge requires a proxy")
            return False

        self.info("Solving Cloudflare challenge via CapSolver...")
        task = {
            "type": "AntiCloudflareTask",
            "websiteURL": "https://" + self.frontendHost,
            "proxy": proxy_str.replace("http:", "", 1) if proxy_str.startswith("http:") else proxy_str,
            "userAgent": self.userAgent,
        }

        try:
            resp = plain_requests.post(
                "https://api.capsolver.com/createTask",
                json={"clientKey": self.capsolver_key, "task": task},
            )
            data = resp.json()
        except Exception as exc:
            self.error(f"CF challenge createTask failed: {exc}")
            return False

        if data.get("errorId") != 0:
            self.error(f"CF challenge error: {data.get('errorDescription', 'unknown')}")
            return False

        task_id = data.get("taskId")
        if not task_id:
            self.error("CF challenge: no taskId")
            return False

        self.info(f"CF challenge taskId: {task_id}")
        for attempt in range(1, 40):
            time.sleep(2)
            try:
                result = plain_requests.post(
                    "https://api.capsolver.com/getTaskResult",
                    json={"clientKey": self.capsolver_key, "taskId": task_id},
                ).json()
            except Exception:
                continue
            if result.get("status") == "ready":
                solution = result.get("solution", {})
                cookies = solution.get("cookies", {})
                cf_clearance = cookies.get("cf_clearance", "")
                ua = solution.get("userAgent", "")
                if cf_clearance:
                    self.session.cookies.set("cf_clearance", cf_clearance, domain=".dice.fm")
                    self.info(f"Got cf_clearance: {cf_clearance[:40]}...")
                if ua:
                    self.userAgent = ua
                    self.info(f"Adopted CF UA: {ua[:60]}...")
                return bool(cf_clearance)
            if result.get("status") == "failed" or result.get("errorId"):
                self.error(f"CF challenge failed: {result.get('errorDescription', 'unknown')}")
                return False

        self.error("CF challenge timed out")
        return False

    def request_otp_code(self) -> dict:
        """Solve reCAPTCHA and request OTP. Returns {ok, status_code, error?}."""
        self.info("Step 3: Requesting OTP code...")
        self.info("Solving reCAPTCHA...")
        captcha_token = self.solve_recaptcha()
        if not captcha_token:
            self.error("Failed to solve reCAPTCHA")
            return {"ok": False, "status_code": 0, "error": "reCAPTCHA solve failed"}

        headers = {
            "sec-ch-ua": self.secCH,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Content-Type": "application/json",
            "x-captcha-type": "invisible",
            "x-captcha-token": captcha_token,
            "x-device-id": self.deviceId,
            "x-request-origin": self.frontendHost,
            "x-api-timestamp": self._api_timestamp(),
            "User-Agent": self.userAgent,
            "Accept": "application/json",
            "Origin": "https://" + self.frontendHost,
            "Referer": "https://" + self.frontendHost + "/",
            "Accept-Language": self.acceptLang,
            "x-client-timezone": "America/New_York",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "priority": "u=1, i",
        }
        body = {"phone": self.phone, "channel": "email"}

        # Log full debug info for CapSolver support diagnostic
        self.info("--- OTP REQUEST DEBUG ---")
        self.info(f"Proxy: {self._raw_proxy or 'none'}")
        self.info(f"Cookies: {list(self.session.cookies.keys())}")
        self.info(f"Time since captcha solve: {int(time.time() - self._last_solve_time)}s" if hasattr(self, '_last_solve_time') else "Time: unknown")
        self.info(f"Headers: {json.dumps({k: (v[:60] + '...' if len(str(v)) > 60 else v) for k, v in headers.items()})}")
        self.info(f"Body: {json.dumps(body)}")
        self.info("--- END DEBUG ---")

        resp = self.session.post(
            "https://" + self.apiHost + "/auth/phone/code",
            headers=headers,
            data=json.dumps(body),
        )

        if resp.status_code == 200:
            self.info("OTP sent successfully!")
            return {"ok": True, "status_code": 200}
        else:
            resp_body = resp.text[:500] if resp.text else ""
            self.error(f"OTP response: HTTP {resp.status_code}")
            self.error(f"OTP response body: {resp_body}")
            self.error(f"OTP response headers: {dict(resp.headers)}")
            return {"ok": False, "status_code": resp.status_code, "error": resp_body}

    def submit_otp_get_token(self, otp_code: str) -> bool:
        """Submit OTP and get bearer token. Returns True on success."""
        self.info("Step 4: Submitting OTP code...")
        self.info(f"Phone: {self.phone} | OTP: {otp_code}")
        auth_value = b64encode(f"Phone {self.phone}:{otp_code}".encode()).decode()
        headers = {
            "sec-ch-ua": self.secCH,
            "sec-ch-ua-platform": '"Windows"',
            "Authorization": "email " + auth_value,
            "Content-Type": "application/json",
            "x-device-id": self.deviceId,
            "x-request-origin": self.frontendHost,
            "x-api-timestamp": self._api_timestamp(),
            "User-Agent": self.userAgent,
            "Accept": "application/json",
            "Origin": "https://" + self.frontendHost,
            "Referer": "https://" + self.frontendHost + "/",
            "Accept-Language": self.acceptLang,
            "x-client-timezone": "Europe/Berlin",
            "x-dice-client": "dice.fm",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "priority": "u=1, i",
        }
        resp = self.session.get("https://" + self.apiHost + "/auth/token", headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            token = data.get("token") or data.get("access_token")
            if not token:
                token = self.session.cookies.get("DICE_API_TOKEN_NEW")
            if token:
                self.bearerToken = token
                self.info("Login successful! Token: " + token[:30] + "...")
                return True
            else:
                self.error("No token found in response")
                return False
        else:
            self.error(f"OTP submit failed ({resp.status_code}): {resp.text}")
            return False

    # ── Steps 5-8: Authenticated browsing ──────────────────────────────────

    def visit_tickets_page(self) -> None:
        self.info("Step 5: Visiting tickets page...")
        headers = {
            "sec-ch-ua": self.secCH,
            "sec-ch-ua-platform": '"Windows"',
            "User-Agent": self.userAgent,
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://" + self.frontendHost + "/login",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            **self._fingerprint_headers(),
        }
        self.session.get("https://" + self.frontendHost + "/tickets", headers=headers)

    def get_my_tickets(self) -> None:
        self.info("Step 6: Getting my tickets...")
        self._api_get("/v2/events/tickets/me", api_ts="2024-03-25")

    def get_ticket_extras(self) -> None:
        self._api_get("/v2/events/tickets/me/extras", api_ts="2024-03-15")

    def get_event_extras(self) -> None:
        if not self.eventId:
            return
        self._api_get("/v2/events/" + self.eventId + "/extras", api_ts="2024-03-25")

    def get_venues_following(self) -> None:
        self._api_get("/venues/following/ids", api_ts="2024-03-25")

    def get_waitinglist(self) -> None:
        self._api_get("/v2/waitinglist/me", api_ts="2024-03-25")

    # ── Access code ────────────────────────────────────────────────────────

    def claim_code(self, code: str | None = None):
        code = code or self.code
        if not code:
            self.warn("No access code provided")
            return None
        if not self.eventId:
            self.warn("No event ID set")
            return None
        self.info(f"Claiming access code '{code}' for event {self.eventId}")
        resp = self._api_put(
            "/v2/codes/claim",
            {"code": code, "event_id": self.eventId},
            api_ts="2024-03-25",
        )
        if 200 <= resp.status_code < 300:
            self.accessCodeClaimed = True
            self.lastAccessCodeError = None
            self.info("Code claimed successfully!")
        else:
            msg = "Unknown"
            try:
                msg = resp.json().get("message", msg)
            except Exception:
                msg = (resp.text or msg).strip() or msg
            if len(msg) > 240:
                msg = msg[:240] + "…"
            self.lastAccessCodeError = f"HTTP {resp.status_code}: {msg}"
            self.warn(f"Code claim failed: {self.lastAccessCodeError}")
        return resp

    # ── CAPTCHA solvers ────────────────────────────────────────────────────

    def _capsolver_proxy_string(self) -> str | None:
        """Format proxy as CapSolver expects: 'http:ip:port:user:pass'."""
        if not self._raw_proxy:
            return None
        raw = self._raw_proxy.strip()
        scheme = "http"
        for prefix in ("socks5://", "socks4://", "https://", "http://"):
            if raw.lower().startswith(prefix):
                scheme = prefix.rstrip(":/")
                raw = raw[len(prefix):]
                break
        # Handle user:pass@host:port
        if "@" in raw:
            creds, hostport = raw.rsplit("@", 1)
            return f"{scheme}:{hostport}:{creds}"
        # Already host:port:user:pass
        return f"{scheme}:{raw}"

    def solve_recaptcha(self) -> str | None:
        """Solve reCAPTCHA v3 Enterprise. Tries captcha.fun → CapSolver → 2Captcha."""
        if self.captchafun_key:
            token = self._solve_via_captchafun()
            if token:
                return token

        if self.capsolver_key:
            token = self._solve_via_capsolver()
            if token:
                return token

        if self.twocaptcha_key:
            token = self._solve_via_2captcha()
            if token:
                return token

        if not self.captchafun_key and not self.capsolver_key and not self.twocaptcha_key:
            self.error("No captcha API key configured")
        return None

    def _solve_via_captchafun(self) -> str | None:
        """Solve via captcha.fun API."""
        self.info("captcha.fun: solving reCAPTCHA v3 enterprise...")
        proxy_str = self._capsolver_proxy_string()
        try:
            resp = plain_requests.post(
                "https://api.captcha.fun/v1/request",
                headers={"API_KEY": self.captchafun_key, "Content-Type": "application/json"},
                json={
                    "kind": "recap_v3_enterprise",
                    "url": "https://" + self.frontendHost,
                    "siteKey": self.recaptchaSiteKey,
                    "action": "login",
                    "proxy": proxy_str or "",
                },
            )
            data = resp.json()
        except Exception as exc:
            self.error(f"captcha.fun request failed: {exc}")
            return None

        task_id = data.get("id")
        if not task_id:
            self.error(f"captcha.fun: no task id: {json.dumps(data)[:200]}")
            return None

        self.info(f"captcha.fun task: {task_id}")

        # Poll v2/response/{id} every 1s
        for attempt in range(1, 120):
            time.sleep(1)
            try:
                result = plain_requests.get(
                    f"https://api.captcha.fun/v2/response/{task_id}",
                    headers={"API_KEY": self.captchafun_key, "Accept": "application/json"},
                ).json()
            except Exception:
                continue

            if not result.get("ready"):
                continue

            if result.get("status") == "SUCCESS" and result.get("token"):
                token = result["token"]
                ua = result.get("userAgent", "")
                sec_ua = result.get("secUa", "")
                self._last_solve_time = time.time()
                self.lastCaptchaProvider = "captcha.fun"
                if ua and ua != self.userAgent:
                    self.userAgent = ua
                if sec_ua:
                    self.secCH = sec_ua
                self.info(f"captcha.fun solved ({attempt}s)! Token: {token[:80]}...")
                return token

            self.error(f"captcha.fun failed: {result.get('status', 'unknown')}")
            return None

        self.error("captcha.fun timed out (120s)")
        return None

    def _solve_via_capsolver(self) -> str | None:
        """Solve via CapSolver API with proxy + anchor/reload context."""
        proxy_str = self._capsolver_proxy_string()

        task = {
            "websiteURL": "https://" + self.frontendHost,
            "websiteKey": self.recaptchaSiteKey,
            "pageAction": "login",
            "userAgent": self.userAgent,
            "isSession": True,
        }
        if self._RECAPTCHA_ANCHOR:
            task["anchor"] = self._RECAPTCHA_ANCHOR
        if self._RECAPTCHA_RELOAD:
            task["reload"] = self._RECAPTCHA_RELOAD
        if proxy_str:
            task["type"] = "ReCaptchaV3EnterpriseTask"
            task["proxy"] = proxy_str
        else:
            task["type"] = "ReCaptchaV3EnterpriseTaskProxyLess"

        self.info(f"CapSolver: {task['type']}...")
        try:
            resp = plain_requests.post(
                "https://api.capsolver.com/createTask",
                json={"clientKey": self.capsolver_key, "task": task},
            )
            data = resp.json()
        except Exception as exc:
            self.error(f"CapSolver createTask failed: {exc}")
            return None

        if data.get("errorId") != 0:
            self.error(f"CapSolver error: {data.get('errorDescription', 'unknown')}")
            return None

        task_id = data.get("taskId")
        if data.get("status") == "ready":
            solution = data.get("solution", {})
            self._last_solve_time = time.time()
            self._apply_capsolver_solution(solution)
            token = solution.get("gRecaptchaResponse", "")
            self.info(f"CapSolver solved! Token: {token[:80]}...")
            self.lastCaptchaProvider = "capsolver"
            return token

        if not task_id:
            return None

        for attempt in range(1, 30):
            time.sleep(3)
            try:
                result = plain_requests.post(
                    "https://api.capsolver.com/getTaskResult",
                    json={"clientKey": self.capsolver_key, "taskId": task_id},
                ).json()
            except Exception:
                continue
            if result.get("status") == "ready":
                solution = result.get("solution", {})
                self._last_solve_time = time.time()
                self._apply_capsolver_solution(solution)
                token = solution.get("gRecaptchaResponse", "")
                self.info(f"CapSolver solved (attempt {attempt})! Token: {token[:80]}...")
                self.lastCaptchaProvider = "capsolver"
                return token
            if result.get("status") == "failed":
                self.error(f"CapSolver task failed")
                return None
        self.error("CapSolver timed out")
        return None

    def _solve_via_2captcha(self) -> str | None:
        """Solve via 2Captcha (proxyless, enterprise, minScore=0.9)."""
        from twocaptcha import TwoCaptcha

        self.info("2Captcha: RecaptchaV3 enterprise, minScore=0.9...")
        solver = TwoCaptcha(self.twocaptcha_key)
        try:
            result = solver.recaptcha(
                sitekey=self.recaptchaSiteKey,
                url="https://" + self.frontendHost,
                version="v3",
                enterprise=1,
                action="login",
                score=0.9,
            )
        except Exception as exc:
            self.error(f"2Captcha failed: {exc}")
            return None

        token = result.get("code", "") if isinstance(result, dict) else str(result)
        self._last_solve_time = time.time()
        self.info(f"2Captcha solved! Token: {token[:80]}...")
        self.lastCaptchaProvider = "2captcha"
        return token

    def _apply_capsolver_solution(self, solution: dict) -> None:
        """Apply userAgent, secChUa, and session cookies from a CapSolver solution."""
        # Adopt returned UA
        ua = solution.get("userAgent") or ""
        if ua and ua != self.userAgent:
            self.info(f"Adopting CapSolver UA: {ua[:80]}...")
            self.userAgent = ua

        # Adopt returned sec-ch-ua (takes priority over auto-derivation)
        sec_ch_ua = solution.get("secChUa") or ""
        if sec_ch_ua:
            self.info(f"Adopting CapSolver secChUa: {sec_ch_ua[:80]}")
            self.secCH = sec_ch_ua
        elif ua:
            # Fallback: derive from UA's Chrome version
            ver_match = re.search(r"Chrome/(\d+)", ua)
            if ver_match:
                ver = ver_match.group(1)
                self.secCH = f'"Chromium";v="{ver}", "Not-A.Brand";v="24", "Google Chrome";v="{ver}"'

        # Apply recaptcha session cookies (v3 session mode)
        ca_t = solution.get("recaptcha-ca-t") or ""
        if ca_t:
            self.session.cookies.set("recaptcha-ca-t", ca_t, domain=".dice.fm")
            self.info(f"Set recaptcha-ca-t cookie: {ca_t[:30]}...")

        ca_e = solution.get("recaptcha-ca-e") or ""
        if ca_e:
            self.session.cookies.set("recaptcha-ca-e", ca_e, domain=".dice.fm")
            self.info(f"Set recaptcha-ca-e cookie: {ca_e[:30]}...")

    def solve_hcaptcha(self) -> str | None:
        if not self.capsolver_key:
            self.warn("No CapSolver key — skipping hCaptcha solve")
            return None
        self.info("Solving hCaptcha for Stripe...")

        proxy_str = self._capsolver_proxy_string()
        task = {
            "websiteURL": "https://b.stripecdn.com",
            "websiteKey": self.hcaptchaSiteKey,
            "userAgent": self.userAgent,
        }
        if proxy_str:
            task["type"] = "HCaptchaTask"
            task["proxy"] = proxy_str
        else:
            task["type"] = "HCaptchaTaskProxyLess"

        try:
            resp = plain_requests.post(
                "https://api.capsolver.com/createTask",
                json={"clientKey": self.capsolver_key, "task": task},
            ).json()
        except Exception as exc:
            self.error(f"hCaptcha createTask failed: {exc}")
            return None

        task_id = resp.get("taskId")
        if not task_id:
            self.error("No taskId returned for hCaptcha")
            return None
        if resp.get("status") == "ready":
            return resp["solution"].get("gRecaptchaResponse") or resp["solution"].get("token")

        for attempt in range(1, 30):
            time.sleep(3)
            try:
                result = plain_requests.post(
                    "https://api.capsolver.com/getTaskResult",
                    json={"clientKey": self.capsolver_key, "taskId": task_id},
                ).json()
            except Exception:
                continue
            if result.get("status") == "ready":
                self.info(f"hCaptcha solved (attempt {attempt})")
                return result["solution"].get("gRecaptchaResponse") or result["solution"].get("token")
            if result.get("errorId"):
                break
        self.warn("hCaptcha solve failed or timed out")
        return None

    # ── Steps 10-12: Purchase + Stripe payment ─────────────────────────────

    def create_purchase(self, reserve_token: str | None = None, quantity: int = 1):
        self.info("Step 10: Creating purchase...")
        rt = reserve_token or self.reserveToken
        if not rt:
            self.error("No reserve_token available")
            return None
        body = {"line_item_attributes": [{"reserve_token": rt, "quantity": quantity}]}
        resp = self._api_post("/purchases", body, api_ts="2022-05-11")
        if not resp or resp.status_code != 200:
            return resp
        try:
            data = resp.json()
        except Exception:
            return resp
        self.purchaseId = data.get("purchase_id")
        charge = data.get("charge", {})
        self.stripePublishableKey = charge.get("payment_tokens", {}).get("stripe_publishable_key")
        self.purchaseTTL = data.get("time_to_live", 30)
        self.purchaseQuantity = quantity
        self.info(f"Purchase ID: {self.purchaseId} | TTL: {self.purchaseTTL}s")
        return resp

    def get_payment_intent(self):
        if not self.purchaseId:
            return None
        self.info("Step 11: Getting payment intent...")
        resp = self._api_post(
            f"/purchases/{self.purchaseId}/payment_intent",
            {"extra_resources": ["customer_session"]},
            api_ts="2022-05-11",
        )
        data = resp.json()
        self.piSecret = data.get("pi_secret")
        self.stripeClientSecret = data.get("pi_secret")
        self.customerSessionSecret = data.get("customer_session_secret")
        return resp

    def get_stripe_elements_session(self):
        if not self.piSecret:
            return None
        self.info("Step 11b: Getting Stripe elements session...")
        pk = self.stripePublishableKey or "pk_live_EEplHzRCrlwJv9NugR8pp6Vl00RVAvfe2q"
        params = {
            "key": pk,
            "client_secret": self.piSecret,
            "customer_session_client_secret": self.customerSessionSecret,
            "blocked_card_brands_beta_2": "2025-03-31.basil",
            "locale": "en-US",
        }
        url = "https://api.stripe.com/v1/elements/sessions?" + "&".join(f"{k}={v}" for k, v in params.items())
        headers = {
            "User-Agent": self.userAgent,
            "sec-ch-ua": self.secCH,
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://js.stripe.com",
            "Referer": "https://js.stripe.com/",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        resp = self.session.get(url, headers=headers)
        data = resp.json()
        self.elementsSessionId = data.get("session_id")
        self.elementsSessionConfigId = data.get("config_id")
        return resp

    def stripe_confirm_payment(
        self,
        card_number: str,
        exp_month: str,
        exp_year: str,
        cvc: str,
        billing_name: str,
        billing_email: str,
        billing_phone: str,
        billing_postal_code: str,
        hcaptcha_token: str | None,
        billing_country: str = "US",
    ):
        if not self.stripeClientSecret:
            self.error("No Stripe client secret")
            return None
        self.info("Step 12: Confirming Stripe payment...")
        self.cardLast4 = card_number[-4:]
        pi_id = self.stripeClientSecret.split("_secret_")[0]
        pk = self.stripePublishableKey or "pk_live_EEplHzRCrlwJv9NugR8pp6Vl00RVAvfe2q"
        time_on_page = int((time.time() - (self.flowStartTime or time.time())) * 1000)
        pua = f"stripe.js/{self.stripeJsVersion}; stripe-js-v3/{self.stripeJsVersion}; payment-element"
        return_url = f"https://dice.fm/payment-redirect-callback/{self.purchaseId}?dice_ticket_type_ids={self.ticketTypeId}"

        headers = {
            "sec-ch-ua-platform": '"Windows"',
            "User-Agent": self.userAgent,
            "Accept": "application/json",
            "sec-ch-ua": self.secCH,
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://js.stripe.com",
            "Referer": "https://js.stripe.com/",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
        }

        form = {
            "return_url": return_url,
            "payment_method_data[billing_details][name]": billing_name,
            "payment_method_data[billing_details][email]": billing_email,
            "payment_method_data[billing_details][phone]": billing_phone,
            "payment_method_data[billing_details][address][postal_code]": billing_postal_code,
            "payment_method_data[billing_details][address][country]": billing_country,
            "payment_method_data[type]": "card",
            "payment_method_data[card][number]": card_number,
            "payment_method_data[card][cvc]": cvc,
            "payment_method_data[card][exp_year]": exp_year,
            "payment_method_data[card][exp_month]": exp_month,
            "payment_method_data[allow_redisplay]": "unspecified",
            "payment_method_data[pasted_fields]": "number",
            "payment_method_data[payment_user_agent]": pua,
            "payment_method_data[referrer]": "https://" + self.frontendHost,
            "payment_method_data[time_on_page]": str(time_on_page),
            "payment_method_data[guid]": self.stripeGuid,
            "payment_method_data[muid]": self.stripeMuid,
            "payment_method_data[sid]": self.stripeSid,
            "expected_payment_method_type": "card",
            "set_as_default_payment_method": "false",
            "use_stripe_sdk": "true",
            "key": pk,
            "_stripe_version": "2025-03-31.basil",
            "client_secret": self.stripeClientSecret,
        }
        if hcaptcha_token:
            form["radar_options[hcaptcha_token]"] = hcaptcha_token

        resp = self.session.post(
            f"https://api.stripe.com/v1/payment_intents/{pi_id}/confirm",
            headers=headers,
            data=urlencode(form),
        )
        return resp

    # ── Stripe fingerprinting ──────────────────────────────────────────────

    def _m_stripe_headers(self) -> dict:
        return {
            "sec-ch-ua-platform": '"Windows"',
            "User-Agent": self.userAgent,
            "sec-ch-ua": self.secCH,
            "Content-Type": "text/plain;charset=UTF-8",
            "Accept": "*/*",
            "Origin": "https://m.stripe.network",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://m.stripe.network/",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": self.acceptLang,
            "priority": "u=1, i",
        }

    def _m_stripe_common_b(self, integrity_url: str) -> dict:
        ts = int(time.time() * 1000)
        h = hashlib.sha256(self.stripeMuid.encode()).hexdigest()
        seg = b64encode(os.urandom(32)).decode().rstrip("=").replace("+", "-").replace("/", "_")
        return {
            "a": h, "b": integrity_url, "c": seg, "d": self.stripeSid,
            "e": round(random.uniform(150, 250), 1), "f": False, "g": True,
            "h": "location", "i": ts, "u": self.stripeMuid, "w": self.frontendHost,
        }

    def _m_stripe_encode(self, data: dict) -> str:
        return b64encode(quote(json.dumps(data, separators=(",", ":"))).encode()).decode()

    def send_stripe_fingerprint(self) -> None:
        fp_id = self._generate_stripe_fingerprint_id()
        integrity = self._generate_integrity_url()
        hdrs = self._m_stripe_headers()

        # Call 1: v2 fingerprint
        p1 = {
            "v2": 1, "id": fp_id,
            "t": hashlib.md5(self.stripeMuid.encode()).hexdigest(),
            "tag": "$npm_package_version", "src": "js",
            "a": round(random.uniform(4, 8), 0),
            "b": self._m_stripe_common_b(integrity),
        }
        try:
            r = self.session.post("https://m.stripe.com/6", headers=hdrs, data=self._m_stripe_encode(p1))
            if r.status_code == 200:
                d = r.json()
                self.stripeMuid = d.get("muid", self.stripeMuid)
                self.stripeGuid = d.get("guid", self.stripeGuid)
        except Exception:
            pass

        # Call 2: canvas/webgl
        p2 = {
            "v": 2, "a": round(random.uniform(3, 6), 0),
            "b": self._m_stripe_common_b(integrity),
            "c": self._generate_canvas_fingerprint(),
            "d": self._generate_webgl_hash(),
            "e": "true",
            "f": {"v": "en-US,en", "t": "Win32"},
            "h": "en-US,en", "i": "Win32",
            "j": "PDF Viewer,internal-pdf-viewer,application/pdf,pdf++text/pdf,pdf, Chrome PDF Viewer,internal-pdf-viewer,application/pdf,pdf++text/pdf,pdf, Chromium PDF Viewer,internal-pdf-viewer,application/pdf,pdf++text/pdf,pdf, Microsoft Edge PDF Viewer,internal-pdf-viewer,application/pdf,pdf++text/pdf,pdf, WebKit built-in PDF,internal-pdf-viewer,application/pdf,pdf++text/pdf,pdf",
            "k": "3440w_1440h_32d_1r",
            "m": "sessionStorage-enabled, localStorage-enabled",
        }
        try:
            self.session.post("https://m.stripe.com/6", headers=hdrs, data=self._m_stripe_encode(p2))
        except Exception:
            pass

        # Call 3: mouse timings
        p3 = {
            "muid": self.stripeMuid, "sid": self.stripeSid,
            "url": integrity, "source": "mouse-timings-10-v2",
            "data": [[random.randint(6, 12), random.randint(6, 12)] for _ in range(10)],
        }
        try:
            self.session.post("https://m.stripe.com/6", headers=hdrs, data=self._m_stripe_encode(p3))
        except Exception:
            pass

        self.info("Stripe device fingerprint registered")

    def send_stripe_mouse_timings(self) -> None:
        try:
            body = self._m_stripe_encode({
                "muid": self.stripeMuid, "sid": self.stripeSid,
                "url": self._generate_integrity_url(), "source": "mouse-timings-10",
                "data": [[random.randint(6, 12), random.randint(6, 12)] for _ in range(10)],
            })
            self.session.post("https://m.stripe.com/6", headers=self._m_stripe_headers(), data=body)
        except Exception:
            pass

    def _stripe_link_get_cookie(self) -> None:
        try:
            hdrs = {
                "User-Agent": self.userAgent, "sec-ch-ua": self.secCH,
                "Accept": "application/json",
                "Origin": "https://js.stripe.com", "Referer": "https://js.stripe.com/",
                "Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
            }
            self.session.get(
                "https://merchant-ui-api.stripe.com/link/get-cookie?referrer_host=" + self.frontendHost,
                headers=hdrs,
            )
        except Exception:
            pass

    # ── Stripe telemetry ───────────────────────────────────────────────────

    def _send_stripe_telemetry(self, events: list[dict]) -> None:
        try:
            pk = self.stripePublishableKey or "pk_live_EEplHzRCrlwJv9NugR8pp6Vl00RVAvfe2q"
            evts = []
            for e in events:
                evt = {
                    "event_name": e.get("event_name"),
                    "created": int(time.time() * 1000),
                    "event_id": str(uuid.uuid4()),
                    "key": pk, "key_mode": "live",
                    "referrer": "https://" + self.frontendHost,
                    "stripe_js_id": self.stripeJsId,
                    "stripe_obj_id": self.stripeObjId,
                    "browser_timezone": "Europe/Berlin",
                    "wrapper": "react-stripe-js",
                    "elements_init_source": "stripe.elements",
                }
                evt.update(e)
                evts.append(evt)
            body = "client_id=stripe-js&num_requests=" + str(len(evts)) + "&events=" + quote(json.dumps(evts))
            self.session.post(
                "https://r.stripe.com/b",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://js.stripe.com", "Referer": "https://js.stripe.com/",
                    "sec-ch-ua": self.secCH, "User-Agent": self.userAgent,
                    "Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
                    "Accept-Encoding": "gzip, deflate, br, zstd",
                },
                data=body,
            )
        except Exception:
            pass

    def _post_payment_telemetry(self, success: bool) -> None:
        try:
            if success:
                self._send_stripe_telemetry([{
                    "event_name": "link_funnel.non_link_checkout_confirmation_success",
                    "surface": "payment-element",
                }])
            else:
                self._send_stripe_telemetry([{
                    "event_name": "link_funnel.non_link_checkout_confirmation_error",
                    "surface": "payment-element",
                }])
        except Exception:
            pass

    # ── Orchestrators ──────────────────────────────────────────────────────

    def run_auth_flow(self) -> str:
        """Run auth + browse flow (everything except purchase).

        Returns:
            "otp_needed" — caller must fetch OTP, then call continue_after_otp()
            "ready"      — authenticated and ticket info loaded
            "auth_failed" — login failed
            "event_locked" — event requires an access code we don't have
            "no_tickets"  — no reserve_token found
        """
        self.flowStartTime = time.time()
        self.visit_event_page()
        time.sleep(0.5)
        self.fetch_ticket_types(authenticated=False)
        time.sleep(0.3)

        if self.load_session():
            self.info("Using saved session, skipping login flow")
            return self._post_auth_browsing()

        self.visit_login_page()

        # Solve Cloudflare challenge to get cf_clearance cookie
        if not self.session.cookies.get("cf_clearance"):
            self.solve_cloudflare_challenge()

        # Request OTP — try captcha.fun (5) → CapSolver (5) → 2Captcha (5)
        otp_sent = False
        saved_keys = (self.captchafun_key, self.capsolver_key, self.twocaptcha_key)
        providers = []
        if self.captchafun_key:
            providers.append(("captcha.fun", self.captchafun_key, None, None))
        if self.capsolver_key:
            providers.append(("capsolver", None, self.capsolver_key, None))
        if self.twocaptcha_key:
            providers.append(("2captcha", None, None, self.twocaptcha_key))

        max_per_provider = 5
        total_attempts = 0

        for provider_name, cfun_key, cap_key, twocap_key in providers:
            self.captchafun_key = cfun_key
            self.capsolver_key = cap_key
            self.twocaptcha_key = twocap_key
            self.info(f"--- Trying {provider_name} (up to {max_per_provider} attempts) ---")

            for attempt in range(1, max_per_provider + 1):
                total_attempts += 1
                result = self.request_otp_code()
                if result["ok"]:
                    otp_sent = True
                    break
                elif result["status_code"] == 429:
                    self.warn(f"Rate limited, waiting 35s ({total_attempts})")
                    time.sleep(35)
                else:
                    self.warn(f"OTP attempt {total_attempts} failed ({provider_name}), retrying in 5s...")
                    time.sleep(5)
            if otp_sent:
                break

        # Restore all keys
        self.captchafun_key, self.capsolver_key, self.twocaptcha_key = saved_keys

        if not otp_sent:
            self.error(f"Failed to send OTP after {total_attempts} attempts across all providers")
            return "auth_failed"

        return "otp_needed"

    def continue_after_otp(self, otp_code: str) -> str:
        """Submit OTP and complete the auth + browse flow.

        Returns: "ready", "auth_failed", "event_locked", or "no_tickets"
        """
        if not self.submit_otp_get_token(otp_code):
            return "auth_failed"
        self.save_session()
        return self._post_auth_browsing()

    def _post_auth_browsing(self) -> str:
        """Authenticated browsing steps after login. Returns final status."""
        self.visit_tickets_page()
        self._api_get("/users/me", api_ts="2024-03-25")
        if self.eventId:
            try:
                self._api_get(f"/events/{self.eventId}/lineup")
            except Exception:
                pass
        try:
            self._api_get("/interests/me/ids")
        except Exception:
            pass

        if self.code:
            if not self.accessCodeClaimed:
                resp = self.claim_code()
                if resp and 200 <= resp.status_code < 300:
                    self.fetch_ticket_types(authenticated=True)
                elif self.eventIsLocked:
                    return "event_locked"
        elif self.eventIsLocked:
            self.error("Event is locked but no access code provided")
            return "event_locked"

        if not self.reserveToken:
            self.fetch_ticket_types(authenticated=True)

        self.get_my_tickets()
        self.get_waitinglist()
        self.get_venues_following()
        self.get_ticket_extras()
        self.get_event_extras()

        self.info("=" * 50)
        self.info("Auth + browse flow complete.")
        self.info(f"Event: {self.eventName} | Date: {self.eventDate} | Venue: {self.eventVenue}")
        self.info(f"Ticket: {self.ticketName} | ${self.ticketPrice}")

        if self.reserveToken:
            return "ready"
        else:
            self.warn("No reserve_token — ticket may be sold out")
            return "no_tickets"

    # hCaptcha tokens issued by the captcha solvers stay valid for ~120s on
    # Stripe's side. If approval drags past this threshold between
    # prepare_purchase and finalize_purchase, finalize re-solves before
    # confirming so the token isn't rejected for staleness.
    _HCAPTCHA_REFRESH_AGE_SECONDS = 90.0

    def prepare_purchase(self, quantity: int = 1) -> tuple[bool, str]:
        """Stage everything up to (but NOT including) the card-confirm step.

        Reserves real inventory on Dice via POST /purchases, fetches the
        Stripe payment intent, runs the Stripe.js handshake mimicry, and
        pre-solves hCaptcha. After this returns ok, the cart is genuinely
        held on Dice for `purchaseTTL` seconds; finalize_purchase only needs
        to fire the single stripe_confirm_payment call.

        SAFETY: this function takes no card arguments and never calls
        stripe_confirm_payment. Card data cannot reach Dice or Stripe from
        this code path.
        """
        if not self.reserveToken:
            self.error("No reserve_token — run auth flow first")
            return False, "No reserve_token"

        self.info(f"Preparing purchase: {self.ticketName} ${self.ticketPrice} x{quantity}")

        max_tier_attempts = 6
        resp = None
        for attempt in range(1, max_tier_attempts + 1):
            # Refresh ticket types every attempt so we always send a fresh
            # reserve_token. This handles tokens that went stale during a
            # scheduled-drop wait, and re-picks the next tier after a failure.
            self.fetch_ticket_types(authenticated=True)
            if not self.reserveToken:
                self.error("No tier available with a reserve token")
                return False, "No tier available with a reserve token"
            quantity = max(1, int(self.selected_quantity or quantity))
            allowed = max(1, int(self.ticketMaxPerOrder or quantity))
            if quantity > allowed:
                self.warn(f"Capping quantity to tier max ({allowed}); rule asked for {quantity}.")
                quantity = allowed
            resp = self.create_purchase(quantity=quantity)
            if resp and resp.status_code == 200 and self.purchaseId:
                break
            err_key = ""
            err_text = ""
            if resp is not None:
                err_text = resp.text or ""
                try:
                    err_key = (resp.json() or {}).get("key", "") or ""
                except Exception:
                    err_key = ""
            retriable = err_key in {"ticket_type_not_enough_tickets", "ticket_type_sold_out"}
            if not retriable:
                self.error(f"Purchase creation failed: {err_text or 'no response'}")
                return False, f"Purchase creation failed: {err_text or 'no response'}"
            failed_tier = self.ticketTypeId
            self.warn(
                f"Tier '{self.ticketName}' rejected ({err_key}); excluding and trying next tier "
                f"(attempt {attempt}/{max_tier_attempts})."
            )
            if failed_tier:
                self.excludedTierIds.add(str(failed_tier))
                if self.ticketTypePreferenceId and str(self.ticketTypePreferenceId) == str(failed_tier):
                    self.ticketTypePreferenceId = None
        else:
            self.error(f"Exhausted tier fallback after {max_tier_attempts} attempts.")
            return False, f"Exhausted tier fallback after {max_tier_attempts} attempts."
        time.sleep(0.5)

        self.purchaseQuantity = quantity

        self.get_payment_intent()
        if not self.piSecret:
            self.error("No payment intent secret returned")
            return False, "No payment intent secret returned"
        time.sleep(0.3)

        # Stripe telemetry + fingerprinting
        self._send_stripe_telemetry([
            {"event_name": "elements.controller.load"},
            {"event_name": "elements.elements"},
        ])
        self.send_stripe_fingerprint()
        time.sleep(0.2)
        self.get_stripe_elements_session()
        self._stripe_link_get_cookie()

        # Pre-solve hCaptcha so finalize is a single confirm POST. Stash the
        # token + solve time so finalize can re-solve if approval drags past
        # the captcha's expiry window.
        self.hcaptcha_token = self.solve_hcaptcha()
        self.hcaptcha_solved_at = time.time()
        self.send_stripe_mouse_timings()

        return True, ""

    def finalize_purchase(
        self,
        card_number: str,
        exp_month: str,
        exp_year: str,
        cvc: str,
        billing_name: str,
        billing_email: str,
        billing_phone: str,
        billing_postal_code: str,
        billing_country: str = "US",
    ) -> bool:
        """Submit card details and confirm the Stripe payment.

        SAFETY: this is the ONLY function in the purchase pipeline that
        accepts or transmits card data. It must only be called after
        prepare_purchase returned ok AND any required approval has been
        granted; the worker enforces the approval gate.
        """
        if not self.purchaseId or not self.piSecret:
            self.error("finalize_purchase called before prepare_purchase succeeded")
            return False

        qty = getattr(self, "purchaseQuantity", 1) or 1
        self.info(f"Finalizing purchase: {self.ticketName} x{qty}")
        self.info(f"Card: ****{card_number[-4:]}")

        hcaptcha_token = getattr(self, "hcaptcha_token", None)
        solved_at = getattr(self, "hcaptcha_solved_at", 0.0) or 0.0
        age = time.time() - solved_at if solved_at else float("inf")
        if not hcaptcha_token or age > self._HCAPTCHA_REFRESH_AGE_SECONDS:
            self.info(
                f"hCaptcha token stale (age {age:.1f}s) — re-solving before confirm."
            )
            hcaptcha_token = self.solve_hcaptcha()
            self.hcaptcha_token = hcaptcha_token
            self.hcaptcha_solved_at = time.time()
            # Refresh mouse-timing telemetry too so it lines up with the new solve.
            self.send_stripe_mouse_timings()

        resp = self.stripe_confirm_payment(
            card_number=card_number,
            exp_month=exp_month,
            exp_year=exp_year,
            cvc=cvc,
            billing_name=billing_name,
            billing_email=billing_email,
            billing_phone=billing_phone,
            billing_postal_code=billing_postal_code,
            hcaptcha_token=hcaptcha_token,
            billing_country=billing_country,
        )

        if resp and resp.status_code == 200:
            status = resp.json().get("status")
            if status in ("succeeded", "requires_action"):
                self._post_payment_telemetry(True)
                self.info(f"Payment successful! Status: {status}")
                return True

        self._post_payment_telemetry(False)
        error_text = resp.text if resp else "no response"
        self.error(f"Payment failed: {error_text}")
        return False

    def run_purchase_flow(
        self,
        quantity: int = 1,
        card_number: str = "",
        exp_month: str = "",
        exp_year: str = "",
        cvc: str = "",
        billing_name: str = "",
        billing_email: str = "",
        billing_phone: str = "",
        billing_postal_code: str = "",
        billing_country: str = "US",
    ) -> bool:
        """Backwards-compatible wrapper: prepare + finalize back to back.

        Callers that need an approval gate between the two phases should
        invoke prepare_purchase and finalize_purchase directly.
        """
        ok, _ = self.prepare_purchase(quantity=quantity)
        if not ok:
            return False
        return self.finalize_purchase(
            card_number=card_number,
            exp_month=exp_month,
            exp_year=exp_year,
            cvc=cvc,
            billing_name=billing_name,
            billing_email=billing_email,
            billing_phone=billing_phone,
            billing_postal_code=billing_postal_code,
            billing_country=billing_country,
        )
