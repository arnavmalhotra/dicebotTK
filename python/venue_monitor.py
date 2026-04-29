from __future__ import annotations

import json
import re
from html import unescape

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

API_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Api-Timestamp": "2024-03-25",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Origin": "https://dice.fm",
    "Referer": "https://dice.fm/",
    "Accept-Language": "en-US,en;q=0.9",
}


def format_proxy(proxy: str) -> str:
    raw = (proxy or "").strip()
    if not raw:
        return ""
    if raw.startswith(("http://", "https://", "socks4://", "socks5://")):
        return raw
    if "@" in raw:
        return f"http://{raw}"
    parts = raw.split(":")
    if len(parts) == 4:
        return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    return f"http://{raw}"


def _request_kwargs(proxy_picker=None) -> dict:
    if not proxy_picker:
        return {}
    proxy = proxy_picker()
    if not proxy:
        return {}
    formatted = format_proxy(proxy)
    if not formatted:
        return {}
    return {"proxies": {"http": formatted, "https": formatted}}


def normalize_venue_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def normalize_event_url(url: str) -> str:
    if not url:
        return ""
    return url.strip().split("#", 1)[0].split("?", 1)[0].rstrip("/")


def venue_display_name(venue_url: str) -> str:
    name = normalize_venue_url(venue_url).split("/")[-1].replace("-", " ").title()
    parts = name.rsplit(" ", 1)
    if len(parts) > 1 and len(parts[-1]) <= 5 and parts[-1].isalnum():
        return parts[0]
    return name


def _json_ld_blocks(html: str) -> list[object]:
    blocks = []
    pattern = re.compile(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        re.IGNORECASE | re.DOTALL,
    )
    for raw in pattern.findall(html or ""):
        text = unescape((raw or "").strip())
        if not text:
            continue
        try:
            blocks.append(json.loads(text))
        except Exception:
            continue
    return blocks


def _collect_events(node) -> list[dict]:
    events = []
    if isinstance(node, list):
        for item in node:
            events.extend(_collect_events(item))
        return events
    if not isinstance(node, dict):
        return events

    node_type = node.get("@type")
    types = node_type if isinstance(node_type, list) else [node_type]
    if any(isinstance(item, str) and item.endswith("Event") for item in types):
        return [node]

    if isinstance(node.get("event"), list):
        events.extend(_collect_events(node.get("event")))
    elif isinstance(node.get("event"), dict):
        events.extend(_collect_events(node.get("event")))

    if isinstance(node.get("@graph"), list):
        events.extend(_collect_events(node.get("@graph")))

    return events


def fetch_venue_events(venue_url: str, proxy_picker=None) -> list[dict]:
    """Fetch events from a DICE venue page via JSON-LD."""
    resp = requests.get(
        normalize_venue_url(venue_url),
        headers=HEADERS,
        timeout=15,
        **_request_kwargs(proxy_picker),
    )
    resp.raise_for_status()

    events: list[dict] = []
    for block in _json_ld_blocks(resp.text):
        for ev in _collect_events(block):
            location = ev.get("location") or {}
            if isinstance(location, list):
                venue_name = (location[0] or {}).get("name", "") if location else ""
            else:
                venue_name = location.get("name", "")
            events.append({
                "name": ev.get("name", "Unknown Event"),
                "url": ev.get("url", ""),
                "start_date": ev.get("startDate", ""),
                "end_date": ev.get("endDate", ""),
                "venue": venue_name,
                "description": ev.get("description", ""),
                "status": ev.get("eventStatus", ""),
            })

    deduped = {}
    for ev in events:
        key = normalize_event_url(ev.get("url") or "")
        if key:
            deduped[key] = ev
    return list(deduped.values())


def _parse_ticket_types(ticket_types: list[dict]) -> list[dict]:
    parsed = []
    for tt in ticket_types or []:
        raw_price = tt.get("price") or {}
        if isinstance(raw_price, dict):
            amount = int(raw_price.get("amount") or 0)
            currency = raw_price.get("currency", "USD")
        else:
            try:
                amount = int(float(raw_price or 0) * 100)
            except (TypeError, ValueError):
                amount = 0
            currency = tt.get("currency", "USD")

        pt = tt.get("price_tier") or {}
        parsed.append({
            "id": str(tt.get("id") or ""),
            "name": tt.get("name", "Unknown"),
            "status": tt.get("status", ""),
            "secondary_status": tt.get("secondary_status"),
            "price_cents": amount,
            "currency": currency,
            "price_tier_index": pt.get("index"),
            "price_tier_name": pt.get("name"),
            "max_per_order": tt.get("limits", {}).get("max_increments", 1),
            "has_reserve_token": bool(tt.get("reserve_token")),
        })

    parsed.sort(key=lambda t: (t.get("price_cents") or 0, t.get("name") or ""))
    return parsed


def fetch_event_preview(
    event_url: str,
    preview_account: dict | None = None,
    session_dir: str | None = None,
    log_fn=None,
    proxy_picker=None,
) -> dict:
    """Return event metadata and live price tiers for a DICE event URL."""
    normalized_url = normalize_event_url(event_url)
    errors: list[str] = []
    auth_used = False

    resp = requests.get(
        normalized_url,
        headers=HEADERS,
        timeout=15,
        **_request_kwargs(proxy_picker),
    )
    resp.raise_for_status()
    html = resp.text

    event_id_match = re.search(r"/events/([a-f0-9]{24})", html)
    event_id = event_id_match.group(1) if event_id_match else ""

    title_match = re.search(r"<title>([^<]+)</title>", html)
    event_name = title_match.group(1).split("|")[0].strip() if title_match else ""

    date_match = re.search(r'"startDate"\s*:\s*"([^"]+)"', html)
    event_date = date_match.group(1) if date_match else ""

    venue_match = re.search(r'"name"\s*:\s*"([^"]+)".*?"address"', html)
    event_venue = venue_match.group(1) if venue_match else ""

    payload = {}
    if event_id:
        try:
            ticket_resp = requests.get(
                f"https://api.dice.fm/events/{event_id}/ticket_types",
                headers=API_HEADERS,
                timeout=15,
                **_request_kwargs(proxy_picker),
            )
            ticket_resp.raise_for_status()
            payload = ticket_resp.json()
        except Exception as exc:
            errors.append(f"public tier fetch failed: {exc}")

        if not payload and preview_account and preview_account.get("bearer_token"):
            try:
                auth_headers = {
                    **API_HEADERS,
                    "Authorization": f"Bearer {preview_account['bearer_token']}",
                }
                ticket_resp = requests.get(
                    f"https://api.dice.fm/events/{event_id}/ticket_types",
                    headers=auth_headers,
                    timeout=15,
                    **_request_kwargs(proxy_picker),
                )
                ticket_resp.raise_for_status()
                payload = ticket_resp.json()
                auth_used = True
            except Exception as exc:
                errors.append(f"authenticated tier fetch failed: {exc}")
    else:
        errors.append("Could not extract event ID from event page")

    tiers = _parse_ticket_types(payload.get("ticket_types") or [])

    return {
        "event_url": normalized_url,
        "event_name": payload.get("name") or event_name,
        "event_date": (
            (payload.get("dates") or {}).get("event_start_date")
            or event_date
        ),
        "event_venue": (
            ((payload.get("venues") or [{}])[0] or {}).get("name", "")
            or event_venue
        ),
        "event_id": payload.get("id") or event_id,
        "event_locked": bool(payload.get("is_locked")),
        "tiers": tiers,
        "tiers_error": " | ".join(errors) if errors else "",
        "auth_used": auth_used,
    }
