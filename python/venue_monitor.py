from __future__ import annotations

import html
import json
import re
from typing import Any

import requests

import dice_requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def venue_display_name(venue_url: str) -> str:
    venue_name = venue_url.rstrip("/").split("/")[-1].replace("-", " ").title()
    parts = venue_name.rsplit(" ", 1)
    if len(parts) > 1 and len(parts[-1]) <= 5 and parts[-1].isalnum():
        venue_name = parts[0]
    return venue_name


def normalize_event_url(url: str) -> str:
    if not url:
        return ""
    return url.strip().split("#", 1)[0].split("?", 1)[0].rstrip("/")


def _extract_raw_events(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("event"), list):
        return [item for item in payload["event"] if isinstance(item, dict)]
    graph = payload.get("@graph")
    if isinstance(graph, list):
        out = []
        for item in graph:
            if not isinstance(item, dict):
                continue
            item_type = item.get("@type")
            if item_type in {"Event", "MusicEvent", "Festival"}:
                out.append(item)
        return out
    item_type = payload.get("@type")
    if item_type in {"Event", "MusicEvent", "Festival"}:
        return [payload]
    return []


def fetch_venue_events(venue_url: str) -> list[dict]:
    resp = requests.get(venue_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    matches = re.findall(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        resp.text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not matches:
        return []

    raw_events: list[dict] = []
    for block in matches:
        try:
            payload = json.loads(html.unescape(block.strip()))
        except Exception:
            continue
        extracted = _extract_raw_events(payload)
        if extracted:
            raw_events = extracted
            break

    events = []
    for ev in raw_events:
        location = ev.get("location")
        if isinstance(location, list):
            venue = location[0].get("name", "") if location and isinstance(location[0], dict) else ""
        elif isinstance(location, dict):
            venue = location.get("name", "")
        else:
            venue = ""
        url = normalize_event_url(ev.get("url", ""))
        if not url:
            continue
        events.append({
            "name": ev.get("name", "Unknown Event"),
            "url": url,
            "start_date": ev.get("startDate", ""),
            "end_date": ev.get("endDate", ""),
            "venue": venue,
            "description": ev.get("description", ""),
            "status": ev.get("eventStatus", ""),
        })
    return events


def fetch_event_snapshot(
    event_url: str,
    bearer_token: str,
    *,
    phone: str = "",
    email: str = "",
    proxy: str = "",
) -> dict:
    client = dice_requests.DiceFM(
        phone=phone or "0000000000",
        email=email or "",
        event_url=event_url,
        proxy_string=proxy or None,
        log_fn=lambda *_args, **_kwargs: None,
    )
    client.bearerToken = bearer_token
    client.visit_event_page()

    payload: dict[str, Any] = {}
    if client.eventId:
        try:
            payload = client.fetch_ticket_types(authenticated=True) or {}
        except Exception:
            payload = {}

    tiers = []
    for tt in client.allTicketTypes or []:
        raw_price = tt.get("price", 0)
        if isinstance(raw_price, dict):
            cents = int(raw_price.get("amount", 0) or 0)
            currency = raw_price.get("currency", "USD")
        else:
            cents = int(float(raw_price or 0) * 100)
            currency = tt.get("currency", "USD")
        price_tier = tt.get("price_tier") or {}
        limits = tt.get("limits") or {}
        tiers.append({
            "id": str(tt.get("id", "")),
            "name": tt.get("name", "Unknown"),
            "status": tt.get("status", ""),
            "secondary_status": tt.get("secondary_status"),
            "price_cents": cents,
            "price": cents / 100.0,
            "currency": currency,
            "price_tier_index": price_tier.get("index"),
            "price_tier_name": price_tier.get("name"),
            "max_per_order": limits.get("max_increments", 1),
            "has_reserve_token": bool(tt.get("reserve_token")),
        })

    return {
        "event_url": normalize_event_url(event_url),
        "event_id": client.eventId or "",
        "event_name": payload.get("name") or client.eventName or "",
        "event_status": payload.get("status") or "",
        "secondary_status": payload.get("secondary_status"),
        "venue_name": ((payload.get("venues") or [{}])[0].get("name", "") if payload.get("venues") else client.eventVenue or ""),
        "event_start": ((payload.get("dates") or {}).get("event_start_date") or client.eventDate or ""),
        "is_locked": bool(payload.get("is_locked", False)),
        "tiers": tiers,
    }
