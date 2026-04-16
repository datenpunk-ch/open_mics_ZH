"""Optional LLM step during enrich: infer venue address from scraped page text.

Enable with ``python -m scrapers enrich --venue-llm`` and set ``OPENAI_API_KEY``.
For OpenAI-compatible gateways, set ``OPENAI_BASE_URL`` (default ``https://api.openai.com``).

The model must only use evidence from the provided text; flatten prefers this only when
the existing location string still looks incomplete (no Swiss PLZ, etc.).
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from typing import Any

_DEFAULT_MODEL = "gpt-4o-mini"

_SYSTEM = """You extract venue location fields for an event page (often comedy / open mic in Zürich, CH).

Hard rules:
- Use ONLY facts supported by the provided page text, URL, or link list. If unsure, use null.
- Do NOT invent house numbers or streets that are not clearly stated (or clearly implied by a full postal address block).
- Swiss postal codes are 4 digits (often 8xxx for Zürich area).
- Output a single JSON object with exactly these keys:
  venue_name (string or null)
  street_line (string or null)   // e.g. "Brauerstrasse 42" or "Zähringerstrasse 33"
  postal_code (string or null)   // 4 digits
  city (string or null)
  country (string or null)       // default "CH" when clearly Switzerland
  confidence (number 0-1)        // your confidence that street_line + postal_code + city are correct together
  evidence (string)              // short verbatim quote from page_text supporting street/postal (may be "")

No markdown, no commentary — JSON only."""


def _strip_json_fence(s: str) -> str:
    t = (s or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.I)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def _parse_llm_json(text: str) -> dict[str, Any] | None:
    raw = _strip_json_fence(text)
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}\s*$", raw)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return obj if isinstance(obj, dict) else None


def _build_formatted_location(obj: dict[str, Any], *, fallback_venue: str) -> str:
    venue = (obj.get("venue_name") or fallback_venue or "").strip()
    street = (obj.get("street_line") or "").strip()
    plz = (obj.get("postal_code") or "").strip()
    city = (obj.get("city") or "").strip() or "Zürich"
    country = (obj.get("country") or "").strip()

    parts: list[str] = []
    if venue:
        parts.append(venue)
    tail_bits = []
    if street:
        tail_bits.append(street)
    if plz or city:
        pc = f"{plz} {city}".strip() if plz else city
        tail_bits.append(pc)
    if country and country.upper() not in {"CH", "CHE"}:
        tail_bits.append(country)
    if tail_bits:
        parts.append(", ".join(tail_bits))
    return ", ".join(parts).strip()


def _call_openai_chat(*, api_key: str, base_url: str, model: str, user_text: str, timeout_s: int = 60) -> str:
    url = base_url.rstrip("/") + "/v1/chat/completions"
    body: dict[str, Any] = {
        "model": model,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": user_text},
        ],
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    msg = choices[0].get("message") or {}
    content = msg.get("content")
    return (content or "").strip() if isinstance(content, str) else ""


def infer_venue_block(*, api_key: str, base_url: str, model: str, event: dict, detail: dict) -> dict[str, Any] | None:
    page_url = (event.get("url") or detail.get("url") or "").strip()
    title = (event.get("title") or "").strip()
    tp = detail.get("text_preview")
    text_preview = tp.strip()[:14_000] if isinstance(tp, str) else ""
    links = detail.get("links")
    link_lines = "\n".join(str(x) for x in links if isinstance(links, list) and isinstance(x, str))[:8000]

    if not text_preview and not title:
        return None

    user = (
        f"page_url: {page_url}\n"
        f"listing_title: {title}\n\n"
        f"links:\n{link_lines}\n\n"
        f"page_text:\n{text_preview}"
    )
    try:
        raw = _call_openai_chat(api_key=api_key, base_url=base_url, model=model, user_text=user)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, json.JSONDecodeError, ValueError):
        return None
    if not raw:
        return None
    obj = _parse_llm_json(raw)
    if not obj:
        return None

    short = (
        (detail.get("og_title") or detail.get("title_tag") or title or "").strip()
    )
    if "|" in short:
        short = short.split("|", 1)[0].strip()
    fallback_venue = short[:120] if short else ""

    formatted = _build_formatted_location(obj, fallback_venue=fallback_venue)
    if not formatted:
        return None
    try:
        conf = float(obj.get("confidence", 0))
    except (TypeError, ValueError):
        conf = 0.0

    # Guardrail: only treat this as "high confidence" if we actually have a usable
    # street + postal code pairing. This prevents vague outputs like "00 Uhr Venue, Zürich"
    # or "Venue, 8001 Zürich" from overriding existing locations downstream.
    street_line = (obj.get("street_line") or "").strip() if isinstance(obj.get("street_line"), str) else ""
    postal_code = (obj.get("postal_code") or "").strip() if isinstance(obj.get("postal_code"), str) else ""
    city = (obj.get("city") or "").strip() if isinstance(obj.get("city"), str) else ""
    if not street_line or not re.fullmatch(r"\d{4}", postal_code or "") or not (city or "Zürich"):
        conf = min(conf, 0.6)
    out = {
        "model": model,
        "venue_name": obj.get("venue_name"),
        "street_line": obj.get("street_line"),
        "postal_code": obj.get("postal_code"),
        "city": obj.get("city"),
        "country": obj.get("country"),
        "confidence": conf,
        "evidence": (obj.get("evidence") or "") if isinstance(obj.get("evidence"), str) else "",
        "formatted_location": formatted,
    }
    return out


def apply_venue_llm_to_events(events: list[dict], *, llm_delay_s: float = 0.35, timeout_s: int = 60) -> list[dict]:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        print("[venue-llm] OPENAI_API_KEY not set; skipping.", flush=True)
        return events
    base_url = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com").strip()
    model = (os.environ.get("OPENAI_VENUE_MODEL") or _DEFAULT_MODEL).strip()

    out: list[dict] = []
    for i, ev in enumerate(events):
        d = ev.get("detail")
        if not isinstance(d, dict):
            out.append(ev)
            continue
        tp = d.get("text_preview")
        if not isinstance(tp, str) or not tp.strip():
            out.append(ev)
            continue
        if i > 0 and llm_delay_s > 0:
            time.sleep(llm_delay_s)
        block = infer_venue_block(api_key=api_key, base_url=base_url, model=model, event=ev, detail=d)
        if block:
            out.append({**ev, "detail": {**d, "venue_llm": block}})
        else:
            out.append(ev)
    return out
