"""Small helpers for pipeline timestamps (no Streamlit / Folium imports)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def latest_listing_scraped_meta(processed_dir: Path) -> tuple[str, str | None]:
    """
    Read listing scrape time from the newest ``events_enriched*.json`` (by mtime).

    Returns:
        (display_dd_mm_yyyy_utc, iso_string_or_none)
    """
    if not processed_dir.is_dir():
        return "", None
    cands = sorted(
        processed_dir.glob("events_enriched*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for p in cands:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        raw = data.get("listing_scraped_at")
        if not isinstance(raw, str) or not raw.strip():
            continue
        iso = raw.strip()
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        disp = dt.astimezone(timezone.utc).strftime("%d/%m/%Y")
        return disp, iso
    return "", None
