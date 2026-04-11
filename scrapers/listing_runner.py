"""Generic listing scrape: source id -> URL + extractor + Playwright behavior."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

from .extractors import run_extractor
from .playwright_helpers import (
    click_show_more_eventfrog,
    dismiss_cookie_banner,
    goto_listing,
    new_browser_context,
)
from .sources import DEFAULT_OUTPUT_DIR, get_listing_source


def fetch_listing_html(
    listing_url: str,
    *,
    listing_behavior: str,
    headless: bool,
    timeout_ms: int,
    max_show_more: int,
    show_more_delay_ms: int,
) -> tuple[str, dict]:
    meta: dict = {
        "listing_url": listing_url,
        "listing_behavior": listing_behavior,
        "show_more_clicks": 0,
        "errors": [],
    }

    with sync_playwright() as p:
        browser, context = new_browser_context(p, headless=headless)
        try:
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            goto_listing(page, listing_url, timeout_ms=timeout_ms, meta_errors=meta["errors"])
            dismiss_cookie_banner(page)

            if listing_behavior == "eventfrog":
                meta["show_more_clicks"] = click_show_more_eventfrog(
                    page, max_show_more, show_more_delay_ms
                )
            elif listing_behavior == "none":
                pass
            else:
                meta["errors"].append(f"unknown_listing_behavior:{listing_behavior}")

            html = page.content()
        finally:
            browser.close()

    return html, meta


def default_listing_output_path(
    project_root: Path,
    source_id: str,
    *,
    utc_stamp: str | None = None,
) -> Path:
    stamp = utc_stamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = project_root / DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in source_id)
    return out_dir / f"{safe}_listing_{stamp}.json"


def run_listing(
    source_id: str,
    *,
    listing_url: str | None,
    headless: bool,
    timeout_ms: int,
    max_show_more: int,
    show_more_delay_ms: int,
) -> dict:
    src = get_listing_source(source_id)
    url = listing_url or src.default_listing_url

    html, meta = fetch_listing_html(
        url,
        listing_behavior=src.listing_behavior,
        headless=headless,
        timeout_ms=timeout_ms,
        max_show_more=max_show_more,
        show_more_delay_ms=show_more_delay_ms,
    )
    events = run_extractor(src.extractor, url, html)

    return {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": src.id,
        "source_label": src.label,
        "extractor": src.extractor,
        "source_url": url,
        "event_count": len(events),
        "events": events,
        "meta": meta,
    }


def write_listing_payload(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run_listing_to_file(
    project_root: Path,
    source_id: str,
    *,
    listing_url: str | None,
    output_path: Path | None,
    headless: bool,
    timeout_ms: int,
    max_show_more: int,
    show_more_delay_ms: int,
    utc_stamp: str | None = None,
) -> tuple[Path, dict]:
    payload = run_listing(
        source_id,
        listing_url=listing_url,
        headless=headless,
        timeout_ms=timeout_ms,
        max_show_more=max_show_more,
        show_more_delay_ms=show_more_delay_ms,
    )
    out = output_path or default_listing_output_path(
        project_root, source_id, utc_stamp=utc_stamp
    )
    if not out.is_absolute():
        out = project_root / out
    write_listing_payload(out, payload)
    return out, payload
