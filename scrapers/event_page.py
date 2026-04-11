"""Generic single event / detail page scrape (any HTTP URL)."""

from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from .playwright_helpers import dismiss_cookie_banner, new_browser_context


def _norm_url(url: str) -> str:
    return urlparse(url)._replace(fragment="", query="").geturl()


def _parse_ld_json_scripts(html: str) -> list[object]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[object] = []
    for tag in soup.select('script[type="application/ld+json"]'):
        raw = (tag.string or tag.get_text() or "").strip()
        if not raw:
            continue
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            out.append({"_raw": raw[:2000], "_error": "json_decode_error"})
    return out


def payload_from_event_html(url: str, html: str, meta_warnings: list[str]) -> dict:
    """Build the event-page dict from raw HTML (no browser)."""
    parsed_host = (urlparse(url).hostname or "").lower()
    soup = BeautifulSoup(html, "html.parser")
    og_title = soup.select_one('meta[property="og:title"]')
    title_tag = soup.find("title")
    h1 = soup.find("h1")

    return {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "kind": "event_page",
        "url": _norm_url(url),
        "host": parsed_host,
        "title_tag": (title_tag.get_text(strip=True) if title_tag else "") or "",
        "og_title": (og_title.get("content") or "").strip() if og_title else "",
        "h1": (h1.get_text(" ", strip=True) if h1 else "") or "",
        "ld_json": _parse_ld_json_scripts(html),
        "meta": {"url": url, "warnings": list(meta_warnings)},
    }


def scrape_event_page(url: str, *, headless: bool = True, timeout_ms: int = 60_000) -> dict:
    meta_warnings: list[str] = []
    norm = _norm_url(url)

    with sync_playwright() as p:
        browser, context = new_browser_context(p, headless=headless)
        try:
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(norm, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except Exception:
                meta_warnings.append("networkidle_timeout")
            dismiss_cookie_banner(page)
            html = page.content()
        finally:
            browser.close()

    return payload_from_event_html(norm, html, meta_warnings)


def scrape_event_urls_batch(
    urls: list[str],
    *,
    headless: bool = True,
    timeout_ms: int = 60_000,
    delay_s: float = 1.5,
) -> list[dict]:
    """
    One browser session, sequential pages. ``urls`` should be unique in desired order;
    duplicate URLs are skipped after the first fetch.
    """
    seen: set[str] = set()
    ordered_unique: list[str] = []
    for u in urls:
        n = _norm_url(u)
        if n in seen:
            continue
        seen.add(n)
        ordered_unique.append(n)

    if not ordered_unique:
        return []

    out: list[dict] = []
    with sync_playwright() as p:
        browser, context = new_browser_context(p, headless=headless)
        try:
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            for i, norm in enumerate(ordered_unique):
                meta_warnings: list[str] = []
                page.goto(norm, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=timeout_ms)
                except Exception:
                    meta_warnings.append("networkidle_timeout")
                dismiss_cookie_banner(page)
                html = page.content()
                out.append(payload_from_event_html(norm, html, meta_warnings))
                if i < len(ordered_unique) - 1 and delay_s > 0:
                    time.sleep(delay_s)
        finally:
            browser.close()
    return out


def default_event_page_output_path(project_root: Path, url: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = re.sub(r"[^\w]+", "_", urlparse(url).path.strip("/"))[-80:] or "event"
    host = (urlparse(url).hostname or "page").replace(".", "_")
    out_dir = project_root / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{host}_{slug}_{stamp}.json"


def run_event_page_to_file(
    project_root: Path,
    url: str,
    *,
    output_path: Path | None,
    headless: bool,
    timeout_ms: int,
) -> Path:
    data = scrape_event_page(url, headless=headless, timeout_ms=timeout_ms)
    out = output_path or default_event_page_output_path(project_root, url)
    if not out.is_absolute():
        out = project_root / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    if data.get("meta", {}).get("warnings"):
        print("Warnings:", data["meta"]["warnings"], file=sys.stderr)
    return out
