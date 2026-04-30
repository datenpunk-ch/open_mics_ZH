"""Generic single event / detail page scrape (any HTTP URL)."""

from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

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


def _visible_text_preview(soup: BeautifulSoup, *, max_chars: int = 16000) -> str:
    """Stripped main content for weekday / language hints (esp. Eventfrog group pages)."""
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    root = (
        soup.select_one("main")
        or soup.select_one('[role="main"]')
        or soup.select_one("article")
        or soup.body
    )
    if not root:
        return ""
    t = root.get_text(" ", strip=True)
    t = re.sub(r"\s+", " ", t)
    return t[:max_chars]


def _extract_links(soup: BeautifulSoup, *, max_links: int = 200) -> list[str]:
    """
    Extract absolute-ish hrefs from main content so downstream steps can pick up
    canonical links (e.g. Google Maps place links on venue pages).
    """
    root = (
        soup.select_one("main")
        or soup.select_one('[role="main"]')
        or soup.select_one("article")
        or soup.body
    )
    if not root:
        return []
    hrefs: list[str] = []
    seen: set[str] = set()
    for a in root.select("a[href]"):
        h = (a.get("href") or "").strip()
        if not h or h.startswith("#"):
            continue
        if h in seen:
            continue
        seen.add(h)
        hrefs.append(h)
        if len(hrefs) >= max_links:
            break
    return hrefs


def _is_eventfrog_group_url(url: str) -> bool:
    u = (url or "").lower()
    return (
        "eventfrog.ch" in u
        and any(seg in u for seg in ("/p/gruppen/", "/p/groups/", "/p/groupes/"))
    )


def _extract_eventfrog_child_event_urls(url: str, soup: BeautifulSoup, *, max_urls: int = 12) -> list[str]:
    """
    Eventfrog group pages list multiple occurrence/detail links (often relative).
    Pull a small set of child event URLs so enrich can capture text that might only
    exist on the individual event pages (e.g. "open mic" in FAQ).
    """
    if not _is_eventfrog_group_url(url):
        return []
    hrefs = _extract_links(soup, max_links=400)
    out: list[str] = []
    seen: set[str] = set()
    for h in hrefs:
        abs_u = urljoin(url, h)
        if not abs_u:
            continue
        # Keep only normal event pages, not other group pages.
        lu = abs_u.lower()
        if "eventfrog.ch" not in lu:
            continue
        if any(seg in lu for seg in ("/p/gruppen/", "/p/groups/", "/p/groupes/")):
            continue
        if not re.search(r"/p/[^\"'\s<>]+\.html(?:$|[?#])", lu):
            continue
        n = _norm_url(abs_u)
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
        if len(out) >= max_urls:
            break
    return out


def payload_from_event_html(url: str, html: str, meta_warnings: list[str]) -> dict:
    """Build the event-page dict from raw HTML (no browser)."""
    parsed_host = (urlparse(url).hostname or "").lower()
    soup = BeautifulSoup(html, "html.parser")
    og_title = soup.select_one('meta[property="og:title"]')
    og_image = soup.select_one('meta[property="og:image"]')
    title_tag = soup.find("title")
    h1 = soup.find("h1")
    text_preview = _visible_text_preview(soup)
    links = _extract_links(soup)
    child_event_urls: list[str] = _extract_eventfrog_child_event_urls(url, soup)

    return {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "kind": "event_page",
        "url": _norm_url(url),
        "host": parsed_host,
        "title_tag": (title_tag.get_text(strip=True) if title_tag else "") or "",
        "og_title": (og_title.get("content") or "").strip() if og_title else "",
        "og_image": (og_image.get("content") or "").strip() if og_image else "",
        "h1": (h1.get_text(" ", strip=True) if h1 else "") or "",
        "text_preview": text_preview,
        "links": links,
        "child_event_urls": child_event_urls,
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

    payload = payload_from_event_html(norm, html, meta_warnings)

    # Eventfrog group pages often omit critical "open mic" hints on the group itself
    # (the wording may only exist on the occurrence detail pages). Fetch a tiny sample
    # of child pages and append their visible text into the preview blob.
    try:
        if _is_eventfrog_group_url(norm):
            child_urls = payload.get("child_event_urls") or []
            if isinstance(child_urls, list):
                child_urls = [u for u in child_urls if isinstance(u, str) and u.strip()]
            else:
                child_urls = []
            child_urls = child_urls[:2]
            if child_urls:
                child_previews: list[str] = []
                with sync_playwright() as p2:
                    b2, c2 = new_browser_context(p2, headless=headless)
                    try:
                        pg2 = c2.new_page()
                        pg2.set_default_timeout(timeout_ms)
                        for cu in child_urls:
                            pg2.goto(cu, wait_until="domcontentloaded")
                            try:
                                pg2.wait_for_load_state("networkidle", timeout=timeout_ms)
                            except Exception:
                                pass
                            dismiss_cookie_banner(pg2)
                            child_html = pg2.content()
                            child_payload = payload_from_event_html(cu, child_html, [])
                            tp = (child_payload.get("text_preview") or "").strip()
                            if tp:
                                child_previews.append(tp[:8000])
                    finally:
                        b2.close()
                if child_previews:
                    payload["child_text_previews"] = child_previews
                    base_tp = (payload.get("text_preview") or "").strip()
                    payload["text_preview"] = (base_tp + "\n\n" + "\n\n".join(child_previews)).strip()
    except Exception:
        # Best-effort enrichment; never fail the scrape.
        pass

    return payload


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
                try:
                    page.goto(norm, wait_until="domcontentloaded", timeout=timeout_ms)
                    try:
                        page.wait_for_load_state("networkidle", timeout=timeout_ms)
                    except Exception:
                        meta_warnings.append("networkidle_timeout")
                    dismiss_cookie_banner(page)
                    html = page.content()
                    payload = payload_from_event_html(norm, html, meta_warnings)
                except Exception as e:
                    # Make batch enrich resilient: one broken/slow page must not block the whole run.
                    payload = {
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                        "kind": "event_page",
                        "url": _norm_url(norm),
                        "host": (urlparse(norm).hostname or "").lower(),
                        "title_tag": "",
                        "og_title": "",
                        "og_image": "",
                        "h1": "",
                        "text_preview": "",
                        "links": [],
                        "child_event_urls": [],
                        "ld_json": [],
                        "meta": {
                            "url": norm,
                            "warnings": meta_warnings + [f"fetch_error:{type(e).__name__}"],
                        },
                    }

                # Same as in scrape_event_page, but keep it lightweight in batch mode:
                # fetch only the first child page.
                try:
                    if _is_eventfrog_group_url(norm):
                        child_urls = payload.get("child_event_urls") or []
                        if isinstance(child_urls, list):
                            child_urls = [u for u in child_urls if isinstance(u, str) and u.strip()]
                        else:
                            child_urls = []
                        child_urls = child_urls[:1]
                        if child_urls:
                            cu = child_urls[0]
                            try:
                                page.goto(cu, wait_until="domcontentloaded", timeout=timeout_ms)
                                try:
                                    page.wait_for_load_state("networkidle", timeout=timeout_ms)
                                except Exception:
                                    pass
                                dismiss_cookie_banner(page)
                                child_html = page.content()
                                child_payload = payload_from_event_html(cu, child_html, [])
                                tp = (child_payload.get("text_preview") or "").strip()
                                if tp:
                                    payload["child_text_previews"] = [tp[:8000]]
                                    base_tp = (payload.get("text_preview") or "").strip()
                                    payload["text_preview"] = (base_tp + "\n\n" + tp).strip()
                            except Exception:
                                # Ignore child failures; keep base payload.
                                pass
                except Exception:
                    pass

                out.append(payload)
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
