"""Per-site HTML extractors: (listing_url, html) -> list of event dicts."""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def run_extractor(extractor_id: str, listing_url: str, html: str) -> list[dict]:
    if extractor_id == "eventfrog":
        return extract_eventfrog_listing(listing_url, html)
    raise ValueError(f"Unknown extractor: {extractor_id}")


# --- Eventfrog -----------------------------------------------------------------


# z. B. /en/events/zuerich.html oder /en/events/zuerich/comedy-cabaret.html (keine Event-Detailseite)
_LISTING_PATH_RE = re.compile(
    r"^/(en|de|fr)/events/(?:[^/]+/)*[^/]+\.html$",
    re.IGNORECASE,
)


def _ef_is_listing_only_page(path: str) -> bool:
    return bool(_LISTING_PATH_RE.match(path))


def _ef_looks_like_event_detail(path: str) -> bool:
    if not path.lower().endswith(".html"):
        return False
    lower = path.lower()
    if "/p/" in lower:
        return True
    if re.search(r"-\d+\.html$", path):
        return True
    if re.search(r"/e/[^/]+", lower):
        return True
    return False


def _ef_should_skip_path(path: str) -> bool:
    p = path.lower()
    return any(
        x in p
        for x in (
            "/help/",
            "/hilfe",
            "/organise",
            "/organize",
            "/service/",
            "/login",
            "sitemap",
            "/app",
            "/blog",
            "/about",
        )
    )


def _ef_add_event(
    out: list[dict],
    seen: set[str],
    abs_url: str,
    title: str,
) -> None:
    parsed = urlparse(abs_url)
    path = parsed.path or ""
    if _ef_should_skip_path(path):
        return
    if _ef_is_listing_only_page(path):
        return
    if not _ef_looks_like_event_detail(path):
        return
    key = parsed._replace(fragment="", query="").geturl()
    if key in seen:
        return
    seen.add(key)
    title = re.sub(r"\s+", " ", title or "").strip()
    out.append({"url": key, "title": title, "path": path})


def extract_eventfrog_listing(listing_url: str, html: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.select("a[href]"):
        raw = (a.get("href") or "").strip()
        if not raw or raw.startswith("#"):
            continue
        abs_url = urljoin(listing_url, raw)
        parsed = urlparse(abs_url)
        if not parsed.hostname or not parsed.hostname.endswith("eventfrog.ch"):
            continue
        title = a.get_text(" ", strip=True) or ""
        if len(title) < 2:
            title = (a.get("title") or "").strip()
        _ef_add_event(out, seen, abs_url, title)

    href_re = re.compile(
        r"https?://(?:www\.)?eventfrog\.ch/(?:en|de|fr)/p/[^\"'\s<>]+\.html",
        re.IGNORECASE,
    )
    for m in href_re.findall(html):
        _ef_add_event(out, seen, m, "")

    return out
