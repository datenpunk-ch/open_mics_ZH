"""Per-site HTML extractors: (listing_url, html) -> list of event dicts."""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def run_extractor(extractor_id: str, listing_url: str, html: str) -> list[dict]:
    if extractor_id == "eventfrog":
        return extract_eventfrog_listing(listing_url, html)
    if extractor_id == "gz_zh_single":
        return extract_gz_zh_single_page(listing_url, html)
    if extractor_id == "single_page":
        return extract_single_page_seed(listing_url, html)
    if extractor_id == "guidle_microsite":
        return extract_guidle_microsite_search(listing_url, html)
    raise ValueError(f"Unknown extractor: {extractor_id}")


# --- Eventfrog -----------------------------------------------------------------


# z. B. /en/events/zuerich.html oder /en/events/zuerich/comedy-cabaret.html (keine Event-Detailseite)
_LISTING_PATH_RE = re.compile(
    r"^/(en|de|fr|es)/events/(?:[^/]+/)*[^/]+\.html$",
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
        r"https?://(?:www\.)?eventfrog\.ch/(?:en|de|fr|es)/p/[^\"'\s<>]+\.html",
        re.IGNORECASE,
    )
    for m in href_re.findall(html):
        _ef_add_event(out, seen, m, "")

    return out


# --- GZ Zürich (single offer/event page) ---------------------------------------


def extract_gz_zh_single_page(listing_url: str, html: str) -> list[dict]:
    """
    Treat the page itself as an "event seed".
    We let the enrich + flatten steps extract details (weekday/time/location) from the page.
    """
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""
    title = re.sub(r"\s+", " ", title).strip()
    return [{"url": listing_url, "title": title or listing_url, "path": urlparse(listing_url).path or ""}]


def extract_single_page_seed(listing_url: str, html: str) -> list[dict]:
    """
    Treat the page itself as an "event seed".
    Used for sources that publish one canonical page for a recurring open mic series.
    """
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""
    title = re.sub(r"\s+", " ", title).strip()
    if not title:
        t = soup.find("title")
        title = t.get_text(" ", strip=True) if t else ""
        title = re.sub(r"\s+", " ", title).strip()
    return [{"url": listing_url, "title": title or listing_url, "path": urlparse(listing_url).path or ""}]


# --- Guidle microsite (used by zuerich.com event finder) -----------------------


_RE_GUIDLE_MATCH = re.compile(r"\b(open[\s-]*mic|stand[\s-]*up|comedy)\b", re.I)


def _http_json(url: str, *, timeout_s: int = 30) -> dict | None:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "open-mics-zurich/0.1"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _guidle_offers_from_payload(payload: dict) -> list[dict]:
    groups = payload.get("groups")
    if not isinstance(groups, list):
        return []
    out: list[dict] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        offers = g.get("offers")
        if not isinstance(offers, list):
            continue
        for o in offers:
            if isinstance(o, dict):
                out.append(o)
    return out


def extract_guidle_microsite_search(listing_url: str, html: str) -> list[dict]:
    """
    zuerich.com "Event finden" embeds a Guidle microsite which exposes a JSON search API.

    We expect listing_url to be a Guidle "search-offers" REST endpoint. We fetch pages
    (currentPageNumber=1..N) until moreExists==False (or a reasonable safety limit),
    then convert offers into the common event dict shape.
    """
    parsed = urllib.parse.urlparse(listing_url)
    qs = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))

    # Ensure we start at page 1.
    qs.setdefault("currentPageNumber", "1")
    try:
        start_page = int(qs.get("currentPageNumber") or "1")
    except ValueError:
        start_page = 1
    qs["currentPageNumber"] = str(max(1, start_page))

    base = parsed._replace(query="").geturl()

    out: list[dict] = []
    seen: set[str] = set()

    max_pages = 12  # safety: 12 * 50 = 600 offers
    page_no = int(qs["currentPageNumber"])

    while True:
        page_url = base + "?" + urllib.parse.urlencode(qs)
        payload = _http_json(page_url)
        if not isinstance(payload, dict):
            break

        offers = _guidle_offers_from_payload(payload)
        for o in offers:
            title = str(o.get("title") or "").strip()
            url = str(o.get("url") or "").strip()
            if not url or not title:
                continue
            # Extra safety filter: keep only results that look relevant.
            blob = f"{title} {o.get('category') or ''} {o.get('textLine2') or ''}"
            if not _RE_GUIDLE_MATCH.search(blob):
                continue
            key = url.split("#", 1)[0]
            if key in seen:
                continue
            seen.add(key)
            out.append({"url": key, "title": title, "path": urlparse(key).path or ""})

        more = payload.get("moreExists")
        if more is not True:
            break
        if page_no >= max_pages:
            break
        page_no += 1
        qs["currentPageNumber"] = str(page_no)

    return out
