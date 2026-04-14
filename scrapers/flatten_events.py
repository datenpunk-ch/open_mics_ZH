"""
Build a flat table from enriched listing JSON:
Weekday, Location, Time, Cost, Comedy_language, Regularity, Event_title, URL.

- Per URL: schema.org Event in ``detail.ld_json``; group pages without LD are filled
  from listing title, meta titles, optional ``text_preview`` (see ``event_page``), and URL.
- **Regularity:** Eventfrog groups, schema.org series hints, and the same normalized series
  name appearing more than once in one export → ``recurring``.
- **Slot dedup:** rows with the same normalized event title, location, weekday, and time are
  merged into one row with ``Regularity`` ``recurring`` (multiple ticket URLs for the same
  recurring slot).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

# ISO weekday 1=Monday … 7=Sunday → English name
_WEEKDAY_EN: dict[int, str] = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}

_GERMAN_WEEKDAY_TO_ISO: dict[str, int] = {
    "montag": 1,
    "dienstag": 2,
    "mittwoch": 3,
    "donnerstag": 4,
    "freitag": 5,
    "samstag": 6,
    "sonntag": 7,
}

# schema.org: strip date suffixes so multiple dates cluster as one series
_DATE_SUFFIX_EN = re.compile(
    r"\s*[-–]\s*("
    r"January|February|March|April|May|June|July|August|September|October|November|December"
    r")\s+\d{1,2}(?:st|nd|rd|th)?\s*$",
    re.I,
)
_DATE_SUFFIX_DE = re.compile(
    r"\s*[-–]\s*\d{1,2}\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s*$",
    re.I,
)

# FAQ / copy: performance language (not website locale)
_RE_LANG_FAQ = re.compile(
    r"Q:\s*[^\n]{0,80}?\blanguage\b[^\n]{0,40}?\?\s*A:\s*([^\nQ]{1,120})",
    re.I,
)
_RE_SHOW_IN_EN = re.compile(
    r"\b(?:show|sets?|event|comedy)\b[^.\n]{0,60}?\bin english\b",
    re.I,
)
_RE_PERFORMED_EN = re.compile(r"\bperformed in english\b", re.I)

_SLUG_LANG_HINTS: tuple[tuple[str, str], ...] = (
    ("dini-muetter", "Swiss German"),
    ("comedia-en-espanol", "Spanish"),
    ("comedia-en-espa", "Spanish"),
)

_LANG_CODE_MAP: dict[str, str] = {
    "en": "English",
    "eng": "English",
    "de": "German",
    "ger": "German",
    "deu": "German",
    "fr": "French",
    "fra": "French",
    "es": "Spanish",
    "spa": "Spanish",
    "it": "Italian",
    "ita": "Italian",
}


def _fold_for_clustering(s: str) -> str:
    t = s.lower().strip()
    t = t.replace("–", "-").replace("—", "-").replace("−", "-")
    for a, b in (
        ("ü", "u"),
        ("ö", "o"),
        ("ä", "a"),
        ("ë", "e"),
        ("ï", "i"),
        ("é", "e"),
        ("è", "e"),
    ):
        t = t.replace(a, b)
    t = re.sub(r"\bzurich\b", "zuerich", t)
    return t


def _normalize_series_name(name: str) -> str:
    """Strip trailing date in the event name (e.g. Kon-Tiki Comedy - April 14th)."""
    if not name or not isinstance(name, str):
        return ""
    n = name.strip()
    n = _DATE_SUFFIX_EN.sub("", n)
    n = _DATE_SUFFIX_DE.sub("", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _weekday_indices_from_text(text: str) -> set[int]:
    """English, German, common abbreviations (Tue, …), and phrases like ``every Thursday``."""
    if not text:
        return set()
    tl = text.lower()
    out: set[int] = set()
    # "every (second) Thursday", "jeden Dienstag, Freitag und Sonntag"
    for m in re.finditer(
        r"\bevery\s+(?:second\s+|third\s+)?(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
        tl,
    ):
        name = m.group(1)
        i = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday").index(
            name
        ) + 1
        out.add(i)
    for m in re.finditer(r"\bjeden\s+([^.\n]{1,120})", tl):
        chunk = m.group(1)
        for word, iso in _GERMAN_WEEKDAY_TO_ISO.items():
            if re.search(rf"\b{re.escape(word)}\b", chunk):
                out.add(iso)
    # Long English names first (word boundaries)
    for i, name in enumerate(
        ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"),
        start=1,
    ):
        if re.search(rf"\b{re.escape(name)}\b", tl):
            out.add(i)
    # German weekday words
    for word, i in _GERMAN_WEEKDAY_TO_ISO.items():
        if re.search(rf"\b{re.escape(word)}\b", tl):
            out.add(i)
    # Abbreviations as separate tokens (Tue, Thu, …)
    abbrevs = (
        ("sun", 7),
        ("mon", 1),
        ("tue", 2),
        ("wed", 3),
        ("thu", 4),
        ("fri", 5),
        ("sat", 6),
    )
    for abbr, i in abbrevs:
        if re.search(rf"\b{re.escape(abbr)}\b", tl):
            out.add(i)
    return out


def _weekday_indices_from_url(url: str, path: str) -> set[int]:
    blob = f"{url} {path}".lower().replace("-", " ")
    return _weekday_indices_from_text(blob)


def _format_weekday_list(indices: set[int]) -> str:
    if not indices:
        return ""
    ordered = sorted(indices)  # ISO 1=Monday … 7=Sunday
    return ", ".join(_WEEKDAY_EN[d] for d in ordered)


def _detail_meta_blob(detail: dict | None) -> str:
    if not detail:
        return ""
    parts = [
        str(detail.get("og_title") or ""),
        str(detail.get("title_tag") or ""),
        str(detail.get("h1") or ""),
        str(detail.get("text_preview") or "")[:12000],
    ]
    return " ".join(p for p in parts if p).strip()


def _location_after_events_count(title: str) -> str:
    """``16 Events Auer & Co., …`` → location segment after the event count (Eventfrog groups)."""
    if not title:
        return ""
    m = re.search(r"\b\d+\s+Events\s+(.+)$", title, re.I)
    if not m:
        return ""
    return m.group(1).strip()


def _detail_text_blob(detail: dict | None) -> str:
    """Shorter meta blob (titles only) for series identity."""
    if not detail:
        return ""
    parts = [
        str(detail.get("og_title") or ""),
        str(detail.get("title_tag") or ""),
    ]
    return " ".join(p for p in parts if p).strip()


def _series_identity_key(
    *,
    title: str,
    detail: dict | None,
    node: dict | None,
) -> str:
    """
    Key for “same series, multiple dates”.
    Empty string = do not use for duplicate counting.
    """
    if node:
        raw = (node.get("name") or "").strip()
        if raw:
            return _fold_for_clustering(_normalize_series_name(raw))
    blob = _detail_text_blob(detail)
    if blob:
        if " in " in blob:
            base = blob.split(" in ", 1)[0].strip()
            if "|" in base:
                base = base.split("|", 1)[0].strip()
            if base:
                return _fold_for_clustering(_normalize_series_name(base))
        if "|" in blob:
            base = blob.split("|", 1)[0].strip()
            if base:
                return _fold_for_clustering(_normalize_series_name(base))
    t = title.strip()
    if not t:
        return ""
    t = re.sub(r"^Tickets\s+", "", t, flags=re.I)
    t = re.sub(r"^Tickets\s+Event\s+group\s+until\s+\w+\s+\d+\s+", "", t, flags=re.I)
    t = re.sub(r"\s+\d+\s+Events\s+.*$", "", t, flags=re.I)
    t = re.sub(r"^\w+\s+\d{1,2}\s+", "", t)
    t = re.sub(r"\s+\d{1,2}\s+\w+,?\s+\d{4}\s*$", "", t)
    t = _normalize_series_name(t)
    if len(t) < 4:
        return ""
    return _fold_for_clustering(t)


def _fill_group_row_gaps(
    *,
    weekday: str,
    location: str,
    time_s: str,
    title: str,
    detail: dict | None,
    url: str,
    path: str,
) -> tuple[str, str, str]:
    """Without full LD-JSON: fill weekday, location, time from listing and page preview."""
    blob = _detail_meta_blob(detail)
    combined = f"{title} {blob}".strip()

    if not weekday:
        idx = _weekday_indices_from_text(combined) | _weekday_indices_from_url(url, path)
        weekday = _format_weekday_list(idx)

    if not location:
        loc = _location_after_events_count(title)
        if not loc and blob and " in " in blob:
            tail = blob.split(" in ", 1)[1]
            tail = tail.split("|", 1)[0].strip()
            if tail:
                loc = tail
        location = loc

    if not time_s:
        m = re.search(r"(\d{1,2}:\d{2})", combined)
        if m:
            time_s = m.group(1)

    return weekday, location, time_s


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def find_latest_enriched(processed_dir: Path) -> Path | None:
    if not processed_dir.is_dir():
        return None
    cands = sorted(
        processed_dir.glob("events_enriched*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return cands[0] if cands else None


def _iter_ld_dicts(block: Any) -> Iterator[dict]:
    """A value from ``ld_json`` may be dict, @graph object, or nested list (Eventfrog)."""
    if isinstance(block, list):
        for item in block:
            yield from _iter_ld_dicts(item)
        return
    if not isinstance(block, dict):
        return
    if "@graph" in block:
        for n in block["@graph"]:
            if isinstance(n, dict):
                yield n
    else:
        yield block


def _types(node: dict) -> list[str]:
    t = node.get("@type")
    if t is None:
        return []
    if isinstance(t, list):
        return [str(x) for x in t]
    return [str(t)]


def _iter_event_nodes(ld_blocks: list[Any]) -> Iterator[dict]:
    for block in ld_blocks:
        for node in _iter_ld_dicts(block):
            if "Event" in _types(node):
                yield node


def _iter_all_ld_dicts(ld_blocks: list[Any]) -> Iterator[dict]:
    for block in ld_blocks:
        yield from _iter_ld_dicts(block)


def _recurrence_from_ld(ld_blocks: list[Any] | None) -> str | None:
    if not isinstance(ld_blocks, list):
        return None
    for node in _iter_all_ld_dicts(ld_blocks):
        types_l = [t.lower() for t in _types(node)]
        if any("eventseries" in t for t in types_l):
            return "recurring"
        if node.get("eventSchedule"):
            return "recurring"
        if node.get("repeatFrequency") or node.get("repeatCount"):
            return "recurring"
        se = node.get("subEvent")
        if isinstance(se, list) and len(se) > 1:
            return "recurring"
        sup = node.get("superEvent")
        if isinstance(sup, dict):
            st = [str(x).lower() for x in _types(sup)]
            if any("eventseries" in t for t in st):
                return "recurring"
    return None


def _recurrence_from_listing(path: str, title: str, url: str) -> str | None:
    combined = f"{path} {url}".lower()
    if "/p/groups/" in combined:
        return "recurring"
    tl = title.lower()
    if "event group" in tl or "veranstaltungsgruppe" in tl or "eventgruppe" in tl:
        return "recurring"
    if re.search(r"\b\d+\s+events\b", title, re.I):
        return "recurring"
    return None


def _recurrence_label(
    *,
    ld_blocks: list[Any] | None,
    path: str,
    title: str,
    url: str,
    has_event_node: bool,
) -> str:
    r = _recurrence_from_listing(path, title, url) or _recurrence_from_ld(ld_blocks)
    if r == "recurring":
        return "recurring"
    if has_event_node:
        return "one-off"
    if url and "eventfrog.ch" in url.lower() and "/p/groups/" not in url.lower():
        return "one-off"
    return "unknown"


def _parse_iso(dt: str | None) -> datetime | None:
    if not dt or not isinstance(dt, str):
        return None
    s = dt.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(s[:19])
    except ValueError:
        return None


def _format_location(loc: Any) -> str:
    if loc is None:
        return ""
    if isinstance(loc, str):
        return loc.strip()
    if not isinstance(loc, dict):
        return str(loc)
    name = (loc.get("name") or "").strip()
    addr = loc.get("address")
    parts: list[str] = []
    if isinstance(addr, dict):
        for k in ("streetAddress", "postalCode", "addressLocality", "addressRegion"):
            v = addr.get(k)
            if v:
                parts.append(str(v).strip())
    elif isinstance(addr, str):
        parts.append(addr.strip())
    geo = loc.get("geo")
    if isinstance(geo, dict) and not parts:
        lat, lon = geo.get("latitude"), geo.get("longitude")
        if lat is not None and lon is not None:
            parts.append(f"{lat},{lon}")
    tail = ", ".join(parts) if parts else ""
    if name and tail:
        return f"{name}, {tail}"
    return name or tail


def _format_offers(offers: Any) -> str:
    if offers is None:
        return ""
    if isinstance(offers, dict):
        offers = [offers]
    if not isinstance(offers, list):
        return str(offers)
    bits: list[str] = []
    for o in offers:
        if not isinstance(o, dict):
            continue
        cur = o.get("priceCurrency") or ""
        price = o.get("price")
        if price is not None and str(price).strip():
            bits.append(f"{cur} {price}".strip() if cur else str(price))
        elif o.get("url") and "free" in str(o.get("url", "")).lower():
            bits.append("0")
        name = (o.get("name") or "").strip()
        if name and not bits:
            bits.append(name)
    return " / ".join(bits) if bits else ""


def _normalize_in_language_token(raw: str) -> str:
    s = raw.strip()
    if not s:
        return ""
    low = s.lower().replace("_", "-")
    known = {
        "english": "English",
        "german": "German",
        "french": "French",
        "spanish": "Spanish",
        "italian": "Italian",
        "swiss german": "Swiss German",
    }
    if low in known:
        return known[low]
    base = low.split("-")[0]
    return _LANG_CODE_MAP.get(base, s)


def _language_from_description(desc: str) -> str:
    if not desc:
        return ""
    dl = desc.lower()
    # English Eventfrog copy (do not use "bringt" — German marketing is common for English shows)
    if "comedy connection brings" in dl:
        return "English"
    if re.search(r"is the show in English\?\s*→\s*Yes", desc, re.I):
        return "English"
    if re.search(
        r"is the show really in English\?\s*Absolutely\.",
        desc,
        re.I,
    ):
        return "English"
    if re.search(r"\ball performances are in English\b", desc, re.I):
        return "English"
    for m in _RE_LANG_FAQ.finditer(desc):
        frag = (m.group(1) or "").strip()
        low = frag.lower()
        if "spanish" in low or "español" in low or "espanol" in low:
            return "Spanish"
        if "french" in low or "français" in low or "francais" in low:
            return "French"
        if "german" in low and "swiss" in low:
            return "Swiss German"
        if "german" in low or "deutsch" in low:
            return "German"
        if "english" in low or low.strip() in ("yes.", "yes", "y"):
            return "English"
    if _RE_SHOW_IN_EN.search(desc) or _RE_PERFORMED_EN.search(desc):
        return "English"
    return ""


def _language_from_slug_path(path: str, url: str) -> str:
    blob = f"{path} {url}".lower()
    for needle, label in _SLUG_LANG_HINTS:
        if needle in blob:
            return label
    return ""


def _infer_comedy_language(
    *,
    node: dict | None,
    detail: dict | None,
    title: str,
    url: str,
    path: str,
) -> str:
    """
    On-stage / performance language. Does **not** treat ``/en/`` (or other locale paths)
    as proof of English — that is only the site language on Eventfrog.
    """
    parts_out: list[str] = []

    if node:
        lang = node.get("inLanguage") or node.get("availableLanguage")
        if isinstance(lang, list):
            for x in lang:
                if x:
                    n = _normalize_in_language_token(str(x))
                    if n:
                        parts_out.append(n)
        elif isinstance(lang, str) and lang.strip():
            parts_out.append(_normalize_in_language_token(lang))

    desc = ""
    if node and isinstance(node.get("description"), str):
        desc = node["description"]
    from_desc = _language_from_description(desc)
    if from_desc and from_desc not in parts_out:
        parts_out.insert(0, from_desc)

    blob = " ".join(
        filter(
            None,
            [
                title,
                _detail_meta_blob(detail) if detail else "",
            ],
        )
    ).lower()

    def add(label: str) -> None:
        if label and label not in parts_out:
            parts_out.append(label)

    # Strong phrase cues (title + visible copy)
    if re.search(r"\bcomedia\b.*\bespañol\b|\ben español\b|\bcomedia en español\b", blob):
        add("Spanish")
    if "open mic in english" in blob or "in english, of course" in blob:
        add("English")
    if "english stand-up" in blob or "english stand up" in blob or "english comedy" in blob:
        add("English")
    if "schwugo" in f"{path} {url} {title}".lower():
        add("Swiss German")
    if "swiss german" in blob or "schwiizertüütsch" in blob or "schweizerdeutsch" in blob:
        add("Swiss German")
    if "auf deutsch" in blob or re.search(r"\bin german\b", blob):
        add("German")
    if "français" in blob or " en français" in blob or "in french" in blob:
        add("French")

    slug_lang = _language_from_slug_path(path, url)
    if slug_lang:
        add(slug_lang)

    if parts_out:
        return "; ".join(dict.fromkeys(parts_out))

    # Unknown — do not infer from ``/en/`` URL locale alone.
    return ""


def _event_node_from_detail(detail: dict | None) -> dict | None:
    if not detail:
        return None
    ld = detail.get("ld_json")
    if not isinstance(ld, list):
        return None
    for ev in _iter_event_nodes(ld):
        return ev
    return None


def _image_url_from_detail(detail: dict | None, node: dict | None) -> str:
    # Prefer the schema.org Event image, then any ImageObject contentUrl.
    if node:
        img = node.get("image")
        if isinstance(img, str) and img.strip():
            return img.strip()
        if isinstance(img, list):
            for it in img:
                if isinstance(it, str) and it.strip():
                    return it.strip()

    if not detail:
        return ""

    og_img = detail.get("og_image")
    if isinstance(og_img, str) and og_img.strip():
        return og_img.strip()

    ld = detail.get("ld_json")
    if not isinstance(ld, list):
        return ""

    for block in ld:
        if isinstance(block, dict):
            blocks = [block]
        elif isinstance(block, list):
            blocks = [x for x in block if isinstance(x, dict)]
        else:
            continue
        for obj in blocks:
            t = str(obj.get("@type") or "").strip()
            if t.lower() == "imageobject":
                cu = obj.get("contentUrl")
                if isinstance(cu, str) and cu.strip():
                    return cu.strip()
    return ""


def _titel_event(node: dict | None, detail: dict | None, listing_title: str) -> str:
    if node:
        n = (node.get("name") or "").strip()
        if n:
            return n[:200]
    blob = (detail.get("og_title") or detail.get("title_tag") or "").strip() if detail else ""
    if blob and "|" in blob:
        blob = blob.split("|", 1)[0].strip()
    if blob:
        return blob[:200]
    return (listing_title or "")[:200]


def _flatten_row_and_key(event: dict) -> tuple[dict[str, str], str]:
    url = (event.get("url") or "").strip()
    title = (event.get("title") or "").strip()
    path = (event.get("path") or "").strip()
    detail = event.get("detail")
    if not isinstance(detail, dict):
        detail = None

    node = _event_node_from_detail(detail)
    weekday = ""
    time_s = ""
    location = ""
    cost = ""

    if node:
        start = _parse_iso(node.get("startDate"))
        if start:
            weekday = _WEEKDAY_EN.get(start.isoweekday(), "")
            time_s = start.strftime("%H:%M")
        location = _format_location(node.get("location"))
        cost = _format_offers(node.get("offers"))

    comedy_lang = _infer_comedy_language(
        node=node, detail=detail, title=title, url=url, path=path
    )

    if not location and title:
        m = re.search(r"\d{1,2}:\d{2}\s+(.+)$", title)
        if m:
            location = m.group(1).strip()

    if not time_s and title:
        m = re.search(r"(\d{1,2}:\d{2})", title)
        if m:
            time_s = m.group(1)

    # Single-date events: keep schema weekday only. Multi-day / groups: infer when empty.
    if not weekday:
        idx = _weekday_indices_from_text(title)
        idx |= _weekday_indices_from_url(url, path)
        weekday = _format_weekday_list(idx)

    weekday, location, time_s = _fill_group_row_gaps(
        weekday=weekday,
        location=location,
        time_s=time_s,
        title=title,
        detail=detail,
        url=url,
        path=path,
    )

    ld_list = detail.get("ld_json") if detail else None
    if not isinstance(ld_list, list):
        ld_list = None
    regularity = _recurrence_label(
        ld_blocks=ld_list,
        path=path,
        title=title,
        url=url,
        has_event_node=node is not None,
    )

    titel_event = _titel_event(node, detail, title)
    series_key = _series_identity_key(title=title, detail=detail, node=node)
    desc_preview = ""
    if detail and isinstance(detail.get("text_preview"), str):
        desc_preview = re.sub(r"\s+", " ", detail["text_preview"]).strip()[:2000]
    image_url = _image_url_from_detail(detail, node)

    row = {
        "Weekday": weekday,
        "Location": location,
        "Time": time_s,
        "Cost": cost,
        "Comedy_language": comedy_lang,
        "Regularity": regularity,
        "Event_title": titel_event,
        "URL": url,
        "Listing_title": title[:200],
        "Description_preview": desc_preview,
        "Image_url": image_url,
    }
    return row, series_key


def _norm_slot_field(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _slot_dedup_title(row: dict[str, str]) -> str:
    raw = (row.get("Event_title") or row.get("Listing_title") or "").strip()
    return _fold_for_clustering(_normalize_series_name(raw))


def _slot_dedup_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        _slot_dedup_title(row),
        _norm_slot_field(row.get("Location", "")),
        _norm_slot_field(row.get("Weekday", "")),
        _norm_slot_field(row.get("Time", "")),
    )


def _dedupe_recurring_slots(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    One row per recurring slot: same normalized title, location, weekday, and time.
    Merged rows are marked ``recurring``; ``Event_title`` is the date-stripped series name.
    """
    groups: OrderedDict[tuple[str, str, str, str], list[dict[str, str]]] = OrderedDict()
    for row in rows:
        k = _slot_dedup_key(row)
        if k not in groups:
            groups[k] = []
        groups[k].append(row)

    out: list[dict[str, str]] = []
    for grp in groups.values():
        if len(grp) == 1:
            out.append(dict(grp[0]))
            continue

        base = dict(grp[0])
        base["Regularity"] = "recurring"

        urls = [r.get("URL", "").strip() for r in grp if r.get("URL", "").strip()]
        if urls:
            pref = [u for u in urls if "/p/groups/" in u.lower()]
            if pref:
                base["URL"] = pref[0]
            # else: keep first row's URL so it stays aligned with Listing_title

        langs = [r.get("Comedy_language", "").strip() for r in grp if r.get("Comedy_language", "").strip()]
        if langs:
            base["Comedy_language"] = "; ".join(dict.fromkeys(langs))

        costs = [r.get("Cost", "").strip() for r in grp if r.get("Cost", "").strip()]
        if costs:
            base["Cost"] = costs[0]

        titles = [r.get("Event_title", "").strip() for r in grp if r.get("Event_title", "").strip()]
        if titles:
            merged_title = _normalize_series_name(titles[0])
            if len(merged_title) < 3:
                merged_title = _normalize_series_name(max(titles, key=len))
            base["Event_title"] = re.sub(r"\s+", " ", merged_title).strip()[:200]

        out.append(base)

    return out


def flatten_events_rows(events: list[Any]) -> list[dict[str, str]]:
    pairs: list[tuple[dict[str, str], str]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        row, key = _flatten_row_and_key(ev)
        pairs.append((row, key))
    counts = Counter(k for _, k in pairs if k)
    multi = {k for k, v in counts.items() if v >= 2}
    for row, key in pairs:
        if key and key in multi:
            row["Regularity"] = "recurring"
    rows = [r for r, _ in pairs]
    return _dedupe_recurring_slots(rows)


def flatten_row(event: dict) -> dict[str, str]:
    row, _ = _flatten_row_and_key(event)
    return row


def cmd_flatten(args: argparse.Namespace) -> int:
    root = _project_root()
    proc = root / "data" / "processed"

    if args.input:
        in_path = Path(args.input)
        if not in_path.is_absolute():
            in_path = root / in_path
    else:
        in_path = find_latest_enriched(proc)
        if in_path is None:
            print(
                "No events_enriched*.json under data/processed.\n"
                "Run: python -m scrapers enrich  (or collect_data.py enrich)",
                file=sys.stderr,
            )
            return 2

    if not in_path.is_file():
        print(f"Missing file: {in_path}", file=sys.stderr)
        return 2

    data = json.loads(in_path.read_text(encoding="utf-8"))
    events = data.get("events")
    if not isinstance(events, list):
        print("JSON has no 'events' list.", file=sys.stderr)
        return 2

    rows = flatten_events_rows(events)
    fieldnames = [
        "Weekday",
        "Location",
        "Time",
        "Cost",
        "Comedy_language",
        "Regularity",
        "Event_title",
        "URL",
        "Listing_title",
        "Description_preview",
        "Image_url",
    ]

    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = root / out_path
    else:
        proc.mkdir(parents=True, exist_ok=True)
        out_path = proc / "events_flat.csv"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        w.writerows(rows)

    print(f"[flatten] {len(rows)} rows → {out_path}")
    return 0


def build_flatten_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Flat CSV from events_enriched*.json (weekday, regularity, comedy language, …)."
    )
    p.add_argument(
        "-i",
        "--input",
        default="",
        help="Path to events_enriched*.json (default: newest under data/processed/)",
    )
    p.add_argument(
        "-o",
        "--output",
        default="",
        help="CSV output (default: data/processed/events_flat.csv)",
    )
    p.set_defaults(func=cmd_flatten)
    return p


def main(argv: list[str] | None = None) -> int:
    p = build_flatten_parser()
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
