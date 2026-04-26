"""
Build a flat table from enriched listing JSON:
Weekday, Location, Time, Cost, Comedy_language, Regularity, Event_title, URL.

- Per URL: schema.org Event in ``detail.ld_json``; group pages without LD are filled
  from listing title, meta titles, optional ``text_preview`` (see ``event_page``), and URL.
- **Regularity:** schema.org series hints, and the same normalized series
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
from urllib.parse import unquote, urlparse

_RULES_JSON_PATH = Path(__file__).resolve().parents[1] / "config" / "rules.json"

_RE_LEADING_TIME_IN_LOCATION = re.compile(
    r"^\s*(?:"
    r"\d{1,2}:\d{2}\s*(?:uhr)?\s+"  # "20:00 Uhr Venue…"
    r"|"
    r"\d{2}\s*uhr\s+"  # "00 Uhr Venue…" / "30 Uhr Venue…" (minutes-only scrape artefact)
    r"|"
    r"(?:00|15|30|45)\s+"  # "00 Venue…" (minutes-only artefact without "Uhr")
    r"|"
    r"uhr\s+"  # stray "Uhr Venue…"
    r")",
    re.I,
)


def _strip_leading_time_from_location(loc: str) -> str:
    s = (loc or "").strip()
    if not s:
        return ""
    s2 = _RE_LEADING_TIME_IN_LOCATION.sub("", s, count=1).strip()
    return s2 or s


def _location_looks_complete(loc: str) -> bool:
    """Heuristic: Swiss PLZ plus structured tail → skip LLM-based replacement."""
    s = (loc or "").strip()
    if not s or "," not in s:
        return False
    if not re.search(r"\b8\d{3}\b", s):
        return False
    return len(s) >= 18


def _location_from_venue_llm(detail: dict | None, location: str) -> str:
    """Prefer ``detail['venue_llm']`` from enrich when location is still vague."""
    if not detail or not isinstance(detail.get("venue_llm"), dict):
        return location
    vr = detail["venue_llm"]
    try:
        conf = float(vr.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0.0
    if conf < 0.65:
        return location
    fl = vr.get("formatted_location")
    if not isinstance(fl, str) or not fl.strip():
        return location
    if _location_looks_complete(location):
        return location
    return fl.strip()


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
    ("dini-muetter", "German"),
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

_COUNTRY_TOKENS = {
    "schweiz",
    "suisse",
    "svizzera",
    "svizra",
    "switzerland",
}


def _clean_display_name(display_name: str) -> str:
    raw = re.sub(r"\s+", " ", (display_name or "").strip())
    if not raw:
        return ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]

    while parts:
        last = parts[-1]
        slash_tokens = {t.strip().lower() for t in last.split("/") if t.strip()}
        if slash_tokens and slash_tokens <= _COUNTRY_TOKENS:
            parts.pop()
            continue
        if last.lower() in _COUNTRY_TOKENS:
            parts.pop()
            continue
        break

    return ", ".join(parts)


def _load_location_geocache() -> dict[str, dict]:
    try:
        p = Path(__file__).resolve().parents[1] / "data" / "processed" / "location_geocache.json"
        if not p.is_file():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


_LOCATION_GEOCACHE = _load_location_geocache()


def _format_address(*, venue_hint: str, display_name: str) -> str:
    """
    Format as: "Venue, StreetName No, ZIP City"
    using a venue hint (usually from the original Location string) plus Nominatim display_name.
    """
    dn = _clean_display_name(display_name)
    parts = [p.strip() for p in dn.split(",") if p.strip()]

    # Venue: keep hint if it looks like a name (not a number/address fragment)
    venue = (venue_hint or "").strip()
    if venue:
        if re.match(r"^\d", venue):
            venue = ""
        if len(venue) < 2:
            venue = ""
    if venue:
        # If the "venue" already includes address fragments (street/zip/city), strip them.
        # This avoids outputs like "GZ Wollishofen Bachstrasse 7 ..., Bachstrasse 7, 8038 Zürich".
        v = re.sub(r"\s+", " ", venue).strip()
        # Remove common "ZIP City" tails inside the venue hint.
        v = re.sub(r"\b\d{4}\s+(?:zürich|zurich)\b", "", v, flags=re.I).strip()
        # Remove embedded "StreetName No" patterns from venue hint.
        v = re.sub(
            r"\b[\wÀ-ÿ.\-']+(?:strasse|straße|gasse|platz|weg|allee|quai|promenade|ring|ufer|hof|berg|steig|bühl|rain|brücke|bruecke)\s*\d+[a-z]?\b",
            "",
            v,
            flags=re.I,
        ).strip()
        v = re.sub(r"\s{2,}", " ", v).strip(" ,-/–—")
        if v and len(v) >= 2:
            venue = v

    # Street + number: Nominatim often gives "No, Street" (e.g. "359, Schaffhauserstrasse")
    street = ""
    number = ""
    venue_fold = (venue or "").strip().casefold()

    def _looks_like_street_name(s: str) -> bool:
        t = (s or "").strip().casefold()
        if not t:
            return False
        # Zurich-centric street tokens; keep broad but avoid obvious venue names.
        return bool(
            re.search(
                # allow suffix matches like "Sihlquai" (not just "... quai")
                r"(strasse|straße|gasse|platz|weg|allee|quai|promenade|ring|ufer|hof|berg|steig|bühl|rain|brücke|bruecke)\b",
                t,
                flags=re.I,
            )
        )

    best = (0, "", "")  # (score, street, number)
    first = ("", "")
    for i in range(len(parts) - 1):
        a = parts[i]
        b = parts[i + 1]
        cand_street = ""
        cand_no = ""
        if re.fullmatch(r"\d+[a-z]?", a, flags=re.I) and re.search(r"[a-zA-ZÄÖÜäöü]", b):
            cand_no, cand_street = a, b
        elif re.search(r"[a-zA-ZÄÖÜäöü]", a) and re.fullmatch(r"\d+[a-z]?", b, flags=re.I):
            cand_street, cand_no = a, b
        else:
            continue

        if not first[0]:
            first = (cand_street, cand_no)

        street_fold = cand_street.strip().casefold()
        # Prefer real street names; avoid using the venue name as street ("Auer & Co, 131").
        score = 0
        if _looks_like_street_name(cand_street):
            score += 3
        if venue_fold and street_fold and street_fold != venue_fold:
            score += 1
        if venue_fold and street_fold and street_fold == venue_fold:
            score -= 5

        if score > best[0]:
            best = (score, cand_street, cand_no)

    if best[1] and best[2]:
        street, number = best[1], best[2]
    elif first[0] and first[1]:
        # Fallback for non-streety places; better than blank, but last resort.
        street, number = first[0], first[1]
    street_line = " ".join(x for x in [street, number] if x).strip()

    # ZIP + City: find the first 4-digit token (prefer 8xxx) and a nearby city token (prefer Zürich)
    zip_code = ""
    for p in parts:
        m = re.search(r"\b(\d{4})\b", p)
        if m:
            zip_code = m.group(1)
            if zip_code.startswith("8"):
                break
    city = ""
    for p in parts:
        if p.lower() in ("zürich", "zurich"):
            city = "Zürich"
            break
    if not city and len(parts) >= 2:
        # fallback: last non-zip part
        for p in reversed(parts):
            if re.search(r"\b\d{4}\b", p):
                continue
            if len(p) >= 3:
                city = p
                break

    tail = " ".join(x for x in [zip_code, city] if x).strip()

    out = ", ".join(x for x in [venue, street_line, tail] if x)
    return out.strip()


def _canonicalize_location(loc: str) -> str:
    """
    Canonicalize different formatting of the same venue/address via geocode cache.
    Example: "Amboss Rampe, Zürich (CH)" -> "Amboss Rampe, 80, Zollstrasse, ... , 8005"
    """
    l = (loc or "").strip()
    if not l:
        return ""
    entry = _LOCATION_GEOCACHE.get(l) if isinstance(_LOCATION_GEOCACHE, dict) else None
    if isinstance(entry, dict):
        dn = entry.get("display_name")
        if isinstance(dn, str) and dn.strip():
            venue_hint = l.split(",", 1)[0].strip() if "," in l else l
            formatted = _format_address(venue_hint=venue_hint, display_name=dn)
            if formatted:
                return formatted
    return l

_LANG_STOPWORDS: dict[str, set[str]] = {
    "English": {
        "the",
        "and",
        "or",
        "with",
        "doors",
        "show",
        "starts",
        "start",
        "free",
        "tickets",
        "comedy",
        "open",
        "mic",
        "night",
        "join",
        "hosted",
        "every",
        "audience",
    },
    "German": {
        "und",
        "oder",
        "mit",
        "tür",
        "tuer",
        "einlass",
        "beginn",
        "kostenlos",
        "tickets",
        "komödie",
        "komodie",
        "kultur",
        "jeden",
        "monat",
        "uhr",
        "anmeldung",
        "hinweis",
        "bühne",
        "buehne",
    },
    "French": {"et", "avec", "entrée", "entree", "gratuit", "spectacle", "comédie", "comedie"},
    "Spanish": {"y", "con", "entrada", "gratis", "comedia", "español", "espanol"},
}


def _infer_language_from_text(blob: str) -> str:
    """
    Lightweight fallback when no explicit language cues exist.
    Uses stopword scoring over title + visible copy.
    """
    if not blob:
        return ""
    t = blob.lower()
    t = re.sub(r"[^a-z0-9äöüéèàçñß\s]", " ", t, flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) < 60:
        return ""
    toks = t.split()
    if len(toks) < 12:
        return ""

    scores: dict[str, int] = {k: 0 for k in _LANG_STOPWORDS}
    for tok in toks:
        for lang, sw in _LANG_STOPWORDS.items():
            if tok in sw:
                scores[lang] += 1

    best_lang, best = max(scores.items(), key=lambda kv: kv[1])
    second = sorted(scores.values(), reverse=True)[1] if len(scores) > 1 else 0
    if best >= 4 and best - second >= 2:
        return best_lang
    return ""


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
    # Strip common category prefixes some sites prepend (e.g. "StandUp: ...").
    n = re.sub(r"^(?:stand\s*up|standup|stand-?up)\s*[:\-–—]\s*", "", n, flags=re.I)
    n = re.sub(r"^(?:comedy)\s*[:\-–—]\s*", "", n, flags=re.I)
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


def _gz_extract_fields(_detail: dict | None) -> tuple[str, str]:
    # No per-source parsing rules.
    return "", ""


def _location_after_events_count(title: str) -> str:
    """``16 Events Auer & Co., …`` → location segment after the event count."""
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
            # Avoid pulling in long descriptive paragraphs as "location".
            tail = re.split(r"[.!?]\s+|\s+[-–—]\s+|\s+·\s+", tail, maxsplit=1)[0].strip()
            if len(tail) > 140:
                tail = tail[:140].rsplit(" ", 1)[0].strip()
            if tail:
                loc = tail
        location = loc

    if not time_s:
        # Prefer show start / begin times over doors/location-open times.
        # This avoids picking up "Location offen 15:00" instead of "Showstart 19:00".
        def _norm_hhmm(
            h: str,
            m: str,
            suffix: str | None,
            *,
            sep: str = ":",
            tail: str = "",
        ) -> str:
            try:
                hh = int(h)
                mm = int(m)
            except Exception:
                return ""
            if not (0 <= hh <= 23 and 0 <= mm <= 59):
                return ""
            # Avoid interpreting dates like "16.04.2026" as a time "16:04".
            # This is a common artifact on listing/landing pages where the date appears
            # near words like "Start:".
            if sep == ".":
                t = (tail or "").lstrip()
                if t.startswith(".") or (t[:1].isdigit()):
                    return ""
            suf = (suffix or "").strip().lower()
            # Convert 12h clock if suffix present.
            if suf in {"am", "pm"}:
                if hh == 12:
                    hh = 0
                if suf == "pm":
                    hh = (hh + 12) % 24
            return f"{hh:02d}:{mm:02d}"

        m = re.search(
            r"\b(?:show\s*starts?|show\s*start|showstart|beginn|start)\b[^0-9]{0,25}(\d{1,2})([:.])(\d{2})\s*(am|pm|uhr)?",
            combined,
            flags=re.I,
        )
        if not m:
            m = re.search(
                r"(\d{1,2})([:.])(\d{2})\s*(am|pm|uhr)?\s*[–-]\s*(?:show\s*start|showstart|beginn|start)",
                combined,
                flags=re.I,
            )
        if m:
            time_s = _norm_hhmm(
                m.group(1),
                m.group(3),
                m.group(4),
                sep=m.group(2),
                tail=combined[m.end() : m.end() + 6],
            )
        else:
            # Generic fallback: only accept HH:MM (colon) to avoid dd.mm dates.
            m2 = re.search(r"\b(\d{1,2}):(\d{2})\s*(am|pm)?\b", combined, flags=re.I)
            if m2:
                time_s = _norm_hhmm(m2.group(1), m2.group(2), m2.group(3), sep=":", tail=combined[m2.end() : m2.end() + 6])

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
    """A value from ``ld_json`` may be dict, @graph object, or nested list."""
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
    # No per-source recurrence inference rules.
    # Recurrence should come from generic schema.org signals or explicit weekday patterns in copy.
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


def _resolve_location_from_ld(ld_blocks: list[Any] | None, loc: Any) -> Any:
    """
    Event pages often store the Place as a separate node in an @graph and the Event.location
    is just a reference ({"@id": "..."}). Resolve such references so we can extract address
    fields (streetAddress, postalCode, ...).
    """
    if not isinstance(ld_blocks, list) or not ld_blocks:
        return loc

    # Reference-only object: {"@id": "..."}
    if isinstance(loc, dict) and isinstance(loc.get("@id"), str) and len(loc.keys()) <= 3:
        target_id = loc.get("@id")
        if target_id:
            for node in _iter_all_ld_dicts(ld_blocks):
                if isinstance(node, dict) and node.get("@id") == target_id:
                    return node
        return loc

    # If location is a string, try to find a Place node with matching name and a richer address.
    if isinstance(loc, str):
        needle = loc.strip()
        if not needle:
            return loc
        needle_fold = needle.casefold()
        best: dict | None = None
        for node in _iter_all_ld_dicts(ld_blocks):
            if not isinstance(node, dict):
                continue
            if "Place" not in _types(node):
                continue
            name = str(node.get("name") or "").strip()
            if not name:
                continue
            if name.casefold() != needle_fold:
                continue
            # Prefer nodes that actually have a structured address.
            addr = node.get("address")
            if isinstance(addr, dict) and any(addr.get(k) for k in ("streetAddress", "postalCode", "addressLocality")):
                return node
            best = node
        return best if best is not None else loc

    return loc


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


def _location_from_google_maps_links(detail: dict | None) -> str:
    """
    Some venue pages include a Google Maps "place" link but no structured address.
    Use the place name as a better geocoding query than a vague description string.
    """
    if not detail:
        return ""
    links = detail.get("links")
    if not isinstance(links, list):
        return ""
    for h in links:
        if not isinstance(h, str) or "google.com/maps" not in h:
            continue
        try:
            p = urlparse(h)
        except Exception:
            continue
        if "google.com" not in (p.hostname or ""):
            continue
        m = re.search(r"/maps/place/([^/?#]+)", p.path)
        if not m:
            continue
        name = unquote(m.group(1)).replace("+", " ").strip()
        name = re.sub(r"\s+", " ", name).strip()
        if name:
            return f"{name}, Zürich"
    return ""


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
        "swiss german": "German",
    }
    if low in known:
        return known[low]
    base = low.split("-")[0]
    return _LANG_CODE_MAP.get(base, s)


def _language_from_description(desc: str) -> str:
    if not desc:
        return ""
    dl = desc.lower()
    # Explicit on-stage language (common on Swiss sites even when schema.org inLanguage is "en").
    if re.search(r"\bdeutschsprachig", dl, re.I):
        return "German"
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
            return "German"
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
    """On-stage / performance language."""
    # Visible page copy (not only schema.org description) — avoids wrong inLanguage from site locale.
    desc_for_lang = ""
    if node and isinstance(node.get("description"), str) and node["description"].strip():
        desc_for_lang = node["description"].strip()
    meta_blob = _detail_meta_blob(detail) if detail else ""
    if meta_blob:
        desc_for_lang = (desc_for_lang + " " + meta_blob).strip() if desc_for_lang else meta_blob

    if desc_for_lang and re.search(r"\bdeutschsprachig", desc_for_lang, re.I):
        return "German"

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

    desc = desc_for_lang if desc_for_lang else ""
    if not desc and node and isinstance(node.get("description"), str):
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
        add("German")
    if "swiss german" in blob or "schwiizertüütsch" in blob or "schweizerdeutsch" in blob:
        add("German")
    if "auf deutsch" in blob or re.search(r"\bin german\b", blob):
        add("German")
    if "français" in blob or " en français" in blob or "in french" in blob:
        add("French")

    slug_lang = _language_from_slug_path(path, url)
    if slug_lang:
        add(slug_lang)

    if parts_out:
        return "; ".join(dict.fromkeys(parts_out))

    return _infer_language_from_text(blob)


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
    def _strip_trailing_city_suffix(s: str) -> str:
        t = re.sub(r"\s+", " ", (s or "").strip())
        if not t:
            return ""
        # Common artifact from some sources: "... in Zürich" / "... in Zurich"
        # Only strip when it appears as a trailing suffix.
        t2 = re.sub(
            r"\s+\bin\s+z(?:ü|u)rich(?:\s*,\s*(?:ch|che))?\s*$",
            "",
            t,
            flags=re.I,
        ).strip()
        return t2 or t

    if node:
        n = (node.get("name") or "").strip()
        if n:
            return _strip_trailing_city_suffix(n)[:200]
    blob = (detail.get("og_title") or detail.get("title_tag") or "").strip() if detail else ""
    if blob and "|" in blob:
        blob = blob.split("|", 1)[0].strip()
    if blob:
        return _strip_trailing_city_suffix(blob)[:200]
    return _strip_trailing_city_suffix(listing_title or "")[:200]


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
        ld_list = detail.get("ld_json") if detail else None
        if not isinstance(ld_list, list):
            ld_list = None
        location_obj = _resolve_location_from_ld(ld_list, node.get("location"))
        location = _format_location(location_obj)
        cost = _format_offers(node.get("offers"))

    comedy_lang = _infer_comedy_language(
        node=node, detail=detail, title=title, url=url, path=path
    )

    if not location and title:
        m = re.search(r"\d{1,2}:\d{2}\s+(.+)$", title)
        if m:
            location = m.group(1).strip()

    if not location and detail:
        location = _location_from_google_maps_links(detail) or location

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

    gz_loc, gz_cost = _gz_extract_fields(detail)
    if gz_loc:
        location = gz_loc
    if gz_cost and not cost:
        cost = gz_cost

    location = _location_from_venue_llm(detail, location)
    location = _strip_leading_time_from_location(location)
    location = _canonicalize_location(location)

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
    if regularity == "unknown" and detail and isinstance(detail.get("text_preview"), str):
        # Generic fallback: recurring wording in visible copy, even when no EventSeries schema exists.
        blob = detail["text_preview"].lower()
        if ("jeden" in blob or "every" in blob) and _weekday_indices_from_text(blob):
            regularity = "recurring"
    # No per-source recurrence inference rules.

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


_RE_OPEN_MIC = re.compile(r"\bopen[\s-]*mic\b", re.I)
_RE_MUSIC_JAM = re.compile(
    r"(?:\bjam\s*session\b|\bjam\b).*?(?:\bmusik\b|\bmusic\b|\bband\b|\bkonzert\b|\bconcert\b|\bmusizieren\b|\bhouse-?band\b)"
    r"|(?:\bmusik\b|\bmusic\b|\bband\b|\bkonzert\b|\bconcert\b|\bmusizieren\b|\bhouse-?band\b).*?(?:\bjam\s*session\b|\bjam\b)",
    re.I,
)
_RE_SOLO_COMEDIAN_EXCLUDE = re.compile(
    r"\b(?:ck\s+presents|comedy\s+kiss\s+presents|presents:)\b", re.I
)


def _is_open_mic_confirmed(*texts: str) -> bool:
    blob = " ".join(t for t in texts if t)
    if not _RE_OPEN_MIC.search(blob):
        return False
    # Exclude music jam sessions that use "open mic" literally.
    if _RE_MUSIC_JAM.search(blob):
        return False
    return True


def _looks_like_single_comedian_show(*texts: str) -> bool:
    blob = " ".join(t for t in texts if t)
    return bool(_RE_SOLO_COMEDIAN_EXCLUDE.search(blob))


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
            # No source-specific URL preference.
            base["URL"] = urls[0]

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


def _token_set(s: str) -> set[str]:
    t = re.sub(r"[^a-z0-9äöüéèàçñß\s]", " ", (s or "").lower(), flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return set()
    return {w for w in t.split(" ") if len(w) >= 3}


def _titles_look_equivalent(a: str, b: str) -> bool:
    """
    Best-effort duplicate detection across sources.
    We only use this for events that already collide on (location, weekday, time).
    """
    fa = _fold_for_clustering(_normalize_series_name(a or ""))
    fb = _fold_for_clustering(_normalize_series_name(b or ""))
    if not fa or not fb:
        return False
    if fa == fb:
        return True
    if fa in fb or fb in fa:
        return True
    ta = _token_set(fa)
    tb = _token_set(fb)
    if not ta or not tb:
        return False
    j = len(ta & tb) / max(1, len(ta | tb))
    return j >= 0.6


def _dedupe_loose_same_slot(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Additional cross-source dedupe: same (location, weekday, time) but minor title formatting
    differences (e.g. EN vs DE listing, punctuation/case, etc.).

    This is intentionally conservative: we only merge if titles look equivalent.
    """
    groups: OrderedDict[tuple[str, str, str], list[dict[str, str]]] = OrderedDict()
    for r in rows:
        k = (
            _norm_slot_field(r.get("Location", "")),
            _norm_slot_field(r.get("Weekday", "")),
            _norm_slot_field(r.get("Time", "")),
        )
        groups.setdefault(k, []).append(r)

    out: list[dict[str, str]] = []
    for grp in groups.values():
        if len(grp) == 1:
            out.append(dict(grp[0]))
            continue

        # Partition by "equivalent title" clusters
        clusters: list[list[dict[str, str]]] = []
        for r in grp:
            placed = False
            for c in clusters:
                if _titles_look_equivalent(r.get("Event_title", ""), c[0].get("Event_title", "")):
                    c.append(r)
                    placed = True
                    break
            if not placed:
                clusters.append([r])

        for c in clusters:
            if len(c) == 1:
                out.append(dict(c[0]))
                continue
            base = dict(c[0])
            # No source-specific URL preference.
            urls = [x.get("URL", "").strip() for x in c if x.get("URL", "").strip()]
            if urls:
                base["URL"] = urls[0]

            # Keep the "best" title (longest non-empty after normalization)
            titles = [x.get("Event_title", "").strip() for x in c if x.get("Event_title", "").strip()]
            if titles:
                best = max(titles, key=lambda s: len(_normalize_series_name(s)))
                base["Event_title"] = re.sub(r"\s+", " ", _normalize_series_name(best)).strip()[:200]

            # Merge language/cost if missing
            if not base.get("Comedy_language", "").strip():
                langs = [x.get("Comedy_language", "").strip() for x in c if x.get("Comedy_language", "").strip()]
                if langs:
                    base["Comedy_language"] = "; ".join(dict.fromkeys(langs))
            if not base.get("Cost", "").strip():
                costs = [x.get("Cost", "").strip() for x in c if x.get("Cost", "").strip()]
                if costs:
                    base["Cost"] = costs[0]

            # Prefer an image if base lacks one
            if not base.get("Image_url", "").strip():
                imgs = [x.get("Image_url", "").strip() for x in c if x.get("Image_url", "").strip()]
                if imgs:
                    base["Image_url"] = imgs[0]

            out.append(base)

    return out


def _venue_key(loc: str) -> str:
    """
    Best-effort venue key: first comma-separated token.
    This lets us dedupe the same recurring series across different sources/hosts
    even when address formatting differs (e.g. "VIOR, Zürich" vs "Vior, Löwenstrasse 2, 8001 Zürich").
    """
    s = (loc or "").strip()
    if not s:
        return ""
    head = s.split(",", 1)[0].strip()
    return _norm_slot_field(head)


def _dedupe_series_across_sources(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Cross-row dedupe for the same recurring series at the same venue.

    No source-specific preferences: pick the "best" row using only generic signals.
    """
    groups: OrderedDict[str, list[dict[str, str]]] = OrderedDict()
    for r in rows:
        k = _venue_key(r.get("Location", ""))
        groups.setdefault(k, []).append(r)

    def score(r: dict[str, str]) -> tuple[int, int, int, int]:
        url = (r.get("URL") or "").strip()
        title = (r.get("Event_title") or "").strip()
        weekday = (r.get("Weekday") or "").strip()
        time_s = (r.get("Time") or "").strip()
        # Prefer entries that cover more weekdays (e.g. "Tue, Fri, Sun")
        wd_n = len([x for x in weekday.split(",") if x.strip()]) if weekday else 0
        s0 = wd_n
        # Prefer non-empty time
        s1 = 1 if time_s else 0
        # Prefer having a URL (generic stability signal)
        s2 = 1 if url else 0
        # Prefer longer, more descriptive title (minor tie-break)
        s3 = min(200, len(title))
        return (s0, s1, s2, s3)

    out: list[dict[str, str]] = []
    for grp in groups.values():
        if len(grp) == 1:
            out.append(dict(grp[0]))
            continue

        # Partition by "equivalent title" clusters within the same venue.
        clusters: list[list[dict[str, str]]] = []
        for r in grp:
            placed = False
            for c in clusters:
                if _titles_look_equivalent(r.get("Event_title", ""), c[0].get("Event_title", "")):
                    c.append(r)
                    placed = True
                    break
            if not placed:
                clusters.append([r])

        for c in clusters:
            if len(c) == 1:
                out.append(dict(c[0]))
                continue
            best = max(c, key=score)
            out.append(dict(best))
    return out


def flatten_events_rows(events: list[Any]) -> list[dict[str, str]]:
    pairs: list[tuple[dict[str, str], str]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        row, key = _flatten_row_and_key(ev)
        # Confirm "open mic" in title OR visible copy
        detail = ev.get("detail") if isinstance(ev.get("detail"), dict) else None
        detail_blob = _detail_meta_blob(detail) if detail else ""
        if _looks_like_single_comedian_show(row.get("Event_title", ""), row.get("Listing_title", "")):
            continue
        if not _is_open_mic_confirmed(row.get("Event_title", ""), row.get("Listing_title", ""), detail_blob):
            continue
        pairs.append((row, key))
    counts = Counter(k for _, k in pairs if k)
    multi = {k for k, v in counts.items() if v >= 2}
    for row, key in pairs:
        if key and key in multi:
            row["Regularity"] = "recurring"
    rows = [r for r, _ in pairs]
    rows = _dedupe_recurring_slots(rows)
    rows = _dedupe_loose_same_slot(rows)
    rows = _dedupe_series_across_sources(rows)
    # Project output is focused on recurring open mics; drop one-off/single-performer shows.
    rows = [r for r in rows if (r.get("Regularity") or "").strip().lower() == "recurring"]
    return rows


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

    # Duplicate precheck (helps spot multi-source duplicates)
    by_url = Counter((r.get("URL") or "").strip() for r in rows if (r.get("URL") or "").strip())
    dup_urls = sum(1 for _, c in by_url.items() if c > 1)
    if dup_urls:
        print(f"[flatten] Note: {dup_urls} duplicate URL(s) in output (after filtering).")
    slot_keys = Counter(_slot_dedup_key(r) for r in rows)
    dup_slots = sum(1 for _, c in slot_keys.items() if c > 1)
    if dup_slots:
        print(f"[flatten] Note: {dup_slots} duplicate slot key(s) in output (after filtering).")
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
