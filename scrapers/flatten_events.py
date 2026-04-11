"""
Aus angereicherter Listing-JSON eine flache Tabelle bauen:
Wochentag, Location, Uhrzeit, Kosten, Sprache, Regelmäßigkeit, Titel_Event, URL.

- Quelle pro URL: schema.org Event in ``detail.ld_json``; Gruppenseiten ohne LD aus
  Listing-Titel / ``og_title`` ergänzen.
- **Regelmäßigkeit:** Eventfrog-Gruppen, schema.org-Serienhinweise, und dieselbe Serie
  (normalisierter Event-Name) **mehrfach im gleichen Export** → ``regelmäßig``.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

_WEEKDAY_DE = {
    1: "Montag",
    2: "Dienstag",
    3: "Mittwoch",
    4: "Donnerstag",
    5: "Freitag",
    6: "Samstag",
    7: "Sonntag",
}

_WEEKDAY_EN_TO_DE = {
    "monday": "Montag",
    "tuesday": "Dienstag",
    "wednesday": "Mittwoch",
    "thursday": "Donnerstag",
    "friday": "Freitag",
    "saturday": "Samstag",
    "sunday": "Sonntag",
}

# schema.org-Name: Datums-Suffixe entfernen, damit mehrere Termine dieselbe Serie treffen
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
    """Datum am Namensende entfernen (z. B. Kon-Tiki Comedy - April 14th)."""
    if not name or not isinstance(name, str):
        return ""
    n = name.strip()
    n = _DATE_SUFFIX_EN.sub("", n)
    n = _DATE_SUFFIX_DE.sub("", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _wochentag_from_en_listing(text: str) -> str:
    if not text:
        return ""
    tl = text.lower()
    for en, de in _WEEKDAY_EN_TO_DE.items():
        if re.search(rf"\b{re.escape(en)}\b", tl):
            return de
    return ""


def _location_after_events_count(title: str) -> str:
    """``16 Events Auer & Co., …`` → Ortsteil nach der Event-Anzahl (Eventfrog-Gruppen)."""
    if not title:
        return ""
    m = re.search(r"\b\d+\s+Events\s+(.+)$", title, re.I)
    if not m:
        return ""
    return m.group(1).strip()


def _detail_text_blob(detail: dict | None) -> str:
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
    Schlüssel für „gleicher Name / gleiche Serie, mehrere Termine“.
    Leerer String = nicht für Mehrfach-Zählung verwenden.
    """
    if node:
        raw = (node.get("name") or "").strip()
        if raw:
            return _fold_for_clustering(_normalize_series_name(raw))
    blob = _detail_text_blob(detail)
    if blob:
        # „Open Mic Comedy, Zurich in Zürich | …“
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
    wochentag: str,
    location: str,
    uhrzeit: str,
    title: str,
    detail: dict | None,
) -> tuple[str, str, str]:
    """Ohne LD-JSON: aus Listing-Titel und Meta-Titel ergänzen (pro URL eine vollständigere Zeile)."""
    blob = _detail_text_blob(detail)
    combined = f"{title} {blob}".strip()
    if not wochentag:
        w = _wochentag_from_en_listing(combined)
        if not w:
            for day in _WEEKDAY_DE.values():
                if day.lower() in combined.lower():
                    w = day
                    break
        wochentag = w
    if not location:
        loc = _location_after_events_count(title)
        if not loc and blob and " in " in blob:
            tail = blob.split(" in ", 1)[1]
            tail = tail.split("|", 1)[0].strip()
            if tail:
                loc = tail
        location = loc
    if not uhrzeit:
        m = re.search(r"(\d{1,2}:\d{2})", combined)
        if m:
            uhrzeit = m.group(1)
    return wochentag, location, uhrzeit


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
    """Ein Wert aus ``ld_json`` kann dict, @graph-Objekt oder verschachtelte Liste sein (Eventfrog)."""
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
    """``regelmäßig`` wenn Serie/Terminplan in schema.org erkennbar, sonst None."""
    if not isinstance(ld_blocks, list):
        return None
    for node in _iter_all_ld_dicts(ld_blocks):
        types_l = [t.lower() for t in _types(node)]
        if any("eventseries" in t for t in types_l):
            return "regelmäßig"
        if node.get("eventSchedule"):
            return "regelmäßig"
        if node.get("repeatFrequency") or node.get("repeatCount"):
            return "regelmäßig"
        se = node.get("subEvent")
        if isinstance(se, list) and len(se) > 1:
            return "regelmäßig"
        sup = node.get("superEvent")
        if isinstance(sup, dict):
            st = [str(x).lower() for x in _types(sup)]
            if any("eventseries" in t for t in st):
                return "regelmäßig"
    return None


def _recurrence_from_listing(path: str, title: str, url: str) -> str | None:
    """Eventfrog-Gruppenseiten und Titel-Muster ohne LD-JSON."""
    combined = f"{path} {url}".lower()
    if "/p/groups/" in combined:
        return "regelmäßig"
    tl = title.lower()
    if "event group" in tl or "veranstaltungsgruppe" in tl or "eventgruppe" in tl:
        return "regelmäßig"
    if re.search(r"\b\d+\s+events\b", title, re.I):
        return "regelmäßig"
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
    if r == "regelmäßig":
        return "regelmäßig"
    if has_event_node:
        return "einmalig"
    if url and "eventfrog.ch" in url.lower() and "/p/groups/" not in url.lower():
        return "einmalig"
    return "unbekannt"


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


def _guess_language_from_url(url: str) -> str:
    u = url.lower()
    if "/de/" in u:
        return "Deutsch"
    if "/fr/" in u:
        return "Französisch"
    if "/en/" in u:
        return "Englisch"
    return ""


def _guess_language_from_text(text: str) -> str:
    t = text.lower()
    if "english" in t or " eng " in t or "stand-up" in t and "english" in t:
        return "Englisch"
    if "français" in t or "französisch" in t or " en français" in t:
        return "Französisch"
    if "deutsch" in t or " auf deutsch" in t:
        return "Deutsch"
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


def _titel_event(node: dict | None, detail: dict | None, listing_title: str) -> str:
    """Kurzer Anzeige-Titel pro URL (schema.org-Name oder Seitentitel)."""
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
    """Eine Tabellenzeile + Serien-Schlüssel für Mehrfach-Termine."""
    url = (event.get("url") or "").strip()
    title = (event.get("title") or "").strip()
    path = (event.get("path") or "").strip()
    detail = event.get("detail")
    if not isinstance(detail, dict):
        detail = None

    node = _event_node_from_detail(detail)
    wochentag = ""
    uhrzeit = ""
    location = ""
    kosten = ""
    sprache = ""

    if node:
        start = _parse_iso(node.get("startDate"))
        if start:
            wochentag = _WEEKDAY_DE.get(start.isoweekday(), "")
            uhrzeit = start.strftime("%H:%M")
        location = _format_location(node.get("location"))
        kosten = _format_offers(node.get("offers"))
        lang = node.get("inLanguage") or node.get("availableLanguage")
        if isinstance(lang, list):
            sprache = ", ".join(str(x) for x in lang if x)
        elif isinstance(lang, str):
            sprache = lang.strip()

    if not sprache:
        sprache = _guess_language_from_url(url)
    if not sprache and title:
        sprache = _guess_language_from_text(title)
    if not sprache and detail:
        blob = " ".join(
            filter(
                None,
                [
                    str(detail.get("og_title") or ""),
                    str(detail.get("h1") or ""),
                    str(detail.get("title_tag") or ""),
                ],
            )
        )
        sprache = _guess_language_from_text(blob)

    if not location and title:
        # z. B. "... 20:00 Regenbogen Bar, Zürich (CH)"
        m = re.search(r"\d{1,2}:\d{2}\s+(.+)$", title)
        if m:
            location = m.group(1).strip()

    if not uhrzeit and title:
        m = re.search(r"(\d{1,2}:\d{2})", title)
        if m:
            uhrzeit = m.group(1)

    if not wochentag and title:
        for day in _WEEKDAY_DE.values():
            if day.lower() in title.lower():
                wochentag = day
                break

    wochentag, location, uhrzeit = _fill_group_row_gaps(
        wochentag=wochentag,
        location=location,
        uhrzeit=uhrzeit,
        title=title,
        detail=detail,
    )

    ld_list = detail.get("ld_json") if detail else None
    if not isinstance(ld_list, list):
        ld_list = None
    regelmäßigkeit = _recurrence_label(
        ld_blocks=ld_list,
        path=path,
        title=title,
        url=url,
        has_event_node=node is not None,
    )

    titel_event = _titel_event(node, detail, title)
    series_key = _series_identity_key(title=title, detail=detail, node=node)

    row = {
        "Wochentag": wochentag,
        "Location": location,
        "Uhrzeit": uhrzeit,
        "Kosten": kosten,
        "Sprache": sprache,
        "Regelmäßigkeit": regelmäßigkeit,
        "Titel_Event": titel_event,
        "URL": url,
        "Titel_Listing": title[:200],
    }
    return row, series_key


def flatten_events_rows(events: list[Any]) -> list[dict[str, str]]:
    """Alle Events flach machen; gleiche Serie (mehrere Termine) → Regelmäßigkeit ``regelmäßig``."""
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
            row["Regelmäßigkeit"] = "regelmäßig"
    return [r for r, _ in pairs]


def flatten_row(event: dict) -> dict[str, str]:
    """Eine Zeile (ohne globale Serien-Zählung). Für Einzeltests; bevorzugt ``flatten_events_rows``."""
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
                "Keine events_enriched*.json unter data/processed gefunden.\n"
                "Zuerst: python -m scrapers enrich  (oder collect_data.py enrich)",
                file=sys.stderr,
            )
            return 2

    if not in_path.is_file():
        print(f"Datei fehlt: {in_path}", file=sys.stderr)
        return 2

    data = json.loads(in_path.read_text(encoding="utf-8"))
    events = data.get("events")
    if not isinstance(events, list):
        print("JSON enthält keine 'events'-Liste.", file=sys.stderr)
        return 2

    rows = flatten_events_rows(events)
    fieldnames = [
        "Wochentag",
        "Location",
        "Uhrzeit",
        "Kosten",
        "Sprache",
        "Regelmäßigkeit",
        "Titel_Event",
        "URL",
        "Titel_Listing",
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

    print(f"[flatten] {len(rows)} Zeilen → {out_path}")
    return 0


def build_flatten_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Flache CSV aus events_enriched*.json (Wochentag, Regelmäßigkeit, …)."
    )
    p.add_argument(
        "-i",
        "--input",
        default="",
        help="Pfad zu events_enriched*.json (Standard: neueste unter data/processed/)",
    )
    p.add_argument(
        "-o",
        "--output",
        default="",
        help="CSV-Ausgabe (Standard: data/processed/events_flat.csv)",
    )
    p.set_defaults(func=cmd_flatten)
    return p


def main(argv: list[str] | None = None) -> int:
    p = build_flatten_parser()
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
