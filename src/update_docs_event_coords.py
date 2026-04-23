#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EVENTS_JSON = ROOT / "docs" / "data" / "events.json"
GEOCACHE_PATH = ROOT / "data" / "processed" / "location_geocache.json"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_geocache(path: Path) -> dict[str, dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def _save_geocache(path: Path, cache: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _expected_zip(query: str) -> str:
    m = re.search(r"\b(\d{4})\b", str(query or ""))
    return m.group(1) if m else ""


def _expects_zurich(query: str) -> bool:
    low = str(query or "").lower()
    return ("zürich" in low) or ("zurich" in low)


def _display_name_ok(display_name: str | None, *, expected_zip: str, expects_zurich: bool) -> bool:
    dn = str(display_name or "")
    if expected_zip and expected_zip not in dn:
        return False
    if expects_zurich and not re.search(r"\bzürich\b|\bzurich\b", dn, flags=re.I):
        return False
    return True


def _pick_best_result(items: list[dict], *, expected_zip: str, expects_zurich: bool) -> dict | None:
    if not items:
        return None
    good = [it for it in items if _display_name_ok(it.get("display_name"), expected_zip=expected_zip, expects_zurich=expects_zurich)]
    if expected_zip and not good:
        return None
    return good[0] if good else items[0]


def _nominatim_geocode(query: str, *, timeout_s: int = 20) -> dict | None:
    params: dict[str, str | int] = {
        "q": query,
        "format": "jsonv2",
        "limit": 10,
        "addressdetails": 0,
        "countrycodes": "ch",
    }
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "open-mics-zurich/0.1 (docs coords updater; contact: datenpunk.ch@gmail.com)",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    if not isinstance(data, list) or not data:
        return None
    item = _pick_best_result(data, expected_zip=_expected_zip(query), expects_zurich=_expects_zurich(query))
    if not item:
        return None
    try:
        return {
            "lat": float(item["lat"]),
            "lon": float(item["lon"]),
            "display_name": item.get("display_name"),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _event_query(e: dict) -> str:
    loc = str(e.get("location_display") or e.get("location") or "").strip()
    if loc:
        return loc
    venue = str(e.get("venue") or "").strip()
    addr = str(e.get("address") or "").strip()
    bits = [b for b in [venue, addr] if b]
    return ", ".join(bits)


def _clean_query(q: str) -> str:
    s = " ".join(str(q or "").strip().split())
    if not s:
        return s
    # Remove accidental duplicate ", 800x Zürich" segments (common during manual edits).
    s = re.sub(r"(,\s*\b8\d{3}\s+Zürich)\s*\1\b", r"\1", s, flags=re.I)
    # Remove duplicate consecutive tokens like ", Zürich, Zürich"
    s = re.sub(r"(,\s*Zürich)\s*\1\b", r"\1", s, flags=re.I)
    # Normalize multiple commas/spaces.
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"(,\s*){2,}", ", ", s)
    return s.strip(" ,")


def _candidate_queries(e: dict) -> list[str]:
    """
    Try a few increasingly generic queries, keeping results in Zürich/CH.
    """
    out: list[str] = []

    base = _clean_query(_event_query(e))
    if base:
        out.append(base)
        # If the query doesn't mention Zürich, add it.
        if not re.search(r"\bzürich\b|\bzurich\b", base, flags=re.I):
            out.append(_clean_query(base + ", Zürich"))
        # Add a country hint that matches existing geocache keys in this repo.
        out.append(_clean_query(base + ", Zürich (CH)"))
        out.append(_clean_query(base + ", Switzerland"))

    venue = _clean_query(str(e.get("venue") or ""))
    addr = _clean_query(str(e.get("address") or ""))

    def venue_variants(v: str) -> list[str]:
        if not v:
            return []
        outv = [v]
        low = v.lower()
        # Common suffixes that break exact venue matches.
        for suf in ["courtyard", "bar & coffee", "bar and coffee", "bar", "coffee"]:
            if low.endswith(" " + suf):
                outv.append(_clean_query(v[: -len(suf)].strip()))
        # Normalise ampersands for broader matches.
        outv.append(_clean_query(v.replace("&", "and")))
        # De-dupe
        seenv: set[str] = set()
        finalv: list[str] = []
        for x in outv:
            x = _clean_query(x)
            if not x or x in seenv:
                continue
            seenv.add(x)
            finalv.append(x)
        return finalv

    if venue and addr:
        for v in venue_variants(venue):
            out.append(_clean_query(f"{v}, {addr}"))
            out.append(_clean_query(f"{v}, {addr}, Zürich (CH)"))
    if venue:
        for v in venue_variants(venue):
            out.append(_clean_query(f"{v}, Zürich (CH)"))
            out.append(_clean_query(f"{v}, Zürich"))

    # Address-only fallback often works best once venue names get creative.
    if addr:
        out.append(_clean_query(addr))
        out.append(_clean_query(addr + ", Zürich"))
        out.append(_clean_query(addr + ", Zürich (CH)"))

    # Dedupe while preserving order.
    seen = set()
    final: list[str] = []
    for q in out:
        if not q:
            continue
        if q in seen:
            continue
        seen.add(q)
        final.append(q)
    return final


def _event_id(e: dict) -> str:
    url = str(e.get("url") or "").strip()
    title = str(e.get("title") or "").strip()
    return url or title or "(event)"


def main() -> int:
    ap = argparse.ArgumentParser(description="Update docs events lat/lon via Nominatim + local cache.")
    ap.add_argument("--path", type=Path, default=DEFAULT_EVENTS_JSON, help="Path to docs events JSON (default: docs/data/events.json)")
    ap.add_argument("--cache", type=Path, default=GEOCACHE_PATH, help="Geocache JSON (default: data/processed/location_geocache.json)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write files; just print what would change")
    ap.add_argument("--force", action="store_true", help="Re-geocode even if lat/lon already present")
    ap.add_argument("--match-url", action="append", default=[], help="Only update events whose url matches this value (repeatable)")
    ap.add_argument("--sleep-s", type=float, default=1.1, help="Sleep between new Nominatim requests (default: 1.1s)")
    args = ap.parse_args()

    payload = _load_json(args.path)
    events = payload.get("events") or []
    if not isinstance(events, list):
        raise SystemExit(f"Invalid JSON shape: expected 'events' to be a list in {args.path}")

    cache = _load_geocache(args.cache)
    changed = 0
    unchanged = 0
    failed = 0
    filtered = 0
    processed = 0
    queried = 0

    for e in events:
        if not isinstance(e, dict):
            continue

        url = str(e.get("url") or "").strip()
        if args.match_url and url not in set(args.match_url):
            filtered += 1
            continue

        have = (e.get("lat") is not None) and (e.get("lon") is not None)
        if have and not args.force:
            filtered += 1
            continue

        candidates = _candidate_queries(e)
        if not candidates:
            failed += 1
            continue
        processed += 1

        lat = None
        lon = None
        used_q = ""

        # Cache first (across all candidate strings).
        for q in candidates:
            hit = cache.get(q)
            if hit and isinstance(hit, dict) and ("lat" in hit) and ("lon" in hit):
                lat = float(hit["lat"])
                lon = float(hit["lon"])
                used_q = q
                break

        # If not in cache: try Nominatim with fallbacks.
        if lat is None or lon is None:
            for q in candidates:
                res = _nominatim_geocode(q)
                queried += 1
                if not res:
                    continue
                lat = float(res["lat"])
                lon = float(res["lon"])
                used_q = q
                cache[q] = {"lat": lat, "lon": lon, "display_name": res.get("display_name")}
                time.sleep(max(0.0, float(args.sleep_s)))
                break

        if lat is None or lon is None:
            print(
                f"[WARN] No geocode result for: {_event_id(e)} :: tried {len(candidates)} queries; first='{candidates[0]}'"
            )
            failed += 1
            continue

        before = (e.get("lat"), e.get("lon"))
        e["lat"] = lat
        e["lon"] = lon
        after = (e.get("lat"), e.get("lon"))
        if before != after:
            changed += 1
            if used_q:
                print(f"[OK] {_event_id(e)} -> ({float(lat):.6f}, {float(lon):.6f})  via '{used_q}'")
            else:
                print(f"[OK] {_event_id(e)} -> ({float(lat):.6f}, {float(lon):.6f})")
        else:
            unchanged += 1

    total = sum(1 for x in events if isinstance(x, dict))
    print(
        "Done."
        f" total={total}"
        f" filtered={filtered}"
        f" processed={processed}"
        f" changed={changed}"
        f" unchanged={unchanged}"
        f" failed={failed}"
        f" queried={queried}"
        f" file={args.path}"
    )
    if args.dry_run:
        return 0

    _save_json(args.path, payload)
    _save_geocache(args.cache, cache)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

