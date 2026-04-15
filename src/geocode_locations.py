#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "processed" / "events_flat.csv"
GEOCACHE_PATH = ROOT / "data" / "processed" / "location_geocache.json"


def _load_geocache(path: Path) -> dict[str, dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def _save_geocache(path: Path, cache: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _nominatim_geocode(query: str, *, timeout_s: int = 20) -> dict | None:
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(
        {"q": query, "format": "jsonv2", "limit": 1, "addressdetails": 0}
    )
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "open-mics-zurich/0.1 (build-time geocoding; contact: datenpunk.ch@gmail.com)",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    if not data:
        return None
    item = data[0]
    try:
        return {
            "lat": float(item["lat"]),
            "lon": float(item["lon"]),
            "display_name": item.get("display_name"),
        }
    except (KeyError, TypeError, ValueError):
        return None


def main() -> int:
    if not DEFAULT_CSV.is_file():
        print(f"[geocode] Missing CSV: {DEFAULT_CSV}")
        return 2

    df = pd.read_csv(DEFAULT_CSV, sep=";", dtype=str).fillna("")
    if "Location" not in df.columns:
        print("[geocode] CSV missing Location column.")
        return 2

    locations = sorted({str(x).strip() for x in df["Location"].tolist() if str(x).strip()})
    cache = _load_geocache(GEOCACHE_PATH)

    missing = [loc for loc in locations if loc not in cache]
    if not missing:
        print(f"[geocode] Nothing to do. Cache already has {len(cache)} locations.")
        return 0

    print(f"[geocode] Geocoding {len(missing)} new locations…")
    for i, loc in enumerate(missing, start=1):
        candidates: list[str] = []
        candidates.append(loc)
        candidates.append(re.sub(r"\bSaal\s*\d+\b", "", loc, flags=re.I).strip())
        if "," in loc:
            candidates.append(loc.split(",", 1)[1].strip())
        if "zürich" not in loc.lower() and "zurich" not in loc.lower():
            candidates.append(f"{loc}, Zürich, Switzerland")
        # Deduplicate while preserving order
        seen: set[str] = set()
        candidates = [c for c in candidates if c and not (c in seen or seen.add(c))]
        try:
            res = None
            for cand in candidates:
                res = _nominatim_geocode(cand)
                if res:
                    break
        except Exception as e:
            print(f"[geocode] {i}/{len(missing)} FAILED: {loc} ({e})")
            res = None

        if res:
            cache[loc] = res
            _save_geocache(GEOCACHE_PATH, cache)
            print(f"[geocode] {i}/{len(missing)} OK: {loc}")
        else:
            print(f"[geocode] {i}/{len(missing)} NORESULT: {loc}")

        time.sleep(1.0)  # be polite to the public endpoint

    print(f"[geocode] Done. Cache locations: {len(cache)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

