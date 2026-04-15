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
RULES_PATH = ROOT / "config" / "rules.json"
DEFAULT_CSV = ROOT / "data" / "processed" / "events_flat.csv"
GEOCACHE_PATH = ROOT / "data" / "processed" / "location_geocache.json"

_DEFAULT_RULES = {
    "geocoding": {
        "countrycodes": "ch",
        "prefer_city_tokens": ["zürich", "zurich"],
        "reverse_zoom": 18,
        "forward_limit": 5,
    }
}


def _load_rules() -> dict:
    try:
        return json.loads(RULES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _DEFAULT_RULES


_RULES = _load_rules()
_GEOCODE_COUNTRYCODES = str(_RULES.get("geocoding", {}).get("countrycodes") or _DEFAULT_RULES["geocoding"]["countrycodes"])
_GEOCODE_FORWARD_LIMIT = int(_RULES.get("geocoding", {}).get("forward_limit") or _DEFAULT_RULES["geocoding"]["forward_limit"])
_GEOCODE_REVERSE_ZOOM = int(_RULES.get("geocoding", {}).get("reverse_zoom") or _DEFAULT_RULES["geocoding"]["reverse_zoom"])
_PREFER_CITY_TOKENS = _RULES.get("geocoding", {}).get("prefer_city_tokens")
if not isinstance(_PREFER_CITY_TOKENS, list) or not _PREFER_CITY_TOKENS:
    _PREFER_CITY_TOKENS = _DEFAULT_RULES["geocoding"]["prefer_city_tokens"]
_ZURICH_VIEWBOX = _RULES.get("geocoding", {}).get("zurich_viewbox") or {}
_ZURICH_CENTER = _RULES.get("geocoding", {}).get("zurich_center") or {}


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


def _expected_zip(loc: str) -> str:
    m = re.search(r"\b(\d{4})\b", str(loc or ""))
    return m.group(1) if m else ""


def _expects_zurich(loc: str) -> bool:
    low = str(loc or "").lower()
    return any(tok in low for tok in _PREFER_CITY_TOKENS)


def _display_name_ok(display_name: str | None, *, expected_zip: str, expects_zurich: bool) -> bool:
    dn = str(display_name or "")
    if expected_zip and expected_zip not in dn:
        return False
    if expects_zurich and not re.search(r"\bzürich\b|\bzurich\b", dn, flags=re.I):
        return False
    return True


def _pick_best_result(items: list[dict], *, expected_zip: str, expects_zurich: bool) -> dict | None:
    """
    Prefer results that match the expected zip/city when the query contains them.
    Fall back to the first result otherwise.
    """
    if not items:
        return None
    good = [
        it
        for it in items
        if _display_name_ok(
            it.get("display_name"), expected_zip=expected_zip, expects_zurich=expects_zurich
        )
    ]
    # If the caller provided an explicit postcode expectation and none match, treat as no result
    # rather than returning a likely-wrong fallback elsewhere.
    if expected_zip and not good:
        return None

    # If we didn't specify a zip but we do expect Zürich, prefer results that look like Zürich city
    # (postcodes starting with 80**) and are closer to the Zürich centre.
    if expects_zurich and not expected_zip and good:
        try:
            zlat = float(_ZURICH_CENTER.get("lat"))
            zlon = float(_ZURICH_CENTER.get("lon"))
        except Exception:
            zlat = 47.3769
            zlon = 8.5417

        def zip_score(dn: str) -> int:
            m = re.search(r"\b(\d{4})\b", dn)
            if not m:
                return 0
            return 2 if m.group(1).startswith("80") else 1 if m.group(1).startswith("8") else 0

        def dist2(it: dict) -> float:
            try:
                lat = float(it.get("lat"))
                lon = float(it.get("lon"))
                return (lat - zlat) ** 2 + (lon - zlon) ** 2
            except Exception:
                return 1e9

        def score(it: dict) -> tuple[int, float]:
            dn = str(it.get("display_name") or "")
            return (zip_score(dn), -dist2(it))

        return max(good, key=score)

    return good[0] if good else items[0]


def _nominatim_geocode(query: str, *, timeout_s: int = 20) -> dict | None:
    params: dict[str, str | int] = {
        "q": query,
        "format": "jsonv2",
            "limit": max(_GEOCODE_FORWARD_LIMIT, 10),
        "addressdetails": 0,
        "countrycodes": _GEOCODE_COUNTRYCODES,
    }
    # If the query expects Zürich, constrain results to a Zürich-ish bounding box.
    # This avoids "good sounding" matches elsewhere in the canton.
    if _expects_zurich(query):
        try:
            left = float(_ZURICH_VIEWBOX.get("left"))
            top = float(_ZURICH_VIEWBOX.get("top"))
            right = float(_ZURICH_VIEWBOX.get("right"))
            bottom = float(_ZURICH_VIEWBOX.get("bottom"))
            params["viewbox"] = f"{left},{top},{right},{bottom}"
            params["bounded"] = 1
        except Exception:
            pass
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
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
    item = _pick_best_result(
        data,
        expected_zip=_expected_zip(query),
        expects_zurich=_expects_zurich(query),
    )
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


def _nominatim_reverse(lat: float, lon: float, *, timeout_s: int = 20) -> dict | None:
    url = "https://nominatim.openstreetmap.org/reverse?" + urllib.parse.urlencode(
        {
            "lat": f"{lat:.7f}",
            "lon": f"{lon:.7f}",
            "format": "jsonv2",
            "zoom": _GEOCODE_REVERSE_ZOOM,
            "addressdetails": 0,
        }
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
    if not isinstance(data, dict):
        return None
    dn = data.get("display_name")
    if not isinstance(dn, str) or not dn.strip():
        return None
    return {"lat": float(lat), "lon": float(lon), "display_name": dn}


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
    # Also refresh suspicious cached entries where the cached display_name clearly mismatches
    # a zip/city present in the original location string (dynamic, not hardcoded).
    refresh: list[str] = []
    for loc in locations:
        entry = cache.get(loc)
        if not isinstance(entry, dict):
            continue
        dn = entry.get("display_name")
        ez = _expected_zip(loc)
        if ez and isinstance(dn, str) and ez not in dn:
            refresh.append(loc)
            continue
        if _expects_zurich(loc) and isinstance(dn, str) and not re.search(r"\bzürich\b|\bzurich\b", dn, flags=re.I):
            refresh.append(loc)
            continue

    todo = [*missing, *[x for x in refresh if x not in missing]]
    if not todo:
        print(f"[geocode] Nothing to do. Cache already has {len(cache)} locations.")
        return 0

    if refresh:
        print(f"[geocode] Refreshing {len(refresh)} suspicious cached location(s)…")
    print(f"[geocode] Geocoding {len(todo)} location(s)…")
    refresh_set = set(refresh)
    for i, loc in enumerate(todo, start=1):
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
            # If we already have coordinates in the cache, prefer reverse-geocoding to get the
            # canonical display name for that exact pin location.
            #
            # BUT: when we explicitly refresh a suspicious entry, we must *not* trust the cached
            # lat/lon (they may be wrong) — use forward geocoding instead.
            if loc not in refresh_set:
                entry_existing = cache.get(loc)
                if isinstance(entry_existing, dict):
                    try:
                        lat0 = float(entry_existing.get("lat")) if entry_existing.get("lat") is not None else None
                        lon0 = float(entry_existing.get("lon")) if entry_existing.get("lon") is not None else None
                    except (TypeError, ValueError):
                        lat0 = lon0 = None
                    if isinstance(lat0, float) and isinstance(lon0, float):
                        res = _nominatim_reverse(lat0, lon0)

            if res is None:
                for cand in candidates:
                    res = _nominatim_geocode(cand)
                    if res:
                        break
        except Exception as e:
            print(f"[geocode] {i}/{len(todo)} FAILED: {loc} ({e})")
            res = None

        if res:
            cache[loc] = res
            _save_geocache(GEOCACHE_PATH, cache)
            print(f"[geocode] {i}/{len(todo)} OK: {loc}")
        else:
            if loc in refresh_set and loc in cache:
                cache.pop(loc, None)
                _save_geocache(GEOCACHE_PATH, cache)
                print(f"[geocode] {i}/{len(todo)} DROP: {loc} (no valid match)")
                time.sleep(1.0)
                continue
            print(f"[geocode] {i}/{len(todo)} NORESULT: {loc}")

        time.sleep(1.0)  # be polite to the public endpoint

    print(f"[geocode] Done. Cache locations: {len(cache)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

