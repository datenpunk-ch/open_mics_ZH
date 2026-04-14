#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
import re
from dataclasses import dataclass
from html import escape
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "processed" / "events_flat.csv"
GEOCACHE_PATH = ROOT / "data" / "processed" / "location_geocache.json"


WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

_RE_OPEN_MIC = re.compile(r"\bopen[\s-]*mic\b", re.I)


def _is_confirmed_open_mic(*texts: str) -> bool:
    blob = " ".join(t for t in texts if t)
    return bool(_RE_OPEN_MIC.search(blob))


def _google_maps_url(location: str) -> str:
    q = urllib.parse.quote_plus(location)
    return f"https://www.google.com/maps/search/?api=1&query={q}"


def _static_map_url(lat: float, lon: float) -> str:
    qs = urllib.parse.urlencode(
        {
            "center": f"{lat},{lon}",
            "zoom": "15",
            "size": "340x200",
            "markers": f"{lat},{lon},lightblue1",
        }
    )
    return f"https://staticmap.openstreetmap.de/staticmap.php?{qs}"


def _looks_like_full_address(s: str) -> bool:
    t = (s or "").strip()
    return bool(re.search(r"\b\d{4}\b", t) or re.search(r"\b\d{1,4}\b", t))


_COUNTRY_TOKENS = {
    "schweiz",
    "suisse",
    "svizzera",
    "svizra",
    "switzerland",
}


def _clean_display_name(display_name: str) -> str:
    """
    Nominatim display_name is comma-separated and often ends with
    "Schweiz/Suisse/Svizzera/Svizra". We drop country and keep a compact
    Zürich-area address.
    """
    raw = " ".join((display_name or "").split()).strip()
    if not raw:
        return ""

    parts = [p.strip() for p in raw.split(",") if p.strip()]

    # Drop trailing country / multilingual country tail.
    while parts:
        last = parts[-1]
        # e.g. "Schweiz/Suisse/Svizzera/Svizra"
        slash_tokens = {t.strip().lower() for t in last.split("/") if t.strip()}
        if slash_tokens and slash_tokens <= _COUNTRY_TOKENS:
            parts.pop()
            continue
        if last.lower() in _COUNTRY_TOKENS:
            parts.pop()
            continue
        break

    # Prefer to end at "#### Zürich" when available.
    out: list[str] = []
    zip_city_idx = None
    for i, p in enumerate(parts):
        if re.search(r"\b8\d{3}\b", p) and ("zürich" in p.lower() or "zurich" in p.lower()):
            zip_city_idx = i
            break
    if zip_city_idx is not None:
        # keep a few leading segments + up to zip/city
        start = max(0, zip_city_idx - 3)
        out = parts[start : zip_city_idx + 1]
    else:
        out = parts[:5]

    return ", ".join(out)


@dataclass(frozen=True)
class GeoResult:
    lat: float
    lon: float
    display_name: str | None = None


def _load_events(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=";", dtype=str).fillna("")
    for col in ["Weekday", "Location", "Time", "Cost", "Comedy_language", "Regularity", "Event_title", "URL"]:
        if col not in df.columns:
            df[col] = ""
    if "Listing_title" not in df.columns:
        df["Listing_title"] = ""
    if "Description_preview" not in df.columns:
        df["Description_preview"] = ""
    if "Image_url" not in df.columns:
        df["Image_url"] = ""

    df["Weekday_norm"] = (
        df["Weekday"]
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    df["Location_norm"] = df["Location"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df["Comedy_language_norm"] = df["Comedy_language"].astype(str).str.strip()
    df["Regularity_norm"] = df["Regularity"].astype(str).str.strip()
    df["Time_norm"] = df["Time"].astype(str).str.strip()

    # Only recurring open mics
    df = df[df["Regularity_norm"].str.lower() == "recurring"].copy()

    # Only confirmed "open mic" mentions (title OR description)
    confirmed_mask = (
        df[["Event_title", "Listing_title", "Description_preview"]]
        .astype(str)
        .agg(" ".join, axis=1)
        .apply(lambda x: bool(_RE_OPEN_MIC.search(x)))
    )
    df = df[confirmed_mask].copy()
    return df


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


def _nominatim_geocode(query: str, *, timeout_s: int = 20) -> GeoResult | None:
    # Keep it simple and dependency-free. Nominatim requires a valid User-Agent.
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(
        {
            "q": query,
            "format": "jsonv2",
            "limit": 1,
            "addressdetails": 0,
        }
    )
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "open-mics-zurich/0.1 (local visualization; contact: datenpunk.ch@gmail.com)",
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
        return GeoResult(
            lat=float(item["lat"]),
            lon=float(item["lon"]),
            display_name=item.get("display_name"),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _weekday_matches(weekday_cell: str, selected: set[str]) -> bool:
    if not selected:
        return True
    parts = {p.strip() for p in str(weekday_cell).split(",") if p.strip()}
    return bool(parts & selected)


def main() -> None:
    st.set_page_config(page_title="Open Mics Zurich", layout="wide")
    st.title("Open Mics Zurich")

    with st.sidebar:
        st.header("Filters")
        csv_path_str = st.text_input("Data file", value=str(DEFAULT_CSV))
        csv_path = Path(csv_path_str)

        if not csv_path.is_file():
            st.error(f"CSV not found: {csv_path}")
            st.stop()

        df = _load_events(csv_path)

        weekday_sel = st.multiselect("Weekday", options=WEEKDAYS, default=WEEKDAYS)
        weekday_set = set(weekday_sel)

        languages = sorted({x for x in df["Comedy_language_norm"].unique().tolist() if x})
        language_sel = st.multiselect("Comedy language", options=languages, default=languages)
        language_set = set(language_sel)

        query = st.text_input("Search (title/location)")

        st.divider()
        st.header("Map")
        auto_geocode = st.toggle("Auto-geocode missing locations", value=True)
        geocode_missing = st.button("Geocode missing locations now (uses OpenStreetMap)")
        st.caption("Geocoding is cached in `data/processed/location_geocache.json`.")
        show_images = st.toggle("Show venue images in list", value=True)

    mask = df["Weekday_norm"].apply(lambda x: _weekday_matches(x, weekday_set))
    if language_set:
        mask &= df["Comedy_language_norm"].isin(language_set) | (df["Comedy_language_norm"] == "")
    if query.strip():
        q = query.strip().lower()
        mask &= (
            df["Event_title"].astype(str).str.lower().str.contains(q, na=False)
            | df["Location_norm"].astype(str).str.lower().str.contains(q, na=False)
        )

    filtered = df[mask].copy()

    cache = _load_geocache(GEOCACHE_PATH)
    unique_locations = sorted({x for x in filtered["Location_norm"].unique().tolist() if x})

    def _geocode_locations(missing: list[str]) -> None:
        if not missing:
            return
        prog = st.progress(0, text="Geocoding locations…")
        for i, loc in enumerate(missing, start=1):
            # Try a couple of reasonable fallbacks.
            candidates: list[str] = []
            base = loc
            candidates.append(base)
            if "," in base:
                # Drop venue name, keep address-like remainder
                candidates.append(base.split(",", 1)[1].strip())
            if "zürich" not in base.lower() and "zurich" not in base.lower():
                candidates.append(f"{base}, Zürich, Switzerland")
            # Deduplicate while preserving order
            seen: set[str] = set()
            candidates = [c for c in candidates if c and not (c in seen or seen.add(c))]

            res = None
            for cand in candidates:
                try:
                    res = _nominatim_geocode(cand)
                except Exception:
                    res = None
                if res:
                    break

            if res:
                cache[loc] = {"lat": res.lat, "lon": res.lon, "display_name": res.display_name}
                _save_geocache(GEOCACHE_PATH, cache)
            prog.progress(i / max(len(missing), 1), text=f"Geocoding {i}/{len(missing)}")
            time.sleep(1.0)  # be polite to the public endpoint
        prog.empty()

    if (geocode_missing or auto_geocode) and unique_locations:
        missing = [loc for loc in unique_locations if loc and loc not in cache]
        if missing:
            _geocode_locations(missing)
        else:
            if geocode_missing:
                st.info("No missing locations to geocode for the current filter.")

    def _lat(loc: str) -> float | None:
        v = cache.get(loc, {}).get("lat")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _lon(loc: str) -> float | None:
        v = cache.get(loc, {}).get("lon")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    filtered["lat"] = filtered["Location_norm"].apply(_lat)
    filtered["lon"] = filtered["Location_norm"].apply(_lon)
    mdf = filtered.dropna(subset=["lat", "lon"]).copy()

    left, right = st.columns([0.55, 0.45], gap="large")

    with left:
        st.subheader("Map")
        if len(mdf) == 0:
            st.info("No map pins yet. Click “Geocode missing locations” in the sidebar.")
        else:
            center_lat = float(mdf["lat"].mean())
            center_lon = float(mdf["lon"].mean())

            m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="OpenStreetMap")
            # Auto-close any opened popup after 5 seconds.
            m.get_root().html.add_child(
                folium.Element(
                    """
<script>
  (function () {
    function bindAutoClose(map) {
      map.on('popupopen', function () {
        window.setTimeout(function () {
          try { map.closePopup(); } catch (e) {}
        }, 5000);
      });
    }
    // Folium exposes the Leaflet map as a global variable with the same name as the div id.
    const mapId = document.currentScript?.closest('html') ? null : null;
  })();
</script>
"""
                )
            )

            for _, r in mdf.iterrows():
                title = (r.get("Event_title") or "").strip() or "(untitled)"
                wd = (r.get("Weekday_norm") or "").strip()
                tm = (r.get("Time_norm") or "").strip()
                loc = (r.get("Location_norm") or "").strip()
                url = (r.get("URL") or "").strip()
                gmaps = _google_maps_url(loc) if loc else ""

                tooltip_html = (
                    "<div style="
                    "'width: 240px; white-space: normal; line-height: 1.25; padding: 2px 0;'"
                    ">"
                    f"<div style='font-weight: 650; margin-bottom: 4px;'>{escape(title)}</div>"
                    f"<div style='opacity: 0.9; margin-bottom: 3px;'>{escape((wd + ' ' + tm).strip())}</div>"
                    f"<div style='opacity: 0.9;'>{escape(loc)}</div>"
                    "</div>"
                )

                popup_html = f"<b>{escape(title)}</b><br/>{escape((wd + ' ' + tm).strip())}<br/>{escape(loc)}"
                if url:
                    popup_html += (
                        f'<br/><a href="{escape(url)}" target="_blank" rel="noreferrer">Open link</a>'
                    )
                if gmaps:
                    popup_html += (
                        f'<br/><a href="{escape(gmaps)}" target="_blank" rel="noreferrer">Open in Google Maps</a>'
                    )

                folium.CircleMarker(
                    location=[float(r["lat"]), float(r["lon"])],
                    radius=6,
                    color="#4da3ff",
                    weight=2,
                    fill=True,
                    fill_color="#4da3ff",
                    fill_opacity=0.8,
                    tooltip=folium.Tooltip(tooltip_html, sticky=False),
                    popup=folium.Popup(popup_html, max_width=360),
                ).add_to(m)

            st_folium(m, width="stretch", height=560)
            st.caption(
                f"Hover a point to see event info. Showing {len(mdf)} pinned events (of {len(filtered)} filtered)."
            )

    with right:
        st.subheader(f"Events ({len(filtered)})")
        if len(filtered) == 0:
            st.warning("No events match the current filters.")
        else:
            for _, row in filtered.sort_values(["Weekday_norm", "Time_norm", "Location_norm"]).iterrows():
                title = (row.get("Event_title") or "").strip() or "(untitled)"
                url = (row.get("URL") or "").strip()
                loc = (row.get("Location_norm") or "").strip()
                loc_display = loc
                if loc and not _looks_like_full_address(loc):
                    dn = (cache.get(loc, {}) or {}).get("display_name") or ""
                    loc_display = _clean_display_name(dn) or loc
                wd = (row.get("Weekday_norm") or "").strip()
                tm = (row.get("Time_norm") or "").strip()
                cost = (row.get("Cost") or "").strip()
                lang = (row.get("Comedy_language_norm") or "").strip()
                reg = (row.get("Regularity_norm") or "").strip()
                gmaps = _google_maps_url(loc) if loc else ""
                img_url = (row.get("Image_url") or "").strip()

                top = f"**{title}**"
                if url:
                    top = f"**[{title}]({url})**"

                meta = " · ".join([x for x in [wd, tm, loc_display] if x])
                details = " | ".join([x for x in [lang, reg, cost] if x])
                show_img = bool(show_images and img_url)
                if show_img:
                    cimg, ctext = st.columns([0.38, 0.62], gap="medium")
                    with cimg:
                        try:
                            st.image(img_url)
                        except Exception:
                            pass
                    with ctext:
                        st.markdown(top)
                        if meta:
                            st.caption(meta)
                        if details:
                            st.caption(details)
                        if gmaps:
                            st.markdown(f"[Open in Google Maps]({gmaps})")
                else:
                    st.markdown(top)
                    if meta:
                        st.caption(meta)
                    if details:
                        st.caption(details)
                    if gmaps:
                        st.markdown(f"[Open in Google Maps]({gmaps})")
                st.divider()


if __name__ == "__main__":
    main()

