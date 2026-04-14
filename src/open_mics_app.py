#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
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

        regularities = sorted({x for x in df["Regularity_norm"].unique().tolist() if x})
        regularity_sel = st.multiselect("Regularity", options=regularities, default=regularities)
        regularity_set = set(regularity_sel)

        query = st.text_input("Search (title/location)")

        st.divider()
        st.header("Map")
        geocode_missing = st.button("Geocode missing locations (uses OpenStreetMap)")
        st.caption("Geocoding is cached in `data/processed/location_geocache.json`.")

    mask = df["Weekday_norm"].apply(lambda x: _weekday_matches(x, weekday_set))
    if language_set:
        mask &= df["Comedy_language_norm"].isin(language_set) | (df["Comedy_language_norm"] == "")
    if regularity_set:
        mask &= df["Regularity_norm"].isin(regularity_set) | (df["Regularity_norm"] == "")
    if query.strip():
        q = query.strip().lower()
        mask &= (
            df["Event_title"].astype(str).str.lower().str.contains(q, na=False)
            | df["Location_norm"].astype(str).str.lower().str.contains(q, na=False)
        )

    filtered = df[mask].copy()

    cache = _load_geocache(GEOCACHE_PATH)
    unique_locations = sorted({x for x in filtered["Location_norm"].unique().tolist() if x})

    if geocode_missing and unique_locations:
        missing = [loc for loc in unique_locations if loc and loc not in cache]
        if missing:
            prog = st.progress(0, text="Geocoding locations…")
            for i, loc in enumerate(missing, start=1):
                # Bias towards Zurich/CH if the string is short.
                query_loc = loc
                if "zürich" not in loc.lower() and "zurich" not in loc.lower():
                    query_loc = f"{loc}, Zürich, Switzerland"
                res = None
                try:
                    res = _nominatim_geocode(query_loc)
                except Exception:
                    res = None
                if res:
                    cache[loc] = {"lat": res.lat, "lon": res.lon, "display_name": res.display_name}
                    _save_geocache(GEOCACHE_PATH, cache)
                prog.progress(i / max(len(missing), 1), text=f"Geocoding {i}/{len(missing)}")
                time.sleep(1.0)  # be polite to the public endpoint
            prog.empty()
        else:
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

            for _, r in mdf.iterrows():
                title = (r.get("Event_title") or "").strip() or "(untitled)"
                wd = (r.get("Weekday_norm") or "").strip()
                tm = (r.get("Time_norm") or "").strip()
                loc = (r.get("Location_norm") or "").strip()
                url = (r.get("URL") or "").strip()

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

                folium.CircleMarker(
                    location=[float(r["lat"]), float(r["lon"])],
                    radius=6,
                    color="#4da3ff",
                    weight=2,
                    fill=True,
                    fill_color="#4da3ff",
                    fill_opacity=0.8,
                    tooltip=folium.Tooltip(tooltip_html, sticky=True),
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
                wd = (row.get("Weekday_norm") or "").strip()
                tm = (row.get("Time_norm") or "").strip()
                cost = (row.get("Cost") or "").strip()
                lang = (row.get("Comedy_language_norm") or "").strip()
                reg = (row.get("Regularity_norm") or "").strip()

                top = f"**{title}**"
                if url:
                    top = f"**[{title}]({url})**"

                meta = " · ".join([x for x in [wd, tm, loc] if x])
                details = " | ".join([x for x in [lang, reg, cost] if x])
                st.markdown(top)
                if meta:
                    st.caption(meta)
                if details:
                    st.caption(details)
                st.divider()


if __name__ == "__main__":
    main()

