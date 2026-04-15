#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
import re
import base64
from dataclasses import dataclass
from html import escape
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import streamlit.components.v1 as components


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "processed" / "events_flat.csv"
GEOCACHE_PATH = ROOT / "data" / "processed" / "location_geocache.json"
PLACEHOLDER_SVG = ROOT / "assets" / "open_mic_placeholder.svg"


WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_WEEKDAY_INDEX = {d: i for i, d in enumerate(WEEKDAYS)}

_RE_OPEN_MIC = re.compile(r"\bopen[\s-]*mic\b", re.I)
_RE_MUSIC_JAM = re.compile(
    r"(?:\bjam\s*session\b|\bjam\b).*?(?:\bmusik\b|\bmusic\b|\bband\b|\bkonzert\b|\bconcert\b|\bmusizieren\b|\bhouse-?band\b)"
    r"|(?:\bmusik\b|\bmusic\b|\bband\b|\bkonzert\b|\bconcert\b|\bmusizieren\b|\bhouse-?band\b).*?(?:\bjam\s*session\b|\bjam\b)",
    re.I,
)


def _is_confirmed_open_mic(*texts: str) -> bool:
    blob = " ".join(t for t in texts if t)
    if not _RE_OPEN_MIC.search(blob):
        return False
    if _RE_MUSIC_JAM.search(blob):
        return False
    return True


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


def _format_address_from_display_name(*, venue_hint: str, display_name: str) -> str:
    dn = _clean_display_name(display_name)
    parts = [p.strip() for p in dn.split(",") if p.strip()]

    venue = (venue_hint or "").strip()
    if venue and re.match(r"^\d", venue):
        venue = ""

    street = ""
    number = ""
    for i in range(len(parts) - 1):
        a = parts[i]
        b = parts[i + 1]
        if re.fullmatch(r"\d+[a-z]?", a, flags=re.I) and re.search(r"[a-zA-ZÄÖÜäöü]", b):
            number, street = a, b
            break
        if re.search(r"[a-zA-ZÄÖÜäöü]", a) and re.fullmatch(r"\d+[a-z]?", b, flags=re.I):
            street, number = a, b
            break
    street_line = " ".join(x for x in [street, number] if x).strip()

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
    if not city:
        for p in reversed(parts):
            if re.search(r"\b\d{4}\b", p):
                continue
            if len(p) >= 3:
                city = p
                break
    tail = " ".join(x for x in [zip_code, city] if x).strip()

    out = ", ".join(x for x in [venue, street_line, tail] if x)
    return out.strip()


def _open_mic_placeholder_svg_bytes() -> bytes:
    try:
        return PLACEHOLDER_SVG.read_bytes()
    except OSError:
        return b""


def _render_svg_image(svg_bytes: bytes) -> None:
    if not svg_bytes:
        return
    b64 = base64.b64encode(svg_bytes).decode("ascii")
    st.markdown(
        f"<img src='data:image/svg+xml;base64,{b64}' style='width: 100%; border-radius: 10px;'/>",
        unsafe_allow_html=True,
    )


def _weekday_sort_index(weekday_cell: str) -> int:
    parts = [p.strip() for p in str(weekday_cell or "").split(",") if p.strip()]
    idxs = [_WEEKDAY_INDEX.get(p, 999) for p in parts] or [999]
    return min(idxs)


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

    # Explode multi-weekday rows into one row per weekday for clearer browsing.
    df["Weekday_norm"] = df["Weekday"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df["_weekday_list"] = df["Weekday_norm"].apply(lambda s: [p.strip() for p in str(s).split(",") if p.strip()] or [""])
    df = df.explode("_weekday_list").copy()
    df["Weekday_norm"] = df["_weekday_list"].astype(str)
    df.drop(columns=["_weekday_list"], inplace=True)
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
        .apply(lambda x: _is_confirmed_open_mic(x))
    )
    df = df[confirmed_mask].copy()
    df["Weekday_sort"] = df["Weekday_norm"].apply(_weekday_sort_index)
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

    # Minor widget chrome tweaks to better match STYLE_CI_GUIDE.md.
    st.markdown(
        """
<style>
  :root{
    --ci-rule:#d9dde1;
    --ci-ink:#0f0f0f;
    --ci-muted:#5f5f5f;
    --ci-accent:#3a677a;
    --ci-bg:#ffffff;
    --ci-bg-soft:#f7f8f9;
  }
  a, a:visited{
    color: var(--ci-accent) !important;
  }
  a:hover{
    color: #2f5a6b !important;
  }
  /* Inputs */
  div[data-baseweb="select"] > div,
  div[data-baseweb="input"] > div{
    border-radius: 0 !important;
    border-color: var(--ci-rule) !important;
    background: var(--ci-bg-soft) !important;
  }
  /* Slider / toggle accents */
  div[role="slider"]{
    color: var(--ci-accent) !important;
  }
  /* Checkboxes / toggles / radios focus */
  *:focus-visible{
    outline: 2px solid var(--ci-accent) !important;
    outline-offset: 2px !important;
  }
</style>
""",
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Filters")
        # Always use the default processed CSV (no visible file/path control).
        csv_path = Path(DEFAULT_CSV)

        if not csv_path.is_file():
            st.error("Event data CSV not found. Run the rebuild pipeline first.")
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
        # Always on (no visible switches).
        auto_geocode = True
        geocode_missing = False
        show_images = True

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
    mdf["_event_key"] = (
        mdf["Event_title"].astype(str)
        + "|"
        + mdf["Weekday_norm"].astype(str)
        + "|"
        + mdf["Time_norm"].astype(str)
        + "|"
        + mdf["Location_norm"].astype(str)
    )

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
                gmaps = _google_maps_url(loc) if loc else ""
                ek = str(r.get("_event_key") or "")

                tooltip_html = (
                    "<div style="
                    "'width: 240px; white-space: normal; line-height: 1.25; padding: 2px 0;'"
                    ">"
                    f"<div style='font-weight: 650; margin-bottom: 4px;'>{escape(title)}</div>"
                    f"<div style='opacity: 0.9; margin-bottom: 3px;'>{escape((wd + ' ' + tm).strip())}</div>"
                    f"<div style='opacity: 0.9;'>{escape(loc)}</div>"
                    f"<span data-ek='{escape(ek)}' style='display:none'></span>"
                    "</div>"
                )

                folium.CircleMarker(
                    location=[float(r["lat"]), float(r["lon"])],
                    radius=6,
                    color="#3a677a",
                    weight=2,
                    fill=True,
                    fill_color="#3a677a",
                    fill_opacity=0.45,
                    tooltip=folium.Tooltip(tooltip_html, sticky=False),
                ).add_to(m)

            map_state = st_folium(m, width="stretch", height=560)
            clicked = (map_state or {}).get("last_object_clicked") or {}
            clicked_tip = (map_state or {}).get("last_object_clicked_tooltip") or ""

            # Prefer exact event key from the clicked marker tooltip.
            if isinstance(clicked_tip, str) and "data-ek=" in clicked_tip:
                m_ek = re.search(r"data-ek=['\\\"]([^'\\\"]+)['\\\"]", clicked_tip)
                if m_ek:
                    sel = m_ek.group(1)
                    if sel and st.session_state.get("selected_event_key") != sel:
                        st.session_state["selected_event_key"] = sel

            elif isinstance(clicked, dict) and "lat" in clicked and "lng" in clicked and len(mdf) > 0:
                try:
                    clat = float(clicked["lat"])
                    clon = float(clicked["lng"])
                    last = st.session_state.get("last_clicked_latlon")
                    cur = (round(clat, 7), round(clon, 7))
                    if last != cur:
                        st.session_state["last_clicked_latlon"] = cur
                    tmp = mdf.copy()
                    tmp["_d2"] = (tmp["lat"].astype(float) - clat) ** 2 + (tmp["lon"].astype(float) - clon) ** 2
                    nearest = tmp.sort_values("_d2").iloc[0]
                    sel = str(nearest["_event_key"])
                    if st.session_state.get("selected_event_key") != sel:
                        st.session_state["selected_event_key"] = sel
                except Exception:
                    pass
            st.caption(
                f"Hover a point to see event info. Showing {len(mdf)} pinned events (of {len(filtered)} filtered)."
            )

    with right:
        st.subheader(f"Events ({len(filtered)})")
        if len(filtered) == 0:
            st.warning("No events match the current filters.")
        else:
            selected_key = st.session_state.get("selected_event_key", "")
            scroll_to_id: str | None = None

            # Keep the event list to the same visual height as the map.
            with st.container(height=560):
                # If a map point was clicked, pin that event to the top.
                render_df = filtered.copy()
                render_df["_event_key"] = (
                    render_df["Event_title"].astype(str)
                    + "|"
                    + render_df["Weekday_norm"].astype(str)
                    + "|"
                    + render_df["Time_norm"].astype(str)
                    + "|"
                    + render_df["Location_norm"].astype(str)
                )
                if selected_key:
                    render_df["_sel_rank"] = (render_df["_event_key"] != selected_key).astype(int)
                else:
                    render_df["_sel_rank"] = 1

                render_df = render_df.sort_values(
                    ["_sel_rank", "Weekday_sort", "Time_norm", "Location_norm"],
                    kind="mergesort",
                )

                for idx, (_, row) in enumerate(render_df.iterrows()):
                    title = (row.get("Event_title") or "").strip() or "(untitled)"
                    url = (row.get("URL") or "").strip()
                    loc = (row.get("Location_norm") or "").strip()
                    loc_display = loc
                    if loc and not _looks_like_full_address(loc):
                        dn = (cache.get(loc, {}) or {}).get("display_name") or ""
                        venue_hint = loc.split(",", 1)[0].strip() if "," in loc else loc
                        loc_display = (
                            _format_address_from_display_name(venue_hint=venue_hint, display_name=dn) or loc
                        )
                    wd = (row.get("Weekday_norm") or "").strip()
                    tm = (row.get("Time_norm") or "").strip()
                    cost = (row.get("Cost") or "").strip()
                    lang = (row.get("Comedy_language_norm") or "").strip()
                    reg = (row.get("Regularity_norm") or "").strip()
                    gmaps = _google_maps_url(loc) if loc else ""
                    img_url = (row.get("Image_url") or "").strip()

                    event_key = str(row.get("_event_key") or "")
                    is_selected = bool(selected_key and event_key == selected_key)
                    anchor_id = f"event-{idx}"
                    if is_selected:
                        scroll_to_id = anchor_id

                    top = f"**{title}**"
                    if url:
                        top = f"**[{title}]({url})**"

                    meta_time = " · ".join([x for x in [wd, tm] if x])
                    details = " | ".join([x for x in [lang, reg, cost] if x])
                    show_img = bool(show_images)

                    st.markdown(f"<div id='{anchor_id}'></div>", unsafe_allow_html=True)
                    if is_selected:
                        st.markdown(
                            "<div style='border-left: 3px solid #3a677a; padding-left: 10px;'>",
                            unsafe_allow_html=True,
                        )
                    if show_img:
                        cimg, ctext = st.columns([0.38, 0.62], gap="medium")
                        with cimg:
                            try:
                                if img_url:
                                    st.image(img_url)
                                else:
                                    _render_svg_image(_open_mic_placeholder_svg_bytes())
                            except Exception:
                                pass
                        with ctext:
                            st.markdown(top)
                            if meta_time:
                                st.markdown(f"**{meta_time}**")
                            venue = (loc.split(",", 1)[0].strip() if loc else "")
                            if venue and gmaps:
                                st.markdown(f"**[{venue}]({gmaps})**")
                            elif venue:
                                st.markdown(f"**{venue}**")
                            if details:
                                st.caption(details)
                    else:
                        st.markdown(top)
                        if meta_time:
                            st.markdown(f"**{meta_time}**")
                        venue = (loc.split(",", 1)[0].strip() if loc else "")
                        if venue and gmaps:
                            st.markdown(f"**[{venue}]({gmaps})**")
                        elif venue:
                            st.markdown(f"**{venue}**")
                        if details:
                            st.caption(details)
                    if is_selected:
                        st.markdown("</div>", unsafe_allow_html=True)
                    st.divider()

            # Event is pinned to top when selected, no scrolling required.


if __name__ == "__main__":
    main()

