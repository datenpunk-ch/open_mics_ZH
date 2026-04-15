#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
RULES_PATH = ROOT / "config" / "rules.json"
CSV_PATH = ROOT / "data" / "processed" / "events_flat.csv"
GEOCACHE_PATH = ROOT / "data" / "processed" / "location_geocache.json"

DOCS_DIR = ROOT / "docs"
DOCS_DATA_DIR = DOCS_DIR / "data"
DOCS_EVENTS_JSON = DOCS_DATA_DIR / "events.json"


WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_DEFAULT_RULES = {
    "content_filters": {
        "open_mic_regex": r"\bopen[\s-]*mic\b",
        "exclude_open_mic_when": {
            "all_of_any_order": [
                {
                    "a": ["jam session", "jam"],
                    "b": [
                        "musik",
                        "music",
                        "band",
                        "konzert",
                        "concert",
                        "musizieren",
                        "house-band",
                        "house band",
                    ],
                }
            ]
        },
    },
    "address_formatting": {
        "street_suffix_tokens": [
            "strasse",
            "straße",
            "gasse",
            "platz",
            "weg",
            "allee",
            "quai",
            "promenade",
            "ring",
            "ufer",
            "hof",
            "berg",
            "steig",
            "bühl",
            "rain",
            "brücke",
            "bruecke",
        ]
    },
}


def _load_rules() -> dict:
    try:
        return json.loads(RULES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _DEFAULT_RULES


_RULES = _load_rules()
_OPEN_MIC_REGEX = str(_RULES.get("content_filters", {}).get("open_mic_regex") or _DEFAULT_RULES["content_filters"]["open_mic_regex"])
_RE_OPEN_MIC = re.compile(_OPEN_MIC_REGEX, re.I)


def _build_exclude_music_jam_regex(rules: dict) -> re.Pattern:
    blocks = rules.get("content_filters", {}).get("exclude_open_mic_when", {}).get("all_of_any_order", [])
    if not isinstance(blocks, list) or not blocks:
        return re.compile(r"a^")  # never matches
    disj: list[str] = []
    for b in blocks:
        if not isinstance(b, dict):
            continue
        a = b.get("a") or []
        c = b.get("b") or []
        if not isinstance(a, list) or not isinstance(c, list) or not a or not c:
            continue
        a_alt = "|".join(re.escape(str(x)) for x in a if str(x))
        c_alt = "|".join(re.escape(str(x)) for x in c if str(x))
        if not a_alt or not c_alt:
            continue
        # a then b OR b then a (any order).
        disj.append(rf"(?:\b(?:{a_alt})\b).*?(?:\b(?:{c_alt})\b)")
        disj.append(rf"(?:\b(?:{c_alt})\b).*?(?:\b(?:{a_alt})\b)")
    if not disj:
        return re.compile(r"a^")
    return re.compile("|".join(disj), re.I)


_RE_MUSIC_JAM = _build_exclude_music_jam_regex(_RULES)
_COUNTRY_TOKENS = {"schweiz", "suisse", "svizzera", "svizra", "switzerland"}


def _load_geocache(path: Path) -> dict[str, dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def _norm(s: str) -> str:
    return " ".join(str(s or "").split()).strip()


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


def _format_address_from_display_name(*, venue_hint: str, display_name: str) -> tuple[str, str, str]:
    """
    Return (venue, address, location_display).
    - venue: cleaned venue name
    - address: "Street No, ZIP City" (may be empty)
    - location_display: "Venue, Street No, ZIP City" (or best-effort fallback)
    """
    dn = _clean_display_name(display_name)
    parts = [p.strip() for p in dn.split(",") if p.strip()]

    venue = (venue_hint or "").strip()
    if venue:
        if re.match(r"^\d", venue):
            venue = ""
        if len(venue) < 2:
            venue = ""
    if venue:
        # Strip embedded address fragments from venue hint to avoid duplication.
        v = re.sub(r"\s+", " ", venue).strip()
        v = re.sub(r"\b\d{4}\s+(?:zürich|zurich)\b", "", v, flags=re.I).strip()
        v = re.sub(
            r"\b[\wÀ-ÿ.\-']+(?:strasse|straße|gasse|platz|weg|allee|quai|promenade|ring|ufer|hof|berg|steig|bühl|rain|brücke|bruecke)\s*\d+[a-z]?\b",
            "",
            v,
            flags=re.I,
        ).strip()
        v = re.sub(r"\s{2,}", " ", v).strip(" ,-/–—")
        if v and len(v) >= 2:
            venue = v

    venue_fold = (venue or "").strip().casefold()

    def _looks_like_street_name(s: str) -> bool:
        t = (s or "").strip().casefold()
        if not t:
            return False
        toks = _RULES.get("address_formatting", {}).get("street_suffix_tokens")
        if not isinstance(toks, list) or not toks:
            toks = _DEFAULT_RULES["address_formatting"]["street_suffix_tokens"]
        alt = "|".join(re.escape(str(x)) for x in toks if str(x))
        if not alt:
            return False
        return bool(re.search(rf"(?:{alt})\b", t, flags=re.I))

    street = ""
    number = ""
    best = (0, "", "")
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
        street, number = first[0], first[1]

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
    if not city and len(parts) >= 2:
        for p in reversed(parts):
            if re.search(r"\b\d{4}\b", p):
                continue
            if len(p) >= 3:
                city = p
                break
    tail = " ".join(x for x in [zip_code, city] if x).strip()

    address = ", ".join(x for x in [street_line, tail] if x).strip()
    location_display = ", ".join(x for x in [venue, address] if x).strip()
    if not location_display:
        location_display = venue or address or dn
    return venue, address, location_display


def _write_index_html(path: Path, *, build_stamp: str) -> None:
    # Static page with optional Google Maps basemap (keyed) and free OSM fallback (Leaflet).
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Open Mics Zurich</title>
    <meta http-equiv="Cache-Control" content="no-store, max-age=0" />
    <meta http-equiv="Pragma" content="no-cache" />
    <meta http-equiv="Expires" content="0" />
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Spectral:opsz,wght@7..72,400,600,700&family=Karla:ital,wght@0,400,500,600;1,400&family=JetBrains+Mono:wght@400,600&display=swap" rel="stylesheet">
    <link
      rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
      crossorigin=""
    />
    <style>
      :root {{
        --color-bg: #ffffff;
        --color-bg-soft: #f7f8f9;
        --color-ink: #0f0f0f;
        --color-ink-body: #171717;
        --color-muted: #5f5f5f;
        --color-rule: #d9dde1;
        --color-accent: #3a677a;
        --color-accent-hover: #2f5a6b;

        --font-display: "Spectral", "Georgia", serif;
        --font-ui: "Karla", system-ui, sans-serif;
        --font-mono: "JetBrains Mono", ui-monospace, monospace;
      }}
      body {{
        margin: 0;
        font-family: var(--font-ui);
        background: var(--color-bg);
        color: var(--color-ink-body);
        line-height: 1.55;
      }}
      a {{
        color: var(--color-accent);
        text-decoration-color: var(--color-accent);
        text-underline-offset: 3px;
      }}
      a:hover {{
        color: var(--color-accent-hover);
      }}
      a:focus-visible {{
        outline: 2px solid var(--color-accent);
        outline-offset: 3px;
      }}
      .skip-link {{
        position: absolute;
        left: 12px;
        top: 10px;
        padding: 8px 10px;
        background: var(--color-bg);
        border: 1px solid var(--color-rule);
        color: var(--color-ink);
        font-family: var(--font-mono);
        font-size: 12px;
        text-decoration: none;
        transform: translateY(-150%);
      }}
      .skip-link:focus {{
        transform: translateY(0);
      }}
      header {{
        padding: 22px 28px 16px 28px;
        border-bottom: 1px solid var(--color-rule);
        background: var(--color-bg);
      }}
      header h1 {{
        font-family: var(--font-display);
        font-size: 42px;
        line-height: 1.05;
        margin: 0 0 10px 0;
        letter-spacing: -0.4px;
        color: var(--color-ink);
      }}
      header .sub {{
        color: var(--color-muted);
        font-size: 14px;
        max-width: 42rem;
      }}
      .layout {{
        display: grid;
        grid-template-columns: 1.35fr 0.85fr;
        gap: 18px;
        padding: 18px 28px 28px 28px;
      }}
      .card {{
        background: transparent;
        border: none;
        min-height: 200px;
      }}
      #map {{
        height: calc(100vh - 140px);
        min-height: 520px;
        border: 1px solid var(--color-rule);
      }}
      /* Leaflet/SVG "focus" styling can leave clicked markers looking selected. */
      .leaflet-container .leaflet-interactive:focus {{
        outline: none;
      }}
      .banner {{
        position: absolute;
        left: 0;
        right: 0;
        top: 0;
        padding: 10px 12px;
        background: rgba(255,255,255,0.92);
        color: var(--color-ink);
        font-size: 12px;
        border-bottom: 1px solid var(--color-rule);
        z-index: 5;
        backdrop-filter: blur(6px);
      }}
      .banner code {{
        color: var(--color-ink);
        font-family: var(--font-mono);
      }}
      .controls {{
        padding: 12px 0 14px 0;
        border-bottom: 1px solid var(--color-rule);
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
      }}
      .controls label {{
        display: block;
        font-size: 11px;
        color: var(--color-muted);
        font-family: var(--font-mono);
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-bottom: 6px;
      }}
      .controls select, .controls input {{
        width: 100%;
        padding: 10px 10px;
        border-radius: 0;
        border: 1px solid var(--color-rule);
        background: var(--color-bg-soft);
        color: var(--color-ink);
        outline: none;
        font-family: var(--font-ui);
      }}
      .controls option {{
        background: var(--color-bg);
        color: var(--color-ink);
      }}
      .list {{
        padding: 10px 0 12px 0;
        height: calc(100vh - 140px);
        min-height: 520px;
        overflow: auto;
      }}
      .item {{
        padding: 10px 0;
        border-bottom: 1px solid var(--color-rule);
      }}
      .item.active {{
        background: transparent;
        outline: 2px solid rgba(58, 103, 122, 0.25);
        outline-offset: 4px;
      }}
      .item.active a {{
        color: var(--color-accent);
      }}
      .item a {{
        color: var(--color-ink);
        text-decoration: none;
        font-family: var(--font-display);
        font-size: 18px;
        line-height: 1.25;
      }}
      .item a:hover {{
        color: var(--color-accent-hover);
      }}
      .meta {{
        color: var(--color-muted);
        font-size: 12px;
        margin-top: 4px;
        line-height: 1.35;
        font-family: var(--font-mono);
      }}
      .meta.venue a {{
        color: var(--color-ink);
        font-family: var(--font-display);
        font-size: 15px;
        line-height: 1.25;
        text-decoration: underline;
        text-decoration-color: rgba(58, 103, 122, 0.55);
        text-underline-offset: 3px;
      }}
      .meta.venue a:hover {{
        color: var(--color-accent-hover);
        text-decoration-color: rgba(47, 90, 107, 0.7);
      }}
      .meta-strong {{
        color: var(--color-ink);
        font-family: var(--font-mono);
        font-size: 12px;
        letter-spacing: 0.02em;
        margin-top: 6px;
        font-weight: 600;
      }}
      .pill {{
        display: inline-block;
        font-size: 11px;
        border: 1px solid var(--color-rule);
        border-radius: 0;
        padding: 2px 6px;
        color: var(--color-muted);
        margin-right: 6px;
        margin-top: 6px;
        font-family: var(--font-mono);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}
      .footer-note {{
        padding: 10px 12px;
        color: var(--color-muted);
        font-size: 12px;
        border-top: 1px solid var(--color-rule);
        font-family: var(--font-mono);
      }}
      @media (max-width: 980px) {{
        .layout {{
          grid-template-columns: 1fr;
        }}
        #map, .list {{
          height: 520px;
          min-height: 520px;
        }}
      }}
    </style>
  </head>
  <body>
    <a class="skip-link" href="#items">Skip to list</a>
    <header>
      <h1>Open Mics Zurich</h1>
      <div class="sub">Recurring open mic events in and around Zürich. Filter by weekday and explore locations on the map. <span style="font-family:var(--font-mono); color:var(--color-muted);">Build: {build_stamp}</span></div>
    </header>

    <div class="layout">
      <div class="card">
        <div style="position: relative;">
          <div class="banner" id="banner" style="display:none;"></div>
          <div id="map"></div>
        </div>
        <div class="footer-note" id="mapNote"></div>
      </div>

      <div class="card">
        <div class="controls">
          <div>
            <label for="weekday">Weekday</label>
            <select id="weekday">
              <option value="__all__">All</option>
              {"".join([f'<option value="{d}">{d}</option>' for d in WEEKDAYS])}
            </select>
          </div>
          <div>
            <label for="q">Search</label>
            <input id="q" placeholder="title or location" />
          </div>
        </div>
        <div class="list">
          <div class="meta" id="count"></div>
          <div id="items"></div>
        </div>
      </div>
    </div>

    <script
      src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
      integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
      crossorigin=""
    ></script>
    <script>
      const BUILD_STAMP = "{build_stamp}";
      let gmap = null;
      let gInfoWindow = null;
      let gMarkers = [];

      let lmap = null;
      let lMarkersLayer = null;

      let _lastActiveId = null;

      function norm(s) {{
        return (s || '').toString().trim().toLowerCase();
      }}

      function weekdayMatches(cell, selected) {{
        if (!selected || selected === '__all__') return true;
        const parts = (cell || '').split(',').map(x => x.trim()).filter(Boolean);
        return parts.includes(selected);
      }}

      function formatLocation(loc) {{
        const s = (loc || '').toString().trim();
        if (!s) return '';
        // Normalise comma-separated parts and remove obvious duplicates / venue echoes.
        const parts = s.split(',').map(x => x.trim()).filter(Boolean);
        if (!parts.length) return s;
        const out = [];
        const seen = new Set();
        const venue = (parts[0] || '').toLowerCase();
        for (let i = 0; i < parts.length; i++) {{
          const p = parts[i];
          const pl = p.toLowerCase();
          if (seen.has(pl)) continue;
          // Drop patterns like "Venue, Venue 131, 8005 Zürich" (venue echoed as "street").
          if (i === 1 && venue && pl.startsWith(venue + ' ') && /\\b\\d+[a-z]?\\b/i.test(p.slice(venue.length))) {{
            continue;
          }}
          seen.add(pl);
          out.push(p);
        }}
        return out.join(', ');
      }}

      function setActive(eventId) {{
        if (_lastActiveId) {{
          const prev = document.getElementById(_lastActiveId);
          if (prev) prev.classList.remove('active');
        }}
        _lastActiveId = eventId;
        const el = document.getElementById(eventId);
        if (el) {{
          el.classList.add('active');
          const scroller = document.querySelector('.list');
          if (scroller && typeof scroller.scrollTo === 'function') {{
            const top = el.getBoundingClientRect().top - scroller.getBoundingClientRect().top + scroller.scrollTop;
            scroller.scrollTo({{ top: Math.max(0, top - 40), behavior: 'smooth' }});
          }} else {{
            el.scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});
          }}
        }}
      }}

      function clearMarkers() {{
        for (const m of gMarkers) {{
          m.setMap(null);
        }}
        gMarkers = [];
        if (lMarkersLayer) {{
          lMarkersLayer.clearLayers();
        }}
      }}

      function render(events) {{
        const weekday = document.getElementById('weekday').value;
        const q = norm(document.getElementById('q').value);

        const filtered = events.filter(e => {{
          if (!weekdayMatches(e.weekday, weekday)) return false;
          if (q) {{
            const hay = norm(e.title) + ' ' + norm(e.location);
            if (!hay.includes(q)) return false;
          }}
          return true;
        }});

        document.getElementById('count').textContent = `${{filtered.length}} event(s)`;

        const items = document.getElementById('items');
        items.innerHTML = '';
        clearMarkers();

        for (let idx = 0; idx < filtered.length; idx++) {{
          const e = filtered[idx];
          const eventId = `event-${{idx}}`;
          const div = document.createElement('div');
          div.className = 'item';
          div.id = eventId;

          const a = document.createElement('a');
          a.href = e.url || '#';
          a.target = '_blank';
          a.rel = 'noreferrer';
          a.textContent = e.title || '(untitled)';
          div.appendChild(a);

          const mt = document.createElement('div');
          mt.className = 'meta-strong';
          mt.textContent = [e.weekday, e.time].filter(Boolean).join(' · ');
          div.appendChild(mt);

          const ml = document.createElement('div');
          ml.className = 'meta venue';
          const venueText = (e.venue || (e.location_display || e.location || '').split(',')[0] || '').toString().trim();
          const locQuery = formatLocation(e.location_display || e.location || venueText);
          const mapsA = document.createElement('a');
          mapsA.href = locQuery ? `https://www.google.com/maps/search/?api=1&query=${{encodeURIComponent(locQuery)}}` : '#';
          mapsA.target = '_blank';
          mapsA.rel = 'noreferrer';
          mapsA.textContent = venueText || '(venue)';
          ml.appendChild(mapsA);
          div.appendChild(ml);

          const pills = document.createElement('div');
          for (const p of [e.language, e.regularity, e.cost].filter(Boolean)) {{
            const s = document.createElement('span');
            s.className = 'pill';
            s.textContent = p;
            pills.appendChild(s);
          }}
          div.appendChild(pills);

          items.appendChild(div);

          if (typeof e.lat === 'number' && typeof e.lon === 'number') {{
            if (gmap) {{
              const m = new google.maps.Marker({{
                position: {{ lat: e.lat, lng: e.lon }},
                map: gmap,
                title: e.title || '(untitled)',
              }});
              const safe = (s) => (s || '').toString().replaceAll('<','&lt;');
              const locText = formatLocation(e.location_display || e.location || '');
              const venueText = (e.venue || (e.location_display || e.location || '').split(',')[0] || '').toString().trim();
              const locQuery = locText || venueText;
              const mapsLine = locQuery
                ? `<a href="https://www.google.com/maps/search/?api=1&query=${{encodeURIComponent(locQuery)}}" target="_blank" rel="noreferrer">${{safe(venueText || locQuery)}}</a><br/>`
                : '';
              const popup = `<strong>${{safe(e.title)}}</strong><br/>` +
                            `${{safe(e.weekday)}} ${{safe(e.time)}}<br/>` +
                            mapsLine +
                            (e.url ? `<a href="${{e.url}}" target="_blank" rel="noreferrer">Open link</a>` : '');
              m.addListener('click', () => {{
                setActive(eventId);
                if (!gInfoWindow) gInfoWindow = new google.maps.InfoWindow();
                gInfoWindow.setContent(popup);
                gInfoWindow.open({{ anchor: m, map: gmap }});
              }});
              // Hover list item -> preview popup on the map (until mouse leaves).
              div.addEventListener('mouseenter', () => {{
                if (!gInfoWindow) gInfoWindow = new google.maps.InfoWindow();
                gInfoWindow.setContent(popup);
                gInfoWindow.open({{ anchor: m, map: gmap }});
              }});
              div.addEventListener('mouseleave', () => {{
                if (gInfoWindow) gInfoWindow.close();
              }});
              gMarkers.push(m);
            }} else if (lmap && lMarkersLayer) {{
              const safe = (s) => (s || '').toString().replaceAll('<','&lt;');
              const locText = formatLocation(e.location_display || e.location || '');
              const venueText = (e.venue || (e.location_display || e.location || '').split(',')[0] || '').toString().trim();
              const locQuery = locText || venueText;
              const mapsLine = locQuery
                ? `<a href="https://www.google.com/maps/search/?api=1&query=${{encodeURIComponent(locQuery)}}" target="_blank" rel="noreferrer">${{safe(venueText || locQuery)}}</a><br/>`
                : '';
              const popup = `<strong>${{safe(e.title)}}</strong><br/>` +
                            `${{safe(e.weekday)}} ${{safe(e.time)}}<br/>` +
                            mapsLine +
                            (e.url ? `<a href="${{e.url}}" target="_blank" rel="noreferrer">Open link</a>` : '');
              const marker = L.circleMarker([e.lat, e.lon], {{
                radius: 7,
                color: '#3a677a',
                weight: 2,
                fillColor: '#3a677a',
                fillOpacity: 0.45,
              }});
              marker.bindPopup(popup);
              marker.on('click', () => setActive(eventId));
              // Hover list item -> preview popup on the map (until mouse leaves).
              div.addEventListener('mouseenter', () => {{
                try {{ marker.openPopup(); }} catch (e) {{}}
              }});
              div.addEventListener('mouseleave', () => {{
                try {{ marker.closePopup(); }} catch (e) {{}}
              }});
              marker.addTo(lMarkersLayer);
            }}
          }}
        }}

        const pinned = filtered.filter(e => typeof e.lat === 'number' && typeof e.lon === 'number').length;
        document.getElementById('mapNote').textContent =
          `Pins: ${{pinned}} / ${{filtered.length}}`;
      }}

      function getGoogleMapsKey() {{
        const params = new URLSearchParams(window.location.search);
        const fromQuery = params.get('gmaps_key');
        if (fromQuery) return fromQuery;
        try {{
          return window.localStorage.getItem('open_mics_gmaps_key') || '';
        }} catch (e) {{
          return '';
        }}
      }}

      function showBanner(html) {{
        const b = document.getElementById('banner');
        b.style.display = 'block';
        b.innerHTML = html;
      }}

      async function boot() {{
        const resp = await fetch(`./data/events.json?v=${{encodeURIComponent(BUILD_STAMP)}}`, {{ cache: 'no-cache' }});
        const payload = await resp.json();
        const events = payload.events || [];

        const key = getGoogleMapsKey();
        if (!key) {{
          // OSM fallback map
          const ZURICH_BOUNDS = L.latLngBounds(
            L.latLng(47.30, 8.45),   // SW
            L.latLng(47.45, 8.65)    // NE
          );
          const ZURICH_MIN_ZOOM = 12;

          lmap = L.map('map', {{
            zoomControl: true,
            minZoom: ZURICH_MIN_ZOOM,
            maxBounds: ZURICH_BOUNDS,
            maxBoundsViscosity: 1.0,
          }}).setView([47.3769, 8.5417], ZURICH_MIN_ZOOM);
          L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            maxZoom: 19,
            attribution: '&copy; OpenStreetMap contributors'
          }}).addTo(lmap);
          lmap.setMaxBounds(ZURICH_BOUNDS);
          lMarkersLayer = L.layerGroup().addTo(lmap);

          showBanner(
            `Using OpenStreetMap (no Google key). To enable Google basemap add <code>?gmaps_key=YOUR_KEY</code> ` +
            `or set <code>localStorage.setItem('open_mics_gmaps_key','YOUR_KEY')</code>.`
          );
          document.getElementById('weekday').addEventListener('change', () => render(events));
          document.getElementById('q').addEventListener('input', () => render(events));
          render(events);
          return;
        }}

        // Load Google Maps JS API dynamically.
        await new Promise((resolve, reject) => {{
          const s = document.createElement('script');
          s.src = `https://maps.googleapis.com/maps/api/js?key=${{encodeURIComponent(key)}}&v=weekly`;
          s.async = true;
          s.onload = resolve;
          s.onerror = reject;
          document.head.appendChild(s);
        }});

        gmap = new google.maps.Map(document.getElementById('map'), {{
          center: {{ lat: 47.3769, lng: 8.5417 }},
          zoom: 12,
          minZoom: 12,
          maxZoom: 19,
          restriction: {{
            latLngBounds: {{
              south: 47.30,
              west: 8.45,
              north: 47.45,
              east: 8.65,
            }},
            strictBounds: true,
          }},
          mapTypeControl: false,
          streetViewControl: false,
          fullscreenControl: true,
        }});

        document.getElementById('weekday').addEventListener('change', () => render(events));
        document.getElementById('q').addEventListener('input', () => render(events));
        render(events);
      }}

      boot().catch(err => {{
        document.getElementById('mapNote').textContent = 'Failed to load event data';
        console.error(err);
      }});
    </script>
  </body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def main() -> int:
    if not CSV_PATH.is_file():
        print(f"[export-site] Missing CSV: {CSV_PATH}")
        return 2

    cache = _load_geocache(GEOCACHE_PATH)
    df = pd.read_csv(CSV_PATH, sep=";", dtype=str).fillna("")
    if "Regularity" in df.columns:
        df = df[df["Regularity"].astype(str).str.strip().str.lower() == "recurring"].copy()
    # Explode multi-weekday rows into one row per weekday for clearer browsing.
    if "Weekday" in df.columns:
        df["Weekday"] = df["Weekday"].astype(str).fillna("")
        df["Weekday"] = df["Weekday"].apply(lambda s: ", ".join([p.strip() for p in s.split(",") if p.strip()]))
        df["_weekday_list"] = df["Weekday"].apply(lambda s: [p.strip() for p in str(s).split(",") if p.strip()] or [""])
        df = df.explode("_weekday_list").copy()
        df["Weekday"] = df["_weekday_list"].astype(str)
        df.drop(columns=["_weekday_list"], inplace=True)
    # Confirmed open mic: title or description must mention it
    for col in ["Event_title", "Listing_title", "Description_preview"]:
        if col not in df.columns:
            df[col] = ""
    confirmed_mask = (
        df[["Event_title", "Listing_title", "Description_preview"]]
        .astype(str)
        .agg(" ".join, axis=1)
        .str.contains(_RE_OPEN_MIC, regex=True, na=False)
    )
    # Filter out music jam sessions (e.g. "Jam Session ... Open Mic" at venues).
    exclude_mask = (
        df[["Event_title", "Listing_title", "Description_preview"]]
        .astype(str)
        .agg(" ".join, axis=1)
        .str.contains(_RE_MUSIC_JAM, regex=True, na=False)
    )
    confirmed_mask = confirmed_mask & (~exclude_mask)
    df = df[confirmed_mask].copy()

    def coord_for(loc: str) -> tuple[float | None, float | None]:
        entry = cache.get(loc)
        if not entry:
            return None, None
        try:
            return float(entry.get("lat")), float(entry.get("lon"))
        except (TypeError, ValueError):
            return None, None

    def formatted_loc(loc: str) -> tuple[str, str, str]:
        entry = cache.get(loc) or {}
        dn = entry.get("display_name")
        venue_hint = loc.split(",", 1)[0].strip() if "," in loc else loc
        if isinstance(dn, str) and dn.strip():
            return _format_address_from_display_name(venue_hint=venue_hint, display_name=dn)
        return (venue_hint, "", loc)

    events = []
    missing_coords = 0
    for _, row in df.iterrows():
        loc = _norm(row.get("Location", ""))
        lat, lon = coord_for(loc)
        if lat is None or lon is None:
            missing_coords += 1
        venue, address, location_display = formatted_loc(loc)
        events.append(
            {
                "weekday": _norm(row.get("Weekday", "")),
                "location": loc,
                "venue": _norm(venue),
                "address": _norm(address),
                "location_display": _norm(location_display),
                "time": _norm(row.get("Time", "")),
                "cost": _norm(row.get("Cost", "")),
                "language": _norm(row.get("Comedy_language", "")),
                "regularity": _norm(row.get("Regularity", "")),
                "title": _norm(row.get("Event_title", "")),
                "url": _norm(row.get("URL", "")),
                "lat": lat,
                "lon": lon,
            }
        )

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    DOCS_EVENTS_JSON.write_text(
        json.dumps(
            {
                "generated_from": str(CSV_PATH.as_posix()),
                "events_total": len(events),
                "events_missing_coords": missing_coords,
                "events": events,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    build_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_index_html(DOCS_DIR / "index.html", build_stamp=build_stamp)
    (DOCS_DIR / ".nojekyll").write_text("", encoding="utf-8")

    print(f"[export-site] Wrote {DOCS_DIR / 'index.html'}")
    print(f"[export-site] Wrote {DOCS_EVENTS_JSON} ({len(events)} events; missing coords: {missing_coords})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

