#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

import pipeline_meta
import base64


ROOT = Path(__file__).resolve().parent.parent
RULES_PATH = ROOT / "config" / "rules.json"
CSV_PATH = ROOT / "data" / "processed" / "events_flat.csv"
GEOCACHE_PATH = ROOT / "data" / "processed" / "location_geocache.json"

DOCS_DIR = ROOT / "docs"
DOCS_DATA_DIR = DOCS_DIR / "data"
DOCS_EVENTS_JSON = DOCS_DATA_DIR / "events.json"
DOCS_VENUES_JSON = DOCS_DATA_DIR / "venues.json"
DOCS_OCCURRENCES_JSON = DOCS_DATA_DIR / "occurrences.json"
PLACEHOLDER_SVG = ROOT / "assets" / "open_mic_placeholder.svg"


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


def _write_index_html(
    path: Path, *, build_stamp: str, site_data_date_display: str, placeholder_data_url: str = ""
) -> None:
    # Static page with optional Google Maps basemap (keyed) and free OSM fallback (Leaflet).
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Open MicZH</title>
    <meta http-equiv="Cache-Control" content="no-store, max-age=0" />
    <meta http-equiv="Pragma" content="no-cache" />
    <meta http-equiv="Expires" content="0" />
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link
      href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400&family=Karla:ital,wght@0,400;0,500;0,600;0,700;1,400&family=Spectral:ital,wght@0,400;0,500;0,600;0,700;1,400&display=swap"
      rel="stylesheet"
    />
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

        --space-1: 0.5rem;
        --space-2: 1rem;
        --space-3: 1.5rem;
        --space-4: 2.25rem;
        --space-5: 3.5rem;
        --space-6: 5rem;

        --measure: 42rem;
        --page-max: 74rem;
        --gutter-start: clamp(1.25rem, 4vw, 2.5rem);
        --gutter-end: clamp(1.5rem, 8vw, 5rem);

        --lh-tight: 1.02;
        --lh-head: 1.08;
        --lh-snug: 1.2;
        --lh-body: 1.65;
        --text-body: clamp(0.95rem, 0.15vw + 0.9rem, 1rem);
      }}
      body {{
        margin: 0;
        background: var(--color-bg);
        color: var(--color-ink-body);
        font-family: var(--font-ui);
        font-size: var(--text-body);
        font-weight: 400;
        line-height: var(--lh-body);
        letter-spacing: 0.005em;
        -webkit-font-smoothing: antialiased;
        text-rendering: optimizeLegibility;
      }}
      /* Editorial measure: constrain overall chrome a bit */
      .layout {{
        max-width: 1440px;
        margin: 0 auto;
      }}
      a {{
        color: var(--color-ink);
        text-decoration: none;
        transition: color 0.15s ease, border-color 0.15s ease, opacity 0.15s ease;
      }}
      a:hover {{
        color: var(--color-accent);
      }}
      a:focus-visible {{
        outline: 2px solid var(--color-accent);
        outline-offset: 3px;
      }}
      .skip-link {{
        position: absolute;
        left: -9999px;
        top: var(--space-2);
        z-index: 200;
        padding: var(--space-1) var(--space-3);
        background: var(--color-ink);
        color: var(--color-bg);
        font-family: var(--font-ui);
        font-size: 0.6875rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }}
      .skip-link:focus {{
        left: var(--space-2);
      }}
      header {{
        position: sticky;
        top: 0;
        z-index: 50;
        background: rgba(255, 255, 255, 0.96);
        border-bottom: 1px solid var(--color-ink);
        backdrop-filter: blur(8px);
        padding: var(--space-3) var(--gutter-end) var(--space-3) var(--gutter-start);
      }}
      header h1 {{
        font-family: var(--font-display);
        font-size: 42px;
        line-height: var(--lh-head);
        margin: 0 0 0.5rem 0;
        letter-spacing: -0.02em;
        color: var(--color-ink);
      }}
      header .sub {{
        color: var(--color-muted);
        font-size: 14px;
        max-width: 42rem;
      }}
      .layout {{
        display: grid;
        /* Match Streamlit layout: filters / map / events */
        grid-template-columns: minmax(260px, 320px) minmax(520px, 1fr) minmax(340px, 420px);
        gap: var(--space-3);
        padding: var(--space-3) var(--gutter-end) var(--space-4) var(--gutter-start);
      }}
      .card {{
        background: transparent;
        border: none;
        min-height: 200px;
      }}
      .panel-title {{
        margin: 0 0 10px 0;
        padding-bottom: 6px;
        border-bottom: 1px solid var(--color-rule);
        /* Match event title styling in sidebar list */
        font-family: var(--font-display);
        font-size: 18px;
        line-height: 1.25;
        font-weight: 400;
        color: var(--color-ink);
        letter-spacing: 0;
      }}
      #map {{
        /* Slightly shorter, editorial feel */
        height: var(--panel-h, clamp(420px, calc(100vh - 240px), 560px));
        min-height: 420px;
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
        padding: 0 0 14px 0;
        border-bottom: 1px solid var(--color-rule);
        display: grid;
        grid-template-columns: 1fr;
        gap: 10px;
      }}
      .controls label {{
        display: block;
        font-family: var(--font-display);
        font-size: 18px;
        line-height: 1.25;
        font-weight: 400;
        color: var(--color-ink);
        text-transform: none;
        letter-spacing: 0;
        margin-bottom: 6px;
      }}
      .controls select, .controls input {{
        width: 100%;
        padding: 10px 10px;
        border-radius: 0;
        box-sizing: border-box;
        border: 1px solid var(--color-rule);
        background: var(--color-bg-soft);
        color: var(--color-ink);
        outline: none;
        font-family: var(--font-ui);
        appearance: none;
        -webkit-appearance: none;
      }}
      /* Compact multiselect popovers (keeps filters small) */
      .filter-row {{
        display: grid;
        grid-template-columns: 1fr;
        gap: 6px;
      }}
      .filter-button {{
        width: 100%;
        text-align: left;
        padding: 10px 10px;
        border: 1px solid var(--color-rule);
        background: var(--color-bg-soft);
        color: var(--color-ink);
        font-family: var(--font-ui);
        font-size: 14px;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 10px;
      }}
      .filter-button .left {{
        display: inline-flex;
        align-items: baseline;
        gap: 8px;
        min-width: 0;
      }}
      .filter-button:focus-visible {{
        outline: 2px solid var(--color-accent);
        outline-offset: 2px;
      }}
      .filter-button .meta {{
        font-family: var(--font-mono);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--color-muted);
        white-space: nowrap;
      }}
      .filter-button .caret {{
        font-family: var(--font-mono);
        font-size: 11px;
        color: var(--color-muted);
        margin-left: 4px;
      }}
      .filter-button[data-open="1"] {{
        background: #ffffff;
        border-color: var(--color-accent);
      }}
      .filter-button[data-open="1"] .meta,
      .filter-button[data-open="1"] .caret {{
        color: var(--color-accent);
      }}
      .filter-popover {{
        position: absolute;
        left: 0;
        right: 0;
        top: calc(100% + 6px);
        border: 1px solid var(--color-rule);
        background: var(--color-bg);
        padding: 8px;
        z-index: 20;
        box-shadow: 0 8px 20px rgba(0,0,0,0.08);
      }}
      .filter-popover[hidden] {{ display: none; }}
      .checklist {{
        display: block;
        padding: 0;
        margin: 0;
        max-height: 220px;
        overflow: auto;
      }}
      .check {{
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 4px 6px;
        border-bottom: 1px solid rgba(0,0,0,0.06);
        font-size: 12.5px;
        user-select: none;
        cursor: pointer;
      }}
      .check.select-all {{
        position: sticky;
        top: 0;
        background: var(--color-bg-soft);
        border-bottom: 1px solid rgba(0,0,0,0.10);
        z-index: 2;
      }}
      .check:hover {{
        background: var(--color-bg-soft);
      }}
      .check input {{
        margin: 0;
        position: absolute;
        opacity: 0;
        width: 1px;
        height: 1px;
      }}
      .check .label {{
        font-family: var(--font-ui);
        line-height: 1.2;
      }}
      .check[data-on="1"] .label {{
        font-weight: 650;
        color: var(--color-ink);
      }}
      .check[data-on="0"] .label {{
        font-weight: 400;
        color: var(--color-ink-body);
      }}
      .check.select-all .label {{
        font-family: var(--font-ui);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--color-muted);
      }}
      .controls select:focus, .controls select:focus-visible,
      .controls input:focus, .controls input:focus-visible {{
        border-color: var(--color-accent);
        outline: none;
      }}
      .controls option {{
        background: var(--color-bg);
        color: var(--color-ink);
      }}
      .list {{
        padding: 10px 0 12px 0;
        height: var(--panel-h, clamp(420px, calc(100vh - 240px), 560px));
        min-height: 420px;
        overflow: auto;
      }}
      .list-heading {{
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 12px;
        flex-wrap: wrap;
        padding-bottom: 6px;
        border-bottom: 1px solid var(--color-rule);
        margin-bottom: 6px;
      }}
      .list-heading-title {{
        margin: 0;
        font-family: var(--font-display);
        font-size: 22px;
        font-weight: 600;
        color: var(--color-ink);
        letter-spacing: -0.02em;
      }}
      .list-heading-updated {{
        font-family: var(--font-mono);
        font-size: 11px;
        color: var(--color-muted);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        white-space: nowrap;
      }}
      .item {{
        padding: 10px 0;
        border-bottom: 1px solid var(--color-rule);
      }}
      .item-inner {{
        display: grid;
        grid-template-columns: 116px 1fr;
        gap: 12px;
        align-items: start;
      }}
      .thumb {{
        width: 116px;
        height: 76px;
        object-fit: cover;
        border: 1px solid var(--color-rule);
        background: var(--color-bg-soft);
      }}
      @media (max-width: 980px) {{
        .item-inner {{
          grid-template-columns: 1fr;
        }}
        .thumb {{
          width: 100%;
          height: 160px;
        }}
      }}
      .item.active {{
        background: rgba(58, 103, 122, 0.08);
        outline: 2px solid rgba(58, 103, 122, 0.30);
        outline-offset: 3px;
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
      .site-footer {{
        padding: 10px 28px 18px 28px;
        color: var(--color-muted);
        font-size: 12px;
        font-family: var(--font-mono);
        border-top: 1px solid var(--color-rule);
        text-align: center;
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
    <a class="skip-link" href="#list-top">Skip to list</a>
    <header>
      <h1>Open Mics Zurich</h1>
      <div class="sub">Recurring open mic events in and around Zürich.</div>
    </header>

    <div class="layout">
      <div class="card">
        <div class="controls">
          <div>
            <label for="weekday">Weekday</label>
            <div class="filter-row" style="position:relative;">
              <button class="filter-button" id="weekdayBtn" type="button" aria-expanded="false" data-open="0">
                <span class="meta" id="weekdayMeta">All</span>
                <span class="caret">▾</span>
              </button>
              <div class="filter-popover" id="weekdayPop" hidden>
                <div class="checklist" id="weekdayChecks"></div>
              </div>
            </div>
          </div>
          <div>
            <label for="language">Comedy language</label>
            <div class="filter-row" style="position:relative;">
              <button class="filter-button" id="languageBtn" type="button" aria-expanded="false" data-open="0">
                <span class="meta" id="languageMeta">All</span>
                <span class="caret">▾</span>
              </button>
              <div class="filter-popover" id="languagePop" hidden>
                <div class="checklist" id="languageChecks"></div>
              </div>
            </div>
          </div>
          <div>
            <label for="q">Search</label>
            <input id="q" placeholder="title or location" />
          </div>
        </div>
        <div class="meta" id="count" style="padding-top:10px;"></div>
      </div>

      <div class="card">
        <div style="position: relative;">
          <div class="banner" id="banner" style="display:none;"></div>
          <div id="map"></div>
        </div>
        <div class="footer-note" id="mapNote"></div>
      </div>

      <div class="card">
        <div class="list" id="list-top">
          <div class="list-heading">
            <h2 class="list-heading-title" style="display:none;">Events</h2>
          </div>
          <div id="items"></div>
        </div>
      </div>
    </div>

    <footer class="site-footer">Data: {site_data_date_display} (UTC) · Build {build_stamp}</footer>

    <script
      src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
      integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
      crossorigin=""
    ></script>
    <script>
      const BUILD_STAMP = "{build_stamp}";
      const PLACEHOLDER_THUMB = "{placeholder_data_url}";
      let gmap = null;
      let gInfoWindow = null;
      let gMarkers = [];

      let lmap = null;
      let lMarkersLayer = null;

      let _lastActiveId = null;

      function norm(s) {{
        return (s || '').toString().trim().toLowerCase();
      }}

      function checkedValues(containerId) {{
        const root = document.getElementById(containerId);
        if (!root) return [];
        const out = [];
        for (const el of Array.from(root.querySelectorAll('input[type=\"checkbox\"][data-value]'))) {{
          if (el && el.checked) out.push(el.getAttribute('data-value'));
        }}
        return out;
      }}

      function weekdayMatches(cell, selectedList) {{
        const selected = (selectedList || []).map(x => (x || '').toString().trim()).filter(Boolean);
        if (!selected.length) return true;
        const parts = (cell || '').split(',').map(x => x.trim()).filter(Boolean);
        return parts.some(p => selected.includes(p));
      }}

      function languageMatches(cell, selectedList) {{
        const selected = (selectedList || []).map(x => (x || '').toString().trim()).filter(Boolean);
        if (!selected.length) return true;
        const parts = (cell || '').split(/[;,]/).map(x => x.trim()).filter(Boolean);
        // Any language overlap is a match.
        return parts.some(p => selected.includes(p));
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

      function cleanVenueLabel(venueText) {{
        // Remove trailing "comedy" from venue labels like "stubä comedy",
        // but keep names like "ComedyHaus" intact.
        const v = (venueText || '').toString().trim();
        return v.replace(/\\s+comedy$/i, '').trim();
      }}

      function capFirst(s) {{
        const v = (s || '').toString();
        if (!v) return v;
        const c0 = v[0];
        // Only adjust if the first char is a lowercase letter (covers ü/ä/ö too).
        if (c0 !== c0.toUpperCase()) {{
          return c0.toUpperCase() + v.slice(1);
        }}
        return v;
      }}

      const PIN_FILL = '#3a677a';
      const PIN_STROKE = '#2d5366';

      function escapeHtml(s) {{
        return (s || '').toString().replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
      }}

      function weekdayAbbr(d) {{
        const x = (d || '').toString().trim().toLowerCase();
        if (x.startsWith('mon')) return 'Mon';
        if (x.startsWith('tue')) return 'Tue';
        if (x.startsWith('wed')) return 'Wed';
        if (x.startsWith('thu')) return 'Thu';
        if (x.startsWith('fri')) return 'Fri';
        if (x.startsWith('sat')) return 'Sat';
        if (x.startsWith('sun')) return 'Sun';
        return (d || '').toString().trim();
      }}

      function venueGroupKey(e) {{
        const venueText = (e.venue || (e.location_display || e.location || '').split(',')[0] || '').toString().trim();
        let k = norm(cleanVenueLabel(venueText));
        if (!k) k = norm(venueText);
        if (!k) k = 'unknown';
        return k;
      }}

      // One pin per location (rounded coordinates). This keeps multiple events at the same
      // place on a single pin even if venue labels vary slightly.
      function pinGroupsForFiltered(filtered) {{
        const groups = new Map();
        const eventToKey = filtered.map(() => '');
        function coordKey(lat, lon) {{
          // Round to avoid tiny float differences creating multiple pins.
          // Use ~1e-4 degrees (~11m lat) to tolerate cache/key differences while
          // still keeping distinct venues separate in practice.
          return `${{lat.toFixed(4)}},${{lon.toFixed(4)}}`;
        }}
        for (let i = 0; i < filtered.length; i++) {{
          const e = filtered[i];
          const lat = Number(e.lat);
          const lon = Number(e.lon);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
          const k = coordKey(lat, lon);
          eventToKey[i] = k;
          if (!groups.has(k)) groups.set(k, {{ idxs: [], lats: [], lons: [] }});
          const g = groups.get(k);
          g.idxs.push(i);
          g.lats.push(lat);
          g.lons.push(lon);
        }}
        function medianNums(arr) {{
          if (!arr.length) return NaN;
          const s = [...arr].sort((a, b) => a - b);
          const m = Math.floor(s.length / 2);
          return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
        }}
        const out = [];
        for (const [k, g] of groups.entries()) {{
          out.push({{
            key: k,
            lat: medianNums(g.lats),
            lon: medianNums(g.lons),
            idxs: g.idxs,
          }});
        }}
        return {{ groups: out, eventToKey }};
      }}

      function groupPopupHtml(filtered, idxs) {{
        const first = filtered[idxs[0]] || {{}};
        const venueText = (first.venue || (first.location_display || first.location || '').split(',')[0] || '').toString().trim();
        const venueLabel = capFirst(cleanVenueLabel(venueText));
        const locText = formatLocation(first.location_display || first.location || '');
        const locQuery = locText || venueText;
        const mapsLine = locQuery
          ? `<a href="https://www.google.com/maps/search/?api=1&query=${{encodeURIComponent(locQuery)}}" target="_blank" rel="noreferrer">${{escapeHtml(venueLabel || venueText || locQuery)}}</a>`
          : escapeHtml(venueLabel || venueText || '(venue)');

        // Header: venue name only (as Google Maps link when possible)
        const addrRaw = (first.address || '').toString().trim();
        const addrFromLoc = (locText || '').split(',').slice(1).map(x => x.trim()).filter(Boolean).join(', ');
        const addr = addrRaw || addrFromLoc;
        const addrLine = addr ? `<span style="opacity:0.85;">${{escapeHtml(addr)}}</span><br/>` : '';
        const header = `<strong>${{mapsLine}}</strong><br/>${{addrLine}}`;

        const rows = idxs.map(i => {{
          const e = filtered[i] || {{}};
          const t = escapeHtml(e.title || '(untitled)');
          const w = escapeHtml(weekdayAbbr(e.weekday));
          const tm = escapeHtml(e.time || '');
          const line = [w, tm].filter(Boolean).join(' ');
          const link = e.url ? `<a href="${{e.url}}" target="_blank" rel="noreferrer">${{t}}</a>` : t;
          return `<div style="margin-top:6px;"><span style="opacity:0.85;">${{escapeHtml(line)}}</span><br/>${{link}}</div>`;
        }}).join('');
        return header + rows;
      }}

      function clearActive() {{
        // Clear any stuck highlights (do not rely only on _lastActiveId).
        try {{
          document.querySelectorAll('.item.active').forEach(el => el.classList.remove('active'));
        }} catch (e) {{}}
        _lastActiveId = null;
      }}

      function setActive(eventId, opts) {{
        const scroll = opts && Object.prototype.hasOwnProperty.call(opts, 'scroll') ? !!opts.scroll : false;
        // Ensure only one active item, even if something else applied `.active`.
        try {{
          document.querySelectorAll('.item.active').forEach(el => el.classList.remove('active'));
        }} catch (e) {{}}
        _lastActiveId = eventId;
        const el = document.getElementById(eventId);
        if (el) {{
          el.classList.add('active');
          if (scroll) {{
            const scroller = document.querySelector('.list');
            const doScroll = () => {{
              if (scroller && typeof scroller.scrollTo === 'function') {{
                const top = el.getBoundingClientRect().top - scroller.getBoundingClientRect().top + scroller.scrollTop;
                scroller.scrollTo({{ top: Math.max(0, top - 40), behavior: 'smooth' }});
              }} else {{
                el.scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});
              }}
            }};
            requestAnimationFrame(() => requestAnimationFrame(doScroll));
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
        const weekdaySel = checkedValues('weekdayChecks');
        const langSel = checkedValues('languageChecks');
        const q = norm(document.getElementById('q').value);

        const filtered = events.filter(e => {{
          if (!weekdayMatches(e.weekday, weekdaySel)) return false;
          if (!languageMatches(e.language, langSel)) return false;
          if (q) {{
            const hay = norm(e.title) + ' ' + norm(e.location);
            if (!hay.includes(q)) return false;
          }}
          return true;
        }});

        // Sort by weekday (Mon..Sun), then time, then venue/title.
        const WD = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
        function wdIndex(cell) {{
          const parts = (cell || '').toString().split(',').map(x => x.trim()).filter(Boolean);
          let best = 999;
          for (const p of parts) {{
            const i = WD.indexOf(p);
            if (i >= 0 && i < best) best = i;
          }}
          return best;
        }}
        function timeKey(t) {{
          const m = (t || '').toString().trim().match(/^(\\d{1,2}):(\\d{2})$/);
          if (!m) return 9999;
          return (parseInt(m[1], 10) * 60) + parseInt(m[2], 10);
        }}
        filtered.sort((a, b) => {{
          const da = wdIndex(a.weekday);
          const db = wdIndex(b.weekday);
          if (da !== db) return da - db;
          const ta = timeKey(a.time);
          const tb = timeKey(b.time);
          if (ta !== tb) return ta - tb;
          const va = norm((a.venue || '').toString());
          const vb = norm((b.venue || '').toString());
          if (va !== vb) return va < vb ? -1 : 1;
          const la = norm((a.title || '').toString());
          const lb = norm((b.title || '').toString());
          if (la !== lb) return la < lb ? -1 : 1;
          return 0;
        }});

        const pins = pinGroupsForFiltered(filtered);
        const pinGroups = pins.groups;
        const eventToKey = pins.eventToKey;
        const markerByKey = new Map();

        document.getElementById('count').textContent = `${{filtered.length}} event(s)`;

        const items = document.getElementById('items');
        items.innerHTML = '';
        clearMarkers();
        clearActive();

        // When leaving the list area, clear the hover selection.
        try {{
          const listEl = document.querySelector('.list');
          if (listEl) {{
            listEl.onmouseleave = () => clearActive();
          }}
        }} catch (e) {{}}

        for (let idx = 0; idx < filtered.length; idx++) {{
          const e = filtered[idx];
          const eventId = `event-${{idx}}`;
          const div = document.createElement('div');
          div.className = 'item';
          div.id = eventId;

          const inner = document.createElement('div');
          inner.className = 'item-inner';

          // Thumbnail (use placeholder when missing)
          const imgUrl = (e.image_url || '').toString().trim();
          const thumbUrl = imgUrl || (PLACEHOLDER_THUMB || '');
          if (thumbUrl) {{
            const img = document.createElement('img');
            img.className = 'thumb';
            img.loading = 'lazy';
            img.alt = '';
            img.src = thumbUrl;
            img.addEventListener('error', () => {{
              // If the real image fails and we have a placeholder, swap to it.
              if (imgUrl && PLACEHOLDER_THUMB) {{
                img.src = PLACEHOLDER_THUMB;
                return;
              }}
              try {{ img.remove(); }} catch (e) {{}}
            }});
            inner.appendChild(img);
          }}

          const textCol = document.createElement('div');

          const a = document.createElement('a');
          a.className = 'title-link';
          a.href = e.url || '#';
          a.target = '_blank';
          a.rel = 'noreferrer';
          a.textContent = e.title || '(untitled)';
          textCol.appendChild(a);

          const mt = document.createElement('div');
          mt.className = 'meta-strong';
          mt.textContent = [e.weekday, e.time].filter(Boolean).join(' · ');
          textCol.appendChild(mt);

          const ml = document.createElement('div');
          ml.className = 'meta venue';
          const venueText = (e.venue || (e.location_display || e.location || '').split(',')[0] || '').toString().trim();
          const venueLabel = capFirst(cleanVenueLabel(venueText));
          // Keep the Maps query as specific as possible; only clean the visible label.
          const locQuery = formatLocation(e.location_display || e.location || venueText);
          const mapsA = document.createElement('a');
          mapsA.href = locQuery ? `https://www.google.com/maps/search/?api=1&query=${{encodeURIComponent(locQuery)}}` : '#';
          mapsA.target = '_blank';
          mapsA.rel = 'noreferrer';
          mapsA.textContent = venueLabel || venueText || '(venue)';
          ml.appendChild(mapsA);
          textCol.appendChild(ml);

          const pills = document.createElement('div');
          for (const p of [e.language, e.cost].filter(Boolean)) {{
            const s = document.createElement('span');
            s.className = 'pill';
            s.textContent = p;
            pills.appendChild(s);
          }}
          textCol.appendChild(pills);

          // If there is no image, let text span full width.
          if (!thumbUrl) {{
            inner.style.gridTemplateColumns = '1fr';
          }}
          inner.appendChild(textCol);
          div.appendChild(inner);

          items.appendChild(div);

          // Hovering in the sidebar should be the only active selection.
          div.addEventListener('mouseenter', () => {{
            setActive(eventId, {{ scroll: false }});
          }});

          const k = eventToKey[idx];
          if (k) {{
            // Show venue tooltip on hover (without auto-panning the map).
            div.addEventListener('mouseenter', () => {{
              const mk = markerByKey.get(k);
              if (!mk) return;
              if (gmap) {{
                if (!gInfoWindow) gInfoWindow = new google.maps.InfoWindow({{ disableAutoPan: true }});
                gInfoWindow.setContent(mk.popupHtml || '');
                gInfoWindow.open({{ anchor: mk.marker, map: gmap }});
              }} else {{
                try {{ mk.marker.openPopup(); }} catch (e) {{}}
              }}
            }});
            div.addEventListener('mouseleave', () => {{
              const mk = markerByKey.get(k);
              if (!mk) return;
              if (gmap) {{
                if (gInfoWindow) gInfoWindow.close();
              }} else {{
                try {{ mk.marker.closePopup(); }} catch (e) {{}}
              }}
            }});

            // Also open the popup when the user explicitly clicks an item.
            div.addEventListener('click', (ev) => {{
              try {{
                const t = ev && ev.target;
                if (t && typeof t.closest === 'function' && t.closest('a')) return;
              }} catch (e) {{}}
              const mk = markerByKey.get(k);
              if (!mk) return;
              if (gmap) {{
                if (!gInfoWindow) gInfoWindow = new google.maps.InfoWindow({{ disableAutoPan: true }});
                gInfoWindow.setContent(mk.popupHtml || '');
                gInfoWindow.open({{ anchor: mk.marker, map: gmap }});
              }} else {{
                try {{ mk.marker.openPopup(); }} catch (e) {{}}
              }}
            }});
          }}
        }}

        // Add one marker per venue group.
        for (const g of pinGroups) {{
          const popupHtml = groupPopupHtml(filtered, g.idxs);
          const firstEventId = `event-${{g.idxs[0]}}`;
          if (gmap) {{
            const first = filtered[g.idxs[0]] || {{}};
            const venueText = (first.venue || (first.location_display || first.location || '').split(',')[0] || '').toString().trim();
            const venueLabel = capFirst(cleanVenueLabel(venueText));
            const marker = new google.maps.Marker({{
              position: {{ lat: g.lat, lng: g.lon }},
              map: gmap,
              // Use venue name for the native hover title (not just the first event).
              title: venueLabel || venueText || '(venue)',
              icon: {{
                path: google.maps.SymbolPath.CIRCLE,
                scale: 7,
                fillColor: PIN_FILL,
                fillOpacity: 1,
                strokeColor: PIN_STROKE,
                strokeWeight: 1,
                strokeOpacity: 1,
              }},
            }});
            // Show popup on hover (no auto-pan), hide on rollout.
            marker.addListener('mouseover', () => {{
              if (!gInfoWindow) gInfoWindow = new google.maps.InfoWindow({{ disableAutoPan: true }});
              gInfoWindow.setContent(popupHtml);
              gInfoWindow.open({{ anchor: marker, map: gmap }});
            }});
            marker.addListener('mouseout', () => {{
              if (gInfoWindow) gInfoWindow.close();
            }});
            marker.addListener('click', () => {{
              setActive(firstEventId, {{ scroll: true }});
              if (!gInfoWindow) gInfoWindow = new google.maps.InfoWindow({{ disableAutoPan: true }});
              gInfoWindow.setContent(popupHtml);
              gInfoWindow.open({{ anchor: marker, map: gmap }});
            }});
            gMarkers.push(marker);
            markerByKey.set(g.key, {{ marker, popupHtml }});
          }} else if (lmap && lMarkersLayer) {{
            const marker = L.circleMarker([g.lat, g.lon], {{
              radius: 7,
              color: PIN_STROKE,
              weight: 1,
              opacity: 1,
              fillColor: PIN_FILL,
              fillOpacity: 1,
            }});
            marker.bindPopup(popupHtml, {{ autoPan: false }});
            marker.on('mouseover', () => {{
              try {{ marker.openPopup(); }} catch (e) {{}}
            }});
            marker.on('mouseout', () => {{
              try {{ marker.closePopup(); }} catch (e) {{}}
            }});
            marker.on('click', () => {{
              setActive(firstEventId, {{ scroll: true }});
            }});
            marker.addTo(lMarkersLayer);
            markerByKey.set(g.key, {{ marker, popupHtml }});
          }}
        }}

        const pinned = pinGroups.length;
        document.getElementById('mapNote').textContent =
          `Venues: ${{pinned}} · ${{filtered.length}} event(s)`;
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
        // Dynamically size map + list to viewport, with safe bounds.
        function updatePanelHeight() {{
          const minH = 420;
          const maxH = 560;
          const header = document.querySelector('header');
          const headerH = header ? header.getBoundingClientRect().height : 120;
          // Reserve space for header + gutters + footer
          const reserve = headerH + 190;
          const h = Math.max(minH, Math.min(maxH, Math.floor(window.innerHeight - reserve)));
          document.documentElement.style.setProperty('--panel-h', `${{h}}px`);
        }}
        updatePanelHeight();
        window.addEventListener('resize', updatePanelHeight, {{ passive: true }});

        const resp = await fetch(`./data/events.json?v=${{encodeURIComponent(BUILD_STAMP)}}`, {{ cache: 'no-cache' }});
        const payload = await resp.json();
        const events = payload.events || [];

        // Build Streamlit-like checkbox filters (all checked by default).
        function mountChecks(containerId, values, metaId) {{
          const root = document.getElementById(containerId);
          if (!root) return;
          root.innerHTML = '';

          function updateRowStates() {{
            for (const lab of Array.from(root.querySelectorAll('label.check'))) {{
              const cb = lab.querySelector('input[type="checkbox"]');
              if (!cb) continue;
              // Do not set state for the "select all" row here.
              if (cb.getAttribute('data-select-all') === '1') continue;
              lab.setAttribute('data-on', cb.checked ? '1' : '0');
            }}
          }}

          function updateSelectAll() {{
            const selAll = root.querySelector('input[type="checkbox"][data-select-all="1"]');
            const boxes = Array.from(root.querySelectorAll('input[type="checkbox"][data-value]'));
            if (!selAll || !boxes.length) return;
            const checkedN = boxes.filter(b => b.checked).length;
            selAll.indeterminate = checkedN > 0 && checkedN < boxes.length;
            selAll.checked = checkedN === boxes.length;

            const selAllLabel = root.querySelector('[data-select-all-label="1"]');
            if (selAllLabel) {{
              selAllLabel.textContent = (checkedN === boxes.length) ? 'Select none' : 'Select all';
            }}

            const metaEl = metaId ? document.getElementById(metaId) : null;
            if (metaEl) {{
              if (checkedN === boxes.length) {{
                metaEl.textContent = 'All';
              }} else if (checkedN === 0) {{
                metaEl.textContent = 'None';
              }} else {{
                const checkedSet = new Set(boxes.filter(b => b.checked).map(b => b.getAttribute('data-value')));
                const ordered = values.filter(v => checkedSet.has(v));
                const maxNames = 3;
                const head = ordered.slice(0, maxNames);
                const rest = Math.max(0, ordered.length - head.length);
                const headText = head.join(', ');
                metaEl.textContent = rest ? `${{headText}} (+${{rest}})` : headText;
              }}
            }}
          }}

          // "Select all" row
          const allLab = document.createElement('label');
          allLab.className = 'check select-all';
          const allCb = document.createElement('input');
          allCb.type = 'checkbox';
          allCb.checked = true;
          allCb.setAttribute('data-select-all', '1');
          allCb.addEventListener('change', () => {{
            const boxes = Array.from(root.querySelectorAll('input[type="checkbox"][data-value]'));
            for (const b of boxes) b.checked = allCb.checked;
            allCb.indeterminate = false;
            updateRowStates();
            updateSelectAll();
            render(events);
          }});
          const allSpan = document.createElement('span');
          allSpan.textContent = 'Select all';
          allSpan.className = 'label';
          allSpan.setAttribute('data-select-all-label', '1');
          allLab.appendChild(allCb);
          allLab.appendChild(allSpan);
          root.appendChild(allLab);

          for (const v of values) {{
            const lab = document.createElement('label');
            lab.className = 'check';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = true;
            cb.setAttribute('data-value', v);
            cb.addEventListener('change', () => {{
              updateRowStates();
              updateSelectAll();
              render(events);
            }});
            const span = document.createElement('span');
            span.textContent = v;
            span.className = 'label';
            lab.appendChild(cb);
            lab.appendChild(span);
            root.appendChild(lab);
          }}

          updateRowStates();
          updateSelectAll();
        }}

        mountChecks('weekdayChecks', {json.dumps(WEEKDAYS)}, 'weekdayMeta');

        const langSet = new Set();
        for (const e of events) {{
          const parts = (e.language || '').toString().split(/[;,]/).map(x => x.trim()).filter(Boolean);
          for (const p of parts) langSet.add(p);
        }}
        const langs = Array.from(langSet).sort((a, b) => a.localeCompare(b));
        mountChecks('languageChecks', langs, 'languageMeta');

        function setOpen(btnId, popId, open) {{
          const btn = document.getElementById(btnId);
          const pop = document.getElementById(popId);
          if (!btn || !pop) return;
          if (open) {{
            pop.removeAttribute('hidden');
            btn.setAttribute('data-open', '1');
            btn.setAttribute('aria-expanded', 'true');
          }} else {{
            pop.setAttribute('hidden','');
            btn.setAttribute('data-open', '0');
            btn.setAttribute('aria-expanded', 'false');
          }}
        }}

        function closeAllPops() {{
          setOpen('weekdayBtn', 'weekdayPop', false);
          setOpen('languageBtn', 'languagePop', false);
        }}

        function togglePop(btnId, popId) {{
          const btn = document.getElementById(btnId);
          const pop = document.getElementById(popId);
          if (!btn || !pop) return;
          btn.addEventListener('click', (ev) => {{
            ev.preventDefault();
            const isOpen = !pop.hasAttribute('hidden');
            closeAllPops();
            if (!isOpen) setOpen(btnId, popId, true);
          }});
        }}
        togglePop('weekdayBtn', 'weekdayPop');
        togglePop('languageBtn', 'languagePop');

        document.addEventListener('click', (ev) => {{
          const t = ev.target;
          const wp = document.getElementById('weekdayPop');
          const lp = document.getElementById('languagePop');
          const wb = document.getElementById('weekdayBtn');
          const lb = document.getElementById('languageBtn');
          const inside = (el) => el && (el === t || (t && el.contains && el.contains(t)));
          if (!inside(wp) && !inside(lp) && !inside(wb) && !inside(lb)) {{
            closeAllPops();
          }}
        }});

        document.addEventListener('keydown', (ev) => {{
          if (ev.key === 'Escape') {{
            closeAllPops();
          }}
        }});

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
    # Normalize weekday lists (e.g. "Tuesday,Friday" -> "Tuesday, Friday").
    if "Weekday" in df.columns:
        df["Weekday"] = df["Weekday"].astype(str).fillna("")
        df["Weekday"] = df["Weekday"].apply(lambda s: ", ".join([p.strip() for p in s.split(",") if p.strip()]))
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
        def _coords_from_entry(entry: dict) -> tuple[float | None, float | None]:
            try:
                return float(entry.get("lat")), float(entry.get("lon"))
            except (TypeError, ValueError):
                return None, None

        # Fallback: the same venue may appear under slightly different `Location` strings
        # (e.g. neighborhood tokens, duplicated "8001 Zürich"). Reuse cached coords by
        # matching venue + ZIP/city in the cache keys.
        venue, _, _ = formatted_loc(loc)
        venue_q = (venue or "").strip().casefold()
        # Allow slight naming differences between cache keys and cleaned venues.
        # e.g. "Auer & Co. Courtyard" vs "Auer & Co., Zürich (CH)"
        venue_variants: list[str] = []
        if venue_q:
            venue_variants.append(venue_q)
            v2 = re.sub(r"\bcourtyard\b", "", venue_q, flags=re.I).strip()
            if v2 and v2 not in venue_variants:
                venue_variants.append(v2)
            v3 = re.sub(r"[^a-z0-9]+", " ", venue_q).strip()
            if v3 and v3 not in venue_variants:
                venue_variants.append(v3)
            v4 = " ".join(v3.split()[:2]).strip() if v3 else ""
            if v4 and len(v4) >= 4 and v4 not in venue_variants:
                venue_variants.append(v4)
        zip_m = re.search(r"\b(8\d{3})\b", loc or "")
        zip_q = zip_m.group(1) if zip_m else ""

        def _score_key(k: str) -> int:
            kk = (k or "").casefold()
            if venue_variants:
                if not any(v and len(v) >= 4 and v in kk for v in venue_variants):
                    return -1
            score = 0
            # Avoid old scrape artefacts like "00 Uhr Venue, 8001 Zürich" which may have
            # wrong coords in the cache.
            if re.match(r"^\s*(?:uhr\s+)?(?:00|15|30|45)\b", kk):
                score -= 25
            if re.search(r"\b(?:00|15|30|45)\s*uhr\b", kk):
                score -= 25
            if venue_variants:
                score += 10
            if zip_q and zip_q in kk:
                score += 10
            if "zürich" in kk or "zurich" in kk:
                score += 2
            # Strongly prefer keys that look like real street addresses.
            has_house_no = bool(re.search(r"\b\d+[a-z]?\b", kk))
            has_street_token = bool(
                re.search(
                    r"\b(strasse|straße|gasse|platz|weg|allee|quai|promenade|ring|ufer|hof|berg|steig|bühl|rain|brücke|bruecke)\b",
                    kk,
                )
            )
            if has_street_token:
                score += 8
            if has_house_no:
                score += 4
            # Slightly prefer more specific keys (street address tends to be longer).
            score += min(len(k), 120) // 30
            return score

        # If there is an exact hit in the cache, still allow a better-scored key to override it.
        # This avoids situations where a low-quality cached string (neighborhood-only or artefact)
        # pins the venue to the wrong place.
        exact_entry = cache.get(loc)
        exact_score = _score_key(loc) if exact_entry else -1
        exact_coords = _coords_from_entry(exact_entry) if exact_entry else (None, None)

        best_key = None
        best_score = -1
        for k, v in cache.items():
            sc = _score_key(k)
            if sc <= best_score:
                continue
            lat, lon = _coords_from_entry(v)
            if lat is None or lon is None:
                continue
            best_key = k
            best_score = sc

        if best_key and best_score >= max(0, exact_score + 2):
            return _coords_from_entry(cache[best_key])
        if exact_entry:
            return exact_coords
        return None, None

    def formatted_loc(loc: str) -> tuple[str, str, str]:
        # Use the cleaned Location string as source of truth for display.
        # This keeps "venue" + "address" stable (and LLM-improved) instead of relying on
        # Nominatim's sometimes-odd display_name formatting.
        s = _norm(loc)
        if not s:
            return ("", "", "")
        parts = [p.strip() for p in s.split(",") if p.strip()]
        # Drop trailing country tokens if present.
        while parts and parts[-1].strip().lower() in {"ch", "che", "schweiz", "switzerland"}:
            parts.pop()
        if not parts:
            return ("", "", s)
        venue = parts[0]
        address = ", ".join(parts[1:]).strip() if len(parts) > 1 else ""
        if address:
            a_parts = [p.strip() for p in address.split(",") if p.strip()]
            # De-duplicate exact repeats while preserving order.
            seen = set()
            deduped: list[str] = []
            for p in a_parts:
                key = p.casefold()
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(p)
            # Prefer "800x Zürich" over "Zürich 800x" when both appear.
            has_zip_city = any(re.fullmatch(r"8\d{3}\s+zürich", p, flags=re.I) for p in deduped)
            if has_zip_city:
                deduped = [p for p in deduped if not re.fullmatch(r"zürich\s+8\d{3}", p, flags=re.I)]
            address = ", ".join(deduped).strip()
        location_display = ", ".join(x for x in [venue, address] if x).strip() or s

        # If we only have "800x Zürich" (no street), try to improve address from the geocode cache
        # but only accept it when it clearly contains a street + house number.
        if venue and re.fullmatch(r"8\d{3}\s+zürich", (address or "").strip(), flags=re.I):
            entry = cache.get(s) or {}
            dn = entry.get("display_name")
            if isinstance(dn, str) and dn.strip():
                v2, a2, ld2 = _format_address_from_display_name(venue_hint=venue, display_name=dn)
                a2n = _norm(a2)
                if a2n:
                    has_house_no = bool(re.search(r"\b\d+[a-z]?\b", a2n, re.I))
                    has_street_token = bool(
                        re.search(
                            r"\b(strasse|straße|gasse|platz|weg|allee|quai|promenade|ring|ufer|hof|berg|steig|bühl|rain|brücke|bruecke)\b",
                            a2n,
                            re.I,
                        )
                    )
                    has_zip = bool(re.search(r"\b8\d{3}\b", a2n))
                    if has_zip and has_house_no and has_street_token:
                        return (_norm(v2) or venue, a2n, _norm(ld2) or location_display)

        return (venue, address, location_display)

    events = []
    missing_coords = 0
    for _, row in df.iterrows():
        loc_raw = _norm(row.get("Location", ""))
        lat, lon = coord_for(loc_raw)
        venue, address, location_display = formatted_loc(loc_raw)
        weekdays = [p.strip() for p in str(row.get("Weekday", "") or "").split(",") if p.strip()]
        if not weekdays:
            weekdays = [""]
        for wd in weekdays:
            if lat is None or lon is None:
                missing_coords += 1
            events.append(
                {
                    "weekday": _norm(wd),
                    "location": _norm(location_display) or loc_raw,
                    "venue": _norm(venue),
                    "address": _norm(address),
                    "location_display": _norm(location_display),
                    "time": _norm(row.get("Time", "")),
                    "cost": _norm(row.get("Cost", "")),
                    "language": _norm(row.get("Comedy_language", "")),
                    "regularity": _norm(row.get("Regularity", "")),
                    "title": _norm(row.get("Event_title", "")),
                    "url": _norm(row.get("URL", "")),
                    "image_url": _norm(row.get("Image_url", "")),
                    "lat": lat,
                    "lon": lon,
                }
            )

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    build_stamp = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    listing_disp, listing_iso = pipeline_meta.latest_listing_scraped_meta(ROOT / "data" / "processed")
    site_data_date_display = listing_disp or now_utc.strftime("%d/%m/%Y")

    def _venue_id(*, venue: str, address: str, lat: float | None, lon: float | None) -> str:
        key = "|".join(
            [
                _norm(venue).casefold(),
                _norm(address).casefold(),
                f"{float(lat):.6f}" if isinstance(lat, (int, float)) else "",
                f"{float(lon):.6f}" if isinstance(lon, (int, float)) else "",
            ]
        )
        h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        return f"v_{h}"

    def _show_id(*, url: str, title: str, venue_id: str) -> str:
        u = _norm(url)
        if u:
            h = hashlib.sha1(u.encode("utf-8")).hexdigest()[:12]
            return f"s_{h}"
        key = "|".join([_norm(title).casefold(), venue_id])
        h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        return f"s_{h}"

    venues_by_id: dict[str, dict] = {}
    occurrences: list[dict] = []
    for e in events:
        vid = _venue_id(venue=e.get("venue", ""), address=e.get("address", ""), lat=e.get("lat"), lon=e.get("lon"))
        if vid not in venues_by_id:
            venues_by_id[vid] = {
                "venue_id": vid,
                "venue": _norm(e.get("venue", "")),
                "address": _norm(e.get("address", "")),
                "location_display": _norm(e.get("location_display", "")),
                "lat": e.get("lat"),
                "lon": e.get("lon"),
            }
        sid = _show_id(url=e.get("url", ""), title=e.get("title", ""), venue_id=vid)
        occurrences.append(
            {
                "show_id": sid,
                "venue_id": vid,
                "weekday": _norm(e.get("weekday", "")),
                "time": _norm(e.get("time", "")),
                "cost": _norm(e.get("cost", "")),
                "language": _norm(e.get("language", "")),
                "regularity": _norm(e.get("regularity", "")),
                "title": _norm(e.get("title", "")),
                "url": _norm(e.get("url", "")),
                "image_url": _norm(e.get("image_url", "")),
            }
        )

    DOCS_VENUES_JSON.write_text(
        json.dumps(
            {
                "generated_at": now_utc.isoformat(),
                "build_stamp": build_stamp,
                "listing_scraped_at": listing_iso,
                "data_updated_display": site_data_date_display,
                "venues_total": len(venues_by_id),
                "venues": list(venues_by_id.values()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    DOCS_OCCURRENCES_JSON.write_text(
        json.dumps(
            {
                "generated_at": now_utc.isoformat(),
                "build_stamp": build_stamp,
                "listing_scraped_at": listing_iso,
                "data_updated_display": site_data_date_display,
                "occurrences_total": len(occurrences),
                "occurrences": occurrences,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    DOCS_EVENTS_JSON.write_text(
        json.dumps(
            {
                "generated_at": now_utc.isoformat(),
                "generated_from": str(CSV_PATH.as_posix()),
                "build_stamp": build_stamp,
                "listing_scraped_at": listing_iso,
                "data_updated_display": site_data_date_display,
                "events_total": len(events),
                "events_missing_coords": missing_coords,
                "events": events,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    # Embed placeholder SVG as data URL for static site thumbnails
    placeholder_data_url = ""
    try:
        svg = PLACEHOLDER_SVG.read_bytes()
        placeholder_data_url = "data:image/svg+xml;base64," + base64.b64encode(svg).decode("ascii")
    except OSError:
        placeholder_data_url = ""

    _write_index_html(
        DOCS_DIR / "index.html",
        build_stamp=build_stamp,
        site_data_date_display=site_data_date_display,
        placeholder_data_url=placeholder_data_url,
    )
    (DOCS_DIR / ".nojekyll").write_text("", encoding="utf-8")

    print(f"[export-site] Wrote {DOCS_DIR / 'index.html'}")
    print(f"[export-site] Wrote {DOCS_EVENTS_JSON} ({len(events)} events; missing coords: {missing_coords})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

