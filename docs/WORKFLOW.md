# Project workflow (end‑to‑end)

This repo has two “outputs”:

- **Interactive app (local)**: Streamlit app in `src/open_mics_app.py`
- **Static website (GitHub Pages)**: files in `docs/` (reads `docs/data/events.json`)

## Quickstart (recommended: Pixi)

### 1) Install dependencies

- Install **Pixi** (once per machine), then in this repo:

```powershell
pixi install
pixi run playwright-browsers
```

### 2) Run the local app (no rebuild)

```powershell
pixi run app
```

## Data pipeline (scrape → process → geocode → export site)

### Option A: one command (recommended)

This runs: listing → enrich → flatten → geocode → export static site.

```powershell
pixi run rebuild-site
```

#### Configuring sources

Active listing sources are defined in `docs/Quellenliste.md` via fenced ` ```source``` ` blocks.
The scraper reads that file at runtime (see `scrapers/sources.py`).

Windows wrappers:

```powershell
.\rebuild_app.ps1
```

```bat
rebuild_app.cmd
```

To rebuild and then start the app:

```powershell
.\rebuild_app.ps1 -App
```

### Option B: run steps manually (useful for debugging)

```powershell
# 1) Scrape + process (uses sources from docs/Quellenliste.md by default)
pixi run collect

# Or individual steps
pixi run listing
pixi run enrich
pixi run flatten

# 2) Build-time geocoding cache for locations (writes data/processed/location_geocache.json)
pixi run geocode

# 3) Export static site to docs/ (writes docs/index.html + docs/data/events.json)
pixi run export-site
```

## Static site (GitHub Pages)

### What gets deployed

- `docs/index.html` (article landing page)
- `docs/map.html` (interactive tool)
- `docs/data/events.json` (event data for the tool)
- `docs/data/venues.json` + `docs/data/occurrences.json` (deduped venue model; preferred by the map)
- `docs/data/events_manual.json` (optional manual metadata + extra events; merged at runtime by the article)
- `docs/data/venues_manual.json` (manual venue overrides; merged by export-site into venues.json)

### Preview locally

Any static server works. Examples:

```powershell
pixi run python -m http.server --directory docs 8000
```

Then open `http://localhost:8000/`.

### Deploy (GitHub Pages)

In GitHub: **Settings → Pages → Deploy from a branch**, set folder to **`/docs`**.

## Manual corrections / overrides (addresses, extra events, status flag)

Manual editing workflow is documented in:

- `docs/MANUAL_EDITS.md`

This includes:
- where to store a persistent “scrape incomplete / modified” flag (`docs/data/events_manual.json`)
- how to correct addresses in `docs/data/events.json`
- how to recompute coordinates (`src/update_docs_event_coords.py`)

## Troubleshooting

### “Python was not found” on Windows

Use Pixi instead of `python`:

```powershell
pixi run python .\src\update_docs_event_coords.py --force
```

### Playwright errors (missing browser)

```powershell
pixi run playwright-browsers
```

