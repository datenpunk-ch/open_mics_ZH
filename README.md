# Open Mics Zurich

Where (and when) can you laugh in Zürich — or get on stage yourself?

## Goal
- Collect open mic events
- Integrate data from multiple sources
- List recurring events and visualise them
  - Weekday, Location, Time, Cost, Comedy_language

## Structure
- `scrapers/`: data collection
- `data/`: raw and processed data
- `src/`: analysis + app code
- `docs/`: static site + notes/methodology
- `src/collect_data.py`: **one script** — uses **Pixi** (`.pixi/envs/default`) if `[tool.pixi.workspace]` is set in `pyproject.toml` and the environment exists / `pixi install` is available; otherwise use `.venv` + `requirements.txt`
- **Without arguments** (or `pixi run collect`): full run — listing → enriched JSON → **`data/processed/events_flat.csv`** (includes Weekday, Location, Time, Cost, **Comedy_language**, Regularity, **Event_title**, URL; same series across multiple dates → Regularity “recurring”). **Comedy_language** = on-stage language (not inferred from `/en/` etc. in the URL)
- Listing JSON only (fewer detail pages): `python src/collect_data.py listing` or `pixi run listing`
- Individual steps still work: `python -m scrapers enrich` / `flatten` (without `--from` / `-i` = uses the latest file under `data/raw` / `data/processed`)
- IDE interpreter (Cursor/VS Code): on Windows e.g. `.pixi/envs/default/python.exe`, on Linux/macOS `.pixi/envs/default/bin/python` (after `pixi install`; adjust `.vscode/settings.json` if needed)

## Source code & contributors

- **Repository:** [github.com/datenpunk-ch/open_mics_ZH](https://github.com/datenpunk-ch/open_mics_ZH)
- **Organisation / contributor:** [datenpunk-ch](https://github.com/datenpunk-ch)

## Visualisation (local web app)

Interactive view with filters (weekday/language), event list, and map.

- **With Pixi**:
  - `pixi run app`
  - or: `pixi run start-app`
- **Windows “one script”**:
  - PowerShell: `.\start_app.ps1` (or `.\src\start_app.ps1`)
  - CMD: `start_app.cmd` (or `src\start_app.cmd`)
- **Without Pixi**:
  - `python -m pip install -r requirements.txt`
  - `streamlit run src/open_mics_app.py`

## Website (GitHub Pages, static)

For embedding/hosting (e.g. GitHub Pages) there is a **static** version under `docs/`.
It loads `docs/data/events.json` and shows map + filters + event list (no server, no runtime geocoding).

- **Project workflow (end-to-end)**: see `docs/WORKFLOW.md`
- **Manual edits workflow**: see `docs/MANUAL_EDITS.md`

- **Build-time geocoding (once / when new locations appear)**:
  - `pixi run geocode`
- **Export the static site**:
  - `pixi run export-site`

Then in GitHub under **Settings → Pages**, choose **“Deploy from a branch”** and set the folder to **`/docs`**.

## Tech specs

### Languages & runtimes
- **Python**: primary language for scraping, processing, geocoding, and exporting the static site.
- **Streamlit (Python)**: optional local interactive app (`src/open_mics_app.py`).
- **Static web (HTML/CSS/JavaScript)**: GitHub Pages site under `docs/` (no backend).

### Core components
- **Listing scraper (Playwright + per-site extractors)**: discovers event URLs from configured listing pages (`scrapers/`).
- **Detail-page enrich**: fetches each event page once and extracts metadata (LD+JSON + meta tags + visible text) into enriched JSON.
- **Flatten step**: converts enriched JSON into `data/processed/events_flat.csv`.
- **Geocoding (Nominatim + cache)**: resolves locations to coordinates at build time and stores results in `data/processed/location_geocache.json` (Zürich-bounded search).
- **Static site export**: writes `docs/index.html`, `docs/map.html`, and datasets under `docs/data/`.

### Data artifacts (inputs/outputs)
- **Raw**: `data/raw/*listing*.json` (listing results), `data/raw/merged_listing_*.json` (merged listing run).
- **Processed**: `data/processed/events_enriched_*.json`, `data/processed/events_flat.csv`, `data/processed/location_geocache.json`.
- **Static site data**: `docs/data/events.json` (back-compat), plus `docs/data/venues.json` + `docs/data/occurrences.json` (deduped venue model).
- **Manual sidecar**: `docs/data/events_manual.json` (optional annotations/extra events; merged at runtime by the static site).

### Configuration
- **Source definitions**: `docs/Quellenliste.md` (parsed from fenced ` ```source ``` ` blocks).
- **Rules**: `config/rules.json` (geocoding preferences and other general rules; Zürich constraints are expected).
