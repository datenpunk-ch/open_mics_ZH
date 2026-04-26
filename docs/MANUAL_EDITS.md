# Manual edits (venues + coordinates)

This repo has a generated dataset (`docs/data/events.json`) and a manual venue layer (`docs/data/venues_manual.json`) for persistent venue name/address fixes and for the article’s **data status** row.

## When to edit which file

- **Quick, direct fix (will be overwritten by future exports)**: edit `docs/data/events.json`
  - Good for: one-off corrections right now.
  - Risk: a future `pixi run export-site` will regenerate `docs/data/events.json` and may overwrite manual fixes.

- **Persistent venue corrections + article status (recommended)**: edit `docs/data/venues_manual.json`
  - Use this to fix a venue once (name/address/display), and have it apply everywhere after each rebuild.
  - This file is merged into `docs/data/venues.json` by `src/export_site.py` on every export.
  - Optional top-level **`data_tag`** / **`data_tag_note`**: `data_tag` of `incomplete` or `stale` highlights the address-fixes card on the article; **`data_tag_note`** (if set) is shown as a short note under the stats.
  - If you set **`address`** or **`location_display`** and omit **`lat`** / **`lon`**, export runs a forward geocode (Nominatim), updates the shared cache at `data/processed/location_geocache.json`, and copies the resolved coords plus venue text onto matching rows in **`docs/data/events.json`** (about one second between network calls per venue).
  - To pin coordinates yourself, set **`lat`** and **`lon`** in the manual block; export will not geocode that venue.

## Editing venues persistently (`docs/data/venues_manual.json`)

Format:

```json
{
  "updated_at": "2026-04-26",
  "data_tag": "",
  "data_tag_note": "",
  "venues": {
    "v_970c36d42823": {
      "venue": "Monroe",
      "address": "Brauerstrasse 26, 8004 Zürich",
      "location_display": "Monroe, Brauerstrasse 26, 8004 Zürich"
    }
  }
}
```

Optional: merge two venue IDs when the exporter created duplicates:

```json
{
  "venues": {
    "v_old123": { "merge_into": "v_keep456" }
  }
}
```

## Flagging scrape completeness in the article

In **`docs/data/venues_manual.json`** set **`data_tag`** / **`data_tag_note`** next to your venue overrides (you can leave **`venues`** unchanged if you only want a status message):

```json
{
  "updated_at": "2026-04-23",
  "data_tag": "incomplete",
  "data_tag_note": "Eventfrog search missed at least one known series.",
  "venues": {}
}
```

Supported `data_tag` values currently used by the article:
- `incomplete` → shows **“Scrape incomplete”**
- `modified` → shows **“Manual edits”**
- `stale` → shows **“Possibly stale”**

The article’s **“Data status & venue overrides”** row reads **`venues_manual.json`** plus **`venues.json`**: it shows **how many exported venues have an address/location_display patch**, and **how many exported venues had no manual row** (“fine”). Optional **`data_tag_note`** appears as text under those two cards.

## Editing an address in `docs/data/events.json`

For an existing event, update **all three** fields so UI/search stay consistent:
- `address`
- `location_display`
- `location`

Example:

```json
"address": "Sihlquai 131, 8005 Zürich",
"location_display": "Auer & Co. Courtyard, Sihlquai 131, 8005 Zürich",
"location": "Auer & Co. Courtyard, Sihlquai 131, 8005 Zürich"
```

If the venue actually moved, also update `lat` and `lon`.

## Updating lat/lon after address changes (geocoding)

There is a helper script:

- `src/update_docs_event_coords.py`

It updates **only** `lat`/`lon` for events in `docs/data/events.json`. Your manual text edits (address/title/etc.) are not removed.
It also updates the shared geocache at `data/processed/location_geocache.json`.

### Recommended: use Pixi (preferred in this repo)

```powershell
pixi run python .\src\update_docs_event_coords.py --force
```

### What the script prints (how to read it)

The script always loops over events and tries multiple queries (cache first, then Nominatim if needed).
The final summary line looks like:

```text
Done. total=15 filtered=0 processed=15 changed=1 unchanged=14 failed=0 queried=12 file=...\docs\data\events.json
```

- **total**: number of event objects in `docs/data/events.json`
- **filtered**: events excluded because you used `--match-url` or you didn’t pass `--force` and coords already existed
- **processed**: events actually attempted (built candidate queries, then cache/Nominatim lookup)
- **changed**: events whose `lat`/`lon` were updated
- **unchanged**: events where geocoding succeeded but resulted in the same `lat`/`lon` as before
- **failed**: events where no geocode result was found (you’ll also see `[WARN] ...` lines)
- **queried**: number of actual Nominatim requests made (cache hits don’t count)

### If you have the Windows Python launcher

```powershell
py .\src\update_docs_event_coords.py --force
```

### Update just one event (recommended for small changes)

```powershell
pixi run python .\src\update_docs_event_coords.py --force --match-url "<EVENT_URL>"
```

### Sanity check: “all events have the same coords”

If the map looks wrong or you suspect coordinates got overwritten, you can quickly check how many unique coordinate pairs exist:

```powershell
pixi run python -c "import json; d=json.load(open('docs/data/events.json','r',encoding='utf-8')); coords=[(e.get('lat'),e.get('lon')) for e in d.get('events',[])]; print('events',len(coords)); print('unique_coords',len(set(coords)))"
```

Note: some repeats are normal when the dataset contains duplicate events (e.g. the same event URL listed for multiple weekdays).

