# Manual edits (events + coordinates)

This repo has a generated dataset (`docs/data/events.json`) and an optional manual sidecar (`docs/data/events_manual.json`).

## When to edit which file

- **Quick, direct fix (will be overwritten by future exports)**: edit `docs/data/events.json`
  - Good for: one-off corrections right now.
  - Risk: a future `pixi run export-site` will regenerate `docs/data/events.json` and may overwrite manual fixes.

- **Persistent “status flag” for the article**: edit `docs/data/events_manual.json`
  - Use this to record that the scrape/export is incomplete or that manual changes were applied.
  - The article reads `data_tag` from `events_manual.json` and shows it as a **status card** (no hover needed).

## Flagging scrape completeness in the article

In `docs/data/events_manual.json` set:

```json
{
  "_comment": "Manual metadata + optional hand-added events. This file is merged into data/events.json at runtime by docs/index.html.",
  "data_tag": "incomplete",
  "data_tag_note": "",
  "updated_at": "2026-04-23",
  "events": []
}
```

Supported `data_tag` values currently used by the article:
- `incomplete` → shows **“Scrape incomplete”**
- `modified` → shows **“Manual edits”**
- `stale` → shows **“Possibly stale”**

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

