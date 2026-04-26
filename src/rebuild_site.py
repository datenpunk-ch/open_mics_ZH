#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

# Ensure repo root is on sys.path so top-level modules import correctly when
# invoked as: python src/rebuild_site.py
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.sources import LISTING_SOURCES  # noqa: E402


def _run(args: list[str]) -> None:
    proc = subprocess.run(args)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def main() -> int:
    # End-to-end rebuild:
    # listing (all configured sources) -> enrich -> flatten -> geocode -> export-site
    # Keep the source list explicit so it's reproducible.
    # Increase navigation timeout to tolerate slow pages.
    #
    # Cleanup: remove older generated artifacts (new crawl each run).
    root = Path(__file__).resolve().parents[1]
    raw_dir = root / "data" / "raw"
    if raw_dir.is_dir():
        for p in raw_dir.glob("*listing_*.json"):
            try:
                p.unlink()
            except OSError:
                pass
        for p in raw_dir.glob("merged_listing_*.json"):
            try:
                p.unlink()
            except OSError:
                pass

    # Processed outputs are derived from the crawl; remove stale versions so a failed
    # run can't leave old results looking "fresh".
    processed_dir = root / "data" / "processed"
    if processed_dir.is_dir():
        for p in processed_dir.glob("events_enriched_*.json"):
            try:
                p.unlink()
            except OSError:
                pass
        for p in (processed_dir / "events_flat.csv",):
            try:
                if p.is_file():
                    p.unlink()
            except OSError:
                pass
        # Keep location_geocache.json (cache) unless user explicitly wants a full reset.

    # Exported docs/data outputs are generated; remove stale ones.
    docs_data_dir = root / "docs" / "data"
    if docs_data_dir.is_dir():
        for name in ("events.json", "venues.json", "occurrences.json"):
            p = docs_data_dir / name
            try:
                if p.is_file():
                    p.unlink()
            except OSError:
                pass
        # Keep venues_manual.json (manual venue layer + optional article status fields).

    source_ids = sorted(LISTING_SOURCES.keys())
    if not source_ids:
        print("No listing sources configured (Quellenliste.md has no ```source``` blocks).", file=sys.stderr)
        return 2
    _run(
        [
            "pixi",
            "run",
            "python",
            "-m",
            "scrapers",
            "run",
            "--timeout-ms",
            "180000",
            "--source",
            *source_ids,
        ]
    )
    _run(["pixi", "run", "geocode"])
    _run(["pixi", "run", "export-site"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

