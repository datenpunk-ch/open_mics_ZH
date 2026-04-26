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
    # Cleanup: remove older listing artifacts (new crawl each run).
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

