#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys


def _run(args: list[str]) -> None:
    proc = subprocess.run(args)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def main() -> int:
    # End-to-end rebuild:
    # listing (all configured sources) -> enrich -> flatten -> geocode -> export-site
    # Keep the source list explicit so it's reproducible.
    # Increase navigation timeout to tolerate slow pages (e.g. gz-zh.ch).
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
            "eventfrog",
            "eventfrog_de",
            "zuerich_com_event_finder",
            "stubae_comedy",
        ]
    )
    _run(["pixi", "run", "geocode"])
    _run(["pixi", "run", "export-site"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

