#!/usr/bin/env python3
"""CLI entry for the scrapers package.

From the repository root:

  python -m scrapers.scrape list-sources
  python -m scrapers.scrape listing --source eventfrog_de
  python -m scrapers.scrape event-page --url https://...

Or (same behaviour):

  python -m scrapers list-sources
  …
"""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
