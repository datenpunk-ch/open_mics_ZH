"""Allow ``python -m scrapers <subcommand> …`` from the repo root."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
