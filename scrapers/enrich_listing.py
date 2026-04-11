"""Load a listing JSON and fetch detail page for each event URL (one browser session)."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from .event_page import _norm_url, scrape_event_urls_batch


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def find_latest_listing_json(raw_dir: Path) -> Path | None:
    """Newest ``*listing*.json`` under ``data/raw``."""
    if not raw_dir.is_dir():
        return None
    candidates = sorted(
        raw_dir.glob("*listing*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def enrich_listing_payload(
    listing: dict,
    *,
    headless: bool,
    timeout_ms: int,
    delay_s: float,
    limit: int | None,
) -> dict:
    events: list[dict] = list(listing.get("events") or [])
    if limit is not None:
        events = events[:limit]

    urls = [e["url"] for e in events if e.get("url")]
    details = scrape_event_urls_batch(
        urls,
        headless=headless,
        timeout_ms=timeout_ms,
        delay_s=delay_s,
    )
    by_url = {d["url"]: d for d in details}

    merged: list[dict] = []
    for ev in events:
        u = ev.get("url") or ""
        key = _norm_url(u) if u else ""
        merged.append({**ev, "detail": by_url.get(key)})

    return {
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "listing_scraped_at": listing.get("scraped_at"),
        "listing_source": listing.get("source"),
        "listing_source_url": listing.get("source_url"),
        "event_count": len(merged),
        "events": merged,
    }


def cmd_enrich(args: argparse.Namespace) -> int:
    root = _project_root()
    raw_dir = root / "data" / "raw"

    if args.from_file:
        in_path = Path(args.from_file)
        if not in_path.is_absolute():
            in_path = root / in_path
    else:
        in_path = find_latest_listing_json(raw_dir)
        if in_path is None:
            print("No listing JSON found under data/raw (*listing*.json). Use --from PATH.", file=sys.stderr)
            return 2

    if not in_path.is_file():
        print(f"Not a file: {in_path}", file=sys.stderr)
        return 2

    listing = json.loads(in_path.read_text(encoding="utf-8"))
    if not isinstance(listing.get("events"), list):
        print("Input JSON has no 'events' list (not a listing export?).", file=sys.stderr)
        return 2

    print(f"[enrich] From: {in_path}")
    print(f"[enrich] Events to fetch: {len(listing['events'])} (limit={args.limit!r})\n")

    out = enrich_listing_payload(
        listing,
        headless=not args.headed,
        timeout_ms=args.timeout_ms,
        delay_s=args.delay,
        limit=args.limit,
    )

    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = root / out_path
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        proc = root / "data" / "processed"
        proc.mkdir(parents=True, exist_ok=True)
        out_path = proc / f"events_enriched_{stamp}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[enrich] Wrote {out_path} ({out['event_count']} events)")
    return 0


def build_enrich_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Enrich listing JSON with per-event detail pages.")
    p.add_argument(
        "--from",
        dest="from_file",
        default="",
        help="Listing JSON path (default: newest data/raw/*listing*.json)",
    )
    p.add_argument(
        "-o",
        "--output",
        default="",
        help="Output JSON (default: data/processed/events_enriched_<utc>.json)",
    )
    p.add_argument("--headed", action="store_true", help="Show browser")
    p.add_argument("--timeout-ms", type=int, default=60_000)
    p.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Seconds between page loads (be nice to the server)",
    )
    p.add_argument("--limit", type=int, default=None, help="Only first N events (debug)")
    p.set_defaults(func=cmd_enrich)
    return p


def main(argv: list[str] | None = None) -> int:
    p = build_enrich_parser()
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
