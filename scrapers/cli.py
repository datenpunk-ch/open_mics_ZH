"""Unified CLI: listing sources + generic event page."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from .enrich_listing import cmd_enrich
from .flatten_events import cmd_flatten
from .event_page import run_event_page_to_file
from .listing_runner import run_listing_to_file
from .sources import LISTING_SOURCES


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def cmd_list_sources(_args: argparse.Namespace) -> int:
    for sid in sorted(LISTING_SOURCES):
        s = LISTING_SOURCES[sid]
        print(f"{sid}\t{s.extractor}\t{s.label}")
        print(f"  default: {s.default_listing_url}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Listing → neuestes Listing anreichern → flache CSV aus neuem enriched."""
    root = _project_root()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    listing_ns = argparse.Namespace(
        source=list(args.source),
        url=args.url or "",
        output=args.listing_output or "",
        headed=args.headed,
        timeout_ms=args.timeout_ms,
        max_show_more=args.max_show_more,
        show_more_delay_ms=args.show_more_delay_ms,
        utc_stamp=stamp,
    )
    rc = cmd_listing(listing_ns)
    if rc != 0:
        return rc

    # Merge all listing payloads from this run into one listing JSON for enrich.
    merged_listing_path = root / "data" / "raw" / f"merged_listing_{stamp}.json"
    merged_events: list[dict] = []
    seen: set[str] = set()
    for sid in listing_ns.source:
        p = root / "data" / "raw" / f"{sid}_listing_{stamp}.json"
        if not p.is_file():
            continue
        payload = json.loads(p.read_text(encoding="utf-8"))
        for ev in payload.get("events") or []:
            u = (ev.get("url") or "").strip()
            if not u or u in seen:
                continue
            seen.add(u)
            merged_events.append(ev)
    merged_listing = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "merged",
        "source_label": "Merged listing sources",
        "extractor": "merged",
        "source_url": "",
        "event_count": len(merged_events),
        "events": merged_events,
        "meta": {"stamp": stamp, "sources": list(listing_ns.source)},
    }
    merged_listing_path.parent.mkdir(parents=True, exist_ok=True)
    merged_listing_path.write_text(json.dumps(merged_listing, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n[run] ——— Detailseiten laden (enrich) ———\n", flush=True)
    enrich_ns = argparse.Namespace(
        from_file=str(merged_listing_path),
        output=args.enrich_output or "",
        headed=args.headed,
        timeout_ms=args.timeout_ms,
        delay=args.delay,
        limit=args.limit,
        venue_llm=bool(getattr(args, "venue_llm", False)),
    )
    rc = cmd_enrich(enrich_ns)
    if rc != 0:
        return rc

    print("\n[run] ——— CSV schreiben (flatten) ———\n", flush=True)
    flat_ns = argparse.Namespace(
        input="",
        output=args.csv_output or "",
    )
    return int(cmd_flatten(flat_ns))


def cmd_listing(args: argparse.Namespace) -> int:
    root = _project_root()
    sources: list[str] = list(args.source)
    stamp = getattr(args, "utc_stamp", "") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if args.url and len(sources) > 1:
        print(
            "Error: --url is only allowed with a single --source "
            "(each source has its own default URL).",
            file=sys.stderr,
        )
        return 2

    out_arg = Path(args.output) if args.output else None
    if out_arg and not out_arg.is_absolute():
        out_arg = root / out_arg

    if len(sources) > 1 and out_arg is not None:
        if out_arg.suffix.lower() == ".json":
            print(
                "Error: with multiple --source, -o/--output must be a directory "
                "(not a .json file). Example: -o data/raw",
                file=sys.stderr,
            )
            return 2
        out_dir = out_arg
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = None

    rc = 0
    for sid in sources:
        if len(sources) > 1 and out_dir is not None:
            safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in sid)
            out_path = out_dir / f"{safe}_listing_{stamp}.json"
        elif len(sources) == 1 and out_arg is not None:
            if out_arg.suffix.lower() == ".json":
                out_path = out_arg
            else:
                safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in sid)
                out_arg.mkdir(parents=True, exist_ok=True)
                out_path = out_arg / f"{safe}_listing_{stamp}.json"
        else:
            out_path = None

        path, payload = run_listing_to_file(
            root,
            sid,
            listing_url=args.url or None,
            output_path=out_path,
            headless=not args.headed,
            timeout_ms=args.timeout_ms,
            max_show_more=args.max_show_more,
            show_more_delay_ms=args.show_more_delay_ms,
            utc_stamp=stamp,
        )
        print(f"[{sid}] Wrote {payload.get('event_count', 0)} events to {path}")
        meta = payload.get("meta") or {}
        if meta.get("errors"):
            print(
                f"[{sid}] Warnings:",
                "; ".join(meta["errors"]),
                file=sys.stderr,
            )
            rc = 1
    return rc


def cmd_event_page(args: argparse.Namespace) -> int:
    root = _project_root()
    out = Path(args.output) if args.output else None
    if out and not out.is_absolute():
        out = root / out
    path = run_event_page_to_file(
        root,
        args.url,
        output_path=out,
        headless=not args.headed,
        timeout_ms=args.timeout_ms,
    )
    print(f"Wrote {path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape Open Mics sources (listings + generic event pages).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list-sources", help="Print configured listing sources")
    p_list.set_defaults(func=cmd_list_sources)

    p_l = sub.add_parser("listing", help="Scrape one or more configured listing sources")
    p_l.add_argument(
        "--source",
        nargs="+",
        required=True,
        choices=sorted(LISTING_SOURCES),
        metavar="SOURCE",
        help="Source id(s); repeat values allowed, e.g. --source eventfrog_de",
    )
    p_l.add_argument(
        "--url",
        default="",
        help="Override listing URL (only if exactly one --source)",
    )
    p_l.add_argument(
        "-o",
        "--output",
        default="",
        help="Output: .json file, or directory (writes <source>_listing_<stamp>.json inside)",
    )
    p_l.add_argument("--headed", action="store_true", help="Show browser")
    p_l.add_argument("--timeout-ms", type=int, default=60_000)
    p_l.add_argument(
        "--max-show-more",
        type=int,
        default=28,
        help="Eventfrog: wie oft „Show more“ geklickt wird (mehr ≈ längere Laufzeit)",
    )
    p_l.add_argument("--show-more-delay-ms", type=int, default=1800)
    p_l.set_defaults(func=cmd_listing)

    p_e = sub.add_parser("event-page", help="Scrape one event/detail URL (any host)")
    p_e.add_argument("--url", required=True)
    p_e.add_argument("-o", "--output", default="")
    p_e.add_argument("--headed", action="store_true")
    p_e.add_argument("--timeout-ms", type=int, default=60_000)
    p_e.set_defaults(func=cmd_event_page)

    p_en = sub.add_parser(
        "enrich",
        help="Fetch detail page per event from a listing JSON (ld+json, titles, …)",
    )
    p_en.add_argument(
        "--from",
        dest="from_file",
        default="",
        help="Listing JSON path (default: newest data/raw/*listing*.json)",
    )
    p_en.add_argument(
        "-o",
        "--output",
        default="",
        help="Output JSON (default: data/processed/events_enriched_<utc>.json)",
    )
    p_en.add_argument("--headed", action="store_true", help="Show browser")
    p_en.add_argument("--timeout-ms", type=int, default=60_000)
    p_en.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Seconds between page loads",
    )
    p_en.add_argument("--limit", type=int, default=None, help="Only first N events (debug)")
    p_en.set_defaults(func=cmd_enrich)

    p_fl = sub.add_parser(
        "flatten",
        help="CSV (Weekday, Location, Time, Cost, Comedy_language, Regularity, Event_title, URL, …) from events_enriched*.json",
    )
    p_fl.add_argument(
        "-i",
        "--input",
        default="",
        help="events_enriched*.json (Standard: neueste unter data/processed/)",
    )
    p_fl.add_argument(
        "-o",
        "--output",
        default="",
        help="CSV (Standard: data/processed/events_flat.csv)",
    )
    p_fl.set_defaults(func=cmd_flatten)

    p_run = sub.add_parser(
        "run",
        help="Kompletter Ablauf: Listing scrapen → anreichern → CSV (events_flat.csv)",
    )
    p_run.add_argument(
        "--source",
        nargs="+",
        default=["eventfrog_de"],
        choices=sorted(LISTING_SOURCES),
        metavar="SOURCE",
        help="Listing-Quelle(n); Standard: eventfrog_de",
    )
    p_run.add_argument(
        "--url",
        default="",
        help="Listing-URL überschreiben (nur bei genau einer --source)",
    )
    p_run.add_argument(
        "--listing-output",
        default="",
        help="Ausgabe Listing-JSON oder Zielordner (wie bei „listing“)",
    )
    p_run.add_argument("--headed", action="store_true", help="Browser sichtbar")
    p_run.add_argument("--timeout-ms", type=int, default=60_000)
    p_run.add_argument("--max-show-more", type=int, default=28)
    p_run.add_argument("--show-more-delay-ms", type=int, default=1800)
    p_run.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Sekunden zwischen Detailseiten beim Anreichern",
    )
    p_run.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nur die ersten N Events anreichern (z. B. Test)",
    )
    p_run.add_argument(
        "--enrich-output",
        default="",
        help="Pfad für events_enriched*.json (Standard: data/processed/events_enriched_<Zeitstempel>.json)",
    )
    p_run.add_argument(
        "--venue-llm",
        action="store_true",
        help="Nach Scraping: LLM für Adresse (OPENAI_API_KEY); siehe enrich --venue-llm",
    )
    p_run.add_argument(
        "-o",
        "--output",
        default="",
        dest="csv_output",
        help="CSV-Ausgabe (Standard: data/processed/events_flat.csv)",
    )
    p_run.set_defaults(func=cmd_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
