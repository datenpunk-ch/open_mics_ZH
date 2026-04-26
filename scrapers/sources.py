"""Registered listing sources.

Single source of truth: ``docs/Quellenliste.md``.
This module parses machine-readable `````source````` blocks from that file.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from pathlib import Path
import re

ListingBehavior = Literal["eventfrog", "none"]


@dataclass(frozen=True)
class ListingSource:
    """One scrapeable listing (search / calendar page)."""

    id: str
    label: str
    default_listing_url: str
    extractor: str
    listing_behavior: ListingBehavior = "none"


ROOT = Path(__file__).resolve().parents[1]
SOURCES_MD = ROOT / "docs" / "Quellenliste.md"


def _parse_source_blocks(md_text: str) -> list[dict[str, str]]:
    """
    Parse fenced blocks:

    ```source
    key: value
    ...
    ```
    """
    blocks: list[dict[str, str]] = []
    for m in re.finditer(r"```source\s*\n([\s\S]*?)\n```", md_text, flags=re.I):
        body = m.group(1) or ""
        d: dict[str, str] = {}
        for raw in body.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            key = k.strip()
            val = v.strip()
            if not key:
                continue
            d[key] = val
        if d:
            blocks.append(d)
    return blocks


def _load_sources_from_md() -> dict[str, ListingSource]:
    if not SOURCES_MD.is_file():
        raise FileNotFoundError(f"Missing sources file: {SOURCES_MD}")
    md = SOURCES_MD.read_text(encoding="utf-8", errors="replace")
    blocks = _parse_source_blocks(md)
    out: dict[str, ListingSource] = {}
    allowed = {"id", "label", "start_url", "extractor", "listing_behavior"}
    for b in blocks:
        b2 = {k: v for k, v in b.items() if k in allowed}
        sid = (b2.get("id") or "").strip()
        if not sid:
            continue
        label = (b2.get("label") or sid).strip()
        start_url = (b2.get("start_url") or "").strip()
        extractor = (b2.get("extractor") or "").strip()
        listing_behavior = (b2.get("listing_behavior") or "none").strip()
        if not start_url or not extractor:
            raise ValueError(f"Source {sid!r} missing start_url/extractor in {SOURCES_MD}")
        if listing_behavior not in ("eventfrog", "none"):
            raise ValueError(f"Source {sid!r} has invalid listing_behavior {listing_behavior!r}")
        out[sid] = ListingSource(
            id=sid,
            label=label,
            default_listing_url=start_url,
            extractor=extractor,
            listing_behavior=listing_behavior,  # type: ignore[arg-type]
        )
    if not out:
        raise ValueError(f"No ```source``` blocks found in {SOURCES_MD}")
    return out


LISTING_SOURCES: dict[str, ListingSource] = _load_sources_from_md()


def get_listing_source(source_id: str) -> ListingSource:
    try:
        return LISTING_SOURCES[source_id]
    except KeyError as e:
        known = ", ".join(sorted(LISTING_SOURCES))
        raise KeyError(f"Unknown listing source {source_id!r}. Known: {known}") from e


DEFAULT_OUTPUT_DIR = "data/raw"
