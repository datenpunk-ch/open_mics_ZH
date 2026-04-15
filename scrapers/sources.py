"""Registered listing sources (extend with new ids + extractors)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ListingBehavior = Literal["eventfrog", "none"]


@dataclass(frozen=True)
class ListingSource:
    """One scrapeable listing (search / calendar page)."""

    id: str
    label: str
    default_listing_url: str
    extractor: str
    listing_behavior: ListingBehavior = "none"


LISTING_SOURCES: dict[str, ListingSource] = {
    "eventfrog": ListingSource(
        id="eventfrog",
        label="Eventfrog (EN, Zürich Comedy & Cabaret / Open Mic)",
        default_listing_url=(
            "https://eventfrog.ch/en/events/zuerich/comedy-cabaret.html"
            "?searchTerm=open+mic&geoRadius=10"
        ),
        extractor="eventfrog",
        listing_behavior="eventfrog",
    ),
    "eventfrog_de": ListingSource(
        id="eventfrog_de",
        label="Eventfrog (DE, gleiche Suche)",
        default_listing_url=(
            "https://eventfrog.ch/de/events/zuerich/comedy-cabaret.html"
            "?searchTerm=open+mic&geoRadius=10"
        ),
        extractor="eventfrog",
        listing_behavior="eventfrog",
    ),
    "gz_wollishofen_open_mic": ListingSource(
        id="gz_wollishofen_open_mic",
        label="GZ Wollishofen – Open Mic (offer page)",
        default_listing_url="https://gz-zh.ch/gz-wollishofen/angebote/open-mic/",
        extractor="gz_zh_single",
        listing_behavior="none",
    ),
}


def get_listing_source(source_id: str) -> ListingSource:
    try:
        return LISTING_SOURCES[source_id]
    except KeyError as e:
        known = ", ".join(sorted(LISTING_SOURCES))
        raise KeyError(f"Unknown listing source {source_id!r}. Known: {known}") from e


DEFAULT_OUTPUT_DIR = "data/raw"
