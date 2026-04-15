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
    "zuerich_com_event_finder": ListingSource(
        id="zuerich_com_event_finder",
        label="Zürich Tourismus Event Finder (Guidle microsite; open mic/standup/comedy)",
        default_listing_url=(
            "https://microsite.guidle.com/api/rest/2.0/portals/search-offers/658578869"
            "?portalName=microsite&pageOfferId=1172134252&sectionId=1096&currentPageNumber=1"
            "&micrositeCrId=e8X87y&language=de&search=open+mic+standup+comedy"
        ),
        extractor="guidle_microsite",
        listing_behavior="none",
    ),
    "stubae_comedy": ListingSource(
        id="stubae_comedy",
        label="Stubä Comedy (Milanski Comedy; recurring open mic page)",
        default_listing_url="https://www.milanski-comedy.ch/stubae-comedy",
        extractor="single_page",
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
