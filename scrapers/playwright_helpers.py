"""Shared Playwright helpers for listing and event-page scrapes."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from playwright.sync_api import TimeoutError as PlaywrightTimeout

if TYPE_CHECKING:
    from playwright.sync_api import Page


def dismiss_cookie_banner(page: "Page") -> None:
    for label in ("Agree", "Zustimmen", "Accept", "Alle akzeptieren"):
        btn = page.get_by_role(
            "button", name=re.compile(f"^{re.escape(label)}$", re.I)
        )
        if btn.count():
            try:
                btn.first.click(timeout=3000)
                page.wait_for_timeout(500)
            except Exception:
                pass


def goto_listing(
    page: "Page",
    url: str,
    *,
    timeout_ms: int,
    meta_errors: list,
) -> None:
    page.goto(url, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except PlaywrightTimeout:
        meta_errors.append("networkidle_timeout_ignored")


def click_show_more_eventfrog(page: "Page", max_clicks: int, settle_ms: int) -> int:
    patterns = [
        re.compile(r"show\s+more", re.I),
        re.compile(r"weitere\s+events", re.I),
        re.compile(r"mehr\s+anzeigen", re.I),
    ]
    clicks = 0
    for _ in range(max_clicks):
        clicked = False
        for pat in patterns:
            loc = page.get_by_role("button", name=pat)
            if loc.count() > 0:
                try:
                    loc.first.click(timeout=5000)
                    clicked = True
                    break
                except Exception:
                    continue
        if not clicked:
            loc = page.locator("text=/show more events/i")
            if loc.count() > 0:
                try:
                    loc.first.click(timeout=5000)
                    clicked = True
                except Exception:
                    pass
        if not clicked:
            break
        clicks += 1
        page.wait_for_timeout(settle_ms)
    return clicks


def new_browser_context(playwright, *, headless: bool, locale: str = "de-CH"):
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context(
        locale=locale,
        user_agent=(
            "Mozilla/5.0 (compatible; OpenMicsZurichResearch/0.1; +local script)"
        ),
    )
    return browser, context
