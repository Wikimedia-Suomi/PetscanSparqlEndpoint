import os
import sys
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, expect, sync_playwright


def read_positive_int_env(name: str, default: int) -> int:
    raw_value = str(os.getenv(name, "")).strip()
    if not raw_value:
        return default

    try:
        parsed = int(raw_value)
    except ValueError:
        return default

    return parsed if parsed > 0 else default


def playwright_browser_channel() -> Optional[str]:
    configured = str(os.getenv("PLAYWRIGHT_BROWSER_CHANNEL", "")).strip()
    if configured:
        return configured
    if sys.platform == "darwin":
        return "chrome"
    return None


def browser_error_message(channel: Optional[str], suite_label: str = "Playwright tests") -> str:
    base_message = "Unable to launch a Playwright browser for {}.".format(suite_label)
    if channel is not None:
        return (
            base_message
            + " Set PLAYWRIGHT_BROWSER_CHANNEL to another installed browser channel, or install a "
            + "Playwright-managed browser with `.venv/bin/python -m playwright install chromium`."
        )
    return (
        base_message
        + " Install a Playwright-managed browser with "
        + "`.venv/bin/python -m playwright install chromium`, or set PLAYWRIGHT_BROWSER_CHANNEL "
        + "to an installed Chrome-compatible browser."
    )


@contextmanager
def managed_page(default_timeout_ms: int = 15000, suite_label: str = "Playwright tests") -> Iterator[Page]:
    browser_channel = playwright_browser_channel()
    headless = str(os.getenv("PLAYWRIGHT_HEADLESS", "1")).strip() != "0"
    timeout_ms = read_positive_int_env("PLAYWRIGHT_DEFAULT_TIMEOUT_MS", default_timeout_ms)

    with sync_playwright() as playwright:
        browser = None
        if browser_channel is not None:
            try:
                browser = playwright.chromium.launch(channel=browser_channel, headless=headless)
            except PlaywrightError:
                browser = None

        if browser is None:
            try:
                browser = playwright.chromium.launch(headless=headless)
            except PlaywrightError as exc:
                raise RuntimeError(browser_error_message(browser_channel, suite_label=suite_label)) from exc

        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(timeout_ms)

        try:
            yield page
        finally:
            context.close()
            browser.close()


def goto_app(page: Page, live_server: Any) -> None:
    page.goto("{}/petscan/".format(live_server.url), wait_until="domcontentloaded")
    expect(page.get_by_role("heading", name="SPARQL Bridge / PetScan")).to_be_visible()
