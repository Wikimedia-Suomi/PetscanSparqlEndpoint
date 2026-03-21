import json
import os
import sys
from typing import Any, Dict, Iterator, Optional

import pytest
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, Route, expect, sync_playwright

pytestmark = [pytest.mark.smoke]

STRUCTURE_RESPONSE = {
    "psid": 43641756,
    "meta": {
        "psid": 43641756,
        "records": 2,
        "source_url": "https://petscan.wmcloud.org/?psid=43641756&format=json",
        "loaded_at": "2026-03-16T07:00:00+00:00",
        "source_params": {"categories": ["Turku"]},
        "structure": {
            "row_count": 2,
            "field_count": 2,
            "fields": [
                {
                    "source_key": "title",
                    "predicate": "https://petscan.wmcloud.org/ontology/title",
                    "present_in_rows": 2,
                    "primary_type": "string",
                    "observed_types": ["string"],
                },
                {
                    "source_key": "namespace",
                    "predicate": "https://petscan.wmcloud.org/ontology/namespace",
                    "present_in_rows": 2,
                    "primary_type": "integer",
                    "observed_types": ["integer"],
                },
            ],
        },
    },
}

SPARQL_SELECT_RESPONSE = {
    "head": {"vars": ["item", "title", "ns"]},
    "results": {
        "bindings": [
            {
                "item": {"type": "uri", "value": "https://fi.wikipedia.org/wiki/Turku"},
                "title": {"type": "literal", "value": "Turku"},
                "ns": {
                    "type": "literal",
                    "value": "0",
                    "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                },
            }
        ]
    },
}


def _playwright_browser_channel() -> Optional[str]:
    configured = str(os.getenv("PLAYWRIGHT_BROWSER_CHANNEL", "")).strip()
    if configured:
        return configured
    if sys.platform == "darwin":
        return "chrome"
    return None


def _browser_error_message(channel: Optional[str]) -> str:
    base_message = "Unable to launch a Playwright browser for smoke tests."
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


def _fulfill_json(route: Route, payload: Dict[str, Any], status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="application/json; charset=utf-8",
        body=json.dumps(payload),
    )


def _goto_app(page: Page, live_server: Any) -> None:
    page.goto("{}/petscan/".format(live_server.url), wait_until="domcontentloaded")
    expect(page.get_by_role("heading", name="PetScan SPARQL Endpoint")).to_be_visible()


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    browser_channel = _playwright_browser_channel()
    headless = str(os.getenv("PLAYWRIGHT_HEADLESS", "1")).strip() != "0"

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
                raise RuntimeError(_browser_error_message(browser_channel)) from exc

        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(15000)

        try:
            yield page
        finally:
            context.close()
            browser.close()


def test_playwright_smoke_can_load_structure(page: Page, live_server: Any) -> None:
    page.route("**/petscan/api/structure**", lambda route: _fulfill_json(route, STRUCTURE_RESPONSE))

    _goto_app(page, live_server)
    expect(page.get_by_label("PetScan ID (psid)")).to_have_value("43641756")

    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("Data structure loaded")
    expect(page.get_by_role("heading", name="SPARQL Query")).to_be_visible()
    expect(page.locator("details table tbody tr")).to_have_count(2)
    expect(page.locator("details table")).to_contain_text("title")
    expect(page.locator("details table")).to_contain_text("namespace")


def test_playwright_smoke_can_run_query_and_render_results(page: Page, live_server: Any) -> None:
    page.route("**/petscan/api/structure**", lambda route: _fulfill_json(route, STRUCTURE_RESPONSE))
    page.route(
        "**/petscan/sparql/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(SPARQL_SELECT_RESPONSE),
        ),
    )

    _goto_app(page, live_server)
    page.get_by_role("button", name="Load data").click()
    expect(page.locator(".status.is-success")).to_contain_text("Data structure loaded")

    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed")
    expect(page.locator(".result-block table tbody tr")).to_have_count(1)
    expect(page.locator(".result-block table")).to_contain_text("Turku")
    expect(page.get_by_role("link", name="w:fi:Turku")).to_be_visible()


def test_playwright_smoke_surfaces_load_errors(page: Page, live_server: Any) -> None:
    page.route(
        "**/petscan/api/structure**",
        lambda route: _fulfill_json(route, {"error": "PetScan upstream returned an error."}, status=502),
    )

    _goto_app(page, live_server)
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-error")).to_contain_text("PetScan upstream returned an error.")
