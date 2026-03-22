import json
from typing import Any, Dict, Iterator

import pytest
from playwright.sync_api import Page, Route, expect

from tests.playwright_support import managed_page

pytestmark = [pytest.mark.smoke]

QUARRY_STRUCTURE_RESPONSE = {
    "quarry_id": 103479,
    "qrun_id": 1084251,
    "meta": {
        "psid": 2000103479,
        "records": 2,
        "source_url": "https://quarry.wmcloud.org/run/1084251/output/0/json",
        "loaded_at": "2026-03-21T07:00:00+00:00",
        "source_params": {
            "quarry_id": ["103479"],
            "qrun_id": ["1084251"],
            "limit": ["10"],
        },
        "structure": {
            "row_count": 2,
            "field_count": 2,
            "fields": [
                {
                    "source_key": "rc_title",
                    "predicate": "https://quarry.wmcloud.org/ontology/rc_title",
                    "present_in_rows": 2,
                    "primary_type": "string",
                    "observed_types": ["string"],
                },
                {
                    "source_key": "rc_namespace",
                    "predicate": "https://quarry.wmcloud.org/ontology/rc_namespace",
                    "present_in_rows": 2,
                    "primary_type": "integer",
                    "observed_types": ["integer"],
                },
            ],
        },
    },
}

QUARRY_SELECT_RESPONSE = {
    "head": {"vars": ["quarry_row_id", "rc_title"]},
    "results": {
        "bindings": [
            {
                "quarry_row_id": {
                    "type": "uri",
                    "value": "https://quarry.wmcloud.org/query/103479#4",
                },
                "rc_title": {"type": "literal", "value": "Example title"},
            }
        ]
    },
}


def _fulfill_json(route: Route, payload: Dict[str, Any], status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="application/json; charset=utf-8",
        body=json.dumps(payload),
    )


def _goto_quarry_app(page: Page, live_server: Any) -> None:
    page.goto("{}/quarry/".format(live_server.url), wait_until="domcontentloaded")
    expect(page.get_by_role("heading", name="Quarry SPARQL endpoint")).to_be_visible()


def _load_quarry_structure_successfully(page: Page, live_server: Any) -> None:
    _goto_quarry_app(page, live_server)
    page.get_by_role("button", name="Load data").click()
    expect(page.locator(".status.is-success")).to_contain_text("Quarry data loaded")


def _stub_quarry_select_query_success(page: Page) -> None:
    page.route(
        "**/quarry/sparql/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(QUARRY_SELECT_RESPONSE),
        ),
    )


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(default_timeout_ms=15000, suite_label="quarry smoke tests") as browser_page:
        yield browser_page


def test_playwright_quarry_smoke_can_load_structure_and_show_links(page: Page, live_server: Any) -> None:
    page.route("**/quarry/api/structure**", lambda route: _fulfill_json(route, QUARRY_STRUCTURE_RESPONSE))

    _goto_quarry_app(page, live_server)

    expect(page.get_by_label("Quarry ID")).to_have_value("103479")
    expect(page.get_by_role("link", name="Open Quarry 103479")).to_have_attribute(
        "href",
        "https://quarry.wmcloud.org/query/103479",
    )

    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("Quarry data loaded")
    expect(page.get_by_role("heading", name="SPARQL Query")).to_be_visible()
    expect(page.get_by_role("textbox", name="SPARQL query")).to_have_value(
        "PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>\n"
        "PREFIX quarry: <https://quarry.wmcloud.org/query/>\n"
        "SELECT ?quarry_row_id ?rc_title ?rc_namespace\n"
        "WHERE {\n"
        "  ?quarry_row_id a quarrycol:Page .\n"
        "  OPTIONAL { ?quarry_row_id quarrycol:rc_title ?rc_title . }\n"
        "  OPTIONAL { ?quarry_row_id quarrycol:rc_namespace ?rc_namespace . }\n"
        "}\n"
        "LIMIT 50"
    )
    expect(page.get_by_role("link", name="Open Quarry 103479 JSON")).to_have_attribute(
        "href",
        "https://quarry.wmcloud.org/run/1084251/output/0/json",
    )
    expect(page.locator("details table")).to_contain_text("rc_title")


def test_playwright_quarry_smoke_can_toggle_structure_details(page: Page, live_server: Any) -> None:
    page.route("**/quarry/api/structure**", lambda route: _fulfill_json(route, QUARRY_STRUCTURE_RESPONSE))

    _load_quarry_structure_successfully(page, live_server)

    details = page.locator("details.structure-collapsible")
    summary = details.locator("summary")

    expect(details).to_have_attribute("open", "")
    expect(page.locator("details table")).to_contain_text("rc_title")

    summary.click()
    expect(details).not_to_have_attribute("open", "")

    summary.click()
    expect(details).to_have_attribute("open", "")
    expect(page.locator("details table")).to_contain_text("rc_namespace")


def test_playwright_quarry_smoke_load_data_always_requests_refresh(page: Page, live_server: Any) -> None:
    seen_urls = []

    def _fulfill_structure(route: Route) -> None:
        seen_urls.append(route.request.url)
        _fulfill_json(route, QUARRY_STRUCTURE_RESPONSE)

    page.route("**/quarry/api/structure**", _fulfill_structure)

    _goto_quarry_app(page, live_server)
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("Quarry data loaded")
    assert seen_urls
    assert "refresh=1" in seen_urls[0]


def test_playwright_quarry_smoke_renders_quarry_row_links_with_short_text(page: Page, live_server: Any) -> None:
    page.route("**/quarry/api/structure**", lambda route: _fulfill_json(route, QUARRY_STRUCTURE_RESPONSE))
    _stub_quarry_select_query_success(page)

    _load_quarry_structure_successfully(page, live_server)
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed")
    expect(page.locator(".result-block table tbody tr")).to_have_count(1)
    expect(page.locator(".result-block table")).to_contain_text("Example title")
    expect(page.get_by_role("link", name="103479#4")).to_have_attribute(
        "href",
        "https://quarry.wmcloud.org/query/103479#4",
    )
