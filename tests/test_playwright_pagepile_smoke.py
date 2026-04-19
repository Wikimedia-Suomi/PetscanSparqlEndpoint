import json
import re
from typing import Any, Dict, Iterator
from urllib.parse import unquote

import pytest
from playwright.sync_api import Page, Route, expect

from tests.playwright_support import managed_page

pytestmark = [pytest.mark.smoke]

PAGEPILE_STRUCTURE_RESPONSE = {
    "source": "pagepile",
    "pagepile_id": 112306,
    "limit": 10,
    "meta": {
        "psid": 4000112306,
        "records": 2,
        "source_url": "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
        "loaded_at": "2026-04-19T08:00:00+00:00",
        "source_params": {
            "pagepile_id": ["112306"],
            "limit": ["10"],
        },
        "structure": {
            "row_count": 2,
            "field_count": 6,
            "fields": [
                {
                    "source_key": "wikidata_entity",
                    "predicate": "http://schema.org/about",
                    "present_in_rows": 2,
                    "primary_type": "uri",
                    "observed_types": ["uri"],
                },
                {
                    "source_key": "lang_code",
                    "predicate": "http://schema.org/inLanguage",
                    "present_in_rows": 2,
                    "primary_type": "string",
                    "observed_types": ["string"],
                },
                {
                    "source_key": "page_label",
                    "predicate": "http://schema.org/name",
                    "present_in_rows": 2,
                    "primary_type": "string",
                    "observed_types": ["string"],
                },
                {
                    "source_key": "site_url",
                    "predicate": "http://schema.org/isPartOf",
                    "present_in_rows": 2,
                    "primary_type": "uri",
                    "observed_types": ["uri"],
                },
                {
                    "source_key": "wiki_group",
                    "predicate": "http://wikiba.se/ontology#wikiGroup",
                    "present_in_rows": 2,
                    "primary_type": "string",
                    "observed_types": ["string"],
                },
                {
                    "source_key": "page_id",
                    "predicate": "https://pagepile.toolforge.org/ontology/page_id",
                    "present_in_rows": 2,
                    "primary_type": "integer",
                    "observed_types": ["integer"],
                },
            ],
        },
    },
}

SPARQL_SELECT_RESPONSE = {
    "head": {"vars": ["page", "page_label", "page_id"]},
    "results": {
        "bindings": [
            {
                "page": {"type": "uri", "value": "https://en.wikipedia.org/wiki/Example"},
                "page_label": {"type": "literal", "value": "Example"},
                "page_id": {
                    "type": "literal",
                    "value": "123",
                    "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                },
            }
        ]
    },
}


def _goto_pagepile_app(page: Page, live_server: Any) -> None:
    page.goto("{}/pagepile/".format(live_server.url), wait_until="domcontentloaded")
    expect(page.get_by_role("heading", name="PagePile SPARQL endpoint")).to_be_visible()


def _fulfill_json(route: Route, payload: Dict[str, Any], status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="application/json; charset=utf-8",
        body=json.dumps(payload),
    )


def _stub_structure_success(page: Page) -> None:
    page.route(
        "**/pagepile/api/structure**",
        lambda route: _fulfill_json(route, PAGEPILE_STRUCTURE_RESPONSE),
    )


def _stub_select_query_success(page: Page) -> None:
    page.route(
        "**/pagepile/sparql/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(SPARQL_SELECT_RESPONSE),
        ),
    )


def _load_structure_successfully(page: Page, live_server: Any) -> None:
    _goto_pagepile_app(page, live_server)
    page.get_by_label("PagePile ID").fill("112306")
    page.get_by_role("button", name="Load data").click()
    expect(page.locator(".status.is-success")).to_contain_text("PagePile data loaded")


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(default_timeout_ms=15000, suite_label="PagePile smoke tests") as browser_page:
        yield browser_page


def test_playwright_pagepile_smoke_can_load_structure(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)

    _goto_pagepile_app(page, live_server)
    page.get_by_label("PagePile ID").fill("112306")
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("PagePile data loaded")
    expect(page.get_by_role("heading", name="SPARQL Query")).to_be_visible()
    expect(page.locator("details table tbody tr")).to_have_count(6)
    expect(page.locator("details table")).to_contain_text("page_id")


def test_playwright_pagepile_smoke_includes_limit_in_structure_request(page: Page, live_server: Any) -> None:
    seen_urls = []

    def _fulfill_structure(route: Route) -> None:
        seen_urls.append(route.request.url)
        _fulfill_json(route, PAGEPILE_STRUCTURE_RESPONSE)

    page.route("**/pagepile/api/structure**", _fulfill_structure)

    _goto_pagepile_app(page, live_server)
    page.get_by_label("PagePile ID").fill("112306")
    page.get_by_label("Load data limit").fill("25")
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("PagePile data loaded")
    assert seen_urls
    assert "pagepile_id=112306" in seen_urls[0]
    assert "limit=25" in seen_urls[0]
    assert "refresh=1" in seen_urls[0]


def test_playwright_pagepile_smoke_shows_backend_error_message(page: Page, live_server: Any) -> None:
    page.route(
        "**/pagepile/api/structure**",
        lambda route: _fulfill_json(
            route,
            {"error": "Failed to load PagePile data from the upstream service."},
            status=502,
        ),
    )

    _goto_pagepile_app(page, live_server)
    page.get_by_label("PagePile ID").fill("112306")
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-error")).to_contain_text(
        "Failed to load PagePile data from the upstream service."
    )


def test_playwright_pagepile_smoke_wizard_updates_query_text(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)

    _load_structure_successfully(page, live_server)

    query_box = page.get_by_role("textbox", name="SPARQL query")
    expect(query_box).not_to_have_value(re.compile(r"\?page_id"))

    page.locator("#wizard-field-page_id").check()

    expect(query_box).to_have_value(re.compile(r"\?page_id"))


def test_playwright_pagepile_smoke_can_run_query_and_render_results(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)
    _stub_select_query_success(page)

    _load_structure_successfully(page, live_server)
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed")
    expect(page.locator(".result-block table tbody tr")).to_have_count(1)
    expect(page.locator(".result-block table")).to_contain_text("Example")
    expect(page.get_by_role("link", name="w:en:Example")).to_be_visible()


def test_playwright_pagepile_smoke_surfaces_query_errors(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)
    page.route(
        "**/pagepile/sparql/**",
        lambda route: route.fulfill(
            status=400,
            content_type="text/plain; charset=utf-8",
            body="SERVICE clauses are not allowed in this endpoint.",
        ),
    )

    _load_structure_successfully(page, live_server)
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-error")).to_contain_text(
        "SERVICE clauses are not allowed in this endpoint."
    )
    expect(page.get_by_role("heading", name="SPARQL Query Result")).to_have_count(0)


def test_playwright_pagepile_smoke_refresh_before_query_adds_refresh_to_request(
    page: Page, live_server: Any
) -> None:
    seen_urls = []

    def _fulfill_select(route: Route) -> None:
        seen_urls.append(route.request.url)
        route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(SPARQL_SELECT_RESPONSE),
        )

    _stub_structure_success(page)
    page.route("**/pagepile/sparql/**", _fulfill_select)

    _load_structure_successfully(page, live_server)
    page.get_by_role("checkbox", name="Refresh data from PagePile before running query").check()
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed")
    assert seen_urls
    assert "refresh=1" in seen_urls[0]


def test_playwright_pagepile_smoke_open_query_dialog_builds_wdqs_url(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)

    _load_structure_successfully(page, live_server)
    page.evaluate(
        """
        () => {
          window.__openedUrls = [];
          window.open = (url) => {
            window.__openedUrls.push(url);
            return {};
          };
        }
        """
    )

    page.get_by_role("button", name="Open query as Federated query in...").click()
    expect(page.get_by_role("heading", name="Open Federated Query In")).to_be_visible()
    dialog = page.locator("dialog.query-target-dialog")
    dialog.get_by_role("button", name="Open", exact=True).click()

    opened_url = page.evaluate("() => window.__openedUrls[0]")

    assert str(opened_url).startswith("https://query.wikidata.org/#")
    decoded_query = unquote(str(opened_url).split("#", 1)[1])
    assert "SERVICE <https://sophox.org/sparql>" in decoded_query
    assert "SERVICE <{}/pagepile/sparql/pagepile_id=112306&limit=10>".format(live_server.url) in decoded_query
