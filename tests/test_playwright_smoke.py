import json
from typing import Any, Dict, Iterator
from urllib.parse import unquote

import pytest
from playwright.sync_api import Page, Route, expect

from tests.playwright_support import goto_app, managed_page

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


def _fulfill_json(route: Route, payload: Dict[str, Any], status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="application/json; charset=utf-8",
        body=json.dumps(payload),
    )


def _stub_structure_success(page: Page) -> None:
    page.route("**/petscan/api/structure**", lambda route: _fulfill_json(route, STRUCTURE_RESPONSE))


def _stub_select_query_success(page: Page) -> None:
    page.route(
        "**/petscan/sparql/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(SPARQL_SELECT_RESPONSE),
        ),
    )


def _load_structure_successfully(page: Page, live_server: Any) -> None:
    goto_app(page, live_server)
    page.get_by_role("button", name="Load data").click()
    expect(page.locator(".status.is-success")).to_contain_text("Data structure loaded")


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(default_timeout_ms=15000, suite_label="smoke tests") as browser_page:
        yield browser_page


def test_playwright_smoke_can_load_structure(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)

    goto_app(page, live_server)
    expect(page.get_by_label("PetScan ID (psid)")).to_have_value("43641756")

    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("Data structure loaded")
    expect(page.get_by_role("heading", name="SPARQL Query")).to_be_visible()
    expect(page.locator("details table tbody tr")).to_have_count(2)
    expect(page.locator("details table")).to_contain_text("title")
    expect(page.locator("details table")).to_contain_text("namespace")


def test_playwright_smoke_hides_source_links_when_psid_is_empty(page: Page, live_server: Any) -> None:
    goto_app(page, live_server)

    page.get_by_label("PetScan ID (psid)").fill("")

    expect(page.get_by_role("link", name="Open PetScan query")).to_have_count(0)
    expect(page.get_by_role("link", name="Open PetScan JSON")).to_have_count(0)


def test_playwright_smoke_load_data_always_requests_refresh(page: Page, live_server: Any) -> None:
    seen_urls = []

    def _fulfill_structure(route: Route) -> None:
        seen_urls.append(route.request.url)
        _fulfill_json(route, STRUCTURE_RESPONSE)

    page.route("**/petscan/api/structure**", _fulfill_structure)

    goto_app(page, live_server)
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("Data structure loaded")
    assert seen_urls
    assert "refresh=1" in seen_urls[0]


def test_playwright_smoke_can_run_query_and_render_results(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)
    _stub_select_query_success(page)

    _load_structure_successfully(page, live_server)

    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed")
    expect(page.locator(".result-block table tbody tr")).to_have_count(1)
    expect(page.locator(".result-block table")).to_contain_text("Turku")
    expect(page.get_by_role("link", name="w:fi:Turku")).to_be_visible()


def test_playwright_smoke_can_toggle_structure_details(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)

    _load_structure_successfully(page, live_server)

    details = page.locator("details.structure-collapsible")
    summary = details.locator("summary")

    expect(details).to_have_attribute("open", "")
    expect(page.locator("details table")).to_contain_text("title")

    summary.click()
    expect(details).not_to_have_attribute("open", "")

    summary.click()
    expect(details).to_have_attribute("open", "")
    expect(page.locator("details table")).to_contain_text("namespace")


def test_playwright_smoke_surfaces_load_errors(page: Page, live_server: Any) -> None:
    page.route(
        "**/petscan/api/structure**",
        lambda route: _fulfill_json(route, {"error": "PetScan upstream returned an error."}, status=502),
    )

    goto_app(page, live_server)
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-error")).to_contain_text("PetScan upstream returned an error.")


def test_playwright_smoke_surfaces_query_errors(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)
    page.route(
        "**/petscan/sparql/**",
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


def test_playwright_smoke_can_toggle_select_results_between_table_and_cards(page: Page, live_server: Any) -> None:
    _stub_structure_success(page)
    _stub_select_query_success(page)

    _load_structure_successfully(page, live_server)
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".result-block .table-wrap")).to_have_count(1)
    page.get_by_role("button", name="Cards").click()

    expect(page.locator(".result-cards")).to_be_visible()
    expect(page.locator(".result-card")).to_contain_text("Turku")
    expect(page.locator(".result-block .table-wrap")).to_have_count(0)

    page.get_by_role("button", name="Table").click()
    expect(page.locator(".result-block .table-wrap")).to_have_count(1)


def test_playwright_smoke_open_query_dialog_builds_wdqs_url(page: Page, live_server: Any) -> None:
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
    self_query = "{}/petscan/sparql/psid=43641756&output_limit=10".format(live_server.url)

    assert str(opened_url).startswith("https://query.wikidata.org/#")
    decoded_query = unquote(str(opened_url).split("#", 1)[1])
    assert "SERVICE <https://sophox.org/sparql>" in decoded_query
    assert "SERVICE <{}>".format(self_query) in decoded_query
    assert "PREFIX petscan: <https://petscan.wmcloud.org/ontology/>" in decoded_query
