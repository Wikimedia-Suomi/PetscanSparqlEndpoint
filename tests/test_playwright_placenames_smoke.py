import json
from collections.abc import Iterator
from typing import Any

import pytest
from playwright.sync_api import Page, Route, expect

from tests.playwright_support import managed_page

pytestmark = [pytest.mark.smoke]

_STRUCTURE_RESPONSE = {
    "source": "placenames",
    "dataset": "saami",
    "meta": {
        "version": "2026-05",
        "records": 11956,
        "places": 10752,
        "quads": 490207,
        "structure": {
            "row_count": 11956,
            "field_count": 4,
            "fields": [
                {
                    "source_key": "place",
                    "predicate": "https://sparqlbridge.toolforge.org/ontology/placenames/place",
                    "present_in_rows": 11956,
                    "primary_type": "iri",
                    "observed_types": ["iri"],
                    "row_side_cardinality": "1",
                },
                {
                    "source_key": "spelling",
                    "predicate": "https://sparqlbridge.toolforge.org/ontology/placenames/spelling",
                    "present_in_rows": 11956,
                    "primary_type": "rdf:langString",
                    "observed_types": ["rdf:langString"],
                    "row_side_cardinality": "1",
                },
                {
                    "source_key": "municipality",
                    "predicate": "https://sparqlbridge.toolforge.org/ontology/placenames/municipality",
                    "present_in_rows": 11956,
                    "primary_type": "xsd:string",
                    "observed_types": ["xsd:string"],
                    "row_side_cardinality": "1",
                },
                {
                    "source_key": "wgs84WKT",
                    "predicate": "https://sparqlbridge.toolforge.org/ontology/placenames/wgs84WKT",
                    "present_in_rows": 11956,
                    "primary_type": "geo:wktLiteral",
                    "observed_types": ["geo:wktLiteral"],
                    "row_side_cardinality": "1",
                },
            ],
        },
    },
}
_SELECT_RESPONSE = {
    "head": {"vars": ["name", "place"]},
    "results": {
        "bindings": [
            {
                "name": {"type": "literal", "value": "Anár", "xml:lang": "smn"},
                "place": {"type": "uri", "value": "https://example.test/place/anar"},
            }
        ]
    },
}


def _fulfill_json(route: Route, payload: dict[str, Any], status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="application/json; charset=utf-8",
        body=json.dumps(payload),
    )


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(
        default_timeout_ms=15000, suite_label="Place-name smoke tests"
    ) as browser_page:
        yield browser_page


def _open_page(page: Page, live_server: Any) -> None:
    page.goto(f"{live_server.url}/placenames/", wait_until="domcontentloaded")
    expect(page.get_by_role("heading", name="Sámi place names SPARQL endpoint")).to_be_visible()


def test_placenames_ui_loads_static_dataset_metadata(page: Page, live_server: Any) -> None:
    page.route(
        "**/placenames/api/structure**",
        lambda route: _fulfill_json(route, _STRUCTURE_RESPONSE),
    )

    _open_page(page, live_server)
    expect(page.locator(".hero p")).to_have_count(0)
    expect(page.get_by_role("heading", name="Data Loading")).to_be_visible()
    expect(page.get_by_role("heading", name="About PlaceNames")).to_be_visible()
    expect(page.locator("#placenames-query-section")).to_be_hidden()
    expect(page.get_by_role("heading", name="SPARQL Endpoint", exact=True)).to_be_visible()
    expect(page.locator("#placenames-endpoint-preview")).to_contain_text(
        "/placenames/sparql/dataset=saami"
    )
    page.get_by_role("button", name="Load dataset").click()

    expect(page.locator("#load-status")).to_contain_text("Data structure loaded")
    expect(page.locator("#dataset-meta")).to_contain_text("11,956")
    expect(page.locator("#dataset-meta")).to_contain_text("490,207")
    expect(page.locator("#placenames-query-section")).to_be_visible()
    expect(page.locator("#selected-field-count")).to_have_text("(4 selected)")
    expect(page.locator("#placenames-structure-field-rows")).to_contain_text("spelling")


def test_placenames_ui_field_selection_updates_query(page: Page, live_server: Any) -> None:
    page.route(
        "**/placenames/api/structure**",
        lambda route: _fulfill_json(route, _STRUCTURE_RESPONSE),
    )

    _open_page(page, live_server)
    page.get_by_role("button", name="Load dataset").click()
    page.get_by_label("municipality", exact=True).uncheck()

    expect(page.locator("#selected-field-count")).to_have_text("(3 selected)")
    query_value = page.locator("#query").input_value()
    assert "?municipality" not in query_value
    assert "?spelling" in query_value

    page.get_by_role("button", name="Clear selection").click()
    expect(page.locator("#selected-field-count")).to_have_text("(0 selected)")
    query_value = page.locator("#query").input_value()
    assert "SELECT ?record" in query_value
    assert "OPTIONAL" not in query_value


def test_placenames_ui_runs_query_and_renders_result(page: Page, live_server: Any) -> None:
    page.route(
        "**/placenames/api/structure**",
        lambda route: _fulfill_json(route, _STRUCTURE_RESPONSE),
    )
    page.route(
        "**/placenames/sparql/**",
        lambda route: _fulfill_json(route, _SELECT_RESPONSE),
    )

    _open_page(page, live_server)
    page.get_by_role("button", name="Load dataset").click()
    page.get_by_role("button", name="Run query").click()

    expect(page.locator("#query-status")).to_contain_text("1 row returned")
    expect(page.locator("#placenames-result-section")).to_be_visible()
    expect(page.get_by_role("heading", name="SPARQL Query Result")).to_be_visible()
    expect(page.locator("#query-result table thead")).to_contain_text("name")
    expect(page.locator("#query-result table tbody")).to_contain_text("Anár@smn")
    expect(page.locator("#query-result table tbody a")).to_have_attribute(
        "href", "https://example.test/place/anar"
    )
    expect(page.get_by_role("button", name="Table", exact=True)).to_be_visible()
    page.get_by_role("button", name="Cards", exact=True).click()
    expect(page.locator("#query-result .result-card")).to_contain_text("Anár@smn")
