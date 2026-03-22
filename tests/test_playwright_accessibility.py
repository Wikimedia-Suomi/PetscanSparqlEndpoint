import json
from typing import Any, Dict, Iterator

import pytest
from axe_playwright_python.sync_playwright import Axe
from playwright.sync_api import Page, Route, expect

from tests.playwright_support import goto_app, goto_home, goto_quarry_app, managed_page

pytestmark = [pytest.mark.a11y]

AXE_TARGET_OPTIONS: Dict[str, Any] = {
    "runOnly": {
        "type": "tag",
        "values": [
            "wcag2a",
            "wcag2aa",
            "wcag21a",
            "wcag21aa",
            "wcag22aa",
            "best-practice",
            "wcag2aaa",
        ],
    },
    "resultTypes": ["violations"],
}

PETSCAN_STRUCTURE_RESPONSE = {
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

PETSCAN_SELECT_RESPONSE = {
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


def _assert_no_accessibility_violations(page: Page) -> None:
    results = Axe().run(page, options=AXE_TARGET_OPTIONS)
    assert results.violations_count == 0, results.generate_report()


def _fulfill_json(route: Route, payload: Dict[str, Any], status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="application/json; charset=utf-8",
        body=json.dumps(payload),
    )


def _stub_petscan_structure_success(page: Page) -> None:
    page.route(
        "**/petscan/api/structure**",
        lambda route: _fulfill_json(route, PETSCAN_STRUCTURE_RESPONSE),
    )


def _stub_petscan_select_query_success(page: Page) -> None:
    page.route(
        "**/petscan/sparql/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(PETSCAN_SELECT_RESPONSE),
        ),
    )


def _stub_quarry_structure_success(page: Page) -> None:
    page.route(
        "**/quarry/api/structure**",
        lambda route: _fulfill_json(route, QUARRY_STRUCTURE_RESPONSE),
    )


def _stub_quarry_select_query_success(page: Page) -> None:
    page.route(
        "**/quarry/sparql/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(QUARRY_SELECT_RESPONSE),
        ),
    )


def _load_petscan_structure(page: Page, live_server: Any) -> None:
    goto_app(page, live_server)
    page.get_by_role("button", name="Load data").click()
    expect(page.locator(".status.is-success")).to_contain_text("Data structure loaded")


def _load_quarry_structure(page: Page, live_server: Any) -> None:
    goto_quarry_app(page, live_server)
    page.get_by_role("button", name="Load data").click()
    expect(page.locator(".status.is-success")).to_contain_text("Quarry data loaded")


def _toggle_structure_details(page: Page) -> None:
    details = page.locator("details.structure-collapsible")
    expect(details).to_have_attribute("open", "")
    details.locator("summary").click()
    expect(details).not_to_have_attribute("open", "")


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(default_timeout_ms=15000, suite_label="accessibility tests") as browser_page:
        yield browser_page


def test_home_page_has_no_detectable_accessibility_violations(page: Page, live_server: Any) -> None:
    goto_home(page, live_server)

    _assert_no_accessibility_violations(page)


def test_petscan_page_has_no_detectable_accessibility_violations(
    page: Page, live_server: Any
) -> None:
    goto_app(page, live_server)

    expect(page.get_by_role("navigation", name="Breadcrumb")).to_be_visible()
    _assert_no_accessibility_violations(page)


def test_quarry_page_has_no_detectable_accessibility_violations(page: Page, live_server: Any) -> None:
    goto_quarry_app(page, live_server)

    expect(page.get_by_role("navigation", name="Breadcrumb")).to_be_visible()
    _assert_no_accessibility_violations(page)


def test_petscan_loaded_state_and_collapsible_remain_accessible(page: Page, live_server: Any) -> None:
    _stub_petscan_structure_success(page)

    _load_petscan_structure(page, live_server)
    expect(page.get_by_role("heading", name="SPARQL Query")).to_be_visible()
    expect(page.locator("details.structure-collapsible table")).to_contain_text("title")
    _assert_no_accessibility_violations(page)

    _toggle_structure_details(page)
    _assert_no_accessibility_violations(page)


def test_petscan_query_result_state_remains_accessible(page: Page, live_server: Any) -> None:
    _stub_petscan_structure_success(page)
    _stub_petscan_select_query_success(page)

    _load_petscan_structure(page, live_server)
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed")
    expect(page.get_by_role("heading", name="SPARQL Query Result")).to_be_visible()
    expect(page.locator(".result-block table")).to_contain_text("Turku")
    _assert_no_accessibility_violations(page)


def test_quarry_loaded_state_and_collapsible_remain_accessible(page: Page, live_server: Any) -> None:
    _stub_quarry_structure_success(page)

    _load_quarry_structure(page, live_server)
    expect(page.get_by_role("heading", name="SPARQL Query")).to_be_visible()
    expect(page.locator("details.structure-collapsible table")).to_contain_text("rc_title")
    _assert_no_accessibility_violations(page)

    _toggle_structure_details(page)
    _assert_no_accessibility_violations(page)


def test_quarry_query_result_state_remains_accessible(page: Page, live_server: Any) -> None:
    _stub_quarry_structure_success(page)
    _stub_quarry_select_query_success(page)

    _load_quarry_structure(page, live_server)
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed")
    expect(page.get_by_role("heading", name="SPARQL Query Result")).to_be_visible()
    expect(page.locator(".result-block table")).to_contain_text("Example title")
    _assert_no_accessibility_violations(page)
