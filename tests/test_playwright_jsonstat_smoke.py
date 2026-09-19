import json
import re
from typing import Any, Iterator

import pytest
from playwright.sync_api import Page, Route, expect

from tests.playwright_support import managed_page

pytestmark = [pytest.mark.smoke]

EXAMPLE_URL = "https://pxdata.stat.fi/PxWeb/sq/552d1f53-bdab-472b-a8e7-68b5b8c37cda"
SOURCE_TOKEN = (
    "aHR0cHM6Ly9weGRhdGEuc3RhdC5maS9QeFdlYi9zcS81NTJkMWY1My1iZGFiLTQ3MmItYThlNy02OGI1YjhjMzdjZGE"
)
STRUCTURE_RESPONSE = {
    "url": EXAMPLE_URL,
    "source_token": SOURCE_TOKEN,
    "meta": {
        "psid": 123,
        "records": 7,
        "source_url": EXAMPLE_URL,
        "loaded_at": "2026-08-30T10:00:00+00:00",
        "source_params": {"url": [EXAMPLE_URL]},
        "structure": {
            "row_count": 7,
            "field_count": 3,
            "fields": [
                {
                    "source_key": "timeperiod_y",
                    "predicate": "https://sparqlbridge.toolforge.org/ontology/jsonstat/timeperiod_y",
                    "present_in_rows": 7,
                    "primary_type": "xsd:string",
                    "observed_types": ["xsd:string"],
                    "row_side_cardinality": "1",
                },
                {
                    "source_key": "timeperiod_y_label",
                    "predicate": "https://sparqlbridge.toolforge.org/ontology/jsonstat/timeperiod_y_label",
                    "present_in_rows": 7,
                    "primary_type": "xsd:string",
                    "observed_types": ["xsd:string"],
                    "row_side_cardinality": "1",
                },
                {
                    "source_key": "value",
                    "predicate": "https://sparqlbridge.toolforge.org/ontology/jsonstat/value",
                    "present_in_rows": 7,
                    "primary_type": "xsd:integer",
                    "observed_types": ["xsd:integer"],
                    "row_side_cardinality": "1",
                },
            ],
        },
    },
}
SELECT_RESPONSE = {
    "head": {"vars": ["observation", "value"]},
    "results": {
        "bindings": [
            {
                "observation": {
                    "type": "uri",
                    "value": "https://sparqlbridge.toolforge.org/jsonstat/dataset/123/observation/1",
                },
                "value": {
                    "type": "literal",
                    "value": "1526457",
                    "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                },
            }
        ]
    },
}


def _fulfill_json(route: Route, payload: dict[str, Any]) -> None:
    route.fulfill(
        status=200,
        content_type="application/json; charset=utf-8",
        body=json.dumps(payload),
    )


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(
        default_timeout_ms=15000, suite_label="JSON-stat smoke tests"
    ) as browser_page:
        yield browser_page


def test_jsonstat_ui_loads_structure_and_runs_query(page: Page, live_server: Any) -> None:
    page.route(
        "**/jsonstat/api/structure**",
        lambda route: _fulfill_json(route, STRUCTURE_RESPONSE),
    )
    page.route(
        "**/jsonstat/sparql/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/sparql-results+json; charset=utf-8",
            body=json.dumps(SELECT_RESPONSE),
        ),
    )

    page.goto("{}/jsonstat/".format(live_server.url), wait_until="domcontentloaded")
    page.get_by_label("JSON-stat 2 file URL").fill(EXAMPLE_URL)
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("7 cells, 3 fields")
    expect(page.locator("details table tbody tr")).to_have_count(3)
    query_editor = page.get_by_role("textbox", name="SPARQL query")
    expect(query_editor).to_have_value(re.compile(r"qb:Observation"))
    expect(query_editor).to_have_value(
        re.compile(r"https://sparqlbridge\.toolforge\.org/ontology/jsonstat/value")
    )

    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("1 rows returned")
    expect(page.locator(".result-block table")).to_contain_text("1526457")
    expect(page.locator("pre").filter(has_text="/jsonstat/sparql/source=")).to_contain_text(
        SOURCE_TOKEN
    )
