import os
import re
from typing import Any, Iterator

import pytest
from playwright.sync_api import Page, expect

from tests.playwright_support import goto_app, managed_page, read_positive_int_env

pytestmark = [pytest.mark.e2e]

LIVE_SELECT_QUERY = """PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
SELECT ?item ?title
WHERE {
  ?item a petscan:Page .
  OPTIONAL { ?item petscan:title ?title }
}
LIMIT 5
"""


def _live_psid() -> str:
    configured = str(os.getenv("PETSCAN_E2E_PSID", "43641756")).strip()
    return configured or "43641756"


def _live_output_limit() -> str:
    configured = str(os.getenv("PETSCAN_E2E_OUTPUT_LIMIT", "5")).strip()
    return configured or "5"


def _e2e_expect_timeout_ms() -> int:
    return read_positive_int_env("PETSCAN_E2E_EXPECT_TIMEOUT_MS", 60000)


def _configure_live_petscan_request(page: Page) -> None:
    page.get_by_label("PetScan ID (psid)").fill(_live_psid())
    page.get_by_label("PetScan extra GET params").fill("")
    page.get_by_label("Load data limit").fill(_live_output_limit())


def _load_live_structure(page: Page, live_server: Any) -> None:
    timeout_ms = _e2e_expect_timeout_ms()

    goto_app(page, live_server)
    _configure_live_petscan_request(page)
    page.get_by_role("button", name="Load data").click()

    expect(page.locator(".status.is-success")).to_contain_text("Data structure loaded", timeout=timeout_ms)
    expect(page.get_by_role("heading", name="SPARQL Query")).to_be_visible(timeout=timeout_ms)
    expect(page.locator("details table tbody tr").first).to_be_visible(timeout=timeout_ms)


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(default_timeout_ms=_e2e_expect_timeout_ms(), suite_label="live E2E tests") as browser_page:
        yield browser_page


def test_playwright_e2e_can_load_live_structure_from_petscan(page: Page, live_server: Any) -> None:
    _load_live_structure(page, live_server)

    assert page.locator("details table tbody tr").count() > 0
    expect(page.locator(".status.is-error")).to_have_count(0)


def test_playwright_e2e_can_refresh_and_query_live_petscan_data(page: Page, live_server: Any) -> None:
    timeout_ms = _e2e_expect_timeout_ms()

    _load_live_structure(page, live_server)

    page.get_by_role("checkbox", name="Refresh data from PetScan before running query").check()
    page.get_by_role("textbox", name="SPARQL query").fill(LIVE_SELECT_QUERY)
    page.get_by_role("button", name="Run query").click()

    expect(page.locator(".status-query.is-success")).to_contain_text("Query completed", timeout=timeout_ms)
    expect(page.get_by_role("heading", name="SPARQL Query Result")).to_be_visible(timeout=timeout_ms)
    expect(page.locator(".result-block table tbody tr").first).to_be_visible(timeout=timeout_ms)

    assert page.locator(".result-block table tbody tr").count() > 0
    expect(page.locator(".result-block table tbody tr a").first).to_have_attribute(
        "href",
        re.compile(r"^https://"),
        timeout=timeout_ms,
    )
