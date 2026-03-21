from typing import Any, Iterator, Sequence
from urllib.parse import unquote

import pytest
from playwright.sync_api import Page

from tests.playwright_support import goto_app, managed_page

pytestmark = [pytest.mark.jshelpers]


def _call_js_helper(page: Page, live_server: Any, export_name: str, args: Sequence[Any]) -> Any:
    goto_app(page, live_server)
    return page.evaluate(
        """
        async ({ moduleUrl, exportName, args }) => {
          const mod = await import(moduleUrl);
          return mod[exportName](...args);
        }
        """,
        {
            "moduleUrl": "{}/static/js/app_logic.js?v=test".format(live_server.url),
            "exportName": export_name,
            "args": list(args),
        },
    )


@pytest.fixture()
def page(live_server: Any) -> Iterator[Page]:
    with managed_page(default_timeout_ms=15000, suite_label="js helper tests") as browser_page:
        yield browser_page


def test_js_helper_parse_forwarded_petscan_params_filters_reserved_values(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "parseForwardedPetscanParams",
        ["?psid=123&categories=Turku&language=fi&output_limit=10&empty="],
    )

    assert result == [["categories", "Turku"], ["language", "fi"]]


def test_js_helper_infer_query_type_ignores_comments_and_prologue(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "inferQueryType",
        [
            "\n".join(
                [
                    "# comment",
                    "PREFIX petscan: <https://petscan.wmcloud.org/ontology/>",
                    "BASE <https://example.org/>",
                    "select * where { ?s ?p ?o }",
                ]
            )
        ],
    )

    assert result == "SELECT"


def test_js_helper_build_service_param_path_encodes_refresh_and_params(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "buildServiceParamPath",
        ["43641756", [["categories", "Turku & Aura"]], True],
    )

    assert result == "psid=43641756&refresh=1&categories=Turku%20%26%20Aura"


def test_js_helper_build_open_query_url_uses_wdqs_and_sophox(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "buildOpenQueryUrl",
        [
            "wdqs",
            "SELECT ?item WHERE { ?item a petscan:Page . }",
            "https://example.test/petscan/sparql/psid=43641756",
        ],
    )

    assert str(result).startswith("https://query.wikidata.org/#")
    decoded_fragment = unquote(str(result).split("#", 1)[1])
    assert "SERVICE <https://sophox.org/sparql>" in decoded_fragment
    assert "SERVICE <https://example.test/petscan/sparql/psid=43641756>" in decoded_fragment


def test_js_helper_build_wizard_query_includes_gil_link_enrichment_block(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "buildWizardQuery",
        [
            [
                {"source_key": "title"},
                {"source_key": "gil_link"},
                {"source_key": "gil_link_wikidata_id"},
            ],
            ["title", "gil_link", "gil_link_wikidata_id"],
        ],
    )

    assert "SELECT ?item ?title ?gil_link ?gil_link_wikidata_id" in result
    assert "?item petscan:gil_link ?gil_link ." in result
    assert "?gil_link petscan:gil_link_wikidata_id ?gil_link_wikidata_id ." in result
