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


def test_js_helper_build_named_service_param_path_supports_quarry_ids(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "buildNamedServiceParamPath",
        ["quarry_id", "103479", [["limit", "25"]], True],
    )

    assert result == "quarry_id=103479&refresh=1&limit=25"


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


def test_js_helper_build_wizard_query_treats_gil_link_count_as_scalar_field(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "buildWizardQuery",
        [
            [
                {"source_key": "title"},
                {"source_key": "gil_link_count"},
                {"source_key": "gil_link"},
            ],
            ["title", "gil_link_count"],
        ],
    )

    assert "SELECT ?item ?title ?gil_link_count" in result
    assert "?item petscan:gil_link_count ?gil_link_count ." in result
    assert "?item petscan:gil_link ?gil_link ." not in result


def test_js_helper_build_incubator_wizard_query_does_not_duplicate_subject_as_incubator_url(
    page: Page, live_server: Any
) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "buildIncubatorWizardQuery",
        [
            [
                {"source_key": "incubator_url", "predicate": "https://incubator.wikimedia.org/ontology/incubator_url"},
                {"source_key": "wikidata_entity", "predicate": "http://schema.org/about"},
            ],
            ["incubator_url", "wikidata_entity"],
            "incubator_page",
        ],
    )

    assert "SELECT ?incubator_page ?wikidata_entity" in result
    assert "?incubator_url" not in result
    assert "BIND(?incubator_page AS ?incubator_url)" not in result


def test_js_helper_normalize_selected_query_field_keys_falls_back_to_first_five_fields(
    page: Page, live_server: Any
) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "normalizeSelectedQueryFieldKeys",
        [
            [
                {"source_key": "alpha"},
                {"source_key": "beta"},
                {"source_key": "gamma"},
                {"source_key": "delta"},
                {"source_key": "epsilon"},
                {"source_key": "zeta"},
            ],
            ["title", "namespace"],
            5,
        ],
    )

    assert result == {
        "keys": ["alpha", "beta", "gamma", "delta", "epsilon"],
        "changed": True,
        "usedFallback": True,
    }


def test_js_helper_build_quarry_urls(page: Page, live_server: Any) -> None:
    query_url = _call_js_helper(page, live_server, "buildQuarryQueryUrl", ["103479"])
    json_url = _call_js_helper(page, live_server, "buildQuarryJsonUrl", ["1084251"])

    assert query_url == "https://quarry.wmcloud.org/query/103479"
    assert json_url == "https://quarry.wmcloud.org/run/1084251/output/0/json"


def test_js_helper_normalize_newpages_user_list_page_from_direct_url(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "normalizeNewpagesUserListPage",
        ["https://fi.wikipedia.org/wiki/Wikipedia:Viikon_kilpailu/Viikon_kilpailu_2026-15"],
    )

    assert result == ":w:fi:Wikipedia:Viikon_kilpailu/Viikon_kilpailu_2026-15"


def test_js_helper_normalize_newpages_user_list_page_keeps_interwiki_shape(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "normalizeNewpagesUserListPage",
        [":meta:Steward_requests/Permissions"],
    )

    assert result == ":meta:Steward_requests/Permissions"


def test_js_helper_normalize_newpages_wikis_prefers_short_canonical_tokens(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "normalizeNewpagesWikis",
        ["fi.wikipedia.org, w:se, b:fi, commons.wikimedia.org, www.wikidata.org, incubator, meta.wikimedia.org"],
    )

    assert result == "fi, se, b:fi, commons, wikidata, incubator, meta"


def test_js_helper_safe_external_href_only_allows_http_and_https(page: Page, live_server: Any) -> None:
    assert (
        _call_js_helper(page, live_server, "safeExternalHref", ["https://example.org/resource"])
        == "https://example.org/resource"
    )
    assert (
        _call_js_helper(page, live_server, "safeExternalHref", ["http://example.org/resource"])
        == "http://example.org/resource"
    )
    assert _call_js_helper(page, live_server, "safeExternalHref", ["gopher://example.org/resource"]) == ""
    assert _call_js_helper(page, live_server, "safeExternalHref", ["javascript://alert(1)"]) == ""


def test_js_helper_build_wizard_query_with_custom_subject_variable(page: Page, live_server: Any) -> None:
    result = _call_js_helper(
        page,
        live_server,
        "buildWizardQueryWithOntology",
        [
            [
                {"source_key": "rc_title"},
            ],
            ["rc_title"],
            "quarrycol",
            "https://quarry.wmcloud.org/ontology/",
            "quarry_row_id",
            [["quarry", "https://quarry.wmcloud.org/query/"]],
        ],
    )

    assert "PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>" in result
    assert "PREFIX quarry: <https://quarry.wmcloud.org/query/>" in result
    assert "SELECT ?quarry_row_id ?rc_title" in result
    assert "?quarry_row_id a quarrycol:Page ." in result
    assert "?quarry_row_id quarrycol:rc_title ?rc_title ." in result
