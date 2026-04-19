import json
from typing import Any, Dict
from unittest.mock import patch

from django.http import HttpResponse
from django.test import SimpleTestCase

from petscan.service_errors import PetscanServiceError

NEWPAGES_API_STRUCTURE_PATH = "/newpages/api/structure"
NEWPAGES_SPARQL_PATH = "/newpages/sparql"
NEWPAGES_FILTERED_SPARQL_PATH = (
    "/newpages/sparql/limit=25&wiki=fi%2Csv&timestamp=202604"
)
NEWPAGES_FILTERED_WITH_USER_LIST_SPARQL_PATH = (
    "/newpages/sparql/limit=25&wiki=fi&timestamp=202604"
    "&user_list_page=%3Aw%3Afi%3AWikipedia%3AUsers"
)
NEWPAGES_FILTERED_WITH_EDITS_SPARQL_PATH = (
    "/newpages/sparql/limit=25&wiki=fi&timestamp=202604"
    "&user_list_page=%3Aw%3Afi%3AWikipedia%3AUsers&include_edited_pages=1"
)

ASK_QUERY = "ASK { ?s ?p ?o }"


class NewpagesViewTests(SimpleTestCase):
    def _post_json(self, path: str, payload: Dict[str, Any]) -> HttpResponse:
        return self.client.post(
            path,
            data=json.dumps(payload),
            content_type="application/json",
        )

    @staticmethod
    def _ask_execution_result() -> Dict[str, Any]:
        return {
            "query_type": "ASK",
            "result_format": "sparql-json",
            "sparql_json": {"head": {}, "boolean": True},
            "meta": {},
        }

    def test_newpages_index_renders(self) -> None:
        response = self.client.get("/newpages/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '<li><a href="/">All data sources</a></li>', html=True)
        self.assertContains(response, '<li aria-current="page">New Pages</li>', html=True)
        self.assertContains(response, "<h1>New Pages SPARQL endpoint</h1>", html=True)
        self.assertContains(response, 'id="newpages-wikis"', html=False)
        self.assertContains(response, 'id="newpages-timestamp"', html=False)
        self.assertContains(response, 'id="newpages-user-list-page"', html=False)
        self.assertContains(response, 'id="newpages-include-edited-pages"', html=False)
        self.assertContains(response, 'id="newpages-limit"', html=False)
        self.assertContains(response, "Open SiteMatrix", html=False)
        self.assertContains(response, "recentchanges", html=False)
        self.assertContains(response, "Supported projects:", html=False)
        self.assertContains(response, "b:fi", html=False)
        self.assertContains(response, "commons", html=False)
        self.assertContains(response, "wikidata", html=False)
        self.assertContains(response, "incubator", html=False)
        self.assertContains(response, "meta", html=False)
        self.assertContains(response, "Full hostnames such as", html=False)
        self.assertContains(response, "*.wikipedia.org", html=False)
        self.assertContains(response, "API mode scans up to", html=False)
        self.assertContains(response, "SQL mode is capped at", html=False)
        self.assertNotContains(response, "{% verbatim %}", html=False)
        self.assertContains(response, 'data-example-query-source="newpages"', html=False)
        self.assertContains(
            response,
            "js/example_query_links.js",
            html=False,
        )

    @patch("newpages.views.newpages_service.ensure_loaded")
    def test_structure_endpoint_returns_meta(self, ensure_loaded: Any) -> None:
        ensure_loaded.return_value = {
            "psid": 4000000000000,
            "records": 2,
            "source_url": "https://meta.wikimedia.org/wiki/Special:SiteMatrix",
            "loaded_at": "2026-04-04T08:00:00+00:00",
            "source_params": {
                "limit": ["10"],
                "wiki": ["fi", "sv"],
                "timestamp": ["20260400000000"],
                "user_list_page": [":w:fi:Wikipedia:Users"],
            },
            "structure": {"row_count": 2, "field_count": 1, "fields": []},
        }

        response = self.client.get(
            NEWPAGES_API_STRUCTURE_PATH,
            data={
                "limit": "10",
                "wiki": "fi.wikipedia.org, sv.wikipedia.org",
                "timestamp": "202604",
                "user_list_page": ":w:fi:Wikipedia:Users",
                "include_edited_pages": "1",
                "refresh": "1",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["source"], "newpages")
        self.assertEqual(payload["limit"], 10)
        self.assertEqual(payload["wiki_domains"], ["fi", "sv"])
        self.assertEqual(payload["timestamp"], "20260400000000")
        self.assertEqual(payload["user_list_page"], ":w:fi:Wikipedia:Users")
        self.assertEqual(payload["include_edited_pages"], True)
        ensure_loaded.assert_called_once_with(
            refresh=True,
            limit=10,
            wiki_domains=["fi", "sv"],
            timestamp="20260400000000",
            user_list_page=":w:fi:Wikipedia:Users",
            include_edited_pages=True,
        )

    def test_structure_endpoint_rejects_non_get(self) -> None:
        response = self._post_json(NEWPAGES_API_STRUCTURE_PATH, {"limit": 10})

        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["error"], "Method not allowed. Use GET.")

    def test_structure_endpoint_rejects_invalid_timestamp(self) -> None:
        response = self.client.get(
            NEWPAGES_API_STRUCTURE_PATH,
            data={"wiki": "fi.wikipedia.org", "timestamp": "20260"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()["error"],
            "timestamp must use YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHH, YYYYMMDDHHMM, or YYYYMMDDHHMMSS.",
        )

    def test_structure_endpoint_rejects_malformed_user_list_page(self) -> None:
        response = self.client.get(
            NEWPAGES_API_STRUCTURE_PATH,
            data={"wiki": "fi.wikipedia.org", "user_list_page": "not-a-valid-user-list-page"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()["error"],
            "user_list_page must be a Wikimedia wiki page in interwiki form or a direct https://.../wiki/... link.",
        )

    @patch("newpages.views.newpages_service.ensure_loaded")
    def test_structure_endpoint_returns_validation_error_when_user_list_page_has_no_user_links(
        self, ensure_loaded: Any
    ) -> None:
        ensure_loaded.side_effect = ValueError("user_list_page must link to at least one Wikimedia user page.")

        response = self.client.get(
            NEWPAGES_API_STRUCTURE_PATH,
            data={"wiki": "fi.wikipedia.org", "user_list_page": ":w:fi:Wikipedia:Users"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()["error"],
            "user_list_page must link to at least one Wikimedia user page.",
        )

    @patch("newpages.views.newpages_service.ensure_loaded")
    def test_structure_endpoint_sanitizes_service_errors_with_public_message(self, ensure_loaded: Any) -> None:
        ensure_loaded.side_effect = PetscanServiceError(
            "Failed to fetch new-page replica data for fi.wikipedia.org: Access denied",
            public_message="Failed to load new pages data from the upstream service.",
        )

        with self.assertLogs("newpages.views", level="ERROR") as captured_logs:
            response = self.client.get(NEWPAGES_API_STRUCTURE_PATH, data={"wiki": "fi.wikipedia.org"})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["error"],
            "Failed to load new pages data from the upstream service.",
        )
        self.assertTrue(any("Returning sanitized backend error response" in entry for entry in captured_logs.output))

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_passes_filters_from_path(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            NEWPAGES_FILTERED_WITH_USER_LIST_SPARQL_PATH,
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(
            ASK_QUERY,
            refresh=False,
            limit=25,
            wiki_domains=["fi"],
            timestamp="20260400000000",
            user_list_page=":w:fi:Wikipedia:Users",
            include_edited_pages=False,
        )

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_passes_include_edited_pages_from_path(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            NEWPAGES_FILTERED_WITH_EDITS_SPARQL_PATH,
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        execute_query.assert_called_once_with(
            ASK_QUERY,
            refresh=False,
            limit=25,
            wiki_domains=["fi"],
            timestamp="20260400000000",
            user_list_page=":w:fi:Wikipedia:Users",
            include_edited_pages=True,
        )

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_accepts_protocol_post_without_required_source_id(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            NEWPAGES_SPARQL_PATH,
            data=ASK_QUERY,
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(
            ASK_QUERY,
            refresh=False,
            limit=None,
            wiki_domains=[],
            timestamp=None,
            user_list_page=None,
            include_edited_pages=False,
        )

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_rejects_invalid_utf8_protocol_post(self, execute_query: Any) -> None:
        response = self.client.post(
            NEWPAGES_SPARQL_PATH,
            data=b"\xff\xfeSELECT ?item WHERE { ?item ?p ?o }",
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SPARQL query body must be valid UTF-8.",
        )
        execute_query.assert_not_called()

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_rejects_oversized_get_query(self, execute_query: Any) -> None:
        response = self.client.get(
            NEWPAGES_SPARQL_PATH,
            data={"query": "A" * (500 * 1024 + 1)},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SPARQL query must be at most 500 KB.",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_not_called()

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_rejects_oversized_protocol_post(self, execute_query: Any) -> None:
        response = self.client.post(
            NEWPAGES_SPARQL_PATH,
            data="A" * (500 * 1024 + 1),
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SPARQL query must be at most 500 KB.",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_not_called()

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_returns_validation_error_when_user_list_page_cannot_be_resolved(
        self, execute_query: Any
    ) -> None:
        execute_query.side_effect = ValueError(
            "user_list_page could not be resolved to an existing Wikimedia page "
            "(ref=:w:fi:Wikipedia:Users, domain=fi.wikipedia.org, namespace=4, db_title=Users)."
        )

        response = self.client.get(
            NEWPAGES_FILTERED_WITH_USER_LIST_SPARQL_PATH,
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            (
                "user_list_page could not be resolved to an existing Wikimedia page "
                "(ref=:w:fi:Wikipedia:Users, domain=fi.wikipedia.org, namespace=4, db_title=Users)."
            ),
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")

    def test_sparql_endpoint_returns_cors_headers_for_options_preflight(self) -> None:
        response = self.client.options(NEWPAGES_SPARQL_PATH)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        self.assertEqual(response["Access-Control-Allow-Methods"], "GET, POST, OPTIONS")
        self.assertEqual(response["Access-Control-Allow-Headers"], "Content-Type, Accept")

    def test_sparql_endpoint_rejects_non_sparql_query_post_content_type(self) -> None:
        with self.assertLogs("newpages.views", level="WARNING") as captured_logs:
            response = self.client.post(
                NEWPAGES_SPARQL_PATH,
                data=json.dumps({"query": ASK_QUERY}),
                content_type="application/json",
                headers={"User-Agent": "external-query-ui-test-agent/1.0"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "POST /newpages/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded.",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        self.assertTrue(any("[sparql-content-type-debug]" in message for message in captured_logs.output))

    @patch("newpages.views.newpages_service.execute_query")
    def test_sparql_endpoint_sanitizes_service_errors_with_public_message(self, execute_query: Any) -> None:
        execute_query.side_effect = PetscanServiceError(
            "SPARQL query failed: temporary backend failure",
            public_message="Failed to load new pages data from the upstream service.",
        )

        with self.assertLogs("newpages.views", level="ERROR") as captured_logs:
            response = self.client.get(
                NEWPAGES_FILTERED_SPARQL_PATH,
                data={"query": ASK_QUERY},
            )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.content.decode("utf-8"),
            "Failed to load new pages data from the upstream service.",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        self.assertTrue(any("Returning sanitized backend error response" in entry for entry in captured_logs.output))
