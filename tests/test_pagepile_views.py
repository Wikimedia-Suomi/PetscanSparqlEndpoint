import json
from typing import Any, Dict
from unittest.mock import patch
from urllib.parse import urlencode

from django.http import HttpResponse
from django.test import SimpleTestCase

from petscan.service_errors import PetscanServiceError

PAGEPILE_API_STRUCTURE_PATH = "/pagepile/api/structure"
PAGEPILE_SPARQL_PATH = "/pagepile/sparql/pagepile_id=112306&limit=25"

ASK_QUERY = "ASK { ?s ?p ?o }"


class PagepileApiViewTests(SimpleTestCase):
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

    def test_pagepile_index_renders(self) -> None:
        response = self.client.get("/pagepile/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="breadcrumb-nav page-breadcrumb"', html=False)
        self.assertContains(response, 'aria-label="Breadcrumb"', html=False)
        self.assertContains(response, '<li><a href="/">All data sources</a></li>', html=True)
        self.assertContains(response, '<li aria-current="page">PagePile</li>', html=True)
        self.assertContains(response, "<h1>PagePile SPARQL endpoint</h1>", html=True)
        self.assertContains(response, '<h2 id="pagepile-info-heading">About PagePile</h2>', html=True)
        self.assertContains(response, "stores reusable lists of Wikimedia pages on a single wiki", html=False)
        self.assertContains(response, "up to <code>300000</code> PagePile pages", html=False)
        self.assertContains(response, "Open PagePile [[ pagepileId ]] JSON", html=False)
        self.assertContains(response, "Example query", html=False)
        self.assertContains(
            response,
            'href="https://qlever.wikidata.dbis.rwth-aachen.de/wikidata/?query=',
            html=False,
        )
        self.assertContains(
            response,
            "https%3A//sparqlbridge.toolforge.org/pagepile/sparql/pagepile_id%3D112306%26limit%3D50",
            html=False,
        )

    @patch("pagepile.views.pagepile_service.ensure_loaded")
    def test_structure_endpoint_returns_meta(self, ensure_loaded: Any) -> None:
        ensure_loaded.return_value = {
            "psid": 4000112306,
            "records": 2,
            "source_url": "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
            "loaded_at": "2026-04-19T08:00:00+00:00",
            "source_params": {
                "pagepile_id": ["112306"],
                "limit": ["10"],
            },
            "structure": {"row_count": 2, "field_count": 1, "fields": []},
        }

        response = self.client.get(
            PAGEPILE_API_STRUCTURE_PATH,
            data={
                "pagepile_id": "112306",
                "limit": "10",
                "refresh": "1",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["source"], "pagepile")
        self.assertEqual(payload["pagepile_id"], 112306)
        self.assertEqual(payload["limit"], 10)
        self.assertEqual(payload["meta"]["records"], 2)
        ensure_loaded.assert_called_once_with(
            112306,
            refresh=True,
            limit=10,
        )

    def test_structure_endpoint_rejects_non_get(self) -> None:
        response = self._post_json(PAGEPILE_API_STRUCTURE_PATH, {"pagepile_id": 112306})

        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["error"], "Method not allowed. Use GET.")

    def test_structure_endpoint_requires_pagepile_id(self) -> None:
        response = self.client.get(PAGEPILE_API_STRUCTURE_PATH)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "A numeric pagepile_id is required.")

    def test_structure_endpoint_rejects_limit_above_maximum(self) -> None:
        response = self.client.get(
            PAGEPILE_API_STRUCTURE_PATH,
            data={"pagepile_id": "112306", "limit": "300001"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "limit must be at most 300000.")

    @patch("pagepile.views.pagepile_service.ensure_loaded")
    def test_structure_endpoint_sanitizes_service_errors_with_public_message(self, ensure_loaded: Any) -> None:
        ensure_loaded.side_effect = PetscanServiceError(
            "Failed to fetch PagePile replica data: Access denied",
            public_message="Failed to load PagePile data from the upstream service.",
        )

        with self.assertLogs("pagepile.views", level="ERROR") as captured_logs:
            response = self.client.get(PAGEPILE_API_STRUCTURE_PATH, data={"pagepile_id": "112306"})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["error"],
            "Failed to load PagePile data from the upstream service.",
        )
        self.assertTrue(any("Returning sanitized backend error response" in entry for entry in captured_logs.output))

    @patch("pagepile.views.pagepile_service.ensure_loaded")
    def test_structure_endpoint_propagates_unsanitized_service_error_when_public_message_missing(
        self, ensure_loaded: Any
    ) -> None:
        ensure_loaded.side_effect = PetscanServiceError("PagePile payload is missing pages array.")

        response = self.client.get(PAGEPILE_API_STRUCTURE_PATH, data={"pagepile_id": "112306"})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"], "PagePile payload is missing pages array.")

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_returns_sparql_json(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            PAGEPILE_SPARQL_PATH,
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertEqual(json.loads(response.content.decode("utf-8"))["boolean"], True)
        execute_query.assert_called_once_with(
            112306,
            ASK_QUERY,
            refresh=False,
            limit=25,
        )

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_rejects_empty_query(self, execute_query: Any) -> None:
        response = self.client.get(PAGEPILE_SPARQL_PATH, data={"query": ""})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content.decode("utf-8"), "query must not be empty.")
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_not_called()

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_rejects_missing_pagepile_id_in_path(self, execute_query: Any) -> None:
        response = self.client.get("/pagepile/sparql/limit=25", data={"query": ASK_QUERY})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "A numeric pagepile_id is required in path parameters.",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_not_called()

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_rejects_invalid_limit_in_path(self, execute_query: Any) -> None:
        response = self.client.get(
            "/pagepile/sparql/pagepile_id=112306&limit=oops",
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content.decode("utf-8"), "limit must be an integer.")
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_not_called()

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_rejects_invalid_utf8_protocol_post(self, execute_query: Any) -> None:
        response = self.client.post(
            PAGEPILE_SPARQL_PATH,
            data=b"\xff\xfeSELECT ?page WHERE { ?page ?p ?o }",
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SPARQL query body must be valid UTF-8.",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_not_called()

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_rejects_oversized_protocol_post_query(self, execute_query: Any) -> None:
        response = self.client.post(
            PAGEPILE_SPARQL_PATH,
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

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_accepts_form_urlencoded_post(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            PAGEPILE_SPARQL_PATH + "&refresh=1",
            data=urlencode({"query": ASK_QUERY}),
            content_type="application/x-www-form-urlencoded",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_called_once_with(
            112306,
            ASK_QUERY,
            refresh=True,
            limit=25,
        )

    def test_sparql_endpoint_rejects_non_sparql_query_post_content_type(self) -> None:
        with self.assertLogs("pagepile.views", level="WARNING") as captured_logs:
            response = self.client.post(
                PAGEPILE_SPARQL_PATH,
                data=json.dumps({"query": ASK_QUERY}),
                content_type="application/json",
                headers={"User-Agent": "external-query-ui-test-agent/1.0"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "POST /pagepile/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded.",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        self.assertTrue(any("[sparql-content-type-debug]" in message for message in captured_logs.output))
        self.assertTrue(any("application/json" in message for message in captured_logs.output))

    def test_sparql_endpoint_returns_cors_headers_for_options_preflight(self) -> None:
        response = self.client.options(PAGEPILE_SPARQL_PATH)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        self.assertEqual(response["Access-Control-Allow-Methods"], "GET, POST, OPTIONS")
        self.assertEqual(response["Access-Control-Allow-Headers"], "Content-Type, Accept")

    @patch("pagepile.views.pagepile_service.execute_query")
    def test_sparql_endpoint_returns_plain_text_for_construct(self, execute_query: Any) -> None:
        execute_query.return_value = {
            "query_type": "CONSTRUCT",
            "result_format": "n-triples",
            "ntriples": "<https://example.org/page> <https://example.org/p> <https://example.org/o> .\n",
            "meta": {},
        }

        response = self.client.get(
            PAGEPILE_SPARQL_PATH,
            data={"query": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/n-triples", response["Content-Type"])
        self.assertEqual(
            response.content.decode("utf-8"),
            "<https://example.org/page> <https://example.org/p> <https://example.org/o> .\n",
        )
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        execute_query.assert_called_once_with(
            112306,
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
            refresh=False,
            limit=25,
        )
