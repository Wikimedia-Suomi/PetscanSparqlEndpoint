import json
from typing import Any, Dict
from unittest.mock import patch

from django.http import HttpResponse
from django.test import SimpleTestCase

from petscan.service_errors import PetscanServiceError

QUARRY_API_STRUCTURE_PATH = "/quarry/api/structure"
QUARRY_SPARQL_PATH = "/quarry/sparql/quarry_id=103479"

ASK_QUERY = "ASK { ?s ?p ?o }"


class QuarryApiViewTests(SimpleTestCase):
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

    def test_quarry_index_renders(self) -> None:
        response = self.client.get("/quarry/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="breadcrumb-nav page-breadcrumb"', html=False)
        self.assertContains(response, 'aria-label="Breadcrumb"', html=False)
        self.assertContains(response, '<li><a href="/">All data sources</a></li>', html=True)
        self.assertContains(response, '<li aria-current="page">Quarry</li>', html=True)
        self.assertContains(response, "<h1>Quarry SPARQL endpoint</h1>", html=True)
        self.assertContains(response, 'class="source-layout"', html=False)
        self.assertContains(response, 'class="card source-info-card"', html=False)
        self.assertContains(response, 'aria-labelledby="quarry-info-heading"', html=False)
        self.assertContains(response, '<h2 id="quarry-info-heading">About Quarry</h2>', html=True)
        self.assertContains(response, '<th scope="col">Cardinality</th>', html=True)
        self.assertContains(
            response,
            "is a public querying interface for Wiki Replicas and ToolsDBs. SPARQL Bridge uses the",
            html=False,
        )
        self.assertContains(response, 'href="https://meta.wikimedia.org/wiki/Research:Quarry"', html=False)
        self.assertContains(response, "Open Quarry [[ quarryId ]]")

    @patch("quarry.views.quarry_service.ensure_loaded")
    def test_structure_endpoint_returns_meta_qrun_id_and_query_db(self, ensure_loaded: Any) -> None:
        ensure_loaded.return_value = {
            "psid": 2000103479,
            "records": 2,
            "source_url": "https://quarry.wmcloud.org/run/1084251/output/0/json",
            "loaded_at": "2026-03-21T10:00:00+00:00",
            "source_params": {
                "quarry_id": ["103479"],
                "qrun_id": ["1084251"],
                "query_db": ["fiwiki_p"],
                "limit": ["10"],
            },
            "structure": {"row_count": 2, "field_count": 1, "fields": []},
        }

        response = self.client.get(
            QUARRY_API_STRUCTURE_PATH,
            data={"quarry_id": 103479, "refresh": "1", "limit": "10"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["quarry_id"], 103479)
        self.assertEqual(payload["qrun_id"], 1084251)
        self.assertEqual(payload["query_db"], "fiwiki_p")
        self.assertEqual(payload["meta"]["records"], 2)
        ensure_loaded.assert_called_once_with(103479, refresh=True, limit=10)

    def test_structure_endpoint_rejects_non_get(self) -> None:
        response = self._post_json(QUARRY_API_STRUCTURE_PATH, {"quarry_id": 103479})

        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["error"], "Method not allowed. Use GET.")

    @patch("quarry.views.quarry_service.ensure_loaded")
    def test_structure_endpoint_sanitizes_service_errors_with_public_message(self, ensure_loaded: Any) -> None:
        ensure_loaded.side_effect = PetscanServiceError(
            "Failed to fetch Quarry JSON data: <urlopen error timed out>",
            public_message="Failed to load Quarry data from the upstream service.",
        )

        with self.assertLogs("quarry.views", level="ERROR") as captured_logs:
            response = self.client.get(QUARRY_API_STRUCTURE_PATH, data={"quarry_id": 103479})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"], "Failed to load Quarry data from the upstream service.")
        self.assertNotIn("timed out", response.content.decode("utf-8"))
        self.assertTrue(any("Returning sanitized backend error response" in entry for entry in captured_logs.output))
        self.assertTrue(any("Failed to fetch Quarry JSON data" in entry for entry in captured_logs.output))

    @patch("quarry.views.quarry_service.execute_query")
    def test_sparql_endpoint_returns_sparql_json(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            QUARRY_SPARQL_PATH + "&limit=25",
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertEqual(json.loads(response.content.decode("utf-8"))["boolean"], True)
        execute_query.assert_called_once_with(103479, ASK_QUERY, refresh=False, limit=25)

    @patch("quarry.views.quarry_service.execute_query")
    def test_sparql_endpoint_accepts_protocol_post(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            QUARRY_SPARQL_PATH + "&refresh=1&limit=10",
            data=ASK_QUERY,
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(103479, ASK_QUERY, refresh=True, limit=10)

    @patch("quarry.views.quarry_service.execute_query")
    def test_sparql_endpoint_rejects_invalid_utf8_protocol_post(self, execute_query: Any) -> None:
        response = self.client.post(
            QUARRY_SPARQL_PATH,
            data=b"\xff\xfeSELECT ?item WHERE { ?item ?p ?o }",
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SPARQL query body must be valid UTF-8.",
        )
        execute_query.assert_not_called()

    @patch("quarry.views.quarry_service.execute_query")
    def test_sparql_endpoint_rejects_oversized_protocol_post(self, execute_query: Any) -> None:
        response = self.client.post(
            QUARRY_SPARQL_PATH,
            data="A" * (500 * 1024 + 1),
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SPARQL query must be at most 500 KB.",
        )
        execute_query.assert_not_called()

    def test_sparql_endpoint_rejects_non_sparql_query_post_content_type(self) -> None:
        with self.assertLogs("quarry.views", level="WARNING") as captured_logs:
            response = self.client.post(
                QUARRY_SPARQL_PATH,
                data=json.dumps({"query": ASK_QUERY}),
                content_type="application/json",
                headers={"User-Agent": "external-query-ui-test-agent/1.0"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "POST /quarry/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded.",
        )
        self.assertTrue(any("[sparql-content-type-debug]" in message for message in captured_logs.output))

    @patch("quarry.views.quarry_service.execute_query")
    def test_sparql_endpoint_sanitizes_service_errors_with_public_message(self, execute_query: Any) -> None:
        execute_query.side_effect = PetscanServiceError(
            "Failed to open Oxigraph store: [Errno 2] No such file or directory: '/srv/quarry/2000103479'",
            public_message="Local data store is unavailable.",
        )

        with self.assertLogs("quarry.views", level="ERROR") as captured_logs:
            response = self.client.get(QUARRY_SPARQL_PATH, data={"query": ASK_QUERY})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.content.decode("utf-8"), "Local data store is unavailable.")
        self.assertNotIn("/srv/quarry/2000103479", response.content.decode("utf-8"))
        self.assertTrue(any("Returning sanitized backend error response" in entry for entry in captured_logs.output))
        self.assertTrue(any("Failed to open Oxigraph store" in entry for entry in captured_logs.output))
