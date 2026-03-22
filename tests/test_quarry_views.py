import json
from typing import Any, Dict
from unittest.mock import patch

from django.http import HttpResponse
from django.test import SimpleTestCase

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
        self.assertContains(response, "<h2 id=\"source-info-heading\">About Quarry Bridge</h2>", html=True)
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
