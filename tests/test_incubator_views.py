import json
from typing import Any, Dict
from unittest.mock import patch

from django.http import HttpResponse
from django.test import SimpleTestCase

from petscan.service_errors import PetscanServiceError

INCUBATOR_API_STRUCTURE_PATH = "/incubator/api/structure"
INCUBATOR_SPARQL_PATH = "/incubator/sparql"
INCUBATOR_FILTERED_SPARQL_PATH = "/incubator/sparql/limit=25"
INCUBATOR_RECENTCHANGES_SPARQL_PATH = "/incubator/sparql/limit=25&recentchanges_only=1"

ASK_QUERY = "ASK { ?s ?p ?o }"


class IncubatorApiViewTests(SimpleTestCase):
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

    def test_incubator_index_renders(self) -> None:
        response = self.client.get("/incubator/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="breadcrumb-nav page-breadcrumb"', html=False)
        self.assertContains(response, 'aria-label="Breadcrumb"', html=False)
        self.assertContains(response, '<li><a href="/">All data sources</a></li>', html=True)
        self.assertContains(response, '<li aria-current="page">Incubator</li>', html=True)
        self.assertContains(response, "<h1>Incubator SPARQL endpoint</h1>", html=True)
        self.assertContains(response, '<h2 id="incubator-info-heading">About Incubator</h2>', html=True)
        self.assertContains(response, "hosts test wikis for new language editions", html=False)
        self.assertContains(response, "Open Incubator category", html=False)
        self.assertContains(response, "Only pages edited during the last 30 days", html=False)

    @patch("incubator.views.incubator_service.ensure_loaded")
    def test_structure_endpoint_returns_meta(
        self,
        ensure_loaded: Any,
    ) -> None:
        ensure_loaded.return_value = {
            "psid": 3000000000000,
            "records": 2,
            "source_url": "https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
            "loaded_at": "2026-04-02T08:00:00+00:00",
            "source_params": {"limit": ["10"], "recentchanges_only": ["1"]},
            "structure": {"row_count": 2, "field_count": 1, "fields": []},
        }

        response = self.client.get(
            INCUBATOR_API_STRUCTURE_PATH,
            data={"limit": "10", "refresh": "1", "recentchanges_only": "1"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["source"], "incubator")
        self.assertEqual(payload["limit"], 10)
        self.assertEqual(payload["recentchanges_only"], True)
        self.assertEqual(payload["meta"]["records"], 2)
        ensure_loaded.assert_called_once_with(refresh=True, limit=10, recentchanges_only=True)

    def test_structure_endpoint_rejects_non_get(self) -> None:
        response = self._post_json(INCUBATOR_API_STRUCTURE_PATH, {"limit": 10})

        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["error"], "Method not allowed. Use GET.")

    @patch("incubator.views.incubator_service.ensure_loaded")
    def test_structure_endpoint_sanitizes_service_errors_with_public_message(self, ensure_loaded: Any) -> None:
        ensure_loaded.side_effect = PetscanServiceError(
            "Failed to fetch Incubator replica data: Access denied",
            public_message="Failed to load Incubator data from the upstream service.",
        )

        with self.assertLogs("incubator.views", level="ERROR") as captured_logs:
            response = self.client.get(INCUBATOR_API_STRUCTURE_PATH, data={"limit": "10"})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["error"],
            "Failed to load Incubator data from the upstream service.",
        )
        self.assertTrue(any("Returning sanitized backend error response" in entry for entry in captured_logs.output))

    @patch("incubator.views.incubator_service.execute_query")
    def test_sparql_endpoint_returns_sparql_json(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            INCUBATOR_FILTERED_SPARQL_PATH,
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertEqual(json.loads(response.content.decode("utf-8"))["boolean"], True)
        execute_query.assert_called_once_with(
            ASK_QUERY,
            refresh=False,
            limit=25,
            recentchanges_only=False,
        )

    @patch("incubator.views.incubator_service.execute_query")
    def test_sparql_endpoint_passes_recentchanges_filter_from_path(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            INCUBATOR_RECENTCHANGES_SPARQL_PATH,
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(
            ASK_QUERY,
            refresh=False,
            limit=25,
            recentchanges_only=True,
        )

    @patch("incubator.views.incubator_service.execute_query")
    def test_sparql_endpoint_accepts_protocol_post_without_required_source_id(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            INCUBATOR_SPARQL_PATH,
            data=ASK_QUERY,
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(
            ASK_QUERY,
            refresh=False,
            limit=None,
            recentchanges_only=False,
        )

    def test_sparql_endpoint_rejects_non_sparql_query_post_content_type(self) -> None:
        with self.assertLogs("incubator.views", level="WARNING") as captured_logs:
            response = self.client.post(
                INCUBATOR_SPARQL_PATH,
                data=json.dumps({"query": ASK_QUERY}),
                content_type="application/json",
                headers={"User-Agent": "external-query-ui-test-agent/1.0"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "POST /incubator/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded.",
        )
        self.assertTrue(any("[sparql-content-type-debug]" in message for message in captured_logs.output))
