import json
from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase

from jsonstat import service_source
from petscan.service_errors import PetscanServiceError

EXAMPLE_URL = "https://pxdata.stat.fi/PxWeb/sq/552d1f53-bdab-472b-a8e7-68b5b8c37cda"
SOURCE_TOKEN = service_source.encode_source_token(EXAMPLE_URL)
SPARQL_PATH = "/jsonstat/sparql/source={}".format(SOURCE_TOKEN)
ASK_QUERY = "ASK { ?s ?p ?o }"


class JsonstatViewTests(SimpleTestCase):
    @staticmethod
    def _ask_execution_result() -> dict[str, Any]:
        return {
            "query_type": "ASK",
            "result_format": "sparql-json",
            "sparql_json": {"head": {}, "boolean": True},
            "meta": {},
        }

    def test_index_renders_url_input_and_examples(self) -> None:
        response = self.client.get("/jsonstat/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "<h1>JSON-stat 2 SPARQL endpoint</h1>", html=True)
        self.assertContains(response, 'id="jsonstat-url"', html=False)
        self.assertContains(response, EXAMPLE_URL, html=False)
        self.assertContains(response, "About JSON-stat 2")
        self.assertContains(response, "Only these domains and", html=False)
        self.assertContains(response, "their subdomains are allowed", html=False)
        self.assertContains(response, "<code>stat.fi</code>", html=True)
        self.assertContains(response, "open classification API")
        self.assertContains(response, "dimension identifier and category code match exactly")
        self.assertContains(response, "W3C RDF Data Cube")
        self.assertContains(response, "Open query as Federated query in...")
        self.assertContains(response, '<th scope="col">Cardinality</th>', html=True)

    @patch("jsonstat.views.jsonstat_service.ensure_loaded")
    def test_structure_endpoint_returns_meta_and_source_token(self, ensure_loaded: Any) -> None:
        ensure_loaded.return_value = {
            "psid": 123,
            "records": 7,
            "source_url": EXAMPLE_URL,
            "loaded_at": "2026-08-30T10:00:00+00:00",
            "source_params": {"url": [EXAMPLE_URL]},
            "structure": {"row_count": 7, "field_count": 1, "fields": []},
        }

        response = self.client.get(
            "/jsonstat/api/structure",
            data={"url": EXAMPLE_URL, "refresh": "1"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["url"], EXAMPLE_URL)
        self.assertEqual(response.json()["source_token"], SOURCE_TOKEN)
        ensure_loaded.assert_called_once_with(EXAMPLE_URL, refresh=True)

    def test_structure_endpoint_rejects_non_https_url(self) -> None:
        response = self.client.get(
            "/jsonstat/api/structure",
            data={"url": "http://example.org/data.json"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "The JSON-stat source URL must use HTTPS.")

    @patch("jsonstat.views.jsonstat_service.ensure_loaded")
    def test_structure_endpoint_rejects_non_pxweb_url_before_loading(
        self, ensure_loaded: Any
    ) -> None:
        response = self.client.get(
            "/jsonstat/api/structure",
            data={"url": "https://stat.fi/data.json"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("must be a PxWeb saved-query URL", response.json()["error"])
        ensure_loaded.assert_not_called()

    @patch("jsonstat.views.jsonstat_service.ensure_loaded")
    def test_structure_endpoint_sanitizes_transport_errors(self, ensure_loaded: Any) -> None:
        ensure_loaded.side_effect = PetscanServiceError(
            "Failed to fetch JSON-stat data: connection refused",
            public_message="Failed to load JSON-stat data from the upstream service.",
        )
        with self.assertLogs("jsonstat.views", level="ERROR"):
            response = self.client.get("/jsonstat/api/structure", data={"url": EXAMPLE_URL})
        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["error"],
            "Failed to load JSON-stat data from the upstream service.",
        )

    @patch("jsonstat.views.jsonstat_service.execute_query")
    def test_sparql_endpoint_accepts_protocol_post(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            SPARQL_PATH + "&refresh=1",
            data=ASK_QUERY,
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertTrue(json.loads(response.content.decode("utf-8"))["boolean"])
        execute_query.assert_called_once_with(EXAMPLE_URL, ASK_QUERY, refresh=True)

    @patch("jsonstat.views.jsonstat_service.execute_query")
    def test_sparql_endpoint_accepts_get(self, execute_query: Any) -> None:
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(SPARQL_PATH, data={"query": ASK_QUERY})

        self.assertEqual(response.status_code, 200)
        execute_query.assert_called_once_with(EXAMPLE_URL, ASK_QUERY, refresh=False)

    def test_sparql_endpoint_rejects_invalid_source_token(self) -> None:
        response = self.client.get(
            "/jsonstat/sparql/source=not-valid!",
            data={"query": ASK_QUERY},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("source token is invalid", response.content.decode("utf-8"))
