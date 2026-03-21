import json
from typing import Any, Dict
from unittest.mock import patch
from urllib.parse import urlencode

from django.http import HttpResponse
from django.test import SimpleTestCase

API_STRUCTURE_PATH = "/petscan/api/structure"
SPARQL_PATH = "/petscan/sparql/psid=123"
SPARQL_FEDERATED_PATH = "/petscan/sparql/psid=43641756&categories=Turku"

ASK_QUERY = "ASK { ?s ?p ?o }"
FEDERATED_SUBQUERY = """
SELECT ?item ?title WHERE {
  ?item a <https://petscan.wmcloud.org/ontology/Page> .
  OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title }
}
LIMIT 20
""".strip()


class ApiViewTests(SimpleTestCase):
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

    def test_root_redirects_to_petscan_ui(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/petscan/")

    @patch("petscan.views.petscan_service.ensure_loaded")
    def test_structure_endpoint_returns_meta(self, ensure_loaded):
        ensure_loaded.return_value = {
            "psid": 123,
            "records": 2,
            "source_url": "https://petscan.wmcloud.org/?psid=123&format=json",
            "loaded_at": "2026-03-13T10:00:00+00:00",
            "source_params": {"category": ["Turku"]},
            "structure": {"row_count": 2, "field_count": 1, "fields": []},
        }

        response = self.client.get(
            API_STRUCTURE_PATH,
            data={"psid": 123, "refresh": "1", "category": "Turku"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["psid"], 123)
        self.assertEqual(payload["meta"]["records"], 2)
        ensure_loaded.assert_called_once_with(
            123,
            refresh=True,
            petscan_params={"category": ["Turku"]},
        )

    def test_structure_endpoint_rejects_non_get(self):
        response = self._post_json(API_STRUCTURE_PATH, {"psid": 123})

        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["error"], "Method not allowed. Use GET.")

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_returns_sparql_json(self, execute_query):
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            SPARQL_PATH,
            data={"query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertEqual(json.loads(response.content.decode("utf-8"))["boolean"], True)
        execute_query.assert_called_once_with(123, ASK_QUERY, refresh=False, petscan_params={})

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_accepts_protocol_post(self, execute_query):
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            SPARQL_PATH + "&refresh=1",
            data=ASK_QUERY,
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(123, ASK_QUERY, refresh=True, petscan_params={})

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_accepts_form_urlencoded_post(self, execute_query):
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            SPARQL_PATH + "&refresh=1",
            data=urlencode({"query": ASK_QUERY}),
            content_type="application/x-www-form-urlencoded",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(123, ASK_QUERY, refresh=True, petscan_params={})

    def test_sparql_endpoint_rejects_non_sparql_query_post_content_type(self):
        with self.assertLogs("petscan.views", level="WARNING") as captured_logs:
            response = self.client.post(
                SPARQL_PATH,
                data=json.dumps({"query": ASK_QUERY}),
                content_type="application/json",
                headers={"User-Agent": "external-query-ui-test-agent/1.0"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "POST /petscan/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded.",
        )
        self.assertTrue(any("[sparql-content-type-debug]" in message for message in captured_logs.output))
        self.assertTrue(any("application/json" in message for message in captured_logs.output))

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_forwards_extra_query_params_to_petscan(self, execute_query):
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            "/petscan/sparql/psid=123&category=Turku&language=fi",
            data={
                "query": ASK_QUERY,
            },
        )

        self.assertEqual(response.status_code, 200)
        execute_query.assert_called_once_with(
            123,
            ASK_QUERY,
            refresh=False,
            petscan_params={"category": ["Turku"], "language": ["fi"]},
        )

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_returns_400_for_service_clause(self, execute_query):
        execute_query.side_effect = ValueError("SERVICE clauses are not allowed in this endpoint.")

        response = self.client.get(
            SPARQL_PATH,
            data={"query": "SELECT * WHERE { SERVICE <https://x> { ?s ?p ?o } }"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SERVICE clauses are not allowed in this endpoint.",
        )

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_accepts_federated_subquery_style_request(self, execute_query):
        execute_query.return_value = {
            "query_type": "SELECT",
            "result_format": "sparql-json",
            "sparql_json": {"head": {"vars": ["item", "title"]}, "results": {"bindings": []}},
            "meta": {},
        }

        response = self.client.post(
            SPARQL_FEDERATED_PATH,
            data=FEDERATED_SUBQUERY,
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(
            43641756,
            FEDERATED_SUBQUERY,
            refresh=False,
            petscan_params={"categories": ["Turku"]},
        )
