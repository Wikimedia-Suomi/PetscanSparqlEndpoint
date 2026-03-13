import json
from typing import Any, Dict
from unittest.mock import patch

from django.test import SimpleTestCase

API_STRUCTURE_PATH = "/api/structure"
SPARQL_PATH = "/sparql"

ASK_QUERY = "ASK { ?s ?p ?o }"


class ApiViewTests(SimpleTestCase):
    def _post_json(self, path: str, payload: Dict[str, Any]):
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
            data={"psid": 123, "query": ASK_QUERY},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertEqual(json.loads(response.content.decode("utf-8"))["boolean"], True)
        execute_query.assert_called_once_with(123, ASK_QUERY, refresh=False, petscan_params={})

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_accepts_protocol_post(self, execute_query):
        execute_query.return_value = self._ask_execution_result()

        response = self.client.post(
            SPARQL_PATH + "?psid=123&refresh=1",
            data=ASK_QUERY,
            content_type="application/sparql-query",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        execute_query.assert_called_once_with(123, ASK_QUERY, refresh=True, petscan_params={})

    def test_sparql_endpoint_rejects_non_sparql_query_post_content_type(self):
        response = self.client.post(
            SPARQL_PATH + "?psid=123",
            data=json.dumps({"query": ASK_QUERY}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "POST /sparql requires Content-Type: application/sparql-query.",
        )

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_forwards_extra_query_params_to_petscan(self, execute_query):
        execute_query.return_value = self._ask_execution_result()

        response = self.client.get(
            SPARQL_PATH,
            data={
                "psid": 123,
                "query": ASK_QUERY,
                "category": "Turku",
                "language": "fi",
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
            data={"psid": 123, "query": "SELECT * WHERE { SERVICE <https://x> { ?s ?p ?o } }"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.content.decode("utf-8"),
            "SERVICE clauses are not allowed in this endpoint.",
        )
