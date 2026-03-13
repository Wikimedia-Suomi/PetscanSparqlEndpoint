import json
from unittest.mock import patch

from django.test import SimpleTestCase


class ApiViewTests(SimpleTestCase):
    @patch("petscan.views.petscan_service.ensure_loaded")
    def test_load_endpoint(self, ensure_loaded):
        ensure_loaded.return_value = {"psid": 123, "records": 2}

        response = self.client.post(
            "/api/load",
            data=json.dumps({"psid": 123, "refresh": True}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["psid"], 123)
        ensure_loaded.assert_called_once_with(123, refresh=True)

    @patch("petscan.views.petscan_service.execute_query")
    def test_query_endpoint_select(self, execute_query):
        execute_query.return_value = {
            "query_type": "SELECT",
            "result_format": "sparql-json",
            "sparql_json": {
                "head": {"vars": ["item"]},
                "results": {"bindings": [{"item": {"type": "uri", "value": "x"}}]},
            },
            "meta": {"records": 1},
        }

        response = self.client.post(
            "/api/query",
            data=json.dumps({"psid": 123, "query": "SELECT * WHERE { ?s ?p ?o } LIMIT 1"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["query_type"], "SELECT")
        self.assertEqual(payload["result_format"], "sparql-json")

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_returns_sparql_json(self, execute_query):
        execute_query.return_value = {
            "query_type": "ASK",
            "result_format": "sparql-json",
            "sparql_json": {"head": {}, "boolean": True},
            "meta": {},
        }

        response = self.client.get(
            "/sparql",
            data={"psid": 123, "query": "ASK { ?s ?p ?o }"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/sparql-results+json", response["Content-Type"])
        self.assertEqual(json.loads(response.content.decode("utf-8"))["boolean"], True)
        execute_query.assert_called_once_with(123, "ASK { ?s ?p ?o }", refresh=False, petscan_params={})

    @patch("petscan.views.petscan_service.execute_query")
    def test_sparql_endpoint_forwards_extra_query_params_to_petscan(self, execute_query):
        execute_query.return_value = {
            "query_type": "ASK",
            "result_format": "sparql-json",
            "sparql_json": {"head": {}, "boolean": True},
            "meta": {},
        }

        response = self.client.get(
            "/sparql",
            data={
                "psid": 123,
                "query": "ASK { ?s ?p ?o }",
                "category": "Turku",
                "language": "fi",
            },
        )

        self.assertEqual(response.status_code, 200)
        execute_query.assert_called_once_with(
            123,
            "ASK { ?s ?p ?o }",
            refresh=False,
            petscan_params={"category": ["Turku"], "language": ["fi"]},
        )

    @patch("petscan.views.petscan_service.execute_query")
    def test_query_endpoint_returns_400_for_service_clause(self, execute_query):
        execute_query.side_effect = ValueError("SERVICE clauses are not allowed in this endpoint.")

        response = self.client.post(
            "/api/query",
            data=json.dumps({"psid": 123, "query": "SELECT * WHERE { SERVICE <https://x> { ?s ?p ?o } }"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "SERVICE clauses are not allowed in this endpoint.")
