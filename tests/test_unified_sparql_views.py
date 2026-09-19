from unittest.mock import Mock, patch

from django.http import HttpRequest, HttpResponse
from django.test import SimpleTestCase

from petscan_endpoint import views


class UnifiedSparqlEndpointTests(SimpleTestCase):
    def test_routes_petscan_parameters_to_existing_endpoint(self) -> None:
        endpoint = Mock(return_value=HttpResponse("ok"))

        with patch.dict(views._SPARQL_ENDPOINTS, {"petscan": endpoint}, clear=True):
            response = self.client.get(
                "/sparql?dataset=petscan&psid=43641756&output_limit=10&query=ASK%20%7B%7D"
            )

        self.assertEqual(response.status_code, 200)
        request = endpoint.call_args.args[0]
        self.assertIsInstance(request, HttpRequest)
        self.assertEqual(request.GET["query"], "ASK {}")
        self.assertEqual(
            endpoint.call_args.kwargs,
            {"service_params": "psid=43641756&output_limit=10"},
        )

    def test_preserves_repeated_source_parameters(self) -> None:
        endpoint = Mock(return_value=HttpResponse("ok"))

        with patch.dict(views._SPARQL_ENDPOINTS, {"incubator": endpoint}, clear=True):
            response = self.client.get(
                "/sparql?dataset=incubator&namespace=0&namespace=14&query=ASK%20%7B%7D"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            endpoint.call_args.kwargs,
            {"service_params": "namespace=0&namespace=14"},
        )

    def test_forwards_post_body_to_existing_endpoint(self) -> None:
        endpoint = Mock(return_value=HttpResponse("ok"))

        with patch.dict(views._SPARQL_ENDPOINTS, {"quarry": endpoint}, clear=True):
            response = self.client.post(
                "/sparql?dataset=quarry&quarry_id=103479&limit=10",
                data=b"ASK {}",
                content_type="application/sparql-query",
            )

        self.assertEqual(response.status_code, 200)
        request = endpoint.call_args.args[0]
        self.assertEqual(request.body, b"ASK {}")
        self.assertEqual(
            endpoint.call_args.kwargs,
            {"service_params": "quarry_id=103479&limit=10"},
        )

    def test_placenames_uses_default_inner_dataset(self) -> None:
        endpoint = Mock(return_value=HttpResponse("ok"))

        with patch.dict(views._SPARQL_ENDPOINTS, {"placenames": endpoint}, clear=True):
            response = self.client.get("/sparql?dataset=placenames&query=ASK%20%7B%7D")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(endpoint.call_args.kwargs, {"service_params": "dataset=saami"})

    def test_placenames_accepts_explicit_inner_dataset_alias(self) -> None:
        endpoint = Mock(return_value=HttpResponse("ok"))

        with patch.dict(views._SPARQL_ENDPOINTS, {"placenames": endpoint}, clear=True):
            response = self.client.get(
                "/sparql?dataset=placenames&placenames_dataset=saami&query=ASK%20%7B%7D"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(endpoint.call_args.kwargs, {"service_params": "dataset=saami"})

    def test_rejects_missing_duplicate_and_unsupported_datasets(self) -> None:
        for path in (
            "/sparql?query=ASK%20%7B%7D",
            "/sparql?dataset=petscan&dataset=&query=ASK%20%7B%7D",
            "/sparql?dataset=petscan&dataset=quarry&query=ASK%20%7B%7D",
            "/sparql?dataset=unknown&query=ASK%20%7B%7D",
        ):
            with self.subTest(path=path):
                response = self.client.get(path)

                self.assertEqual(response.status_code, 400)
                self.assertEqual(response["Access-Control-Allow-Origin"], "*")

    def test_options_without_dataset_returns_cors_preflight_response(self) -> None:
        response = self.client.options("/sparql")

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        self.assertEqual(response["Access-Control-Allow-Methods"], "GET, POST, OPTIONS")
