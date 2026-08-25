from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from petscan.service_errors import PetscanServiceError


class PlacenamesViewTests(SimpleTestCase):
    def test_index_documents_static_dataset(self) -> None:
        response = self.client.get("/placenames/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            "<h1>Sámi place names SPARQL endpoint</h1>",
            html=True,
        )
        self.assertContains(response, 'class="source-layout"', html=False)
        self.assertContains(response, 'class="card source-info-card"', html=False)
        self.assertContains(response, "<h2 id=\"data-loading-heading\">Data Loading</h2>", html=True)
        self.assertContains(
            response,
            "<h2 id=\"placenames-info-heading\">About PlaceNames</h2>",
            html=True,
        )
        self.assertContains(
            response,
            "Select fields to include in the SPARQL query from loaded Sámi place-name data.",
        )
        self.assertContains(
            response,
            'href="https://www.maanmittauslaitos.fi/en/maps-and-spatial-data/'
            'datasets-and-interfaces/product-descriptions/geographic-names"',
            html=False,
        )
        self.assertContains(response, "CC BY 4.0")
        self.assertContains(response, 'data-example-query-source="placenames"')
        self.assertContains(response, '<main id="app" class="page" v-cloak>', html=False)
        self.assertContains(response, 'v-if="querySectionReady"', html=False)
        self.assertContains(response, "vendor/vue.global.prod.min.js", html=False)
        self.assertContains(response, 'id="placenames-query-section"', html=False)
        self.assertContains(response, "<h2 id=\"endpoint-heading\">SPARQL Endpoint</h2>", html=True)
        self.assertContains(response, "/placenames/sparql/dataset=saami")

    @patch("placenames.views.service.ensure_loaded")
    def test_structure_returns_dataset_metadata(self, ensure_loaded: Any) -> None:
        ensure_loaded.return_value = {"dataset": "saami", "records": 11956}

        response = self.client.get("/placenames/api/structure", data={"dataset": "saami"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["source"], "placenames")
        self.assertEqual(response.json()["meta"]["records"], 11956)
        ensure_loaded.assert_called_once_with("saami")

    @patch("placenames.views.service.execute_query")
    def test_sparql_get_returns_json_and_cors(self, execute_query: Any) -> None:
        execute_query.return_value = {
            "query_type": "ASK",
            "result_format": "sparql-json",
            "sparql_json": {"head": {}, "boolean": True},
            "meta": {},
        }

        response = self.client.get(
            "/placenames/sparql/dataset=saami", data={"query": "ASK { ?s ?p ?o }"}
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Access-Control-Allow-Origin"], "*")
        self.assertEqual(response.json()["boolean"], True)
        execute_query.assert_called_once_with("saami", "ASK { ?s ?p ?o }")

    def test_unknown_path_parameter_is_rejected(self) -> None:
        response = self.client.get(
            "/placenames/sparql/dataset=saami&refresh=1",
            data={"query": "ASK { ?s ?p ?o }"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported path parameter", response.content.decode())

    @patch("placenames.views.service.ensure_loaded")
    def test_backend_details_are_sanitized(self, ensure_loaded: Any) -> None:
        ensure_loaded.side_effect = PetscanServiceError(
            "secret filesystem path", public_message="Local place-name data is unavailable."
        )

        response = self.client.get("/placenames/api/structure", data={"dataset": "saami"})

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"error": "Local place-name data is unavailable."})
        self.assertNotContains(response, "secret filesystem path", status_code=503)

    @patch("placenames.views.service.ensure_loaded")
    def test_production_sanitizes_service_errors_without_public_message(
        self, ensure_loaded: Any
    ) -> None:
        ensure_loaded.side_effect = PetscanServiceError(
            "failed to open /srv/private/oxigraph/store"
        )

        with override_settings(DEBUG=False):
            response = self.client.get(
                "/placenames/api/structure", data={"dataset": "saami"}
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"error": "Local place-name service is unavailable."})
        self.assertNotContains(response, "/srv/private/oxigraph/store", status_code=503)

    def test_options_advertises_federated_query_methods(self) -> None:
        response = self.client.options("/placenames/sparql/dataset=saami")

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response["Access-Control-Allow-Methods"], "GET, POST, OPTIONS")
