from unittest.mock import patch

from petscan import service_store as store
from petscan import service_store_builder as store_builder
from petscan.service_errors import PetscanServiceError
from tests.service_test_support import STORE_GIL_TEST_PSID, ServiceTestCase


class ServiceStoreBuilderTests(ServiceTestCase):
    @patch("petscan.service_store_builder.Store", None)
    def test_build_store_raises_clear_error_when_pyoxigraph_missing(self):
        with self.assertRaises(PetscanServiceError) as context:
            store_builder.build_store(123, [{"id": 1, "title": "Example"}], "https://example.invalid")
        self.assertIn("pyoxigraph is not installed", str(context.exception))

    @patch("petscan.service_store_builder.links.build_gil_link_enrichment_map")
    def test_store_contains_gil_link_relation_triples(self, gil_map_mock):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        link_uri = "https://en.wikipedia.org/wiki/Federalist_No._42"
        gil_map_mock.return_value = {
            link_uri: {
                "wikidata_id": "Q5440615",
                "page_len": 12345,
                "rev_timestamp": "2026-03-15T10:00:00Z",
            }
        }
        psid = STORE_GIL_TEST_PSID
        self._cleanup_store(psid)

        records = [{"id": 1, "title": "Example", "gil": "enwiki:0:Federalist_No._42"}]
        store_builder.build_store(psid, records, "https://example.invalid")
        store_instance = store_builder.Store(str(store.store_path(psid)))

        ask_query = """
        PREFIX ps: <https://petscan.wmcloud.org/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          ?item ps:gil_link <https://en.wikipedia.org/wiki/Federalist_No._42> .
          <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_wikidata_id "Q5440615" .
          <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_wikidata_entity <http://www.wikidata.org/entity/Q5440615> .
          <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_page_len "12345"^^xsd:integer .
          <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_rev_timestamp "2026-03-15T10:00:00Z"^^xsd:dateTime .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

    @patch("petscan.service_store_builder.rdf.summarize_structure")
    @patch("petscan.service_store_builder.links.build_gil_link_enrichment_map", return_value={})
    def test_build_store_uses_one_pass_structure_accumulator(self, _gil_map_mock, summarize_structure_mock):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = STORE_GIL_TEST_PSID + 1
        self._cleanup_store(psid)

        meta = store_builder.build_store(
            psid,
            [{"id": 1, "title": "Example"}],
            "https://example.invalid",
        )

        summarize_structure_mock.assert_not_called()
        self.assertEqual(meta["structure"]["row_count"], 1)
        self.assertEqual(meta["records"], 1)

    def test_build_store_resolves_gil_links_once_per_row(self):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = STORE_GIL_TEST_PSID + 2
        self._cleanup_store(psid)
        records = [
            {"id": 1, "title": "One", "gil": "enwiki:0:Albert_Einstein"},
            {"id": 2, "title": "Two", "gil": "dewiki:0:Berlin"},
        ]

        with patch(
            "petscan.service_store_builder.links.resolve_gil_links",
            wraps=store_builder.links.resolve_gil_links,
        ) as resolve_gil_links_mock:
            store_builder.build_store(psid, records, "https://example.invalid")

        self.assertEqual(resolve_gil_links_mock.call_count, len(records))
