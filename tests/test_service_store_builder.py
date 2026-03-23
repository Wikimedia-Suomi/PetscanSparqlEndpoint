from unittest.mock import patch

from petscan import service_links as links
from petscan import service_store as store
from petscan import service_store_builder as store_builder
from petscan.service_errors import GilLinkEnrichmentError, PetscanServiceError
from tests.service_test_support import STORE_GIL_TEST_PSID, ServiceTestCase


class ServiceStoreBuilderTests(ServiceTestCase):
    @patch("petscan.service_store_builder.Store", None)
    def test_build_store_raises_clear_error_when_pyoxigraph_missing(self):
        with self.assertRaises(PetscanServiceError) as context:
            store_builder.build_store(123, [{"id": 1, "title": "Example"}], "https://example.invalid")
        self.assertIn("pyoxigraph is not installed", str(context.exception))

    @patch("petscan.service_store_builder.links.build_gil_link_enrichment")
    def test_store_contains_gil_link_relation_triples(self, gil_map_mock):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        link_uri = "https://en.wikipedia.org/wiki/Federalist_No._42"
        enrichment_map = {
            link_uri: {
                "wikidata_id": "Q5440615",
                "page_len": 12345,
                "rev_timestamp": "2026-03-15T10:00:00Z",
            }
        }

        def _mock_build_enrichment(records, backend=None):
            resolved_links_by_row = [
                store_builder.links.resolve_gil_links(row, gil_link_enrichment_map=enrichment_map)
                for row in records
            ]
            return links.GilLinkEnrichmentBuildResult(
                enrichment_by_link=enrichment_map,
                resolved_links_by_row=resolved_links_by_row,
                lookup_stats=links.GilLinkLookupStats(),
            )

        gil_map_mock.side_effect = _mock_build_enrichment
        psid = STORE_GIL_TEST_PSID
        self._cleanup_store(psid)

        records = [{"id": 1, "title": "Example", "gil": "enwiki:0:Federalist_No._42"}]
        store_builder.build_store(psid, records, "https://example.invalid")
        store_instance = store_builder.Store(str(store.store_path(psid)))

        ask_query = """
        PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          ?item petscan:gil_link <https://en.wikipedia.org/wiki/Federalist_No._42> .
          <https://en.wikipedia.org/wiki/Federalist_No._42> petscan:gil_link_wikidata_id "Q5440615" .
          <https://en.wikipedia.org/wiki/Federalist_No._42> petscan:gil_link_wikidata_entity <http://www.wikidata.org/entity/Q5440615> .
          <https://en.wikipedia.org/wiki/Federalist_No._42> petscan:gil_link_page_len "12345"^^xsd:integer .
          <https://en.wikipedia.org/wiki/Federalist_No._42> petscan:gil_link_rev_timestamp "2026-03-15T10:00:00Z"^^xsd:dateTime .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

    @patch("petscan.service_store_builder._optimize_store")
    @patch("petscan.service_store_builder.rdf.summarize_structure")
    @patch("petscan.service_store_builder.links.build_gil_link_enrichment")
    def test_build_store_uses_one_pass_structure_accumulator(
        self,
        gil_map_mock,
        summarize_structure_mock,
        optimize_store_mock,
    ):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        def _mock_build_enrichment(records, backend=None):
            return links.GilLinkEnrichmentBuildResult(
                enrichment_by_link={},
                resolved_links_by_row=[
                    store_builder.links.resolve_gil_links(row, gil_link_enrichment_map={})
                    for row in records
                ],
                lookup_stats=links.GilLinkLookupStats(),
            )

        gil_map_mock.side_effect = _mock_build_enrichment

        psid = STORE_GIL_TEST_PSID + 1
        self._cleanup_store(psid)

        meta = store_builder.build_store(
            psid,
            [{"id": 1, "title": "Example"}],
            "https://example.invalid",
        )

        summarize_structure_mock.assert_not_called()
        optimize_store_mock.assert_called_once()
        self.assertEqual(meta["structure"]["row_count"], 1)
        self.assertEqual(meta["records"], 1)

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=store_builder.links.LOOKUP_BACKEND_API)
    @patch("petscan.service_links.fetch_wikibase_items_for_site_api", return_value={})
    def test_build_store_uses_precomputed_gil_links_in_write_loop(
        self,
        _api_fetch_mock,
        _backend_mock,
    ):
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

        self.assertEqual(resolve_gil_links_mock.call_count, 0)

    def test_store_writes_img_timestamp_and_touched_as_xsd_datetime(self):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = STORE_GIL_TEST_PSID + 3
        self._cleanup_store(psid)

        records = [
            {
                "id": 1,
                "title": "Example",
                "img_timestamp": "20260315123456",
                "touched": "2026-03-15T12:35:30Z",
            }
        ]
        meta = store_builder.build_store(psid, records, "https://example.invalid")
        store_instance = store_builder.Store(str(store.store_path(psid)))

        ask_query = """
        PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          ?item petscan:img_timestamp "2026-03-15T12:34:56Z"^^xsd:dateTime .
          ?item petscan:touched "2026-03-15T12:35:30Z"^^xsd:dateTime .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["img_timestamp"]["primary_type"], "xsd:dateTime")
        self.assertEqual(field_map["touched"]["primary_type"], "xsd:dateTime")

    @patch("petscan.service_store_builder.links.build_gil_link_enrichment")
    def test_build_store_persists_row_side_cardinality_metadata(self, gil_map_mock):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        enrichment_map = {
            "https://en.wikipedia.org/wiki/Alpha": {
                "wikidata_id": "Q1",
                "page_len": 100,
                "rev_timestamp": "2026-03-15T10:00:00Z",
            },
            "https://en.wikipedia.org/wiki/Beta": {
                "wikidata_id": "Q2",
                "page_len": 200,
                "rev_timestamp": "2026-03-15T11:00:00Z",
            },
            "https://en.wikipedia.org/wiki/Gamma": {
                "wikidata_id": "Q3",
                "page_len": 300,
                "rev_timestamp": "2026-03-15T12:00:00Z",
            },
        }

        def _mock_build_enrichment(records, backend=None):
            resolved_links_by_row = [
                store_builder.links.resolve_gil_links(row, gil_link_enrichment_map=enrichment_map)
                for row in records
            ]
            return links.GilLinkEnrichmentBuildResult(
                enrichment_by_link=enrichment_map,
                resolved_links_by_row=resolved_links_by_row,
                lookup_stats=links.GilLinkLookupStats(),
            )

        gil_map_mock.side_effect = _mock_build_enrichment

        psid = STORE_GIL_TEST_PSID + 7
        self._cleanup_store(psid)

        meta = store_builder.build_store(
            psid,
            [
                {"id": 1, "title": "Example 1", "gil": "enwiki:0:Alpha|enwiki:0:Beta"},
                {"id": 2, "title": "Example 2", "gil": "enwiki:0:Gamma"},
            ],
            "https://example.invalid",
        )

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["title"]["row_side_cardinality"], "1")
        self.assertEqual(field_map["gil_link_count"]["row_side_cardinality"], "1")
        self.assertEqual(field_map["gil_link"]["row_side_cardinality"], "M")
        self.assertEqual(field_map["gil_link_wikidata_id"]["row_side_cardinality"], "M")
        self.assertEqual(field_map["gil_link_page_len"]["row_side_cardinality"], "M")

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=store_builder.links.LOOKUP_BACKEND_API)
    @patch("petscan.service_links.fetch_wikibase_items_for_site_api")
    def test_build_store_raises_on_api_enrichment_failure_and_writes_no_meta(
        self,
        api_fetch_mock,
        _backend_mock,
    ):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = STORE_GIL_TEST_PSID + 4
        self._cleanup_store(psid)
        api_fetch_mock.side_effect = GilLinkEnrichmentError("api down")

        with self.assertRaisesMessage(GilLinkEnrichmentError, "api down"):
            store_builder.build_store(
                psid,
                [{"id": 1, "title": "Example", "gil": "enwiki:0:Albert_Einstein"}],
                "https://example.invalid",
            )

        self.assertFalse(store.meta_path(psid).exists())

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=store_builder.links.LOOKUP_BACKEND_TOOLFORGE_SQL)
    @patch("petscan.service_links.enrichment_sql.fetch_wikibase_items_for_site_sql")
    def test_build_store_raises_on_sql_enrichment_failure_and_writes_no_meta(
        self,
        sql_fetch_mock,
        _backend_mock,
    ):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = STORE_GIL_TEST_PSID + 5
        self._cleanup_store(psid)
        sql_fetch_mock.side_effect = GilLinkEnrichmentError("sql down")

        with self.assertRaisesMessage(GilLinkEnrichmentError, "sql down"):
            store_builder.build_store(
                psid,
                [{"id": 1, "title": "Example", "gil": "enwiki:0:Albert_Einstein"}],
                "https://example.invalid",
            )

        self.assertFalse(store.meta_path(psid).exists())

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=store_builder.links.LOOKUP_BACKEND_API)
    @patch("petscan.service_links.fetch_wikibase_items_for_site_api", return_value={})
    def test_build_store_allows_successful_empty_enrichment_response(
        self,
        _api_fetch_mock,
        _backend_mock,
    ):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = STORE_GIL_TEST_PSID + 6
        self._cleanup_store(psid)

        store_builder.build_store(
            psid,
            [{"id": 1, "title": "Example", "gil": "enwiki:0:Albert_Einstein"}],
            "https://example.invalid",
        )
        store_instance = store_builder.Store(str(store.store_path(psid)))

        ask_query = """
        PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
        ASK {
          ?item petscan:gil_link <https://en.wikipedia.org/wiki/Albert_Einstein> .
        }
        """
        self.assertTrue(store_instance.query(ask_query))
        self.assertTrue(store.meta_path(psid).exists())
