from unittest.mock import patch

from petscan import service_links as links
from petscan import service_store as store
from petscan import service_store_builder as store_builder
from petscan.service_errors import GilLinkEnrichmentError, PetscanServiceError
from tests.service_test_support import PRIMARY_EXAMPLE_FILE, STORE_GIL_TEST_PSID, ServiceTestCase


class ServiceStoreBuilderTests(ServiceTestCase):
    def test_quad_buffer_target_uses_benchmarked_memory_bound(self):
        self.assertEqual(store_builder._QUAD_BUFFER_TARGET, 100_000)
        self.assertEqual(store_builder._CATEGORY_METADATA_DEDUPE_LIMIT, 250_000)

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
        category_uri = "https://en.wikipedia.org/wiki/Category:United_States_history"
        enrichment_map = {
            link_uri: {
                "wikidata_id": "Q5440615",
                "page_len": 12345,
                "rev_timestamp": "2026-03-15T10:00:00Z",
                "categories": [
                    {
                        "title": "Category:United_States_history",
                        "link_uri": category_uri,
                        "wikidata_id": "Q8676",
                        "hiddencat": True,
                    }
                ],
            }
        }

        def _mock_build_enrichment(records, backend=None, include_categories=False):
            self.assertTrue(include_categories)
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
        meta = store_builder.build_store(
            psid,
            records,
            "https://example.invalid",
            include_gil_categories=True,
        )
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
          <https://en.wikipedia.org/wiki/Federalist_No._42>
            petscan:gil_link_category
            <https://en.wikipedia.org/wiki/Category:United_States_history> .
          <https://en.wikipedia.org/wiki/Category:United_States_history>
            petscan:gil_link_category_title "Category:United_States_history" ;
            petscan:gil_link_category_hiddencat true ;
            petscan:gil_link_category_wikidata_id "Q8676" ;
            petscan:gil_link_category_wikidata_entity <http://www.wikidata.org/entity/Q8676> .
        }
        """
        self.assertTrue(store_instance.query(ask_query))
        self.assertEqual(
            meta["enrichment_options"],
            {
                "petscan_store_schema_version": 1,
                "gil_categories": True,
                "gil_categories_schema_version": 3,
            },
        )

    @patch("petscan.service_links.fetch_category_enrichment_for_site")
    def test_store_contains_item_page_and_optional_category_triples(
        self,
        category_fetch_mock,
    ):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        category_uri = "https://fi.wikipedia.org/wiki/Category:Turku"
        category_fetch_mock.return_value = {
            "Turku": [
                {
                    "title": "Category:Turku",
                    "link_uri": category_uri,
                    "wikidata_id": "Q8357355",
                    "hiddencat": True,
                }
            ]
        }
        psid = STORE_GIL_TEST_PSID + 9
        self._cleanup_store(psid)

        meta = store_builder.build_store(
            psid,
            [{"id": 1240, "title": "Turku", "namespace": 0, "nstext": ""}],
            "https://example.invalid",
            petscan_project="wikipedia",
            petscan_language="fi",
            include_item_categories=True,
        )
        store_instance = store_builder.Store(str(store.store_path(psid)))

        ask_query = """
        PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
        ASK {
          ?item petscan:item_page <https://fi.wikipedia.org/wiki/Turku> ;
            petscan:item_category <https://fi.wikipedia.org/wiki/Category:Turku> .
          <https://fi.wikipedia.org/wiki/Category:Turku>
            petscan:item_category_title "Category:Turku" ;
            petscan:item_category_hiddencat true ;
            petscan:item_category_wikidata_id "Q8357355" ;
            petscan:item_category_wikidata_entity <http://www.wikidata.org/entity/Q8357355> .
        }
        """
        self.assertTrue(store_instance.query(ask_query))
        self.assertEqual(
            meta["enrichment_options"],
            {
                "petscan_store_schema_version": 1,
                "item_categories": True,
                "item_categories_schema_version": 2,
            },
        )
        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["item_page"]["primary_type"], "iri")
        self.assertEqual(field_map["item_category"]["row_side_cardinality"], "1")
        self.assertEqual(field_map["item_category_title"]["primary_type"], "xsd:string")

    def test_record_writer_globally_deduplicates_exact_category_metadata_quads(self):
        if store_builder.Quad is None:
            self.skipTest("pyoxigraph is not installed")

        gil_link_uri = "https://en.wikipedia.org/wiki/Example"
        gil_category_uri = "https://en.wikipedia.org/wiki/Category:Shared"
        item_category_uri = "https://fi.wikipedia.org/wiki/Category:Shared"
        context = store_builder._RecordWriteContext(
            predicates=store_builder._build_store_predicates(),
            psid=123,
            gil_link_enrichment_map={
                gil_link_uri: {
                    "categories": [
                        {
                            "link_uri": gil_category_uri,
                            "title": "Category:Shared",
                            "hiddencat": False,
                        }
                    ]
                }
            },
            img_user_registration_by_name={},
            xsd_integer_type=store_builder.NamedNode(store_builder.rdf.XSD_INTEGER_IRI),
            psid_literal=store_builder.Literal(
                "123",
                datatype=store_builder.NamedNode(store_builder.rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=store_builder.Literal(
                "2026-09-23T00:00:00Z",
                datatype=store_builder.NamedNode(store_builder.rdf.XSD_DATE_TIME_IRI),
            ),
        )
        shared_item_category = {
            "categories": [
                {
                    "link_uri": item_category_uri,
                    "title": "Category:Shared",
                    "hiddencat": False,
                }
            ]
        }
        quad_buffer = []

        for index in range(2):
            row_kinds, _row_counts = store_builder._write_record_quads(
                index=index,
                row={"id": index + 1, "title": "Example", "gil": "enwiki:0:Example"},
                context=context,
                resolved_gil_links=[(gil_link_uri, None)],
                item_page_enrichment=shared_item_category,
                quad_buffer=quad_buffer,
            )

        gil_category_node = store_builder.NamedNode(gil_category_uri)
        item_category_node = store_builder.NamedNode(item_category_uri)
        self.assertEqual(sum(quad.subject == gil_category_node for quad in quad_buffer), 2)
        self.assertEqual(sum(quad.subject == item_category_node for quad in quad_buffer), 2)
        self.assertEqual(
            sum(
                quad.predicate == store_builder.rdf.predicate_for("gil_link_category")
                for quad in quad_buffer
            ),
            2,
        )
        self.assertEqual(
            sum(
                quad.predicate == store_builder.rdf.predicate_for("item_category")
                for quad in quad_buffer
            ),
            2,
        )
        self.assertIn("gil_link_category_title", row_kinds)
        self.assertIn("item_category_title", row_kinds)

        renamed_item_category = {
            "categories": [
                {
                    "link_uri": item_category_uri,
                    "title": "Category:Renamed",
                    "hiddencat": False,
                }
            ]
        }
        store_builder._write_record_quads(
            index=2,
            row={"id": 3, "title": "Example"},
            context=context,
            resolved_gil_links=[],
            item_page_enrichment=renamed_item_category,
            quad_buffer=quad_buffer,
        )

        item_metadata_quads = [quad for quad in quad_buffer if quad.subject == item_category_node]
        self.assertEqual(len(item_metadata_quads), 3)
        self.assertEqual(
            {str(quad.object) for quad in item_metadata_quads if "_title" in str(quad.predicate)},
            {'"Category:Renamed"', '"Category:Shared"'},
        )

    @patch.object(store_builder, "_CATEGORY_METADATA_DEDUPE_LIMIT", 2)
    def test_category_metadata_dedupe_set_stops_growing_at_limit(self):
        if store_builder.Quad is None:
            self.skipTest("pyoxigraph is not installed")

        context = store_builder._RecordWriteContext(
            predicates=store_builder._build_store_predicates(),
            psid=123,
            gil_link_enrichment_map={},
            img_user_registration_by_name={},
            xsd_integer_type=store_builder.NamedNode(store_builder.rdf.XSD_INTEGER_IRI),
            psid_literal=store_builder.Literal(
                "123",
                datatype=store_builder.NamedNode(store_builder.rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=store_builder.Literal(
                "2026-09-23T00:00:00Z",
                datatype=store_builder.NamedNode(store_builder.rdf.XSD_DATE_TIME_IRI),
            ),
        )
        quad_buffer = []

        def _write_category(index, category_uri, title):
            store_builder._write_record_quads(
                index=index,
                row={"id": index + 1, "title": "Example"},
                context=context,
                resolved_gil_links=[],
                item_page_enrichment={
                    "categories": [
                        {
                            "link_uri": category_uri,
                            "title": title,
                            "hiddencat": False,
                        }
                    ]
                },
                quad_buffer=quad_buffer,
            )

        tracked_uri = "https://fi.wikipedia.org/wiki/Category:Tracked"
        overflow_uri = "https://fi.wikipedia.org/wiki/Category:Overflow"
        _write_category(0, tracked_uri, "Category:Tracked")
        _write_category(1, overflow_uri, "Category:Overflow")
        _write_category(2, overflow_uri, "Category:Overflow")
        _write_category(3, tracked_uri, "Category:Tracked")

        self.assertEqual(len(context.category_metadata_quads_seen), 2)
        tracked_node = store_builder.NamedNode(tracked_uri)
        overflow_node = store_builder.NamedNode(overflow_uri)
        self.assertEqual(sum(quad.subject == tracked_node for quad in quad_buffer), 2)
        self.assertEqual(sum(quad.subject == overflow_node for quad in quad_buffer), 4)

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

        def _mock_build_enrichment(records, backend=None, include_categories=False):
            self.assertFalse(include_categories)
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

    @patch("petscan.file_user_enrichment._fetch_registrations")
    def test_extended_file_data_adds_img_user_registration(
        self,
        fetch_registrations_mock,
    ):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        source_records = store_builder.source.extract_records(
            self._load_payload(PRIMARY_EXAMPLE_FILE)
        )
        records = [source_records[2], source_records[13]]
        self.assertEqual(
            [record["img_user_text"] for record in records],
            ["Amgine", "Dirtyliberal~commonswiki"],
        )
        fetch_registrations_mock.return_value = {
            "Amgine": "2004-12-31T23:59:59Z",
            "Dirtyliberal~commonswiki": "2005-01-02T00:00:00Z",
        }

        psid = STORE_GIL_TEST_PSID + 8
        self._cleanup_store(psid)
        meta = store_builder.build_store(
            psid,
            records,
            "https://example.invalid",
        )
        store_instance = store_builder.Store(str(store.store_path(psid)))

        ask_query = """
        PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          <https://commons.wikimedia.org/entity/M117734>
            petscan:img_user_registration "2004-12-31T23:59:59Z"^^xsd:dateTime .
          <https://commons.wikimedia.org/entity/M280450>
            petscan:img_user_registration "2005-01-02T00:00:00Z"^^xsd:dateTime .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(
            field_map["img_user_registration"]["primary_type"],
            "xsd:dateTime",
        )

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

        def _mock_build_enrichment(records, backend=None, include_categories=False):
            self.assertFalse(include_categories)
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
