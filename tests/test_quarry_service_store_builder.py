from typing import Any
from unittest.mock import patch

from petscan import service_store as store
from quarry import service_store_builder as quarry_store_builder
from tests.service_test_support import ServiceTestCase

QUARRY_TEST_STORE_ID = 2999981
QUARRY_TEST_QUERY_ID = 103479


class QuarryServiceStoreBuilderTests(ServiceTestCase):
    def test_store_uses_hash_row_uri_subjects(self) -> None:
        if quarry_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = QUARRY_TEST_STORE_ID + 20
        self._cleanup_store(store_id)

        quarry_store_builder.build_store(
            store_id=store_id,
            quarry_id=QUARRY_TEST_QUERY_ID,
            records=[{"rc_title": "Turku"}],
            source_url="https://example.invalid/quarry.json",
        )
        store_instance = quarry_store_builder.Store(str(store.store_path(store_id)))

        ask_hash_query = """
        PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>
        ASK {
          <https://quarry.wmcloud.org/query/103479#1> a quarrycol:Page .
        }
        """
        ask_legacy_query = """
        PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>
        ASK {
          <https://quarry.wmcloud.org/query/103479/row/1> a quarrycol:Page .
        }
        """

        self.assertTrue(store_instance.query(ask_hash_query))
        self.assertFalse(store_instance.query(ask_legacy_query))

    def test_store_writes_compact_timestamps_and_entity_ids_with_typed_terms(self) -> None:
        if quarry_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = QUARRY_TEST_STORE_ID
        self._cleanup_store(store_id)

        records = [
            {
                "timestamp_col": "20260219191017",
                "wikidata_item": "Q1258081",
                "commons_media": "M215253",
                "https_link": "https://example.org/resource",
                "legacy_timestamp": "19991231235959",
                "external_link": "gopher://gopher.example/1/world",
            }
        ]

        meta = quarry_store_builder.build_store(
            store_id=store_id,
            quarry_id=QUARRY_TEST_QUERY_ID,
            records=records,
            source_url="https://example.invalid/quarry.json",
        )
        store_instance = quarry_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          ?row quarrycol:timestamp_col "2026-02-19T19:10:17Z"^^xsd:dateTime .
          ?row quarrycol:wikidata_item <http://www.wikidata.org/entity/Q1258081> .
          ?row quarrycol:commons_media <https://commons.wikimedia.org/entity/M215253> .
          ?row quarrycol:https_link <https://example.org/resource> .
          ?row quarrycol:external_link "gopher://gopher.example/1/world" .
          ?row quarrycol:legacy_timestamp "19991231235959" .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["timestamp_col"]["primary_type"], "xsd:dateTime")
        self.assertEqual(field_map["wikidata_item"]["primary_type"], "iri")
        self.assertEqual(field_map["commons_media"]["primary_type"], "iri")
        self.assertEqual(field_map["https_link"]["primary_type"], "iri")
        self.assertEqual(field_map["external_link"]["primary_type"], "xsd:string")
        self.assertEqual(field_map["legacy_timestamp"]["primary_type"], "xsd:string")

    def test_store_fast_path_normalizer_falls_back_when_later_rows_change_shape(self) -> None:
        if quarry_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = QUARRY_TEST_STORE_ID + 2
        self._cleanup_store(store_id)

        records = [
            {"mixed_value": "Q1258081"},
            {"mixed_value": "plain text"},
            {"mixed_value": "https://example.org/resource"},
        ]

        meta = quarry_store_builder.build_store(
            store_id=store_id,
            quarry_id=QUARRY_TEST_QUERY_ID,
            records=records,
            source_url="https://example.invalid/quarry.json",
        )
        store_instance = quarry_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>
        ASK {
          <https://quarry.wmcloud.org/query/103479#1> quarrycol:mixed_value <http://www.wikidata.org/entity/Q1258081> .
          <https://quarry.wmcloud.org/query/103479#2> quarrycol:mixed_value "plain text" .
          <https://quarry.wmcloud.org/query/103479#3> quarrycol:mixed_value <https://example.org/resource> .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["mixed_value"]["observed_types"], ["iri", "xsd:string"])

    def test_store_fast_path_preserves_generic_string_special_cases_in_later_rows(self) -> None:
        if quarry_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = QUARRY_TEST_STORE_ID + 3
        self._cleanup_store(store_id)

        records = [
            {"mixed_value": "plain text"},
            {"mixed_value": "M51"},
            {"mixed_value": "Q1258081"},
            {"mixed_value": "https://example.org/resource"},
        ]

        meta = quarry_store_builder.build_store(
            store_id=store_id,
            quarry_id=QUARRY_TEST_QUERY_ID,
            records=records,
            source_url="https://example.invalid/quarry.json",
        )
        store_instance = quarry_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>
        ASK {
          <https://quarry.wmcloud.org/query/103479#1> quarrycol:mixed_value "plain text" .
          <https://quarry.wmcloud.org/query/103479#2> quarrycol:mixed_value <https://commons.wikimedia.org/entity/M51> .
          <https://quarry.wmcloud.org/query/103479#3> quarrycol:mixed_value <http://www.wikidata.org/entity/Q1258081> .
          <https://quarry.wmcloud.org/query/103479#4> quarrycol:mixed_value <https://example.org/resource> .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["mixed_value"]["observed_types"], ["iri", "xsd:string"])

    @patch("quarry.service_uri_derivation._siteinfo_for_query_db")
    def test_store_derives_mediawiki_and_interwiki_uri_columns(self, siteinfo_mock: Any) -> None:
        if quarry_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        siteinfo_mock.return_value = {
            "domain": "fi.wikipedia.org",
            "article_path": "/wiki/$1",
            "namespace_names": {
                0: "",
                6: "Tiedosto",
                14: "Luokka",
            },
            "interwiki_urls": {
                "commons": "https://commons.wikimedia.org/wiki/$1",
            },
        }

        store_id = QUARRY_TEST_STORE_ID + 1
        self._cleanup_store(store_id)

        records = [
            {
                "page_namespace": 0,
                "page_title": "Turku",
                "rc_namespace": 14,
                "rc_title": "Esimerkkiluokka",
                "img_name": "Example file.jpg",
                "cl_to": "Esimerkkiluokka",
                "iwl_prefix": "commons",
                "iwl_from": 123,
                "iwl_title": "Main Page",
            }
        ]

        meta = quarry_store_builder.build_store(
            store_id=store_id,
            quarry_id=QUARRY_TEST_QUERY_ID,
            records=records,
            source_url="https://example.invalid/quarry.json",
            query_db="fiwiki_p",
        )
        store_instance = quarry_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>
        ASK {
          ?row quarrycol:page_uri <https://fi.wikipedia.org/wiki/Turku> .
          ?row quarrycol:rc_uri <https://fi.wikipedia.org/wiki/Luokka:Esimerkkiluokka> .
          ?row quarrycol:img_uri <https://fi.wikipedia.org/wiki/Tiedosto:Example_file.jpg> .
          ?row quarrycol:cl_uri <https://fi.wikipedia.org/wiki/Luokka:Esimerkkiluokka> .
          ?row quarrycol:iwl_uri <https://commons.wikimedia.org/wiki/Main_Page> .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["page_uri"]["primary_type"], "iri")
        self.assertEqual(field_map["rc_uri"]["primary_type"], "iri")
        self.assertEqual(field_map["img_uri"]["primary_type"], "iri")
        self.assertEqual(field_map["cl_uri"]["primary_type"], "iri")
        self.assertEqual(field_map["iwl_uri"]["primary_type"], "iri")
