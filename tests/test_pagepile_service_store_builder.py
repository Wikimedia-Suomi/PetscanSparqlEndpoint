from pagepile import service_store_builder as pagepile_store_builder
from petscan import service_store as store
from tests.service_test_support import ServiceTestCase

PAGEPILE_TEST_STORE_ID = 5999981


class PagepileServiceStoreBuilderTests(ServiceTestCase):
    def test_store_writes_sitelink_style_triples_for_pagepile_rows(self) -> None:
        if pagepile_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = PAGEPILE_TEST_STORE_ID
        self._cleanup_store(store_id)

        meta = pagepile_store_builder.build_store(
            store_id=store_id,
            records=[
                {
                    "page_url": "https://en.wikipedia.org/wiki/Example",
                    "page_id": 123,
                    "page_title": "Example",
                    "page_label": "Example",
                    "namespace": 0,
                    "site_url": "https://en.wikipedia.org/",
                    "wiki_domain": "en.wikipedia.org",
                    "wiki_dbname": "enwiki",
                    "wiki_group": "wikipedia",
                    "lang_code": "en",
                    "wikidata_id": "Q1757",
                    "wikidata_entity": "http://www.wikidata.org/entity/Q1757",
                }
            ],
            source_url="https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
        )
        store_instance = pagepile_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <http://schema.org/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX pagepile: <https://pagepile.toolforge.org/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          <https://en.wikipedia.org/wiki/Example> schema:about <http://www.wikidata.org/entity/Q1757> .
          <https://en.wikipedia.org/wiki/Example> schema:inLanguage "en" .
          <https://en.wikipedia.org/wiki/Example> rdf:type schema:Article .
          <https://en.wikipedia.org/wiki/Example> schema:name "Example"@en .
          <https://en.wikipedia.org/wiki/Example> schema:isPartOf <https://en.wikipedia.org/> .
          <https://en.wikipedia.org/wiki/Example> pagepile:page_id "123"^^xsd:integer .
          <https://en.wikipedia.org/wiki/Example> pagepile:namespace "0"^^xsd:integer .
          <https://en.wikipedia.org/> wikibase:wikiGroup "wikipedia" .
        }
        """
        self.assertTrue(store_instance.query(ask_query))
        self.assertFalse(
            store_instance.query(
                """
                ASK {
                  <https://en.wikipedia.org/wiki/Example>
                    <https://pagepile.toolforge.org/ontology/page_url> ?pageUrl .
                }
                """
            )
        )

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["page_id"]["primary_type"], "xsd:integer")
        self.assertEqual(field_map["namespace"]["primary_type"], "xsd:integer")
        self.assertEqual(field_map["wikidata_entity"]["predicate"], "http://schema.org/about")
        self.assertEqual(field_map["page_label"]["predicate"], "http://schema.org/name")
        self.assertEqual(field_map["site_url"]["predicate"], "http://schema.org/isPartOf")
        self.assertNotIn("page_url", field_map)

    def test_store_keeps_rows_without_wikidata_id(self) -> None:
        if pagepile_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = PAGEPILE_TEST_STORE_ID + 1
        self._cleanup_store(store_id)

        meta = pagepile_store_builder.build_store(
            store_id=store_id,
            records=[
                {
                    "page_url": "https://en.wikipedia.org/wiki/No_qid_page",
                    "page_id": 124,
                    "page_title": "No_qid_page",
                    "page_label": "No qid page",
                    "namespace": 0,
                    "site_url": "https://en.wikipedia.org/",
                    "wiki_domain": "en.wikipedia.org",
                    "wiki_dbname": "enwiki",
                    "wiki_group": "wikipedia",
                    "lang_code": "en",
                }
            ],
            source_url="https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
        )
        store_instance = pagepile_store_builder.Store(str(store.store_path(store_id)))

        self.assertTrue(
            store_instance.query(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <http://schema.org/>
                PREFIX pagepile: <https://pagepile.toolforge.org/ontology/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                ASK {
                  <https://en.wikipedia.org/wiki/No_qid_page> rdf:type schema:Article .
                  <https://en.wikipedia.org/wiki/No_qid_page> schema:name "No qid page"@en .
                  <https://en.wikipedia.org/wiki/No_qid_page> schema:isPartOf <https://en.wikipedia.org/> .
                  <https://en.wikipedia.org/wiki/No_qid_page> pagepile:page_id "124"^^xsd:integer .
                }
                """
            )
        )
        self.assertFalse(
            store_instance.query(
                """
                PREFIX schema: <http://schema.org/>
                ASK {
                  <https://en.wikipedia.org/wiki/No_qid_page> schema:about ?wikidataEntity .
                }
                """
            )
        )

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertNotIn("wikidata_id", field_map)
        self.assertNotIn("wikidata_entity", field_map)

    def test_store_writes_commons_mediaitem_entity_without_wikidata_id(self) -> None:
        if pagepile_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = PAGEPILE_TEST_STORE_ID + 2
        self._cleanup_store(store_id)

        meta = pagepile_store_builder.build_store(
            store_id=store_id,
            records=[
                {
                    "page_url": "https://commons.wikimedia.org/wiki/File:Example.jpg",
                    "page_id": 574781,
                    "page_title": "File:Example.jpg",
                    "page_label": "File:Example.jpg",
                    "namespace": 6,
                    "site_url": "https://commons.wikimedia.org/",
                    "wiki_domain": "commons.wikimedia.org",
                    "wiki_dbname": "commonswiki",
                    "wiki_group": "commons",
                    "lang_code": "en",
                    "wikidata_entity": "https://commons.wikimedia.org/entity/M574781",
                }
            ],
            source_url="https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
        )
        store_instance = pagepile_store_builder.Store(str(store.store_path(store_id)))

        self.assertTrue(
            store_instance.query(
                """
                PREFIX schema: <http://schema.org/>
                PREFIX pagepile: <https://pagepile.toolforge.org/ontology/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                ASK {
                  <https://commons.wikimedia.org/wiki/File:Example.jpg>
                    schema:about <https://commons.wikimedia.org/entity/M574781> ;
                    pagepile:page_id "574781"^^xsd:integer ;
                    pagepile:namespace "6"^^xsd:integer .
                }
                """
            )
        )

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertNotIn("wikidata_id", field_map)
        self.assertEqual(field_map["wikidata_entity"]["predicate"], "http://schema.org/about")
