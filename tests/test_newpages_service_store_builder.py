from newpages import service_store_builder as newpages_store_builder
from petscan import service_store as store
from tests.service_test_support import ServiceTestCase

NEWPAGES_TEST_STORE_ID = 4999981


class NewpagesServiceStoreBuilderTests(ServiceTestCase):
    def test_store_writes_sitelink_style_triples_for_new_pages(self) -> None:
        if newpages_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = NEWPAGES_TEST_STORE_ID
        self._cleanup_store(store_id)

        meta = newpages_store_builder.build_store(
            store_id=store_id,
            records=[
                {
                    "page_url": "https://fi.wikipedia.org/wiki/Turku",
                    "page_id": 123,
                    "page_title": "Turku",
                    "page_label": "Turku",
                    "namespace": 0,
                    "created_timestamp": "2026-04-03T01:02:03Z",
                    "site_url": "https://fi.wikipedia.org/",
                    "wiki_domain": "fi.wikipedia.org",
                    "wiki_dbname": "fiwiki",
                    "wiki_group": "wikipedia",
                    "lang_code": "fi",
                    "wikidata_id": "Q1757",
                    "wikidata_entity": "http://www.wikidata.org/entity/Q1757",
                }
            ],
            source_url="https://fi.wikipedia.org/wiki/Special:RecentChanges",
        )
        store_instance = newpages_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <http://schema.org/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX newpages: <https://sparqlbridge.toolforge.org/newpages/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          <https://fi.wikipedia.org/wiki/Turku> schema:about <http://www.wikidata.org/entity/Q1757> .
          <https://fi.wikipedia.org/wiki/Turku> schema:inLanguage "fi" .
          <https://fi.wikipedia.org/wiki/Turku> rdf:type schema:Article .
          <https://fi.wikipedia.org/wiki/Turku> schema:name "Turku"@fi .
          <https://fi.wikipedia.org/wiki/Turku> schema:isPartOf <https://fi.wikipedia.org/> .
          <https://fi.wikipedia.org/> wikibase:wikiGroup "wikipedia" .
          <https://fi.wikipedia.org/wiki/Turku> newpages:page_id "123"^^xsd:integer .
          <https://fi.wikipedia.org/wiki/Turku> newpages:created_timestamp "2026-04-03T01:02:03Z"^^xsd:dateTime .
        }
        """
        self.assertTrue(store_instance.query(ask_query))
        self.assertFalse(
            store_instance.query(
                """
                ASK {
                  <https://fi.wikipedia.org/wiki/Turku>
                    <https://sparqlbridge.toolforge.org/newpages/ontology/page_url> ?pageUrl .
                }
                """
            )
        )

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["created_timestamp"]["primary_type"], "xsd:dateTime")
        self.assertEqual(field_map["page_id"]["primary_type"], "xsd:integer")
        self.assertEqual(field_map["wikidata_entity"]["predicate"], "http://schema.org/about")
        self.assertEqual(field_map["page_label"]["predicate"], "http://schema.org/name")
        self.assertEqual(field_map["site_url"]["predicate"], "http://schema.org/isPartOf")
        self.assertNotIn("page_url", field_map)

    def test_store_uses_root_site_url_for_incubator_records(self) -> None:
        if newpages_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = NEWPAGES_TEST_STORE_ID + 2
        self._cleanup_store(store_id)

        newpages_store_builder.build_store(
            store_id=store_id,
            records=[
                {
                    "page_url": "https://incubator.wikimedia.org/wiki/Wp/sms/Uusi_sivu",
                    "page_id": 702,
                    "page_title": "Wp/sms/Uusi_sivu",
                    "page_label": "Wp/sms/Uusi sivu",
                    "namespace": 0,
                    "created_timestamp": "2026-04-05T07:08:09Z",
                    "site_url": "https://incubator.wikimedia.org/",
                    "wiki_domain": "incubator.wikimedia.org",
                    "wiki_dbname": "incubatorwiki",
                    "wiki_group": "wikipedia",
                    "lang_code": "sms",
                    "wikidata_id": "Q123",
                    "wikidata_entity": "http://www.wikidata.org/entity/Q123",
                }
            ],
            source_url="https://incubator.wikimedia.org/wiki/Special:Log/create",
        )
        store_instance = newpages_store_builder.Store(str(store.store_path(store_id)))

        self.assertTrue(
            store_instance.query(
                """
                PREFIX schema: <http://schema.org/>
                PREFIX wikibase: <http://wikiba.se/ontology#>
                ASK {
                  <https://incubator.wikimedia.org/wiki/Wp/sms/Uusi_sivu>
                    schema:isPartOf <https://incubator.wikimedia.org/> .
                  <https://incubator.wikimedia.org/> wikibase:wikiGroup "wikipedia" .
                }
                """
            )
        )
        self.assertFalse(
            store_instance.query(
                """
                PREFIX schema: <http://schema.org/>
                ASK {
                  <https://incubator.wikimedia.org/wiki/Wp/sms/Uusi_sivu>
                    schema:isPartOf <https://incubator.wikimedia.org/wiki/Wp/sms/> .
                }
                """
            )
        )

    def test_store_writes_current_timestamp_for_edited_page_rows(self) -> None:
        if newpages_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = NEWPAGES_TEST_STORE_ID + 1
        self._cleanup_store(store_id)

        meta = newpages_store_builder.build_store(
            store_id=store_id,
            records=[
                {
                    "page_url": "https://fi.wikipedia.org/wiki/Turku",
                    "page_id": 123,
                    "page_title": "Turku",
                    "page_label": "Turku",
                    "namespace": 0,
                    "current_timestamp": "2026-04-05T03:02:01Z",
                    "site_url": "https://fi.wikipedia.org/",
                    "wiki_domain": "fi.wikipedia.org",
                    "wiki_dbname": "fiwiki",
                    "wiki_group": "wikipedia",
                    "lang_code": "fi",
                    "wikidata_id": "Q1757",
                    "wikidata_entity": "http://www.wikidata.org/entity/Q1757",
                }
            ],
            source_url="https://fi.wikipedia.org/wiki/Special:Contributions",
        )
        store_instance = newpages_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX newpages: <https://sparqlbridge.toolforge.org/newpages/ontology/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        ASK {
          <https://fi.wikipedia.org/wiki/Turku> newpages:current_timestamp "2026-04-05T03:02:01Z"^^xsd:dateTime .
        }
        """
        self.assertTrue(store_instance.query(ask_query))

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["current_timestamp"]["primary_type"], "xsd:dateTime")
        self.assertNotIn("created_timestamp", field_map)
