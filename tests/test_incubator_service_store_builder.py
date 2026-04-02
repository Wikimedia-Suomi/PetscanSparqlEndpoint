from incubator import service_store_builder as incubator_store_builder
from petscan import service_store as store
from tests.service_test_support import ServiceTestCase

INCUBATOR_TEST_STORE_ID = 3999981


class IncubatorServiceStoreBuilderTests(ServiceTestCase):
    def test_store_writes_wikidata_style_sitelink_triples(self) -> None:
        if incubator_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        store_id = INCUBATOR_TEST_STORE_ID
        self._cleanup_store(store_id)

        meta = incubator_store_builder.build_store(
            store_id=store_id,
            records=[
                {
                    "page_title": "Wp/sms/Katja_Gauriloff",
                    "wiki_project": "Wp",
                    "project_name": "Wikipedia",
                    "wiki_group": "wikipedia",
                    "lang_code": "sms",
                    "page_name": "Katja_Gauriloff",
                    "page_label": "Katja Gauriloff",
                    "incubator_url": "https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff",
                    "site_url": "https://incubator.wikimedia.org/wiki/Wp/sms/",
                    "wikidata_id": "Q138849357",
                    "wikidata_entity": "http://www.wikidata.org/entity/Q138849357",
                }
            ],
            source_url="https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
        )
        store_instance = incubator_store_builder.Store(str(store.store_path(store_id)))

        ask_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <http://schema.org/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        ASK {
          <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff> schema:about <http://www.wikidata.org/entity/Q138849357> .
          <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff> schema:inLanguage "sms" .
          <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff> rdf:type schema:Article .
          <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff> schema:name "Katja Gauriloff"@sms .
          <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff> schema:isPartOf <https://incubator.wikimedia.org/wiki/Wp/sms/> .
          <https://incubator.wikimedia.org/wiki/Wp/sms/> wikibase:wikiGroup "wikipedia" .
        }
        """
        self.assertTrue(store_instance.query(ask_query))
        self.assertFalse(
            store_instance.query(
                """
                ASK {
                  <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff>
                    <https://incubator.wikimedia.org/ontology/incubator_url> ?incubatorUrl .
                }
                """
            )
        )
        self.assertFalse(
            store_instance.query(
                """
                ASK {
                  <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff>
                    <https://incubator.wikimedia.org/ontology/position> ?position .
                }
                """
            )
        )
        self.assertFalse(
            store_instance.query(
                """
                ASK {
                  <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff>
                    <https://incubator.wikimedia.org/ontology/loadedAt> ?loadedAt .
                }
                """
            )
        )
        self.assertFalse(
            store_instance.query(
                """
                ASK {
                  <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff>
                    <https://incubator.wikimedia.org/ontology/page_name> ?pageName .
                }
                """
            )
        )
        self.assertFalse(
            store_instance.query(
                """
                ASK {
                  <https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff>
                    <https://incubator.wikimedia.org/ontology/project_name> ?projectName .
                }
                """
            )
        )

        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["site_url"]["primary_type"], "iri")
        self.assertEqual(field_map["wikidata_entity"]["primary_type"], "iri")
        self.assertEqual(field_map["wikidata_entity"]["predicate"], "http://schema.org/about")
        self.assertEqual(field_map["lang_code"]["predicate"], "http://schema.org/inLanguage")
        self.assertEqual(field_map["page_label"]["predicate"], "http://schema.org/name")
        self.assertEqual(field_map["site_url"]["predicate"], "http://schema.org/isPartOf")
        self.assertNotIn("incubator_url", field_map)
        self.assertNotIn("namespace", field_map)
        self.assertNotIn("page_name", field_map)
        self.assertNotIn("project_name", field_map)
        self.assertEqual(
            field_map["wiki_group"]["predicate"],
            "http://wikiba.se/ontology#wikiGroup",
        )
