from unittest.mock import patch

from petscan import service_store as store
from petscan import service_store_builder as store_builder
from tests.service_test_support import STORE_GIL_TEST_PSID, ServiceTestCase


class ServiceStoreBuilderTests(ServiceTestCase):
    @patch("petscan.service_store_builder.links.build_gil_link_wikidata_map")
    def test_store_contains_gil_link_relation_triples(self, gil_map_mock):
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        link_uri = "https://en.wikipedia.org/wiki/Federalist_No._42"
        gil_map_mock.return_value = {link_uri: "Q5440615"}
        psid = STORE_GIL_TEST_PSID
        self._cleanup_store(psid)

        records = [{"id": 1, "title": "Example", "gil": "enwiki:0:Federalist_No._42"}]
        store_builder.build_store(psid, records, "https://example.invalid")
        store_instance = store_builder.Store(str(store.store_path(psid)))

        ask_query = """
        PREFIX ps: <https://petscan.wmcloud.org/ontology/>
        ASK {
          ?item ps:gil_link <https://en.wikipedia.org/wiki/Federalist_No._42> .
          <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_wikidata_id "Q5440615" .
          <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_wikidata_entity <http://www.wikidata.org/entity/Q5440615> .
        }
        """
        self.assertTrue(store_instance.query(ask_query))
