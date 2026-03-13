from unittest.mock import patch

from petscan import service
from petscan import service_store as store
from petscan import service_store_builder as store_builder
from tests.service_test_support import (
    EXECUTE_QUERY_TEST_PSID,
    STORE_REBUILD_TEST_PSID,
    ServiceTestCase,
)


class ServiceModuleTests(ServiceTestCase):
    def test_meta_source_params_must_match_requested_params(self):
        meta = {"source_params": {"category": ["Turku"], "language": ["fi"]}}

        self.assertTrue(
            service.meta_has_matching_source_params(meta, {"category": ["Turku"], "language": "fi"})
        )
        self.assertFalse(service.meta_has_matching_source_params(meta, {"category": ["Helsinki"]}))

    @patch("petscan.service.store_builder.build_store")
    @patch("petscan.service.source.extract_records")
    @patch("petscan.service.source.fetch_petscan_json")
    @patch("petscan.service._ensure_oxigraph")
    def test_ensure_loaded_rebuilds_when_meta_json_is_corrupt(
        self,
        _ensure_oxigraph_mock,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_store_mock,
    ):
        psid = STORE_REBUILD_TEST_PSID
        self._cleanup_store(psid)

        store_path = store.store_path(psid)
        meta_path = store.meta_path(psid)

        fetch_petscan_json_mock.return_value = (
            {"*": [{"id": 1, "title": "Example"}]},
            "https://example.invalid",
        )
        extract_records_mock.return_value = [{"id": 1, "title": "Example"}]
        build_store_mock.return_value = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": "2026-01-01T00:00:00+00:00",
        }

        store_path.mkdir(parents=True, exist_ok=True)
        meta_path.write_text("{invalid-json", encoding="utf-8")

        result = service.ensure_loaded(psid, refresh=False)

        self.assertEqual(result["psid"], psid)
        fetch_petscan_json_mock.assert_called_once_with(psid, petscan_params={})
        extract_records_mock.assert_called_once()
        build_store_mock.assert_called_once()

    def test_execute_query_handles_prefix_name_that_matches_query_keyword(self):
        if service.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = EXECUTE_QUERY_TEST_PSID
        self._cleanup_store(psid)

        records = [{"id": 1, "title": "Example"}]
        query = """
        PREFIX select: <http://example.org/ns#>
        ASK { ?s ?p ?o }
        """

        store_builder.build_store(psid, records, "https://example.invalid")
        execution = service.execute_query(psid, query, refresh=False)
        self.assertEqual(execution["query_type"], "ASK")
        self.assertEqual(execution["result_format"], "sparql-json")
        self.assertIn("boolean", execution["sparql_json"])
