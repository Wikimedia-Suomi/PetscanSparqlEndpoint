import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from petscan import service
from petscan import service_store as store
from tests.service_test_support import (
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
    @patch("petscan.service.store.prune_expired_stores")
    @patch("petscan.service._ensure_oxigraph")
    def test_ensure_loaded_rebuilds_when_meta_json_is_corrupt(
        self,
        _ensure_oxigraph_mock,
        prune_expired_stores_mock,
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
        prune_expired_stores_mock.assert_called_once_with(exclude_psids=[psid])
        fetch_petscan_json_mock.assert_called_once_with(psid, petscan_params={})
        extract_records_mock.assert_called_once()
        build_store_mock.assert_called_once()

    @patch("petscan.service.store_builder.build_store")
    @patch("petscan.service.source.extract_records")
    @patch("petscan.service.source.fetch_petscan_json")
    @patch("petscan.service._ensure_oxigraph")
    def test_ensure_loaded_uses_existing_store_when_meta_is_fresh(
        self,
        _ensure_oxigraph_mock,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_store_mock,
    ):
        psid = STORE_REBUILD_TEST_PSID + 101
        self._cleanup_store(psid)
        store_path = store.store_path(psid)
        meta_path = store.meta_path(psid)

        fresh_meta = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        store_path.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(fresh_meta), encoding="utf-8")

        result = service.ensure_loaded(psid, refresh=False)

        self.assertEqual(result, fresh_meta)
        fetch_petscan_json_mock.assert_not_called()
        extract_records_mock.assert_not_called()
        build_store_mock.assert_not_called()

    @patch("petscan.service.store_builder.build_store")
    @patch("petscan.service.source.extract_records")
    @patch("petscan.service.source.fetch_petscan_json")
    @patch("petscan.service._ensure_oxigraph")
    def test_ensure_loaded_rebuilds_when_meta_is_older_than_30_minutes(
        self,
        _ensure_oxigraph_mock,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_store_mock,
    ):
        psid = STORE_REBUILD_TEST_PSID + 102
        self._cleanup_store(psid)
        store_path = store.store_path(psid)
        meta_path = store.meta_path(psid)

        stale_meta = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": (
                datetime.now(timezone.utc) - timedelta(minutes=31)
            ).replace(microsecond=0).isoformat(),
        }
        store_path.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(stale_meta), encoding="utf-8")

        fetch_petscan_json_mock.return_value = (
            {"*": [{"id": 1, "title": "Example"}]},
            "https://example.invalid",
        )
        extract_records_mock.return_value = [{"id": 1, "title": "Example"}]
        rebuilt_meta = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": "2026-01-01T00:00:00+00:00",
        }
        build_store_mock.return_value = rebuilt_meta

        result = service.ensure_loaded(psid, refresh=False)

        self.assertEqual(result, rebuilt_meta)
        fetch_petscan_json_mock.assert_called_once_with(psid, petscan_params={})
        extract_records_mock.assert_called_once()
        build_store_mock.assert_called_once()

    @patch("petscan.service.ensure_loaded")
    @patch("petscan.service.Store")
    def test_execute_query_handles_prefix_name_that_matches_query_keyword(
        self,
        store_class_mock,
        ensure_loaded_mock,
    ):
        query = """
        PREFIX select: <http://example.org/ns#>
        ASK { ?s ?p ?o }
        """
        psid = 123
        ensure_loaded_mock.return_value = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": "2026-01-01T00:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }
        store_class_mock.read_only.return_value.query.return_value = True

        execution = service.execute_query(psid, query, refresh=False)
        self.assertEqual(execution["query_type"], "ASK")
        self.assertEqual(execution["result_format"], "sparql-json")
        self.assertEqual(execution["sparql_json"]["boolean"], True)
        ensure_loaded_mock.assert_called_once_with(psid, refresh=False, petscan_params=None)
        store_class_mock.read_only.assert_called_once_with(str(store.store_path(psid)))

    @patch("petscan.service.ensure_loaded")
    def test_execute_query_rejects_forbidden_service_clause_before_loading_store(
        self,
        ensure_loaded_mock,
    ):
        psid = 123
        query = """
        PREFIX : <https://example.org/sparql>
        SELECT * WHERE {
          SERVICE : {
            ?s ?p ?o .
          }
        }
        """

        with self.assertRaisesMessage(ValueError, "SERVICE clauses are not allowed in this endpoint."):
            service.execute_query(psid, query, refresh=False)

        ensure_loaded_mock.assert_not_called()

    @patch("petscan.service.ensure_loaded")
    @patch("petscan.service._open_query_store")
    def test_execute_query_returns_client_error_for_missing_prefix_in_oxigraph(
        self,
        open_query_store_mock,
        ensure_loaded_mock,
    ):
        psid = 123
        query = "SELECT ?s WHERE { ?s a petscan:Page . }"
        ensure_loaded_mock.return_value = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": "2026-01-01T00:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }
        open_query_store_mock.return_value.query.side_effect = SyntaxError(
            "error at 1:36: expected one of Prefix not found"
        )

        with self.assertRaisesMessage(
            ValueError,
            "SPARQL query is invalid: missing PREFIX declaration",
        ):
            service.execute_query(psid, query, refresh=False)

    @patch("petscan.service.ensure_loaded")
    @patch("petscan.service._open_query_store")
    def test_execute_query_keeps_unexpected_store_query_errors_as_server_errors(
        self,
        open_query_store_mock,
        ensure_loaded_mock,
    ):
        psid = 123
        query = "ASK { ?s ?p ?o }"
        ensure_loaded_mock.return_value = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": "2026-01-01T00:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }
        open_query_store_mock.return_value.query.side_effect = RuntimeError("temporary backend failure")

        with self.assertRaisesMessage(service.PetscanServiceError, "SPARQL query failed:"):
            service.execute_query(psid, query, refresh=False)

    @patch("petscan.service.ensure_loaded")
    @patch("petscan.service.Store")
    def test_execute_query_wraps_store_open_errors(
        self,
        store_class_mock,
        ensure_loaded_mock,
    ):
        psid = 123
        query = "ASK { ?s ?p ?o }"
        ensure_loaded_mock.return_value = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": "2026-01-01T00:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }
        store_class_mock.read_only.side_effect = OSError("LOCK: No locks available")

        with self.assertRaisesMessage(service.PetscanServiceError, "Failed to open Oxigraph store:"):
            service.execute_query(psid, query, refresh=False)
