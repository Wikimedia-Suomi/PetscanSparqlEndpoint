from datetime import datetime, timezone
from threading import Lock
from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase

from newpages import service as newpages_service
from petscan.service_errors import PetscanServiceError


class NewpagesServiceModuleTests(SimpleTestCase):
    @staticmethod
    def _meta() -> dict[str, object]:
        return {
            "psid": 4000000000000,
            "records": 1,
            "source_url": "https://fi.wikipedia.org/wiki/Special:Log/create",
            "source_params": {
                "wiki": ["fi"],
            },
            "loaded_at": "2026-04-04T09:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }

    def test_internal_store_id_changes_with_requested_filters(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            base_store_id = newpages_service.internal_store_id(limit=None, wiki_domains=["fi.wikipedia.org"])

            self.assertEqual(
                base_store_id,
                newpages_service.internal_store_id(limit=None, wiki_domains=["fi.wikipedia.org"]),
            )
            self.assertNotEqual(
                base_store_id,
                newpages_service.internal_store_id(limit=25, wiki_domains=["fi.wikipedia.org"]),
            )
            self.assertNotEqual(
                base_store_id,
                newpages_service.internal_store_id(limit=None, wiki_domains=["sv.wikipedia.org"]),
            )
            self.assertNotEqual(
                base_store_id,
                newpages_service.internal_store_id(
                    limit=None,
                    wiki_domains=["fi.wikipedia.org"],
                    timestamp="202604",
                ),
            )
            self.assertNotEqual(
                base_store_id,
                newpages_service.internal_store_id(
                    limit=None,
                    wiki_domains=["fi.wikipedia.org"],
                    user_list_page=":w:fi:Wikipedia:Users",
                ),
            )
            self.assertNotEqual(
                base_store_id,
                newpages_service.internal_store_id(
                    limit=None,
                    wiki_domains=["fi.wikipedia.org"],
                    user_list_page=":w:fi:Wikipedia:Users",
                    include_edited_pages=True,
                ),
            )

    def test_internal_store_id_changes_with_backend(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            api_store_id = newpages_service.internal_store_id(
                limit=25,
                wiki_domains=["fi.wikipedia.org"],
                timestamp="202604",
            )

        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            sql_store_id = newpages_service.internal_store_id(
                limit=25,
                wiki_domains=["fi.wikipedia.org"],
                timestamp="202604",
            )

        self.assertNotEqual(api_store_id, sql_store_id)

    def test_ensure_loaded_reuses_fresh_meta_when_source_params_match(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            store_id = newpages_service.internal_store_id(
                limit=25,
                wiki_domains=["fi.wikipedia.org", "sv.wikipedia.org"],
                timestamp="202604",
                user_list_page=":w:fi:Wikipedia:Users",
            )
            fresh_meta = {
                "psid": store_id,
                "records": 2,
                "source_url": "https://meta.wikimedia.org/wiki/Special:SiteMatrix",
                "source_params": {
                    "limit": ["25"],
                    "wiki": ["fi", "sv"],
                    "timestamp": ["20260400000000"],
                    "user_list_page": [":w:fi:Wikipedia:Users"],
                },
                "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            }

            with patch("newpages.service._ensure_oxigraph"):
                with patch("newpages.service.store.prune_expired_stores") as prune_expired_stores_mock:
                    with patch("newpages.service.store.get_psid_lock") as get_psid_lock_mock:
                        with patch("newpages.service.store.has_existing_store") as has_existing_store_mock:
                            with patch("newpages.service.store.read_meta") as read_meta_mock:
                                with patch("newpages.service.source.fetch_newpage_records") as fetch_newpage_records_mock:
                                    with patch("newpages.service.store_builder.build_store") as build_store_mock:
                                        get_psid_lock_mock.return_value = Lock()
                                        has_existing_store_mock.return_value = True
                                        read_meta_mock.return_value = fresh_meta

                                        result = newpages_service.ensure_loaded(
                                            refresh=False,
                                            limit=25,
                                            wiki_domains=["fi.wikipedia.org", "sv.wikipedia.org"],
                                            timestamp="202604",
                                            user_list_page=":w:fi:Wikipedia:Users",
                                            include_edited_pages=False,
                                        )

        self.assertEqual(result, fresh_meta)
        prune_expired_stores_mock.assert_called_once_with(exclude_psids=[store_id])
        fetch_newpage_records_mock.assert_not_called()
        build_store_mock.assert_not_called()

    def test_ensure_loaded_rebuilds_when_cached_meta_source_params_do_not_match(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            store_id = newpages_service.internal_store_id(
                limit=25,
                wiki_domains=["fi.wikipedia.org"],
                timestamp="202604",
                user_list_page=":w:fi:Wikipedia:Users",
            )
            cached_meta = {
                "psid": store_id,
                "records": 1,
                "source_url": "https://fi.wikipedia.org/wiki/Special:Log/create",
                "source_params": {"limit": ["25"]},
                "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            }
            rebuilt_meta = {
                "psid": store_id,
                "records": 1,
                "source_url": "https://fi.wikipedia.org/wiki/Special:Log/create",
                "source_params": {
                    "limit": ["25"],
                    "wiki": ["fi"],
                    "timestamp": ["20260400000000"],
                    "user_list_page": [":w:fi:Wikipedia:Users"],
                },
                "loaded_at": "2026-04-04T09:00:00+00:00",
                "structure": {"row_count": 1, "field_count": 1, "fields": []},
            }
            records = [
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
            ]

            with patch("newpages.service._ensure_oxigraph"):
                with patch("newpages.service.store.prune_expired_stores"):
                    with patch("newpages.service.store.get_psid_lock") as get_psid_lock_mock:
                        with patch("newpages.service.store.has_existing_store") as has_existing_store_mock:
                            with patch("newpages.service.store.read_meta") as read_meta_mock:
                                with patch("newpages.service.source.fetch_newpage_records") as fetch_newpage_records_mock:
                                    with patch("newpages.service.store_builder.build_store") as build_store_mock:
                                        get_psid_lock_mock.return_value = Lock()
                                        has_existing_store_mock.return_value = True
                                        read_meta_mock.return_value = cached_meta
                                        fetch_newpage_records_mock.return_value = (
                                            records,
                                            "https://fi.wikipedia.org/wiki/Special:Log/create",
                                        )
                                        build_store_mock.return_value = rebuilt_meta

                                        result = newpages_service.ensure_loaded(
                                            refresh=False,
                                            limit=25,
                                            wiki_domains=["fi.wikipedia.org"],
                                            timestamp="202604",
                                            user_list_page=":w:fi:Wikipedia:Users",
                                            include_edited_pages=False,
                                        )

        self.assertEqual(result, rebuilt_meta)
        fetch_newpage_records_mock.assert_called_once_with(
            limit=25,
            wiki_domains=["fi.wikipedia.org"],
            timestamp="202604",
            user_list_page=":w:fi:Wikipedia:Users",
            include_edited_pages=False,
        )
        build_store_mock.assert_called_once_with(
            store_id=store_id,
            records=records,
            source_url="https://fi.wikipedia.org/wiki/Special:Log/create",
            source_params={
                "limit": ["25"],
                "wiki": ["fi"],
                "timestamp": ["20260400000000"],
                "user_list_page": [":w:fi:Wikipedia:Users"],
            },
        )

    @patch("newpages.service.sparql.serialize_ask")
    @patch("newpages.service._open_query_store")
    @patch("newpages.service.ensure_loaded")
    @patch("newpages.service.sparql.validate_query")
    def test_execute_query_returns_ask_json_and_meta(
        self,
        validate_query_mock: Any,
        ensure_loaded_mock: Any,
        open_query_store_mock: Any,
        serialize_ask_mock: Any,
    ) -> None:
        raw_result = object()
        meta = self._meta()
        validate_query_mock.return_value = "ASK"
        ensure_loaded_mock.return_value = meta
        open_query_store_mock.return_value.query.return_value = raw_result
        serialize_ask_mock.return_value = {"head": {}, "boolean": True}

        execution = newpages_service.execute_query(
            "ASK { ?s ?p ?o }",
            refresh=True,
            limit=25,
            wiki_domains=["fi.wikipedia.org"],
            timestamp="202604",
            user_list_page=":w:fi:Wikipedia:Users",
            include_edited_pages=False,
        )

        self.assertEqual(execution["query_type"], "ASK")
        self.assertEqual(execution["result_format"], "sparql-json")
        self.assertEqual(execution["sparql_json"], {"head": {}, "boolean": True})
        self.assertEqual(execution["meta"], meta)
        ensure_loaded_mock.assert_called_once_with(
            refresh=True,
            limit=25,
            wiki_domains=["fi.wikipedia.org"],
            timestamp="202604",
            user_list_page=":w:fi:Wikipedia:Users",
            include_edited_pages=False,
        )
        open_query_store_mock.assert_called_once()
        serialize_ask_mock.assert_called_once_with(raw_result)

    @patch("newpages.service.sparql.serialize_select")
    @patch("newpages.service._open_query_store")
    @patch("newpages.service.ensure_loaded")
    @patch("newpages.service.sparql.validate_query")
    def test_execute_query_returns_select_json_and_meta(
        self,
        validate_query_mock: Any,
        ensure_loaded_mock: Any,
        open_query_store_mock: Any,
        serialize_select_mock: Any,
    ) -> None:
        raw_result = object()
        meta = self._meta()
        validate_query_mock.return_value = "SELECT"
        ensure_loaded_mock.return_value = meta
        open_query_store_mock.return_value.query.return_value = raw_result
        serialize_select_mock.return_value = {"head": {"vars": ["s"]}, "results": {"bindings": []}}

        execution = newpages_service.execute_query("SELECT ?s WHERE { ?s ?p ?o }")

        self.assertEqual(execution["query_type"], "SELECT")
        self.assertEqual(execution["result_format"], "sparql-json")
        self.assertEqual(
            execution["sparql_json"],
            {"head": {"vars": ["s"]}, "results": {"bindings": []}},
        )
        self.assertEqual(execution["meta"], meta)
        serialize_select_mock.assert_called_once_with(raw_result)

    @patch("newpages.service.sparql.serialize_graph")
    @patch("newpages.service._open_query_store")
    @patch("newpages.service.ensure_loaded")
    @patch("newpages.service.sparql.validate_query")
    def test_execute_query_returns_graph_serialization_and_meta(
        self,
        validate_query_mock: Any,
        ensure_loaded_mock: Any,
        open_query_store_mock: Any,
        serialize_graph_mock: Any,
    ) -> None:
        raw_result = object()
        meta = self._meta()
        validate_query_mock.return_value = "CONSTRUCT"
        ensure_loaded_mock.return_value = meta
        open_query_store_mock.return_value.query.return_value = raw_result
        serialize_graph_mock.return_value = "<https://fi.wikipedia.org/wiki/Turku> <http://schema.org/about> <http://www.wikidata.org/entity/Q1757> .\n"

        execution = newpages_service.execute_query("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")

        self.assertEqual(execution["query_type"], "CONSTRUCT")
        self.assertEqual(execution["result_format"], "n-triples")
        self.assertEqual(
            execution["ntriples"],
            "<https://fi.wikipedia.org/wiki/Turku> <http://schema.org/about> <http://www.wikidata.org/entity/Q1757> .\n",
        )
        self.assertEqual(execution["meta"], meta)
        serialize_graph_mock.assert_called_once_with(raw_result)

    @patch("newpages.service._open_query_store")
    @patch("newpages.service.ensure_loaded")
    @patch("newpages.service.sparql.validate_query")
    def test_execute_query_returns_client_error_for_missing_prefix_in_oxigraph(
        self,
        validate_query_mock: Any,
        ensure_loaded_mock: Any,
        open_query_store_mock: Any,
    ) -> None:
        validate_query_mock.return_value = "SELECT"
        ensure_loaded_mock.return_value = self._meta()
        open_query_store_mock.return_value.query.side_effect = SyntaxError(
            "error at 1:36: expected one of Prefix not found"
        )

        with self.assertRaisesMessage(
            ValueError,
            "SPARQL query is invalid: missing PREFIX declaration",
        ):
            newpages_service.execute_query("SELECT ?s WHERE { ?s a newpages:Page . }")

    @patch("newpages.service._open_query_store")
    @patch("newpages.service.ensure_loaded")
    @patch("newpages.service.sparql.validate_query")
    def test_execute_query_keeps_unexpected_store_query_errors_as_server_errors(
        self,
        validate_query_mock: Any,
        ensure_loaded_mock: Any,
        open_query_store_mock: Any,
    ) -> None:
        validate_query_mock.return_value = "ASK"
        ensure_loaded_mock.return_value = self._meta()
        open_query_store_mock.return_value.query.side_effect = RuntimeError("temporary backend failure")

        with self.assertRaisesMessage(PetscanServiceError, "SPARQL query failed:"):
            newpages_service.execute_query("ASK { ?s ?p ?o }")
