from datetime import datetime, timezone
from threading import Lock
from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase

from pagepile import service as pagepile_service
from petscan.service_errors import PetscanServiceError


class PagepileServiceModuleTests(SimpleTestCase):
    @staticmethod
    def _meta(limit: int | None = 25) -> dict[str, object]:
        source_params: dict[str, list[str]] = {
            "pagepile_id": ["112306"],
        }
        if limit is not None:
            source_params["limit"] = [str(limit)]

        return {
            "psid": pagepile_service.internal_store_id(112306),
            "records": 1,
            "source_url": "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
            "source_params": source_params,
            "loaded_at": "2026-04-19T08:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }

    @staticmethod
    def _records() -> list[dict[str, object]]:
        return [
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
        ]

    def test_ensure_loaded_reuses_fresh_meta_when_source_params_match(self) -> None:
        store_id = pagepile_service.internal_store_id(112306)
        fresh_meta = {
            **self._meta(limit=25),
            "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

        with patch("pagepile.service._ensure_oxigraph"):
            with patch("pagepile.service.store.prune_expired_stores") as prune_expired_stores_mock:
                with patch("pagepile.service.store.get_psid_lock") as get_psid_lock_mock:
                    with patch("pagepile.service.store.has_existing_store") as has_existing_store_mock:
                        with patch("pagepile.service.store.read_meta") as read_meta_mock:
                            with patch("pagepile.service.source.fetch_pagepile_records") as fetch_mock:
                                with patch("pagepile.service.store_builder.build_store") as build_store_mock:
                                    get_psid_lock_mock.return_value = Lock()
                                    has_existing_store_mock.return_value = True
                                    read_meta_mock.return_value = fresh_meta

                                    result = pagepile_service.ensure_loaded(
                                        112306,
                                        refresh=False,
                                        limit=25,
                                    )

        self.assertEqual(result, fresh_meta)
        prune_expired_stores_mock.assert_called_once_with(exclude_psids=[store_id])
        get_psid_lock_mock.assert_called_once_with(store_id)
        has_existing_store_mock.assert_called_once_with(store_id)
        read_meta_mock.assert_called_once_with(store_id)
        fetch_mock.assert_not_called()
        build_store_mock.assert_not_called()

    def test_ensure_loaded_rebuilds_when_cached_meta_source_params_do_not_match(self) -> None:
        store_id = pagepile_service.internal_store_id(112306)
        cached_meta = {
            "psid": store_id,
            "records": 1,
            "source_url": "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
            "source_params": {"pagepile_id": ["112306"]},
            "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        rebuilt_meta = self._meta(limit=25)
        records = self._records()

        with patch("pagepile.service._ensure_oxigraph"):
            with patch("pagepile.service.store.prune_expired_stores"):
                with patch("pagepile.service.store.get_psid_lock") as get_psid_lock_mock:
                    with patch("pagepile.service.store.has_existing_store") as has_existing_store_mock:
                        with patch("pagepile.service.store.read_meta") as read_meta_mock:
                            with patch("pagepile.service.source.fetch_pagepile_records") as fetch_mock:
                                with patch("pagepile.service.store_builder.build_store") as build_store_mock:
                                    get_psid_lock_mock.return_value = Lock()
                                    has_existing_store_mock.return_value = True
                                    read_meta_mock.return_value = cached_meta
                                    fetch_mock.return_value = (
                                        records,
                                        "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
                                    )
                                    build_store_mock.return_value = rebuilt_meta

                                    result = pagepile_service.ensure_loaded(
                                        112306,
                                        refresh=False,
                                        limit=25,
                                    )

        self.assertEqual(result, rebuilt_meta)
        fetch_mock.assert_called_once_with(pagepile_id=112306, limit=25)
        build_store_mock.assert_called_once_with(
            store_id=store_id,
            records=records,
            source_url="https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
            source_params={
                "pagepile_id": ["112306"],
                "limit": ["25"],
            },
        )

    def test_ensure_loaded_rebuilds_when_refresh_is_true(self) -> None:
        store_id = pagepile_service.internal_store_id(112306)
        rebuilt_meta = self._meta(limit=25)
        records = self._records()

        with patch("pagepile.service._ensure_oxigraph"):
            with patch("pagepile.service.store.prune_expired_stores"):
                with patch("pagepile.service.store.get_psid_lock") as get_psid_lock_mock:
                    with patch("pagepile.service.store.has_existing_store") as has_existing_store_mock:
                        with patch("pagepile.service.store.read_meta") as read_meta_mock:
                            with patch("pagepile.service.source.fetch_pagepile_records") as fetch_mock:
                                with patch("pagepile.service.store_builder.build_store") as build_store_mock:
                                    get_psid_lock_mock.return_value = Lock()
                                    fetch_mock.return_value = (
                                        records,
                                        "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
                                    )
                                    build_store_mock.return_value = rebuilt_meta

                                    result = pagepile_service.ensure_loaded(
                                        112306,
                                        refresh=True,
                                        limit=25,
                                    )

        self.assertEqual(result, rebuilt_meta)
        has_existing_store_mock.assert_not_called()
        read_meta_mock.assert_not_called()
        fetch_mock.assert_called_once_with(pagepile_id=112306, limit=25)
        build_store_mock.assert_called_once_with(
            store_id=store_id,
            records=records,
            source_url="https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
            source_params={
                "pagepile_id": ["112306"],
                "limit": ["25"],
            },
        )

    def test_ensure_loaded_caps_api_limit_in_source_params_and_fetch(self) -> None:
        store_id = pagepile_service.internal_store_id(112306)
        rebuilt_meta = self._meta(limit=2)
        records = self._records()

        with patch.object(pagepile_service.source, "_MAX_PAGEPILE_API_SAMPLE_LIMIT", 2):
            with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
                with patch("pagepile.service._ensure_oxigraph"):
                    with patch("pagepile.service.store.prune_expired_stores"):
                        with patch("pagepile.service.store.get_psid_lock") as get_psid_lock_mock:
                            with patch(
                                "pagepile.service.store.has_existing_store"
                            ) as has_existing_store_mock:
                                with patch(
                                    "pagepile.service.source.fetch_pagepile_records"
                                ) as fetch_mock:
                                    with patch(
                                        "pagepile.service.store_builder.build_store"
                                    ) as build_store_mock:
                                        get_psid_lock_mock.return_value = Lock()
                                        has_existing_store_mock.return_value = False
                                        fetch_mock.return_value = (
                                            records,
                                            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit=&format=json&limit=2",
                                        )
                                        build_store_mock.return_value = rebuilt_meta

                                        result = pagepile_service.ensure_loaded(
                                            112306,
                                            refresh=False,
                                            limit=1000,
                                        )

        self.assertEqual(result, rebuilt_meta)
        fetch_mock.assert_called_once_with(pagepile_id=112306, limit=2)
        build_store_mock.assert_called_once_with(
            store_id=store_id,
            records=records,
            source_url="https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit=&format=json&limit=2",
            source_params={
                "pagepile_id": ["112306"],
                "limit": ["2"],
            },
        )

    def test_ensure_loaded_raises_for_empty_records_current_behavior(self) -> None:
        with patch("pagepile.service._ensure_oxigraph"):
            with patch("pagepile.service.store.prune_expired_stores"):
                with patch("pagepile.service.store.get_psid_lock") as get_psid_lock_mock:
                    with patch("pagepile.service.store.has_existing_store") as has_existing_store_mock:
                        with patch("pagepile.service.source.fetch_pagepile_records") as fetch_mock:
                            with patch("pagepile.service.store_builder.build_store") as build_store_mock:
                                get_psid_lock_mock.return_value = Lock()
                                has_existing_store_mock.return_value = False
                                fetch_mock.return_value = (
                                    [],
                                    "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
                                )

                                with self.assertRaisesMessage(
                                    PetscanServiceError,
                                    "PagePile returned zero pages with Wikidata sitelinks for pile 112306.",
                                ):
                                    pagepile_service.ensure_loaded(
                                        112306,
                                        refresh=False,
                                        limit=25,
                                    )

        build_store_mock.assert_not_called()

    @patch("pagepile.service.sparql.serialize_select")
    @patch("pagepile.service._open_query_store")
    @patch("pagepile.service.ensure_loaded")
    @patch("pagepile.service.sparql.validate_query")
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
        serialize_select_mock.return_value = {"head": {"vars": ["page"]}, "results": {"bindings": []}}

        execution = pagepile_service.execute_query(
            112306,
            "SELECT ?page WHERE { ?page ?p ?o }",
            refresh=True,
            limit=25,
        )

        self.assertEqual(execution["query_type"], "SELECT")
        self.assertEqual(execution["result_format"], "sparql-json")
        self.assertEqual(
            execution["sparql_json"],
            {"head": {"vars": ["page"]}, "results": {"bindings": []}},
        )
        self.assertEqual(execution["meta"], meta)
        ensure_loaded_mock.assert_called_once_with(112306, refresh=True, limit=25)
        serialize_select_mock.assert_called_once_with(raw_result)

    @patch("pagepile.service.sparql.serialize_graph")
    @patch("pagepile.service._open_query_store")
    @patch("pagepile.service.ensure_loaded")
    @patch("pagepile.service.sparql.validate_query")
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
        serialize_graph_mock.return_value = (
            "<https://en.wikipedia.org/wiki/Example> <http://schema.org/about> "
            "<http://www.wikidata.org/entity/Q1757> .\n"
        )

        execution = pagepile_service.execute_query(
            112306,
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
        )

        self.assertEqual(execution["query_type"], "CONSTRUCT")
        self.assertEqual(execution["result_format"], "n-triples")
        self.assertEqual(
            execution["ntriples"],
            "<https://en.wikipedia.org/wiki/Example> <http://schema.org/about> "
            "<http://www.wikidata.org/entity/Q1757> .\n",
        )
        self.assertEqual(execution["meta"], meta)
        serialize_graph_mock.assert_called_once_with(raw_result)

    @patch("pagepile.service._open_query_store")
    @patch("pagepile.service.ensure_loaded")
    @patch("pagepile.service.sparql.validate_query")
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
            pagepile_service.execute_query(
                112306,
                "SELECT ?page WHERE { ?page a pagepile:Page . }",
            )
