from datetime import datetime, timezone
from threading import Lock
from unittest.mock import patch

from django.test import SimpleTestCase

from incubator import service as incubator_service


class IncubatorServiceModuleTests(SimpleTestCase):
    def test_internal_store_id_changes_with_requested_filters(self) -> None:
        base_store_id = incubator_service.internal_store_id(limit=None, recentchanges_only=False)

        self.assertEqual(
            base_store_id,
            incubator_service.internal_store_id(limit=None, recentchanges_only=False),
        )
        self.assertNotEqual(
            base_store_id,
            incubator_service.internal_store_id(limit=25, recentchanges_only=False),
        )
        self.assertNotEqual(
            base_store_id,
            incubator_service.internal_store_id(limit=None, recentchanges_only=True),
        )
        self.assertNotEqual(
            base_store_id,
            incubator_service.internal_store_id(limit=None, recentchanges_only=False, page_latest=123456789),
        )
        self.assertNotEqual(
            base_store_id,
            incubator_service.internal_store_id(
                limit=None,
                recentchanges_only=False,
                page_prefixes=["Wp/sms/"],
            ),
        )
        self.assertNotEqual(
            base_store_id,
            incubator_service.internal_store_id(
                limit=None,
                namespaces=[14],
                recentchanges_only=False,
            ),
        )

    def test_ensure_loaded_reuses_fresh_meta_when_source_params_match(self) -> None:
        store_id = incubator_service.internal_store_id(
            limit=25,
            namespaces=[14],
            recentchanges_only=True,
            page_latest=123456789,
            page_prefixes=["Wp/sms/"],
        )
        fresh_meta = {
            "psid": store_id,
            "records": 2,
            "source_url": "https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
            "source_params": {
                "limit": ["25"],
                "namespace": ["14"],
                "recentchanges_only": ["1"],
                "page_latest": ["123456789"],
                "page_prefix": ["Wp/sms/"],
            },
            "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

        with patch("incubator.service._ensure_oxigraph"):
            with patch("incubator.service.store.prune_expired_stores") as prune_expired_stores_mock:
                with patch("incubator.service.store.get_psid_lock") as get_psid_lock_mock:
                    with patch("incubator.service.store.has_existing_store") as has_existing_store_mock:
                        with patch("incubator.service.store.read_meta") as read_meta_mock:
                            with patch(
                                "incubator.service.source.fetch_incubator_records"
                            ) as fetch_incubator_records_mock:
                                with patch(
                                    "incubator.service.store_builder.build_store"
                                ) as build_store_mock:
                                    get_psid_lock_mock.return_value = Lock()
                                    has_existing_store_mock.return_value = True
                                    read_meta_mock.return_value = fresh_meta

                                    result = incubator_service.ensure_loaded(
                                        refresh=False,
                                        limit=25,
                                        namespaces=[14],
                                        page_latest=123456789,
                                        page_prefixes=["Wp/sms/"],
                                        recentchanges_only=True,
                                    )

        self.assertEqual(result, fresh_meta)
        prune_expired_stores_mock.assert_called_once_with(exclude_psids=[store_id])
        get_psid_lock_mock.assert_called_once_with(store_id)
        has_existing_store_mock.assert_called_once_with(store_id)
        read_meta_mock.assert_called_once_with(store_id)
        fetch_incubator_records_mock.assert_not_called()
        build_store_mock.assert_not_called()

    def test_ensure_loaded_rebuilds_when_cached_meta_source_params_do_not_match(self) -> None:
        store_id = incubator_service.internal_store_id(
            limit=25,
            namespaces=[14],
            recentchanges_only=True,
            page_latest=123456789,
            page_prefixes=["Wp/sms/"],
        )
        cached_meta = {
            "psid": store_id,
            "records": 2,
            "source_url": "https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
            "source_params": {"limit": ["25"]},
            "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        records = [
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
        ]
        rebuilt_meta = {
            "psid": store_id,
            "records": 1,
            "source_url": "https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
            "source_params": {
                "limit": ["25"],
                "namespace": ["14"],
                "recentchanges_only": ["1"],
                "page_latest": ["123456789"],
                "page_prefix": ["Wp/sms/"],
            },
            "loaded_at": "2026-04-02T09:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }

        with patch("incubator.service._ensure_oxigraph"):
            with patch("incubator.service.store.prune_expired_stores") as prune_expired_stores_mock:
                with patch("incubator.service.store.get_psid_lock") as get_psid_lock_mock:
                    with patch("incubator.service.store.has_existing_store") as has_existing_store_mock:
                        with patch("incubator.service.store.read_meta") as read_meta_mock:
                            with patch(
                                "incubator.service.source.fetch_incubator_records"
                            ) as fetch_incubator_records_mock:
                                with patch(
                                    "incubator.service.store_builder.build_store"
                                ) as build_store_mock:
                                    get_psid_lock_mock.return_value = Lock()
                                    has_existing_store_mock.return_value = True
                                    read_meta_mock.return_value = cached_meta
                                    fetch_incubator_records_mock.return_value = (
                                        records,
                                        "https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
                                    )
                                    build_store_mock.return_value = rebuilt_meta

                                    result = incubator_service.ensure_loaded(
                                        refresh=False,
                                        limit=25,
                                        namespaces=[14],
                                        page_latest=123456789,
                                        page_prefixes=["Wp/sms/"],
                                        recentchanges_only=True,
                                    )

        self.assertEqual(result, rebuilt_meta)
        prune_expired_stores_mock.assert_called_once_with(exclude_psids=[store_id])
        get_psid_lock_mock.assert_called_once_with(store_id)
        has_existing_store_mock.assert_called_once_with(store_id)
        read_meta_mock.assert_called_once_with(store_id)
        fetch_incubator_records_mock.assert_called_once_with(
            limit=25,
            namespaces=[14],
            recentchanges_only=True,
            page_latest=123456789,
            page_prefixes=["Wp/sms/"],
        )
        build_store_mock.assert_called_once_with(
            store_id=store_id,
            records=records,
            source_url="https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
            source_params={
                "limit": ["25"],
                "namespace": ["14"],
                "recentchanges_only": ["1"],
                "page_latest": ["123456789"],
                "page_prefix": ["Wp/sms/"],
            },
        )

    def test_ensure_loaded_builds_empty_store_when_source_returns_zero_rows(self) -> None:
        store_id = incubator_service.internal_store_id(limit=25, namespaces=[12])
        empty_meta = {
            "psid": store_id,
            "records": 0,
            "source_url": "https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
            "source_params": {
                "limit": ["25"],
                "namespace": ["12"],
            },
            "loaded_at": "2026-04-02T09:00:00+00:00",
            "structure": {"row_count": 0, "field_count": 0, "fields": []},
        }

        with patch("incubator.service._ensure_oxigraph"):
            with patch("incubator.service.store.prune_expired_stores"):
                with patch("incubator.service.store.get_psid_lock") as get_psid_lock_mock:
                    with patch("incubator.service.store.has_existing_store") as has_existing_store_mock:
                        with patch("incubator.service.source.fetch_incubator_records") as fetch_mock:
                            with patch("incubator.service.store_builder.build_store") as build_store_mock:
                                get_psid_lock_mock.return_value = Lock()
                                has_existing_store_mock.return_value = False
                                fetch_mock.return_value = (
                                    [],
                                    "https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
                                )
                                build_store_mock.return_value = empty_meta

                                result = incubator_service.ensure_loaded(
                                    refresh=False,
                                    limit=25,
                                    namespaces=[12],
                                )

        self.assertEqual(result, empty_meta)
        build_store_mock.assert_called_once_with(
            store_id=store_id,
            records=[],
            source_url="https://incubator.wikimedia.org/wiki/Category:Maintenance:Wikidata_interwiki_links",
            source_params={
                "limit": ["25"],
                "namespace": ["12"],
            },
        )
