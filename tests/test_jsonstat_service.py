from typing import Any
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from jsonstat import service

SOURCE_URL = "https://pxdata.stat.fi/PxWeb/sq/example"


class JsonstatServiceTests(SimpleTestCase):
    @patch("jsonstat.service.runtime_links_service.load_runtime_links")
    def test_runtime_links_are_limited_to_statistics_finland_sources(self, load: Any) -> None:
        self.assertIsNone(service._runtime_links_for_source("https://example.org/data.json"))
        load.assert_not_called()

    @patch("jsonstat.service.runtime_links_service.load_runtime_links")
    def test_runtime_links_can_be_disabled_with_an_empty_path(self, load: Any) -> None:
        with self.settings(JSONSTAT_LINKING_SNAPSHOT_PATH=""):
            self.assertIsNone(service._runtime_links_for_source(SOURCE_URL))
        load.assert_not_called()

    def test_ensure_loaded_passes_best_effort_classifications_to_store_builder(self) -> None:
        payload: dict[str, Any] = {"version": "2.0", "class": "dataset"}
        records = [{"value": 1}]
        enrichments: dict[str, Any] = {"area": object()}
        runtime_links = object()
        expected_meta: dict[str, Any] = {
            "psid": service.internal_store_id(SOURCE_URL),
            "records": 1,
            "source_url": SOURCE_URL,
            "source_params": {"url": [SOURCE_URL]},
            "loaded_at": "2026-08-30T10:00:00+00:00",
            "structure": {"row_count": 1, "field_count": 1, "fields": []},
        }
        lock = MagicMock()

        with (
            patch("jsonstat.service._ensure_oxigraph"),
            patch("jsonstat.service.store.prune_expired_stores"),
            patch("jsonstat.service.store.get_psid_lock", return_value=lock),
            patch("jsonstat.service.store.has_existing_store", return_value=False),
            patch(
                "jsonstat.service.source.fetch_jsonstat_json",
                return_value=(payload, SOURCE_URL),
            ),
            patch("jsonstat.service.source.extract_records", return_value=records),
            patch(
                "jsonstat.service.classification.enrich_classifications",
                return_value=enrichments,
            ) as enrich,
            patch(
                "jsonstat.service.runtime_links_service.load_runtime_links",
                return_value=runtime_links,
            ) as load_runtime_links,
            patch(
                "jsonstat.service.store_builder.build_store",
                return_value=expected_meta,
            ) as build_store,
        ):
            meta = service.ensure_loaded(SOURCE_URL, refresh=True)

        self.assertEqual(meta, expected_meta)
        enrich.assert_called_once_with(payload, SOURCE_URL)
        load_runtime_links.assert_called_once_with()
        build_store.assert_called_once_with(
            store_id=service.internal_store_id(SOURCE_URL),
            records=records,
            payload=payload,
            source_url=SOURCE_URL,
            source_params={"url": [SOURCE_URL]},
            classification_enrichments=enrichments,
            runtime_links=runtime_links,
        )
