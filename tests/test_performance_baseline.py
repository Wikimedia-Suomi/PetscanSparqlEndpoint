import gzip
import hashlib
import json
import unittest
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any, Dict
from unittest.mock import patch
from urllib.parse import urlparse

from django.conf import settings
from django.test import SimpleTestCase

from petscan import service_source as source
from petscan import service_store_builder as store_builder

BASELINE_PATH = Path(settings.BASE_DIR) / "data" / "benchmarks" / "lightweight_baseline.json"
EXAMPLES_DIR = Path(settings.BASE_DIR) / "data" / "examples"


def _load_baseline_spec() -> list[Dict[str, Any]]:
    payload = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    datasets = payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise AssertionError("lightweight_baseline.json must contain a non-empty datasets list")
    return [dict(item) for item in datasets]


def _load_records(file_name: str) -> list[Dict[str, Any]]:
    with gzip.open(EXAMPLES_DIR / file_name, mode="rt", encoding="utf-8") as payload_file:
        payload = json.load(payload_file)
    return list(source.extract_records(payload))


def _fake_enrichment_payload(site: str, title: str) -> Dict[str, Any] | None:
    seed = hashlib.blake2b(
        "{}|{}".format(site, title).encode("utf-8"),
        digest_size=16,
        person=b"gil-parity-seed",
    ).digest()
    selector = seed[0] % 4
    qid = "Q{}".format(1 + (int.from_bytes(seed[1:5], "big") % 90_000_000))
    page_len = 100 + (int.from_bytes(seed[5:9], "big") % 900_000)
    timestamp = "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}".format(
        2020 + (seed[9] % 7),
        1 + (seed[10] % 12),
        1 + (seed[11] % 28),
        seed[12] % 24,
        seed[13] % 60,
        seed[14] % 60,
    )

    if selector == 0:
        return {"wikidata_id": qid, "page_len": None, "rev_timestamp": None}
    if selector == 1:
        return {"wikidata_id": None, "page_len": page_len, "rev_timestamp": timestamp}
    if selector == 2:
        return {"wikidata_id": qid, "page_len": page_len, "rev_timestamp": timestamp}
    return None


def _fake_enrichment_fetch(api_url: str, titles: list[str], **_kwargs: Any) -> Dict[str, Dict[str, Any]]:
    site = urlparse(api_url).netloc.lower()
    resolved: Dict[str, Dict[str, Any]] = {}
    for title in titles:
        payload = _fake_enrichment_payload(site, title)
        if payload is not None:
            resolved[title] = payload
    return resolved


def _measure_build_store_ms(psid: int, records: list[Dict[str, Any]], use_fake_api: bool) -> float:
    with TemporaryDirectory(prefix="perf-baseline-") as temp_dir:
        temp_root = Path(temp_dir)

        def _store_path(value_psid: int) -> Path:
            return temp_root / str(value_psid)

        def _meta_path(value_psid: int) -> Path:
            return _store_path(value_psid) / "meta.json"

        enrichment_context = (
            patch(
                "petscan.service_links.fetch_wikibase_items_for_site_api",
                side_effect=_fake_enrichment_fetch,
            )
            if use_fake_api
            else patch("petscan.service_links.fetch_wikibase_items_for_site_api")
        )

        with (
            patch("petscan.service_links.wikidata_lookup_backend", return_value="api"),
            patch("petscan.service_store_builder.store.store_path", side_effect=_store_path),
            patch("petscan.service_store_builder.store.meta_path", side_effect=_meta_path),
            enrichment_context,
        ):
            started_at = perf_counter()
            store_builder.build_store(psid, records, "https://example.invalid")
            return (perf_counter() - started_at) * 1000.0


@unittest.skipUnless(
    bool(getattr(settings, "PERFORMANCE_BASELINE_TESTS", False)),
    "Performance baseline tests are disabled.",
)
class PerformanceBaselineTests(SimpleTestCase):
    def test_example_dataset_build_times_stay_within_lightweight_budget(self) -> None:
        if store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")

        for dataset in _load_baseline_spec():
            name = str(dataset["name"])
            file_name = str(dataset["file_name"])
            psid = int(dataset["psid"])
            expected_records = int(dataset["expected_records"])
            use_fake_api = bool(dataset["use_fake_api"])
            warmup_runs = int(dataset["warmup_runs"])
            measured_runs = int(dataset["measured_runs"])
            baseline_median_ms = float(dataset["baseline_median_ms"])
            max_median_ms = float(dataset["max_median_ms"])

            records = _load_records(file_name)
            self.assertEqual(len(records), expected_records)

            total_runs = warmup_runs + measured_runs
            durations_ms: list[float] = []
            for iteration_index in range(total_runs):
                elapsed_ms = _measure_build_store_ms(
                    psid=psid,
                    records=records,
                    use_fake_api=use_fake_api,
                )
                if iteration_index >= warmup_runs:
                    durations_ms.append(elapsed_ms)

            measured_median_ms = median(durations_ms)
            rounded_runs = [round(value, 1) for value in durations_ms]

            with self.subTest(dataset=name):
                self.assertLessEqual(
                    measured_median_ms,
                    max_median_ms,
                    msg=(
                        "{} exceeded lightweight performance budget: median_ms={:.1f}, "
                        "max_median_ms={:.1f}, baseline_median_ms={:.1f}, runs_ms={}"
                    ).format(
                        name,
                        measured_median_ms,
                        max_median_ms,
                        baseline_median_ms,
                        rounded_runs,
                    ),
                )
