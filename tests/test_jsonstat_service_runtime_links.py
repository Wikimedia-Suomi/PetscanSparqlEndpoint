import json
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase

from jsonstat import service_runtime_links


class JsonstatRuntimeLinksTests(SimpleTestCase):
    def setUp(self) -> None:
        service_runtime_links.clear_runtime_links_cache()

    def tearDown(self) -> None:
        service_runtime_links.clear_runtime_links_cache()

    def test_curated_snapshot_has_bounded_reviewed_scope(self) -> None:
        snapshot_path = Path(settings.JSONSTAT_LINKING_SNAPSHOT_PATH)
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        metadata = payload["metadata"]

        self.assertLess(snapshot_path.stat().st_size, 2 * 1024 * 1024)
        self.assertEqual(metadata["snapshotType"], "statfi-linking-runtime")
        self.assertEqual(metadata["classificationCount"], 7)
        self.assertEqual(metadata["classificationItemCount"], 1_134)
        self.assertEqual(metadata["classificationExternalTargetCount"], 3_903)
        self.assertEqual(metadata["statisticsCount"], 134)
        self.assertEqual(metadata["statisticsWithExternalLinksCount"], 28)
        self.assertEqual(metadata["unitMappingCount"], 79)
        self.assertEqual(
            metadata["conceptIdentifiers"],
            {
                "included": False,
                "wikidataProperty": "P14864",
                "policy": (
                    "Statistics Finland concept IDs are intentionally omitted; "
                    "Wikidata property P14864 is the canonical integration path."
                ),
            },
        )
        self.assertFalse(metadata["reproducibility"]["inRepository"])
        self.assertNotIn("reviewCandidates", payload)

        classifications = payload["classifications"]
        classification_items = [
            item for classification in classifications for item in classification["items"]
        ]
        self.assertEqual(len(classifications), metadata["classificationCount"])
        self.assertEqual(len(classification_items), metadata["classificationItemCount"])
        self.assertTrue(all(item["externalLinks"] for item in classification_items))
        self.assertEqual(
            sum(
                len(link["targets"])
                for item in classification_items
                for link in item["externalLinks"]
            ),
            metadata["classificationExternalTargetCount"],
        )
        self.assertEqual(len(payload["statistics"]), metadata["statisticsCount"])
        self.assertEqual(
            sum(bool(statistics["externalLinks"]) for statistics in payload["statistics"]),
            metadata["statisticsWithExternalLinksCount"],
        )
        self.assertEqual(len(payload["units"]), metadata["unitMappingCount"])

    def test_loader_indexes_statistics_and_units(self) -> None:
        snapshot = service_runtime_links.load_runtime_links()

        self.assertEqual(len(snapshot.statistics), 134)
        self.assertEqual(len(snapshot.units), 79)
        self.assertEqual(snapshot.statistics["kora"].url, "https://stat.fi/tilasto/kora")
        self.assertIn("KORA", snapshot.statistics["kora"].aliases)
        self.assertIn(
            "http://www.wikidata.org/entity/Q2144402",
            snapshot.statistics["kora"].external_links[0].targets,
        )
        self.assertEqual(snapshot.units["prosenttia"].mapping_status, "accepted")
        self.assertEqual(snapshot.units["prosenttia"].model["ucumCode"], "%")
