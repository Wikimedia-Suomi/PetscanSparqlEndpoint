import copy
import json
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from jsonstat import service_classification as classification

SOURCE_URL = "https://pxdata.stat.fi/PxWeb/sq/example"
CLASSIFICATION_ID = "ikaryhma_10_20180101"


def _payload() -> dict[str, Any]:
    return {
        "version": "2.0",
        "class": "dataset",
        "id": [CLASSIFICATION_ID, "timeperiod_y"],
        "size": [2, 1],
        "dimension": {
            CLASSIFICATION_ID: {
                "category": {
                    "index": ["SSS", "18-24"],
                    "label": {"SSS": "Total", "18-24": "18-24"},
                }
            },
            "timeperiod_y": {"category": {"index": ["2026"], "label": {"2026": "2026"}}},
        },
        "value": [1, 2],
    }


def _classification_metadata() -> list[dict[str, Any]]:
    return [
        {
            "classificationSerie": {
                "localId": "ikaryhma",
                "classificationSerieName": [
                    {"lang": "fi", "name": "Ikäryhmät"},
                ],
            },
            "localId": CLASSIFICATION_ID,
            "modifiedDate": "2026-03-12T14:21:45Z",
            "releaseDate": "2018-01-01",
            "terminationDate": None,
            "internationalRecommendation": False,
            "nationalRecommendation": False,
            "classificationName": [{"lang": "fi", "name": "Px-koodisto ikä"}],
            "classificationDescription": [
                {"lang": "fi", "description": "Taulukoinnissa käytettävä ikäkoodisto."}
            ],
            "classificationPurpose": [{"lang": "fi", "purpose": "Ikätietojen taulukointi."}],
        }
    ]


def _classification_items() -> list[dict[str, Any]]:
    return [
        {
            "classification": {"localId": CLASSIFICATION_ID},
            "localId": "{}/SSS".format(CLASSIFICATION_ID),
            "level": 0.0,
            "code": "SSS",
            "order": 10,
            "parentCode": None,
            "classificationItemNames": [{"lang": "fi", "name": "Yhteensä"}],
            "explanatoryNotes": [
                {
                    "lang": ["fi"],
                    "generalNote": ["Kaikki ikäryhmät yhteensä."],
                }
            ],
        },
        {
            "classification": {"localId": CLASSIFICATION_ID},
            "localId": "{}/not-in-source".format(CLASSIFICATION_ID),
            "level": 1.0,
            "code": "not-in-source",
            "order": 20,
            "parentCode": "SSS",
            "classificationItemNames": [{"lang": "fi", "name": "Muu"}],
            "explanatoryNotes": [],
        },
    ]


def _api_response(url: str) -> Any:
    if "/classificationItems?" in url:
        return _classification_items()
    if "/classifications/{}?".format(CLASSIFICATION_ID) in url:
        return _classification_metadata()
    if "/classifications?" in url:
        return [
            {"localId": CLASSIFICATION_ID},
            {"localId": "{}_near_match".format(CLASSIFICATION_ID)},
        ]
    raise AssertionError("Unexpected classification API URL: {}".format(url))


def _local_snapshot_payload() -> dict[str, Any]:
    metadata = copy.deepcopy(_classification_metadata()[0])
    items = copy.deepcopy(_classification_items())
    items[0]["externalLinks"] = [
        {
            "relation": "http://www.w3.org/2004/02/skos/core#closeMatch",
            "targets": [
                "http://www.wikidata.org/entity/Q159",
                "http://www.yso.fi/onto/yso/p12345",
            ],
        }
    ]
    for item in items:
        item.pop("classification")
        item["classificationId"] = CLASSIFICATION_ID
    metadata["items"] = items
    return {
        "metadata": {
            "language": "fi",
            "retrievedAt": "2026-08-30T11:54:34+00:00",
            "classificationCount": 1,
            "classificationItemCount": 2,
            "classificationItemFailureCount": 0,
            "schemaVersion": "1.0",
        },
        "classificationFamilies": [],
        "classificationSeries": [],
        "classifications": [metadata],
    }


@override_settings(
    JSONSTAT_CLASSIFICATION_SNAPSHOT_PATH="",
    JSONSTAT_CLASSIFICATION_NETWORK_FALLBACK_ENABLED=True,
)
class JsonstatClassificationTests(SimpleTestCase):
    def setUp(self) -> None:
        classification.clear_classification_cache()

    def tearDown(self) -> None:
        classification.clear_classification_cache()

    @patch("jsonstat.service_classification._fetch_api_json", side_effect=_api_response)
    def test_enrichment_joins_exact_dimension_and_category_codes(self, fetch: Any) -> None:
        enrichments = classification.enrich_classifications(_payload(), SOURCE_URL)

        self.assertEqual(list(enrichments), [CLASSIFICATION_ID])
        enrichment = enrichments[CLASSIFICATION_ID]
        self.assertEqual(enrichment.local_id, CLASSIFICATION_ID)
        self.assertEqual(enrichment.labels, {"fi": ["Px-koodisto ikä"]})
        self.assertEqual(enrichment.series_id, "ikaryhma")
        self.assertEqual(enrichment.release_date, "2018-01-01")
        self.assertFalse(enrichment.national_recommendation)
        self.assertEqual(list(enrichment.items), ["SSS"])
        self.assertEqual(enrichment.items["SSS"].labels, {"fi": ["Yhteensä"]})
        self.assertEqual(
            enrichment.items["SSS"].notes["generalNote"]["fi"],
            ["Kaikki ikäryhmät yhteensä."],
        )
        self.assertNotIn("18-24", enrichment.items)
        self.assertEqual(fetch.call_count, 3)

    @patch("jsonstat.service_classification._fetch_api_json", side_effect=_api_response)
    def test_enrichment_caches_successful_api_responses(self, fetch: Any) -> None:
        classification.enrich_classifications(_payload(), SOURCE_URL)
        classification.enrich_classifications(_payload(), SOURCE_URL)

        self.assertEqual(fetch.call_count, 3)

    @patch("jsonstat.service_classification._fetch_api_json")
    def test_enrichment_does_not_use_fuzzy_dimension_matches(self, fetch: Any) -> None:
        fetch.return_value = [{"localId": "{}_near_match".format(CLASSIFICATION_ID)}]

        self.assertEqual(classification.enrich_classifications(_payload(), SOURCE_URL), {})
        fetch.assert_called_once()

    @patch("jsonstat.service_classification._fetch_api_json")
    def test_enrichment_requires_exact_item_local_id(self, fetch: Any) -> None:
        invalid_items = _classification_items()
        invalid_items[0]["localId"] = "{}/WRONG".format(CLASSIFICATION_ID)

        def response(url: str) -> Any:
            if "/classificationItems?" in url:
                return invalid_items
            return _api_response(url)

        fetch.side_effect = response

        enrichments = classification.enrich_classifications(_payload(), SOURCE_URL)

        self.assertEqual(enrichments[CLASSIFICATION_ID].items, {})

    @patch("jsonstat.service_classification._fetch_api_json", side_effect=_api_response)
    def test_network_fallback_uses_one_item_collection_request(self, fetch: Any) -> None:
        enrichments = classification.enrich_classifications(_payload(), SOURCE_URL)

        self.assertEqual(list(enrichments[CLASSIFICATION_ID].items), ["SSS"])
        requested_urls = [call.args[0] for call in fetch.call_args_list]
        self.assertTrue(any("/classificationItems?" in url for url in requested_urls))
        self.assertFalse(any("/classificationItems/SSS?" in url for url in requested_urls))

    @patch("jsonstat.service_classification._fetch_api_json")
    def test_local_snapshot_enriches_without_network_requests(self, fetch: Any) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            snapshot_path = Path(temporary_directory) / "classifications.json"
            snapshot_path.write_text(
                json.dumps(_local_snapshot_payload()),
                encoding="utf-8",
            )
            with self.settings(
                JSONSTAT_CLASSIFICATION_SNAPSHOT_PATH=str(snapshot_path),
                JSONSTAT_CLASSIFICATION_NETWORK_FALLBACK_ENABLED=False,
            ):
                enrichments = classification.enrich_classifications(_payload(), SOURCE_URL)

        self.assertEqual(list(enrichments), [CLASSIFICATION_ID])
        self.assertEqual(enrichments[CLASSIFICATION_ID].labels, {"fi": ["Px-koodisto ikä"]})
        self.assertEqual(list(enrichments[CLASSIFICATION_ID].items), ["SSS"])
        self.assertEqual(
            enrichments[CLASSIFICATION_ID].items["SSS"].external_links,
            [
                (
                    "http://www.w3.org/2004/02/skos/core#closeMatch",
                    "http://www.wikidata.org/entity/Q159",
                ),
                (
                    "http://www.w3.org/2004/02/skos/core#closeMatch",
                    "http://www.yso.fi/onto/yso/p12345",
                ),
            ],
        )
        fetch.assert_not_called()

    @patch("jsonstat.service_classification._fetch_api_json")
    def test_missing_local_classification_does_not_trigger_network_by_default(
        self,
        fetch: Any,
    ) -> None:
        payload = _local_snapshot_payload()
        payload["classifications"] = []
        payload["metadata"]["classificationCount"] = 0
        payload["metadata"]["classificationItemCount"] = 0
        with tempfile.TemporaryDirectory() as temporary_directory:
            snapshot_path = Path(temporary_directory) / "classifications.json"
            snapshot_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.settings(
                JSONSTAT_CLASSIFICATION_SNAPSHOT_PATH=str(snapshot_path),
                JSONSTAT_CLASSIFICATION_NETWORK_FALLBACK_ENABLED=False,
            ):
                enrichments = classification.enrich_classifications(_payload(), SOURCE_URL)

        self.assertEqual(enrichments, {})
        fetch.assert_not_called()

    @patch("jsonstat.service_classification._fetch_api_json")
    def test_enrichment_is_skipped_for_non_statistics_finland_sources(self, fetch: Any) -> None:
        self.assertEqual(
            classification.enrich_classifications(_payload(), "https://example.org/data.json"),
            {},
        )
        fetch.assert_not_called()

    @patch("jsonstat.service_classification._fetch_api_json")
    def test_enrichment_failure_does_not_fail_dataset_loading(self, fetch: Any) -> None:
        fetch.side_effect = classification.ClassificationServiceError("service unavailable")

        with self.assertLogs("jsonstat.service_classification", level="WARNING"):
            enrichments = classification.enrich_classifications(_payload(), SOURCE_URL)

        self.assertEqual(enrichments, {})

    def test_classification_languages_rejects_unknown_languages(self) -> None:
        with self.settings(JSONSTAT_CLASSIFICATION_LANGUAGES=("fi", "de")):
            with self.assertRaisesMessage(ValueError, "supports only fi, sv, and en"):
                classification.classification_languages()
