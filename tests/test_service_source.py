import json
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from petscan import service_source as source
from tests.service_test_support import (
    PRIMARY_EXAMPLE_FILE,
    PRIMARY_EXAMPLE_PSID,
    PRIMARY_RECORD_COUNT,
    SECONDARY_EXAMPLE_FILE,
    SECONDARY_RECORD_COUNT,
    ServiceTestCase,
)


class ServiceSourceTests(ServiceTestCase):
    def test_normalize_petscan_params_filters_reserved_blank_and_empty_values(self):
        normalized = source.normalize_petscan_params(
            {
                " category ": [" Turku ", "", None],
                "language": " fi ",
                "psid": "999",
                "format": "xml",
                "refresh": "1",
                "query": "SELECT * WHERE { ?s ?p ?o }",
                " ": "skip",
                "tags": (" first ", " ", 2),
                "empty_list": [],
            }
        )

        self.assertEqual(
            normalized,
            {
                "category": ["Turku"],
                "language": ["fi"],
                "tags": ["first", "2"],
            },
        )

    def test_extract_records_from_example_json_has_expected_count(self):
        payload = self._load_payload(PRIMARY_EXAMPLE_FILE)
        records = source.extract_records(payload)
        self.assertEqual(len(records), PRIMARY_RECORD_COUNT)

    def test_extract_records_from_second_example_has_expected_count(self):
        payload = self._load_payload(SECONDARY_EXAMPLE_FILE)
        records = source.extract_records(payload)
        self.assertEqual(len(records), SECONDARY_RECORD_COUNT)

    def test_extract_records_matches_exhaustive_logic_for_example_payloads(self):
        for file_name in (PRIMARY_EXAMPLE_FILE, SECONDARY_EXAMPLE_FILE):
            payload = self._load_payload(file_name)
            optimized = source.extract_records(payload)
            exhaustive = source._extract_records_exhaustive(payload)
            self.assertEqual(optimized, exhaustive)

    def test_build_petscan_url_forwards_extra_query_params(self):
        url = source.build_petscan_url(
            PRIMARY_EXAMPLE_PSID,
            petscan_params={
                "category": ["Turku"],
                "language": "fi",
                "psid": "999",
                "format": "xml",
            },
        )
        parsed_query = parse_qs(urlparse(url).query)

        self.assertEqual(parsed_query.get("psid"), [str(PRIMARY_EXAMPLE_PSID)])
        self.assertEqual(parsed_query.get("format"), ["json"])
        self.assertEqual(parsed_query.get("category"), ["Turku"])
        self.assertEqual(parsed_query.get("language"), ["fi"])

    def test_extract_records_prefers_direct_candidates_that_look_like_record_rows(self):
        payload = {
            "pages": [
                {"id": 1, "title": "Preferred row"},
                {"id": 2, "title": "Second row"},
            ],
            "*": [
                {"not_a_record": "value"},
                {"still_not": "record rows"},
            ],
        }

        self.assertEqual(
            source.extract_records(payload),
            [
                {"id": 1, "title": "Preferred row"},
                {"id": 2, "title": "Second row"},
            ],
        )

    def test_extract_records_supports_nested_a_star_direct_candidate(self):
        payload = {
            "*": [
                {
                    "a": {
                        "*": [
                            {"pageid": 10, "title": "Nested row"},
                            {"pageid": 11, "title": "Nested row 2"},
                        ]
                    }
                }
            ]
        }

        self.assertEqual(
            source.extract_records(payload),
            [
                {"pageid": 10, "title": "Nested row"},
                {"pageid": 11, "title": "Nested row 2"},
            ],
        )

    def test_extract_records_falls_back_to_exhaustive_nested_search(self):
        payload = {
            "metadata": {"generated": "2026-03-21T00:00:00Z"},
            "outer": {
                "inner": {
                    "rows": [
                        {"id": 1, "title": "Deep row"},
                        {"id": 2, "title": "Another deep row"},
                    ]
                }
            },
        }

        self.assertEqual(
            source.extract_records(payload),
            [
                {"id": 1, "title": "Deep row"},
                {"id": 2, "title": "Another deep row"},
            ],
        )

    def test_extract_records_raises_when_payload_contains_no_dict_rows(self):
        with self.assertRaisesMessage(
            source.PetscanServiceError,
            "Could not locate row data in PetScan JSON payload.",
        ):
            source.extract_records({"rows": [1, 2, 3], "other": {"nested": ["x", "y"]}})

    @patch("petscan.service_source.urlopen")
    def test_fetch_petscan_json_returns_payload_and_source_url(self, urlopen_mock):
        response = urlopen_mock.return_value.__enter__.return_value
        response.read.return_value = json.dumps({"pages": [{"id": 1, "title": "Example"}]}).encode("utf-8")

        payload, source_url = source.fetch_petscan_json(
            PRIMARY_EXAMPLE_PSID,
            petscan_params={"category": ["Turku"], "language": "fi"},
        )

        self.assertEqual(payload, {"pages": [{"id": 1, "title": "Example"}]})
        self.assertIn("psid={}".format(PRIMARY_EXAMPLE_PSID), source_url)
        self.assertIn("category=Turku", source_url)
        self.assertIn("language=fi", source_url)

        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.full_url, source_url)
        self.assertEqual(dict(request.header_items())["Accept"], "application/json")
        self.assertEqual(dict(request.header_items())["User-agent"], source.HTTP_USER_AGENT)

    @patch("petscan.service_source.urlopen")
    def test_fetch_petscan_json_rejects_non_json_payload(self, urlopen_mock):
        response = urlopen_mock.return_value.__enter__.return_value
        response.read.return_value = b"not-json"

        with self.assertRaisesMessage(source.PetscanServiceError, "PetScan returned non-JSON payload."):
            source.fetch_petscan_json(PRIMARY_EXAMPLE_PSID)

    @patch("petscan.service_source.urlopen")
    def test_fetch_petscan_json_rejects_non_object_json_payload(self, urlopen_mock):
        response = urlopen_mock.return_value.__enter__.return_value
        response.read.return_value = json.dumps([{"id": 1}]).encode("utf-8")

        with self.assertRaisesMessage(
            source.PetscanServiceError,
            "Unexpected PetScan JSON format (expected object).",
        ):
            source.fetch_petscan_json(PRIMARY_EXAMPLE_PSID)

    @patch("petscan.service_source.urlopen")
    def test_fetch_petscan_json_sets_public_message_for_transport_errors(self, urlopen_mock):
        urlopen_mock.side_effect = OSError("Temporary failure in name resolution")

        with self.assertRaisesMessage(source.PetscanServiceError, "Failed to fetch PetScan data:") as captured:
            source.fetch_petscan_json(PRIMARY_EXAMPLE_PSID)

        self.assertEqual(
            captured.exception.public_message,
            "Failed to fetch PetScan data: Temporary failure in name resolution",
        )
