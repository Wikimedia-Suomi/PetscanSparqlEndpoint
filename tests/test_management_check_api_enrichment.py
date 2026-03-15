import io
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase


class CheckApiEnrichmentCommandTests(SimpleTestCase):
    @patch("petscan.management.commands.check_api_enrichment._build_filtered_enrichment_map")
    @patch("petscan.management.commands.check_api_enrichment.source.extract_records")
    @patch("petscan.management.commands.check_api_enrichment.source.fetch_petscan_json")
    def test_command_succeeds_when_all_gil_links_have_page_len_and_timestamp(
        self,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_filtered_enrichment_map_mock,
    ):
        records = [{"gil": "enwiki:0:Albert_Einstein|wikidatawiki:0:Q42"}]
        fetch_petscan_json_mock.return_value = ({}, "https://petscan.wmcloud.org/?psid=43641756")
        extract_records_mock.return_value = records
        build_filtered_enrichment_map_mock.return_value = (
            {
                "https://en.wikipedia.org/wiki/Albert_Einstein": object(),
                "https://www.wikidata.org/wiki/Q42": object(),
            },
            {
                "https://en.wikipedia.org/wiki/Albert_Einstein": {
                    "wikidata_id": "Q937",
                    "page_len": 886543,
                    "rev_timestamp": "2026-03-15T10:00:00Z",
                },
                "https://www.wikidata.org/wiki/Q42": {
                    "wikidata_id": "Q42",
                    "page_len": 12345,
                    "rev_timestamp": "2026-03-15T10:00:00Z",
                },
            },
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        call_command(
            "check_api_enrichment",
            "--psid",
            "43641756",
            stdout=stdout,
            stderr=stderr,
        )

        build_filtered_enrichment_map_mock.assert_called_once_with(records, None)
        self.assertIn("gil_links_total=2", stdout.getvalue())
        self.assertIn("links_with_both=2", stdout.getvalue())
        self.assertIn("All gil links have page_len and rev_timestamp.", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    @patch("petscan.management.commands.check_api_enrichment._probe_target_api_payload")
    @patch("petscan.management.commands.check_api_enrichment._build_filtered_enrichment_map")
    @patch("petscan.management.commands.check_api_enrichment.source.extract_records")
    @patch("petscan.management.commands.check_api_enrichment.source.fetch_petscan_json")
    def test_command_fails_by_default_when_any_gil_link_is_missing_fields(
        self,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_filtered_enrichment_map_mock,
        probe_target_api_payload_mock,
    ):
        records = [{"gil": "enwiki:0:Albert_Einstein|wikidatawiki:0:Q42"}]
        fetch_petscan_json_mock.return_value = ({}, "https://petscan.wmcloud.org/?psid=43641756")
        extract_records_mock.return_value = records
        target = object()
        build_filtered_enrichment_map_mock.return_value = (
            {
                "https://en.wikipedia.org/wiki/Albert_Einstein": target,
                "https://www.wikidata.org/wiki/Q42": object(),
            },
            {
                "https://en.wikipedia.org/wiki/Albert_Einstein": {
                    "wikidata_id": "Q937",
                    "page_len": 886543,
                    "rev_timestamp": "2026-03-15T10:00:00Z",
                },
                "https://www.wikidata.org/wiki/Q42": {
                    "wikidata_id": "Q42",
                    "page_len": None,
                    "rev_timestamp": None,
                },
            },
        )
        probe_target_api_payload_mock.return_value = (
            "https://www.wikidata.org/w/api.php?action=query&titles=Q42",
            {
                "wikidata_id": "Q42",
                "page_len": None,
                "rev_timestamp": None,
            },
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        with self.assertRaises(CommandError) as ctx:
            call_command(
                "check_api_enrichment",
                "--psid",
                "43641756",
                stdout=stdout,
                stderr=stderr,
            )

        self.assertIn("first missing link detected", str(ctx.exception))

    @patch("petscan.management.commands.check_api_enrichment._build_filtered_enrichment_map")
    @patch("petscan.management.commands.check_api_enrichment.source.extract_records")
    @patch("petscan.management.commands.check_api_enrichment.source.fetch_petscan_json")
    def test_command_ignores_missing_fields_for_allowed_missing_exception_link(
        self,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_filtered_enrichment_map_mock,
    ):
        allowed_link_1 = (
            "https://sat.wikipedia.org/wiki/"
            "%E1%B1%A2%E1%B1%A9%E1%B1%AC%E1%B1%A9%E1%B1%9B:%E1%B1%9E%E1%B1%9F_"
            "%E1%B1%AF%E1%B1%9F%E1%B1%A1%E1%B1%BD"
        )
        allowed_link_2 = (
            "https://sat.wikipedia.org/wiki/"
            "%E1%B1%A2%E1%B1%A9%E1%B1%AC%E1%B1%A9%E1%B1%9B:%E1%B1%AE%E1%B1%9E%E1%B1%9F%E1%B1%9D_"
            "%E1%B1%AE%E1%B1%B8%E1%B1%9C%E1%B1%AE%E1%B1%9E"
        )
        records = [{"gil": "satwiki:0:dummy"}]
        fetch_petscan_json_mock.return_value = ({}, "https://petscan.wmcloud.org/?psid=43641756")
        extract_records_mock.return_value = records
        build_filtered_enrichment_map_mock.return_value = (
            {
                allowed_link_1: object(),
                allowed_link_2: object(),
            },
            {
                allowed_link_1: {
                    "wikidata_id": "Q123",
                    "page_len": None,
                    "rev_timestamp": None,
                },
                allowed_link_2: {
                    "wikidata_id": "Q123",
                    "page_len": None,
                    "rev_timestamp": None,
                },
            },
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        call_command(
            "check_api_enrichment",
            "--psid",
            "43641756",
            stdout=stdout,
            stderr=stderr,
        )

        self.assertIn("gil_links_total=2", stdout.getvalue())
        self.assertIn("allowed_missing_exceptions=2", stdout.getvalue())
        self.assertIn("All gil links have page_len and rev_timestamp.", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")
