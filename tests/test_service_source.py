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
