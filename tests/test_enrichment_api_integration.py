import json
import re
import unittest
from typing import Any, Mapping
from urllib.request import Request, urlopen

from django.conf import settings
from django.test import SimpleTestCase

from petscan import enrichment_api
from petscan.service_errors import GilLinkEnrichmentError

LIVE_API_URL = "https://en.wikipedia.org/w/api.php"
LIVE_INVALID_API_URL = "https://en.wikipedia.org/wiki/Albert_Einstein"
LIVE_BAD_ACTION_URL = "https://en.wikipedia.org/w/api.php?action=doesnotexist&format=json&formatversion=2"
LIVE_SAMPLE_TITLES = [
    "Albert_Einstein",
    "Málaga",
    "Beyoncé",
]
TEST_USER_AGENT = "PetscanSparqlEndpoint integration tests"
REV_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
QID_RE = re.compile(r"^Q[1-9][0-9]*$")


@unittest.skipUnless(
    bool(getattr(settings, "LIVE_API_INTEGRATION_TESTS", False)),
    "Live MediaWiki API integration tests are disabled.",
)
class LiveEnrichmentApiTests(SimpleTestCase):
    def _assert_live_payload(self, payload: Mapping[str, Any], title: str) -> None:
        self.assertIsInstance(payload, Mapping, msg=title)

        qid = payload.get("wikidata_id")
        self.assertIsInstance(qid, str, msg=title)
        self.assertRegex(qid, QID_RE, msg=title)

        page_len = payload.get("page_len")
        self.assertIsInstance(page_len, int, msg=title)
        self.assertGreater(page_len, 0, msg=title)

        rev_timestamp = payload.get("rev_timestamp")
        self.assertIsInstance(rev_timestamp, str, msg=title)
        self.assertRegex(rev_timestamp, REV_TIMESTAMP_RE, msg=title)

    def test_live_api_lookup_returns_real_enrichment_payloads(self) -> None:
        resolved = enrichment_api.fetch_wikibase_items_for_site_api(
            LIVE_API_URL,
            LIVE_SAMPLE_TITLES,
            user_agent=TEST_USER_AGENT,
            timeout_seconds=30,
        )

        self.assertEqual(set(resolved.keys()), set(LIVE_SAMPLE_TITLES))
        for title in LIVE_SAMPLE_TITLES:
            self._assert_live_payload(resolved[title], title)

    def test_live_non_api_endpoint_raises_gil_link_enrichment_error(self) -> None:
        with self.assertRaisesMessage(
            GilLinkEnrichmentError,
            "Wikibase enrichment API request failed for {}".format(LIVE_INVALID_API_URL),
        ):
            enrichment_api.fetch_wikibase_items_for_site_api(
                LIVE_INVALID_API_URL,
                ["Albert_Einstein"],
                user_agent=TEST_USER_AGENT,
                timeout_seconds=30,
            )

    def test_live_mediawiki_error_payload_shape_matches_mocked_unit_test_case(self) -> None:
        request = Request(
            LIVE_BAD_ACTION_URL,
            headers={
                "Accept": "application/json",
                "User-Agent": TEST_USER_AGENT,
            },
        )

        with urlopen(request, timeout=30) as response:  # nosec B310
            payload = json.loads(response.read().decode("utf-8"))

        self.assertIsInstance(payload, Mapping)
        error = payload.get("error")
        self.assertIsInstance(error, Mapping)
        self.assertEqual(error.get("code"), "badvalue")
        self.assertIn('Unrecognized value for parameter "action"', str(error.get("info", "")))
