import json
from unittest.mock import patch

from django.test import SimpleTestCase

from petscan import enrichment_api
from petscan.service_errors import GilLinkEnrichmentError


class _FakeHttpResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class EnrichmentApiTests(SimpleTestCase):
    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_wikibase_items_raises_on_transport_error(self, urlopen_mock):
        urlopen_mock.side_effect = RuntimeError("boom")

        with self.assertRaisesMessage(
            GilLinkEnrichmentError,
            "Wikibase enrichment API request failed",
        ) as captured:
            enrichment_api.fetch_wikibase_items_for_site_api(
                "https://fi.wikipedia.org/w/api.php",
                ["Turku"],
                user_agent="test-agent",
                timeout_seconds=5,
            )

        self.assertEqual(
            captured.exception.public_message,
            "Failed to enrich linked pages from an upstream service.",
        )

    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_wikibase_items_raises_on_api_error_payload(self, urlopen_mock):
        payload = {
            "error": {
                "code": "badvalue",
                "info": 'Unrecognized value for parameter "action": doesnotexist.',
            }
        }
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))

        with self.assertRaisesMessage(
            GilLinkEnrichmentError,
            "Wikibase enrichment API returned error badvalue",
        ):
            enrichment_api.fetch_wikibase_items_for_site_api(
                "https://fi.wikipedia.org/w/api.php",
                ["Turku"],
                user_agent="test-agent",
                timeout_seconds=5,
            )

    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_wikibase_items_returns_qid_page_len_and_rev_timestamp(self, urlopen_mock):
        payload = {
            "query": {
                "pages": [
                    {
                        "title": "Turku",
                        "length": 201234,
                        "pageprops": {"wikibase_item": "Q38517"},
                        "revisions": [{"timestamp": "2026-03-15T10:00:00Z"}],
                    },
                    {
                        "title": "Raisio",
                        "length": 50221,
                        "pageprops": {"wikibase_item": "Q716197"},
                        "revisions": [{"timestamp": "2026-03-14T23:59:59Z"}],
                    },
                ]
            }
        }
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))

        resolved = enrichment_api.fetch_wikibase_items_for_site_api(
            "https://fi.wikipedia.org/w/api.php",
            ["Turku", "Raisio"],
            user_agent="test-agent",
            timeout_seconds=5,
        )

        self.assertEqual(
            resolved,
            {
                "Turku": {
                    "wikidata_id": "Q38517",
                    "page_len": 201234,
                    "rev_timestamp": "2026-03-15T10:00:00Z",
                },
                "Raisio": {
                    "wikidata_id": "Q716197",
                    "page_len": 50221,
                    "rev_timestamp": "2026-03-14T23:59:59Z",
                },
            },
        )

        request_url = urlopen_mock.call_args.args[0].full_url
        self.assertIn("prop=pageprops%7Cinfo%7Crevisions", request_url)
        self.assertIn("rvprop=timestamp", request_url)
        self.assertNotIn("rvlimit=", request_url)

    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_wikibase_items_resolves_redirect_aliases_for_enriched_payload(self, urlopen_mock):
        payload = {
            "query": {
                "redirects": [
                    {"from": "Raisio_(kaupunki)", "to": "Raisio"},
                ],
                "pages": [
                    {
                        "title": "Raisio",
                        "length": 50221,
                        "pageprops": {"wikibase_item": "Q716197"},
                        "revisions": [{"timestamp": "2026-03-14T23:59:59Z"}],
                    }
                ],
            }
        }
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))

        resolved = enrichment_api.fetch_wikibase_items_for_site_api(
            "https://fi.wikipedia.org/w/api.php",
            ["Raisio_(kaupunki)"],
            user_agent="test-agent",
            timeout_seconds=5,
        )

        self.assertEqual(
            resolved,
            {
                "Raisio_(kaupunki)": {
                    "wikidata_id": "Q716197",
                    "page_len": 50221,
                    "rev_timestamp": "2026-03-14T23:59:59Z",
                }
            },
        )

    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_wikibase_items_keeps_nulls_when_optional_fields_missing(self, urlopen_mock):
        payload = {
            "query": {
                "pages": [
                    {
                        "title": "No_Data_Page",
                        "length": "123",
                        "pageprops": {},
                        "revisions": [{}],
                    }
                ]
            }
        }
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))

        resolved = enrichment_api.fetch_wikibase_items_for_site_api(
            "https://fi.wikipedia.org/w/api.php",
            ["No_Data_Page"],
            user_agent="test-agent",
            timeout_seconds=5,
        )

        self.assertEqual(
            resolved,
            {
                "No_Data_Page": {
                    "wikidata_id": None,
                    "page_len": 123,
                    "rev_timestamp": None,
                }
            },
        )

    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_wikibase_items_allows_successful_empty_response(self, urlopen_mock):
        payload = {"query": {"pages": []}}
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))

        resolved = enrichment_api.fetch_wikibase_items_for_site_api(
            "https://fi.wikipedia.org/w/api.php",
            ["Turku"],
            user_agent="test-agent",
            timeout_seconds=5,
        )

        self.assertEqual(resolved, {})
