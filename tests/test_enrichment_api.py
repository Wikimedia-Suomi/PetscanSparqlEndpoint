import json
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

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
    @patch("petscan.enrichment_api.fetch_wikibase_items_for_site_api")
    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_page_categories_with_wikidata_follows_continuation(
        self,
        urlopen_mock,
        wikibase_fetch_mock,
    ):
        first_payload = {
            "continue": {"continue": "||", "clcontinue": "1|Category:B"},
            "query": {
                "pages": [
                    {
                        "title": "Example",
                        "categories": [
                            {
                                "title": "Category:A",
                                "hidden": True,
                            }
                        ],
                    }
                ]
            },
        }
        second_payload = {
            "query": {
                "pages": [
                    {
                        "title": "Example",
                        "categories": [
                            {"title": "Category:A"},
                            {"title": "Category:B"},
                        ],
                    }
                ]
            }
        }
        urlopen_mock.side_effect = [
            _FakeHttpResponse(json.dumps(first_payload).encode("utf-8")),
            _FakeHttpResponse(json.dumps(second_payload).encode("utf-8")),
        ]
        wikibase_fetch_mock.return_value = {
            "Category:A": {
                "wikidata_id": "Q1",
                "page_len": None,
                "rev_timestamp": None,
            },
            "Category:B": {
                "wikidata_id": None,
                "page_len": None,
                "rev_timestamp": None,
            },
        }

        resolved = enrichment_api.fetch_page_categories_with_wikidata_api(
            "https://en.wikipedia.org/w/api.php",
            ["Example"],
            user_agent="test-agent",
            timeout_seconds=5,
        )

        self.assertEqual(
            resolved,
            {
                "Example": [
                    {
                        "title": "Category:A",
                        "hiddencat": True,
                        "wikidata_id": "Q1",
                    },
                    {
                        "title": "Category:B",
                        "hiddencat": False,
                        "wikidata_id": None,
                    },
                ]
            },
        )
        self.assertEqual(urlopen_mock.call_count, 2)
        self.assertIn(
            "clcontinue=1%7CCategory%3AB",
            urlopen_mock.call_args_list[1].args[0].full_url,
        )
        self.assertIn("clprop=hidden", urlopen_mock.call_args.args[0].full_url)
        self.assertNotIn("sortkey", urlopen_mock.call_args.args[0].full_url)
        self.assertNotIn("defaultsort", urlopen_mock.call_args.args[0].full_url)
        wikibase_fetch_mock.assert_called_once()

    @patch("petscan.enrichment_api.fetch_wikibase_items_for_site_api")
    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_page_categories_uses_page_ids_and_maps_back_to_input_title(
        self,
        urlopen_mock,
        wikibase_fetch_mock,
    ):
        payload = {
            "query": {
                "pages": [
                    {
                        "pageid": 42,
                        "title": "Current title",
                        "categories": [{"title": "Category:A"}],
                    }
                ]
            }
        }
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))
        wikibase_fetch_mock.return_value = {
            "Category:A": {
                "wikidata_id": "Q1",
                "page_len": None,
                "rev_timestamp": None,
            }
        }

        resolved = enrichment_api.fetch_page_categories_with_wikidata_api(
            "https://en.wikipedia.org/w/api.php",
            ["Original_title"],
            user_agent="test-agent",
            timeout_seconds=5,
            page_ids_by_title={"Original_title": 42},
        )

        self.assertEqual(
            resolved,
            {
                "Original_title": [
                    {
                        "title": "Category:A",
                        "hiddencat": False,
                        "wikidata_id": "Q1",
                    }
                ]
            },
        )
        request_url = urlopen_mock.call_args.args[0].full_url
        self.assertIn("pageids=42", request_url)
        self.assertNotIn("titles=", request_url)
        self.assertNotIn("redirects=", request_url)

    @patch("petscan.enrichment_api.fetch_wikibase_items_for_site_api")
    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_page_categories_maps_shared_page_id_to_every_input_title(
        self,
        urlopen_mock,
        wikibase_fetch_mock,
    ):
        payload = {
            "query": {
                "pages": [
                    {
                        "pageid": 42,
                        "title": "Current title",
                        "categories": [{"title": "Category:A"}],
                    }
                ]
            }
        }
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))
        wikibase_fetch_mock.return_value = {
            "Category:A": {
                "wikidata_id": "Q1",
                "page_len": None,
                "rev_timestamp": None,
            }
        }

        resolved = enrichment_api.fetch_page_categories_with_wikidata_api(
            "https://en.wikipedia.org/w/api.php",
            ["First_redirect", "Second_redirect"],
            user_agent="test-agent",
            timeout_seconds=5,
            page_ids_by_title={"First_redirect": 42, "Second_redirect": 42},
        )

        expected_categories = [
            {
                "title": "Category:A",
                "hiddencat": False,
                "wikidata_id": "Q1",
            }
        ]
        self.assertEqual(resolved["First_redirect"], expected_categories)
        self.assertEqual(resolved["Second_redirect"], expected_categories)
        request_url = urlopen_mock.call_args.args[0].full_url
        self.assertEqual(parse_qs(urlsplit(request_url).query)["pageids"], ["42"])

    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_global_user_registrations_uses_batched_centralauth_query(
        self,
        urlopen_mock,
    ):
        payload = {
            "query": {
                "globalusers": [
                    {
                        "name": "New uploader",
                        "registration": "2026-03-15T10:00:00Z",
                    },
                    {"name": "Missing registration"},
                ]
            }
        }
        urlopen_mock.return_value = _FakeHttpResponse(json.dumps(payload).encode("utf-8"))

        resolved = enrichment_api.fetch_global_user_registrations_api(
            "https://meta.wikimedia.org/w/api.php",
            ["New uploader", "Missing registration"],
            user_agent="test-agent",
            timeout_seconds=5,
        )

        self.assertEqual(resolved, {"New uploader": "2026-03-15T10:00:00Z"})
        request = urlopen_mock.call_args.args[0]
        self.assertIn("list=globalusers", request.full_url)
        self.assertIn("gusprop=registration", request.full_url)
        self.assertIn("gususers=New+uploader%7CMissing+registration", request.full_url)

    @patch("petscan.enrichment_api.urlopen")
    def test_fetch_global_user_registrations_sanitizes_transport_errors(self, urlopen_mock):
        urlopen_mock.side_effect = RuntimeError("private upstream detail")

        with self.assertRaisesMessage(
            enrichment_api.PetscanServiceError,
            "CentralAuth globalusers API request failed",
        ) as captured:
            enrichment_api.fetch_global_user_registrations_api(
                "https://meta.wikimedia.org/w/api.php",
                ["Uploader"],
                user_agent="test-agent",
                timeout_seconds=5,
            )

        self.assertEqual(
            captured.exception.public_message,
            "Failed to enrich file uploader registration data from CentralAuth.",
        )

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
                        "pageid": 736,
                        "title": "Turku",
                        "length": 201234,
                        "pageprops": {"wikibase_item": "Q38517"},
                        "revisions": [{"timestamp": "2026-03-15T10:00:00Z"}],
                    },
                    {
                        "pageid": 934,
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
                    "page_id": 736,
                },
                "Raisio": {
                    "wikidata_id": "Q716197",
                    "page_len": 50221,
                    "rev_timestamp": "2026-03-14T23:59:59Z",
                    "page_id": 934,
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
                        "pageid": 934,
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
                    "page_id": 934,
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
