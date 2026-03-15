from unittest.mock import patch
from urllib.parse import urlparse

from petscan import service_links as links
from tests.service_test_support import ServiceTestCase


class ServiceLinksTests(ServiceTestCase):
    def test_iter_gil_link_uris_normalizes_known_links(self):
        record = {
            "gil": "enwiki:0:Federalist_No._42|dewiki:0:Berlin|invalid|enwiki:0:Federalist_No._42"
        }

        self.assertEqual(
            links.iter_gil_link_uris(record),
            [
                "https://en.wikipedia.org/wiki/Federalist_No._42",
                "https://de.wikipedia.org/wiki/Berlin",
            ],
        )

    def test_site_to_mediawiki_api_url_returns_valid_https_url(self):
        url = links.site_to_mediawiki_api_url("enwiki")
        parsed = urlparse(url)

        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "en.wikipedia.org")
        self.assertEqual(parsed.path, "/w/api.php")

    def test_site_to_mediawiki_api_url_rejects_malformed_site_tokens(self):
        malformed_sites = [
            "localhost/wiki",
            "a/bwiki",
            "foo..wiki",
            "evil.comwiki",
            "evil.com/wikivoyage",
            " ",
        ]
        for site in malformed_sites:
            self.assertIsNone(links.site_to_mediawiki_api_url(site), msg=site)

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=links.LOOKUP_BACKEND_API)
    @patch("petscan.service_links.fetch_wikibase_items_for_site_api")
    def test_gil_enrichment_lookup_batches_by_site_and_max_50_titles(self, fetch_mock, _backend_mock):
        def fake_fetch(_api_url, titles, **_kwargs):
            # Return deterministic fake enrichment payloads without network.
            return {
                title: {"wikidata_id": "Q{}".format(index), "page_len": None, "rev_timestamp": None}
                for index, title in enumerate(titles, start=1)
            }

        fetch_mock.side_effect = fake_fetch

        en_links = ["enwiki:0:Article_{}".format(i) for i in range(61)]
        de_links = ["dewiki:0:Artikel_{}".format(i) for i in range(3)]
        records = [{"gil": "|".join(en_links + de_links)}]

        link_map = links.build_gil_link_enrichment_map(records)

        self.assertEqual(len(link_map), 64)
        self.assertIn("https://en.wikipedia.org/wiki/Article_0", link_map)

        call_sizes = [len(call.args[1]) for call in fetch_mock.call_args_list]
        self.assertTrue(call_sizes)
        self.assertLessEqual(max(call_sizes), 50)

        en_calls = [call for call in fetch_mock.call_args_list if "en.wikipedia.org" in call.args[0]]
        de_calls = [call for call in fetch_mock.call_args_list if "de.wikipedia.org" in call.args[0]]
        self.assertEqual(len(en_calls), 2)
        self.assertEqual(sum(len(call.args[1]) for call in en_calls), 61)
        self.assertEqual(len(de_calls), 1)
        self.assertEqual(sum(len(call.args[1]) for call in de_calls), 3)

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=links.LOOKUP_BACKEND_API)
    @patch("petscan.service_links.fetch_wikibase_items_for_site_api")
    def test_wikidata_item_gil_link_resolves_directly_with_enrichment(self, fetch_mock, _backend_mock):
        fetch_mock.return_value = {
            "Albert_Einstein": {
                "wikidata_id": "Q937",
                "page_len": 886543,
                "rev_timestamp": "2026-03-14T23:59:59Z",
            }
        }
        records = [{"gil": "wikidatawiki:0:Q42|enwiki:0:Albert_Einstein"}]

        link_map = links.build_gil_link_enrichment_map(records)

        self.assertEqual(
            link_map.get("https://www.wikidata.org/wiki/Q42"),
            {"wikidata_id": "Q42", "page_len": None, "rev_timestamp": None},
        )
        self.assertEqual(
            link_map.get("https://en.wikipedia.org/wiki/Albert_Einstein"),
            {
                "wikidata_id": "Q937",
                "page_len": 886543,
                "rev_timestamp": "2026-03-14T23:59:59Z",
            },
        )
        self.assertEqual(len(fetch_mock.call_args_list), 2)
        self.assertTrue(any("en.wikipedia.org" in call.args[0] for call in fetch_mock.call_args_list))
        self.assertTrue(any("www.wikidata.org" in call.args[0] for call in fetch_mock.call_args_list))

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=links.LOOKUP_BACKEND_API)
    @patch("petscan.service_links.fetch_wikibase_items_for_site_api")
    def test_enrichment_map_fetches_page_len_and_timestamp_for_direct_wikidata_qid_links(
        self,
        fetch_mock,
        _backend_mock,
    ):
        def fake_fetch(api_url, titles, **_kwargs):
            if "www.wikidata.org" in api_url:
                return {
                    "Q42": {
                        "wikidata_id": "Q42",
                        "page_len": 12345,
                        "rev_timestamp": "2026-03-15T10:00:00Z",
                    }
                }
            return {
                "Albert_Einstein": {
                    "wikidata_id": "Q937",
                    "page_len": 886543,
                    "rev_timestamp": "2026-03-14T23:59:59Z",
                }
            }

        fetch_mock.side_effect = fake_fetch
        records = [{"gil": "wikidatawiki:0:Q42|enwiki:0:Albert_Einstein"}]

        enrichment = links.build_gil_link_enrichment_map(records)

        self.assertEqual(
            enrichment["https://www.wikidata.org/wiki/Q42"],
            {
                "wikidata_id": "Q42",
                "page_len": 12345,
                "rev_timestamp": "2026-03-15T10:00:00Z",
            },
        )
        self.assertEqual(
            enrichment["https://en.wikipedia.org/wiki/Albert_Einstein"],
            {
                "wikidata_id": "Q937",
                "page_len": 886543,
                "rev_timestamp": "2026-03-14T23:59:59Z",
            },
        )
        self.assertTrue(any("www.wikidata.org" in call.args[0] for call in fetch_mock.call_args_list))

    @patch("petscan.service_links.fetch_wikibase_items_for_site_api")
    def test_api_lookup_accepts_enriched_payload_shape(self, api_fetch_mock):
        api_fetch_mock.return_value = {
            "Albert_Einstein": {
                "wikidata_id": "Q937",
                "page_len": 886543,
                "rev_timestamp": "20260314235959",
            },
            "Unknown_Page": {
                "wikidata_id": None,
                "page_len": 42,
                "rev_timestamp": "20200101000000",
            },
        }

        resolved = links.fetch_wikibase_enrichment_for_site(
            "enwiki",
            [
                links.SiteLookupTarget(namespace=0, api_title="Albert_Einstein", db_title="Albert_Einstein"),
                links.SiteLookupTarget(namespace=0, api_title="Unknown_Page", db_title="Unknown_Page"),
            ],
            backend=links.LOOKUP_BACKEND_API,
        )

        self.assertEqual(
            resolved,
            {
                "Albert_Einstein": {
                    "wikidata_id": "Q937",
                    "page_len": 886543,
                    "rev_timestamp": "2026-03-14T23:59:59Z",
                },
                "Unknown_Page": {
                    "wikidata_id": None,
                    "page_len": 42,
                    "rev_timestamp": "2020-01-01T00:00:00Z",
                },
            },
        )

    @patch("petscan.service_links.wikidata_lookup_backend", return_value=links.LOOKUP_BACKEND_TOOLFORGE_SQL)
    @patch("petscan.service_links.enrichment_sql.fetch_wikibase_items_for_site_sql")
    def test_build_gil_link_enrichment_map_normalizes_sql_timestamp_to_xsd(self, sql_fetch_mock, _backend_mock):
        sql_fetch_mock.return_value = {
            "Albert_Einstein": {
                "wikidata_id": "Q937",
                "page_len": 886543,
                "rev_timestamp": "20260314235959",
            }
        }
        records = [{"gil": "enwiki:0:Albert_Einstein"}]

        enrichment = links.build_gil_link_enrichment_map(records)

        self.assertEqual(
            enrichment,
            {
                "https://en.wikipedia.org/wiki/Albert_Einstein": {
                    "wikidata_id": "Q937",
                    "page_len": 886543,
                    "rev_timestamp": "2026-03-14T23:59:59Z",
                }
            },
        )

    @patch("petscan.service_links.enrichment_sql.fetch_wikibase_items_for_site_sql")
    def test_toolforge_sql_lookup_accepts_enriched_payload_shape(self, sql_fetch_mock):
        sql_fetch_mock.return_value = {
            "Albert_Einstein": {
                "wikidata_id": "Q937",
                "page_len": 886543,
                "rev_timestamp": "20260314235959",
            },
            "Unknown_Page": {
                "wikidata_id": None,
                "page_len": 42,
                "rev_timestamp": "20200101000000",
            },
        }

        resolved = links.fetch_wikibase_enrichment_for_site(
            "enwiki",
            [
                links.SiteLookupTarget(namespace=0, api_title="Albert_Einstein", db_title="Albert_Einstein"),
                links.SiteLookupTarget(namespace=0, api_title="Unknown_Page", db_title="Unknown_Page"),
            ],
            backend=links.LOOKUP_BACKEND_TOOLFORGE_SQL,
        )

        self.assertEqual(
            resolved,
            {
                "Albert_Einstein": {
                    "wikidata_id": "Q937",
                    "page_len": 886543,
                    "rev_timestamp": "2026-03-14T23:59:59Z",
                },
                "Unknown_Page": {
                    "wikidata_id": None,
                    "page_len": 42,
                    "rev_timestamp": "2020-01-01T00:00:00Z",
                },
            },
        )
