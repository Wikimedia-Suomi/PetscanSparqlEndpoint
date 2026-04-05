import io
from typing import Any
from unittest.mock import call, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase

from newpages import service_source


class CheckNewpagesWqsUriParityCommandTests(SimpleTestCase):
    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._fetch_wqs_article_uris")
    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._load_sample_records")
    def test_command_succeeds_when_all_sampled_uris_match_wqs(
        self,
        load_sample_records_mock: Any,
        fetch_wqs_article_uris_mock: Any,
    ) -> None:
        fi_sample_records = [
            {
                "wiki_domain": "fi.wikipedia.org",
                "namespace": 14,
                "wikidata_id": "Q1757",
                "site_url": "https://fi.wikipedia.org/",
                "page_url": "https://fi.wikipedia.org/wiki/Luokka:Turku",
                "page_title": "Luokka:Turku",
            }
        ]
        sv_sample_records = [
            {
                "wiki_domain": "sv.wikipedia.org",
                "namespace": 10,
                "wikidata_id": "Q42",
                "site_url": "https://sv.wikipedia.org/",
                "page_url": "https://sv.wikipedia.org/wiki/Mall:Exempel",
                "page_title": "Mall:Exempel",
            },
        ]
        load_sample_records_mock.side_effect = [
            ("toolforge_sql", "20260306000000", fi_sample_records),
            ("toolforge_sql", "20260306000000", sv_sample_records),
        ]
        fetch_wqs_article_uris_mock.side_effect = [
            {
                ("Q1757", "https://fi.wikipedia.org/"): ["https://fi.wikipedia.org/wiki/Luokka:Turku"],
            },
            {
                ("Q42", "https://sv.wikipedia.org/"): ["https://sv.wikipedia.org/wiki/Mall:Exempel"],
            },
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()

        call_command(
            "check_newpages_wqs_uri_parity",
            "--wiki",
            "fi.wikipedia.org,sv.wikipedia.org",
            stdout=stdout,
            stderr=stderr,
        )

        self.assertEqual(
            load_sample_records_mock.call_args_list,
            [
                call(["fi.wikipedia.org"], days=30, sample_size=3),
                call(["sv.wikipedia.org"], days=30, sample_size=3),
            ],
        )
        self.assertEqual(
            fetch_wqs_article_uris_mock.call_args_list,
            [
                call(fi_sample_records),
                call(sv_sample_records),
            ],
        )
        self.assertIn("target_wikis=2", stdout.getvalue())
        self.assertIn("backend=toolforge_sql", stdout.getvalue())
        self.assertIn("checking wiki=fi.wikipedia.org", stdout.getvalue())
        self.assertIn("checking wiki=sv.wikipedia.org", stdout.getvalue())
        self.assertIn("sample_records=1", stdout.getvalue())
        self.assertIn("sample wiki=fi.wikipedia.org namespace=14 pages=1", stdout.getvalue())
        self.assertIn("sample wiki=sv.wikipedia.org namespace=10 pages=1", stdout.getvalue())
        self.assertIn(
            "page=Luokka:Turku\n"
            "  local_uri: https://fi.wikipedia.org/wiki/Luokka:Turku\n"
            "  wqs_uri:   https://fi.wikipedia.org/wiki/Luokka:Turku",
            stdout.getvalue(),
        )
        self.assertIn(
            "page=Mall:Exempel\n"
            "  local_uri: https://sv.wikipedia.org/wiki/Mall:Exempel\n"
            "  wqs_uri:   https://sv.wikipedia.org/wiki/Mall:Exempel",
            stdout.getvalue(),
        )
        self.assertIn("checked=1", stdout.getvalue())
        self.assertIn("WQS URI parity passed for wiki=fi.wikipedia.org checked=1.", stdout.getvalue())
        self.assertIn("WQS URI parity passed for wiki=sv.wikipedia.org checked=1.", stdout.getvalue())
        self.assertIn("WQS URI parity passed for 2 sampled non-mainspace new pages.", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._fetch_wqs_article_uris")
    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._load_sample_records")
    def test_command_fails_when_any_sampled_uri_differs_from_wqs(
        self,
        load_sample_records_mock: Any,
        fetch_wqs_article_uris_mock: Any,
    ) -> None:
        sample_records = [
            {
                "wiki_domain": "sv.wikipedia.org",
                "namespace": 14,
                "wikidata_id": "Q42",
                "site_url": "https://sv.wikipedia.org/",
                "page_url": "https://sv.wikipedia.org/wiki/Kategori:Exempel",
                "page_title": "Kategori:Exempel",
            }
        ]
        load_sample_records_mock.return_value = ("api", "20260306000000", sample_records)
        fetch_wqs_article_uris_mock.return_value = {
            ("Q42", "https://sv.wikipedia.org/"): ["https://sv.wikipedia.org/wiki/Kategori:Annat"]
        }
        stdout = io.StringIO()
        stderr = io.StringIO()

        with self.assertRaises(CommandError) as context:
            call_command(
                "check_newpages_wqs_uri_parity",
                "--wiki",
                "sv.wikipedia.org",
                stdout=stdout,
                stderr=stderr,
            )

        self.assertIn("WQS URI parity failed for wiki sv.wikipedia.org (1 sampled page(s)).", str(context.exception))
        self.assertIn("expected=https://sv.wikipedia.org/wiki/Kategori:Exempel", stderr.getvalue())
        self.assertIn("wqs=https://sv.wikipedia.org/wiki/Kategori:Annat", stderr.getvalue())
        self.assertIn("checking wiki=sv.wikipedia.org", stdout.getvalue())
        self.assertIn(
            "page=Kategori:Exempel\n"
            "  local_uri: https://sv.wikipedia.org/wiki/Kategori:Exempel\n"
            "  wqs_uri:   https://sv.wikipedia.org/wiki/Kategori:Annat",
            stdout.getvalue(),
        )
        self.assertIn("checked=0", stdout.getvalue())

    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._load_sample_records")
    def test_command_fails_when_no_samples_are_found(self, load_sample_records_mock: Any) -> None:
        load_sample_records_mock.return_value = ("api", "20260306000000", [])

        with self.assertRaises(CommandError) as context:
            call_command(
                "check_newpages_wqs_uri_parity",
                "--wiki",
                "fi.wikipedia.org",
            )

        self.assertIn("No non-mainspace new pages with Wikidata items were found", str(context.exception))

    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._fetch_wqs_article_uris")
    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._load_sample_records")
    @patch("newpages.management.commands.check_newpages_wqs_uri_parity.source._known_wikis_by_domain")
    def test_command_without_wiki_uses_sitematrix_and_start_wiki(
        self,
        known_wikis_by_domain_mock: Any,
        load_sample_records_mock: Any,
        fetch_wqs_article_uris_mock: Any,
    ) -> None:
        known_wikis_by_domain_mock.return_value = {
            "commons.wikimedia.org": service_source._WikiDescriptor(
                domain="commons.wikimedia.org",
                dbname="commonswiki",
                lang_code="en",
                wiki_group="commons",
                site_url="https://commons.wikimedia.org/",
            ),
            "fi.wikipedia.org": service_source._WikiDescriptor(
                domain="fi.wikipedia.org",
                dbname="fiwiki",
                lang_code="fi",
                wiki_group="wikipedia",
                site_url="https://fi.wikipedia.org/",
            ),
            "fi.wikivoyage.org": service_source._WikiDescriptor(
                domain="fi.wikivoyage.org",
                dbname="fiwikivoyage",
                lang_code="fi",
                wiki_group="wikivoyage",
                site_url="https://fi.wikivoyage.org/",
            ),
            "sv.wikipedia.org": service_source._WikiDescriptor(
                domain="sv.wikipedia.org",
                dbname="svwiki",
                lang_code="sv",
                wiki_group="wikipedia",
                site_url="https://sv.wikipedia.org/",
            ),
            "zh.wikipedia.org": service_source._WikiDescriptor(
                domain="zh.wikipedia.org",
                dbname="zhwiki",
                lang_code="zh",
                wiki_group="wikipedia",
                site_url="https://zh.wikipedia.org/",
            ),
        }
        fi_wikivoyage_sample_records = [
            {
                "wiki_domain": "fi.wikivoyage.org",
                "namespace": 10,
                "wikidata_id": "Q33",
                "site_url": "https://fi.wikivoyage.org/",
                "page_url": "https://fi.wikivoyage.org/wiki/Malline:Esimerkki",
                "page_title": "Malline:Esimerkki",
            }
        ]
        sv_sample_records = [
            {
                "wiki_domain": "sv.wikipedia.org",
                "namespace": 14,
                "wikidata_id": "Q42",
                "site_url": "https://sv.wikipedia.org/",
                "page_url": "https://sv.wikipedia.org/wiki/Kategori:Exempel",
                "page_title": "Kategori:Exempel",
            }
        ]
        zh_sample_records = [
            {
                "wiki_domain": "zh.wikipedia.org",
                "namespace": 10,
                "wikidata_id": "Q1",
                "site_url": "https://zh.wikipedia.org/",
                "page_url": "https://zh.wikipedia.org/wiki/Template:Example",
                "page_title": "Template:Example",
            }
        ]
        load_sample_records_mock.side_effect = [
            ("api", "20260306000000", fi_wikivoyage_sample_records),
            ("api", "20260306000000", sv_sample_records),
            ("api", "20260306000000", zh_sample_records),
        ]
        fetch_wqs_article_uris_mock.side_effect = [
            {
                ("Q33", "https://fi.wikivoyage.org/"): ["https://fi.wikivoyage.org/wiki/Malline:Esimerkki"],
            },
            {
                ("Q42", "https://sv.wikipedia.org/"): ["https://sv.wikipedia.org/wiki/Kategori:Exempel"],
            },
            {
                ("Q1", "https://zh.wikipedia.org/"): ["https://zh.wikipedia.org/wiki/Template:Example"],
            },
        ]
        stdout = io.StringIO()

        call_command(
            "check_newpages_wqs_uri_parity",
            "--start-wiki",
            "fi.wikivoyage.org",
            stdout=stdout,
        )

        self.assertEqual(
            load_sample_records_mock.call_args_list,
            [
                call(["fi.wikivoyage.org"], days=30, sample_size=3),
                call(["sv.wikipedia.org"], days=30, sample_size=3),
                call(["zh.wikipedia.org"], days=30, sample_size=3),
            ],
        )
        self.assertIn("target_wikis=3", stdout.getvalue())
        self.assertNotIn("checking wiki=fi.wikipedia.org", stdout.getvalue())
        self.assertNotIn("checking wiki=commons.wikimedia.org", stdout.getvalue())
        self.assertIn("checking wiki=fi.wikivoyage.org", stdout.getvalue())
        self.assertIn("checking wiki=sv.wikipedia.org", stdout.getvalue())
        self.assertIn("checking wiki=zh.wikipedia.org", stdout.getvalue())

    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._fetch_wqs_article_uris")
    @patch("newpages.management.commands.check_newpages_wqs_uri_parity._load_sample_records")
    @patch("newpages.management.commands.check_newpages_wqs_uri_parity.source._known_wikis_by_domain")
    def test_command_without_wiki_stops_on_first_failure(
        self,
        known_wikis_by_domain_mock: Any,
        load_sample_records_mock: Any,
        fetch_wqs_article_uris_mock: Any,
    ) -> None:
        known_wikis_by_domain_mock.return_value = {
            "fi.wikipedia.org": service_source._WikiDescriptor(
                domain="fi.wikipedia.org",
                dbname="fiwiki",
                lang_code="fi",
                wiki_group="wikipedia",
                site_url="https://fi.wikipedia.org/",
            ),
            "sv.wikipedia.org": service_source._WikiDescriptor(
                domain="sv.wikipedia.org",
                dbname="svwiki",
                lang_code="sv",
                wiki_group="wikipedia",
                site_url="https://sv.wikipedia.org/",
            ),
            "zh.wikipedia.org": service_source._WikiDescriptor(
                domain="zh.wikipedia.org",
                dbname="zhwiki",
                lang_code="zh",
                wiki_group="wikipedia",
                site_url="https://zh.wikipedia.org/",
            ),
        }
        fi_sample_records = [
            {
                "wiki_domain": "fi.wikipedia.org",
                "namespace": 14,
                "wikidata_id": "Q1757",
                "site_url": "https://fi.wikipedia.org/",
                "page_url": "https://fi.wikipedia.org/wiki/Luokka:Turku",
                "page_title": "Luokka:Turku",
            }
        ]
        sv_sample_records = [
            {
                "wiki_domain": "sv.wikipedia.org",
                "namespace": 14,
                "wikidata_id": "Q42",
                "site_url": "https://sv.wikipedia.org/",
                "page_url": "https://sv.wikipedia.org/wiki/Kategori:Exempel",
                "page_title": "Kategori:Exempel",
            }
        ]
        load_sample_records_mock.side_effect = [
            ("api", "20260306000000", fi_sample_records),
            ("api", "20260306000000", sv_sample_records),
        ]
        fetch_wqs_article_uris_mock.side_effect = [
            {
                ("Q1757", "https://fi.wikipedia.org/"): ["https://fi.wikipedia.org/wiki/Luokka:Turku"],
            },
            {
                ("Q42", "https://sv.wikipedia.org/"): ["https://sv.wikipedia.org/wiki/Kategori:Annat"],
            },
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()

        with self.assertRaises(CommandError) as context:
            call_command(
                "check_newpages_wqs_uri_parity",
                stdout=stdout,
                stderr=stderr,
            )

        self.assertIn("WQS URI parity failed for wiki sv.wikipedia.org", str(context.exception))
        self.assertEqual(load_sample_records_mock.call_count, 2)
        self.assertNotIn("checking wiki=zh.wikipedia.org", stdout.getvalue())
        self.assertIn("checking wiki=fi.wikipedia.org", stdout.getvalue())
        self.assertIn("checking wiki=sv.wikipedia.org", stdout.getvalue())
