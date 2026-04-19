import json
from types import TracebackType
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.error import URLError

from django.test import SimpleTestCase

from newpages import service_source as newpages_source
from pagepile import service_source


class _FakeHttpResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


class PagepileServiceSourceTests(SimpleTestCase):
    def test_pagepile_lookup_backend_follows_global_wikidata_backend_setting(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            self.assertEqual(
                service_source.pagepile_lookup_backend(),
                service_source.LOOKUP_BACKEND_TOOLFORGE_SQL,
            )

        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            self.assertEqual(
                service_source.pagepile_lookup_backend(),
                service_source.LOOKUP_BACKEND_API,
            )

    def test_normalize_pagepile_id_requires_positive_integer(self) -> None:
        self.assertEqual(service_source.normalize_pagepile_id("112306"), 112306)

        with self.assertRaisesMessage(ValueError, "pagepile_id must be greater than zero."):
            service_source.normalize_pagepile_id("0")

    def test_normalize_load_limit_supports_blank_and_positive_values(self) -> None:
        self.assertIsNone(service_source.normalize_load_limit(""))
        self.assertEqual(service_source.normalize_load_limit("25"), 25)

        with self.assertRaisesMessage(ValueError, "limit must be greater than zero."):
            service_source.normalize_load_limit("0")

        with self.assertRaisesMessage(ValueError, "limit must be at most 300000."):
            service_source.normalize_load_limit("300001")

    def test_effective_load_limit_caps_api_backend_to_sample_size(self) -> None:
        with patch.object(service_source, "_MAX_PAGEPILE_API_SAMPLE_LIMIT", 2):
            with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
                self.assertEqual(service_source.effective_load_limit(5), 2)

        with patch.object(service_source, "_MAX_PAGEPILE_API_SAMPLE_LIMIT", 2):
            with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
                self.assertEqual(service_source.effective_load_limit(5), 5)

    def test_build_pagepile_json_url_includes_limit_when_present(self) -> None:
        self.assertEqual(
            service_source.build_pagepile_json_url(112306, limit=100),
            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit=&format=json&limit=100",
        )

    @patch("pagepile.service_source.urlopen")
    def test_fetch_pagepile_json_returns_payload_and_source_url(self, urlopen_mock: Any) -> None:
        urlopen_mock.return_value = _FakeHttpResponse(
            json.dumps(
                {
                    "id": 112306,
                    "wiki": "enwiki",
                    "pages": ["Example"],
                }
            ).encode("utf-8")
        )

        payload, source_url = service_source.fetch_pagepile_json(112306)

        self.assertEqual(payload["wiki"], "enwiki")
        self.assertEqual(
            source_url,
            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit=&format=json",
        )
        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.full_url, source_url)
        self.assertEqual(dict(request.header_items())["Accept"], "application/json")
        self.assertEqual(dict(request.header_items())["User-agent"], service_source.HTTP_USER_AGENT)

    @patch("pagepile.service_source.urlopen")
    def test_fetch_pagepile_json_forwards_limit_to_upstream_request(self, urlopen_mock: Any) -> None:
        urlopen_mock.return_value = _FakeHttpResponse(
            json.dumps(
                {
                    "id": 112306,
                    "wiki": "enwiki",
                    "pages": ["Example"],
                }
            ).encode("utf-8")
        )

        _payload, source_url = service_source.fetch_pagepile_json(112306, limit=100)

        self.assertEqual(
            source_url,
            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit=&format=json&limit=100",
        )
        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.full_url, source_url)

    @patch("pagepile.service_source.urlopen")
    def test_fetch_pagepile_json_sanitizes_transport_failures(self, urlopen_mock: Any) -> None:
        urlopen_mock.side_effect = URLError("temporary upstream outage")

        with self.assertRaisesMessage(
            service_source.PetscanServiceError,
            "Failed to fetch PagePile JSON data: <urlopen error temporary upstream outage>",
        ) as raised:
            service_source.fetch_pagepile_json(112306)

        self.assertEqual(
            getattr(raised.exception, "public_message", None),
            "Failed to load PagePile data from the upstream service.",
        )

    @patch("pagepile.service_source.urlopen")
    def test_fetch_pagepile_json_rejects_non_json_payload(self, urlopen_mock: Any) -> None:
        urlopen_mock.return_value = _FakeHttpResponse(b"<html>not json</html>")

        with self.assertRaisesMessage(
            service_source.PetscanServiceError,
            "Upstream service returned non-JSON payload.",
        ):
            service_source.fetch_pagepile_json(112306)

    @patch("pagepile.service_source.urlopen")
    def test_fetch_pagepile_json_rejects_non_object_payload(self, urlopen_mock: Any) -> None:
        urlopen_mock.return_value = _FakeHttpResponse(
            json.dumps(["not", "an", "object"]).encode("utf-8")
        )

        with self.assertRaisesMessage(
            service_source.PetscanServiceError,
            "Unexpected upstream API format (expected object).",
        ):
            service_source.fetch_pagepile_json(112306)

    @patch("pagepile.service_source._site_context_for_site")
    @patch("pagepile.service_source._fetch_page_rows_api")
    @patch("pagepile.service_source.fetch_pagepile_json")
    def test_fetch_pagepile_records_applies_limit_before_resolution(
        self,
        fetch_pagepile_json_mock: Any,
        fetch_page_rows_api_mock: Any,
        site_context_mock: Any,
    ) -> None:
        site_context_mock.return_value = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )
        fetch_pagepile_json_mock.return_value = (
            {
                "id": 112306,
                "wiki": "enwiki",
                "pages": ["Alpha", "Beta", "Gamma"],
            },
            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
        )
        fetch_page_rows_api_mock.return_value = {
            "Alpha": {
                "page_id": 11,
                "namespace": 0,
                "db_title": "Alpha",
                "wikidata_id": "Q1",
            },
            "Beta": {
                "page_id": 12,
                "namespace": 0,
                "db_title": "Beta",
                "wikidata_id": "Q2",
            },
        }

        records, source_url = service_source.fetch_pagepile_records(112306, limit=2)

        self.assertEqual(source_url, fetch_pagepile_json_mock.return_value[1])
        self.assertEqual([record["page_title"] for record in records], ["Alpha", "Beta"])
        fetch_pagepile_json_mock.assert_called_once_with(112306, limit=2)
        fetch_page_rows_api_mock.assert_called_once()
        self.assertEqual(fetch_page_rows_api_mock.call_args.args[1], ["Alpha", "Beta"])

    @patch("pagepile.service_source._site_context_for_site")
    @patch("pagepile.service_source._fetch_page_rows_api")
    @patch("pagepile.service_source.fetch_pagepile_json")
    def test_fetch_pagepile_records_applies_server_side_maximum_when_limit_missing(
        self,
        fetch_pagepile_json_mock: Any,
        fetch_page_rows_api_mock: Any,
        site_context_mock: Any,
    ) -> None:
        site_context_mock.return_value = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )
        fetch_pagepile_json_mock.return_value = (
            {
                "id": 112306,
                "wiki": "enwiki",
                "pages": ["Alpha", "Beta", "Gamma"],
            },
            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
        )
        fetch_page_rows_api_mock.return_value = {
            "Alpha": {
                "page_id": 11,
                "namespace": 0,
                "db_title": "Alpha",
                "wikidata_id": "Q1",
            },
            "Beta": {
                "page_id": 12,
                "namespace": 0,
                "db_title": "Beta",
                "wikidata_id": "Q2",
            },
        }

        with patch.object(service_source, "_MAX_PAGEPILE_API_SAMPLE_LIMIT", 2):
            records, source_url = service_source.fetch_pagepile_records(112306)

        self.assertEqual(source_url, fetch_pagepile_json_mock.return_value[1])
        self.assertEqual([record["page_title"] for record in records], ["Alpha", "Beta"])
        fetch_pagepile_json_mock.assert_called_once_with(112306, limit=2)
        fetch_page_rows_api_mock.assert_called_once()
        self.assertEqual(fetch_page_rows_api_mock.call_args.args[1], ["Alpha", "Beta"])

    @patch("pagepile.service_source._site_context_for_site")
    @patch("pagepile.service_source._fetch_page_rows_api")
    @patch("pagepile.service_source.fetch_pagepile_json")
    def test_fetch_pagepile_records_caps_api_limit_before_any_followup_work(
        self,
        fetch_pagepile_json_mock: Any,
        fetch_page_rows_api_mock: Any,
        site_context_mock: Any,
    ) -> None:
        site_context_mock.return_value = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )
        fetch_pagepile_json_mock.return_value = (
            {
                "id": 112306,
                "wiki": "enwiki",
                "pages": ["Alpha", "Beta", "Gamma"],
            },
            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit=&format=json&limit=2",
        )
        fetch_page_rows_api_mock.return_value = {
            "Alpha": {
                "page_id": 11,
                "namespace": 0,
                "db_title": "Alpha",
                "wikidata_id": "Q1",
            },
            "Beta": {
                "page_id": 12,
                "namespace": 0,
                "db_title": "Beta",
                "wikidata_id": "Q2",
            },
        }

        with patch.object(service_source, "_MAX_PAGEPILE_API_SAMPLE_LIMIT", 2):
            with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
                records, source_url = service_source.fetch_pagepile_records(112306, limit=5)

        self.assertEqual(source_url, fetch_pagepile_json_mock.return_value[1])
        self.assertEqual([record["page_title"] for record in records], ["Alpha", "Beta"])
        fetch_pagepile_json_mock.assert_called_once_with(112306, limit=2)
        fetch_page_rows_api_mock.assert_called_once()
        self.assertEqual(fetch_page_rows_api_mock.call_args.args[1], ["Alpha", "Beta"])

    @patch("pagepile.service_source._site_context_for_site")
    @patch("pagepile.service_source._fetch_page_rows_sql")
    @patch("pagepile.service_source._fetch_page_rows_api")
    @patch("pagepile.service_source.fetch_pagepile_json")
    def test_fetch_pagepile_records_uses_sql_backend_when_configured(
        self,
        fetch_pagepile_json_mock: Any,
        fetch_page_rows_api_mock: Any,
        fetch_page_rows_sql_mock: Any,
        site_context_mock: Any,
    ) -> None:
        site_context_mock.return_value = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )
        fetch_pagepile_json_mock.return_value = (
            {
                "id": 112306,
                "wiki": "enwiki",
                "pages": ["Alpha", "Beta"],
            },
            "https://pagepile.toolforge.org/api.php?id=112306&action=get_data&doit&format=json",
        )
        fetch_page_rows_sql_mock.return_value = {
            "Alpha": {
                "page_id": 11,
                "namespace": 0,
                "db_title": "Alpha",
                "wikidata_id": "Q1",
            },
            "Beta": {
                "page_id": 12,
                "namespace": 0,
                "db_title": "Beta",
                "wikidata_id": "Q2",
            },
        }

        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            records, source_url = service_source.fetch_pagepile_records(112306, limit=2)

        self.assertEqual(source_url, fetch_pagepile_json_mock.return_value[1])
        self.assertEqual([record["page_title"] for record in records], ["Alpha", "Beta"])
        fetch_pagepile_json_mock.assert_called_once_with(112306, limit=2)
        fetch_page_rows_sql_mock.assert_called_once()
        fetch_page_rows_api_mock.assert_not_called()

    @patch("pagepile.service_source._request_json")
    def test_fetch_page_rows_api_uses_incubator_sortkey_when_pageprops_missing(
        self,
        request_json_mock: Any,
    ) -> None:
        request_json_mock.return_value = {
            "query": {
                "pages": [
                    {
                        "pageid": 11,
                        "ns": 0,
                        "title": "Wp/sms/Katja_Gauriloff",
                        "categories": [
                            {
                                "title": "Category:Maintenance:Wikidata_interwiki_links",
                                "sortkeyprefix": "Q138849357",
                            }
                        ],
                    }
                ]
            }
        }
        site_context = service_source._SiteContext(
            site="incubatorwiki",
            domain="incubator.wikimedia.org",
            dbname="incubatorwiki",
            lang_code="en",
            site_url="https://incubator.wikimedia.org/",
            wiki_group="wikimedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        rows = service_source._fetch_page_rows_api(site_context, ["Wp/sms/Katja_Gauriloff"])

        self.assertEqual(
            rows["Wp/sms/Katja_Gauriloff"]["wikidata_id"],
            "Q138849357",
        )
        request_url = request_json_mock.call_args.args[0]
        self.assertIn("clcategories=Category%3AMaintenance%3AWikidata_interwiki_links", request_url)
        self.assertIn("clprop=sortkey", request_url)

    @patch("pagepile.service_source._request_json")
    def test_fetch_page_rows_api_keeps_pages_without_wikidata_id(
        self,
        request_json_mock: Any,
    ) -> None:
        request_json_mock.return_value = {
            "query": {
                "pages": [
                    {
                        "pageid": 21,
                        "ns": 0,
                        "title": "No_qid_page",
                    }
                ]
            }
        }
        site_context = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        rows = service_source._fetch_page_rows_api(site_context, ["No_qid_page"])

        self.assertEqual(rows["No_qid_page"]["page_id"], 21)
        self.assertIsNone(rows["No_qid_page"]["wikidata_id"])

    @patch("pagepile.service_source._request_json")
    def test_fetch_page_rows_api_resolves_normalized_title_alias(
        self,
        request_json_mock: Any,
    ) -> None:
        request_json_mock.return_value = {
            "query": {
                "normalized": [
                    {
                        "from": "old_title",
                        "to": "Old_title",
                    }
                ],
                "pages": [
                    {
                        "pageid": 22,
                        "ns": 0,
                        "title": "Old_title",
                        "pageprops": {
                            "wikibase_item": "Q22",
                        },
                    }
                ],
            }
        }
        site_context = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        rows = service_source._fetch_page_rows_api(site_context, ["old_title"])

        self.assertEqual(rows["old_title"]["page_id"], 22)
        self.assertEqual(rows["old_title"]["db_title"], "Old_title")
        self.assertEqual(rows["old_title"]["wikidata_id"], "Q22")

    @patch("pagepile.service_source._request_json")
    def test_fetch_page_rows_api_resolves_redirect_target_back_to_requested_title(
        self,
        request_json_mock: Any,
    ) -> None:
        request_json_mock.return_value = {
            "query": {
                "normalized": [
                    {
                        "from": "old_title",
                        "to": "Old_title",
                    }
                ],
                "redirects": [
                    {
                        "from": "Old_title",
                        "to": "Target_title",
                    }
                ],
                "pages": [
                    {
                        "pageid": 23,
                        "ns": 0,
                        "title": "Target_title",
                        "pageprops": {
                            "wikibase_item": "Q23",
                        },
                    }
                ],
            }
        }
        site_context = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        rows = service_source._fetch_page_rows_api(site_context, ["old_title"])

        self.assertEqual(rows["old_title"]["page_id"], 23)
        self.assertEqual(rows["old_title"]["db_title"], "Target_title")
        self.assertEqual(rows["old_title"]["wikidata_id"], "Q23")

    @patch("pagepile.service_source._request_json")
    def test_fetch_page_rows_api_ignores_missing_pages_without_breaking_other_rows(
        self,
        request_json_mock: Any,
    ) -> None:
        request_json_mock.return_value = {
            "query": {
                "pages": [
                    {
                        "ns": 0,
                        "title": "Missing_page",
                        "missing": True,
                    },
                    {
                        "pageid": 24,
                        "ns": 0,
                        "title": "Present_page",
                        "pageprops": {
                            "wikibase_item": "Q24",
                        },
                    },
                ]
            }
        }
        site_context = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        rows = service_source._fetch_page_rows_api(site_context, ["Missing_page", "Present_page"])

        self.assertNotIn("Missing_page", rows)
        self.assertEqual(rows["Present_page"]["page_id"], 24)
        self.assertEqual(rows["Present_page"]["wikidata_id"], "Q24")

    def test_fetch_page_rows_sql_keeps_pages_without_wikidata_id(self) -> None:
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [
            (0, "No_qid_page", 31, None),
        ]
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor

        fake_pymysql = MagicMock()
        fake_pymysql.connect.return_value = fake_connection

        site_context = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        with patch("pagepile.service_source.pymysql", fake_pymysql):
            rows = service_source._fetch_page_rows_sql(site_context, ["No_qid_page"])

        self.assertEqual(rows["No_qid_page"]["page_id"], 31)
        self.assertIsNone(rows["No_qid_page"]["wikidata_id"])

    def test_fetch_page_rows_sql_uses_incubator_sortkey_when_pageprops_missing(self) -> None:
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [
            (0, "Wp/sms/Katja_Gauriloff", 32, "Q138849357"),
        ]
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor

        fake_pymysql = MagicMock()
        fake_pymysql.connect.return_value = fake_connection

        site_context = service_source._SiteContext(
            site="incubatorwiki",
            domain="incubator.wikimedia.org",
            dbname="incubatorwiki",
            lang_code="en",
            site_url="https://incubator.wikimedia.org/",
            wiki_group="wikimedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        with patch("pagepile.service_source.pymysql", fake_pymysql):
            rows = service_source._fetch_page_rows_sql(site_context, ["Wp/sms/Katja_Gauriloff"])

        self.assertEqual(rows["Wp/sms/Katja_Gauriloff"]["page_id"], 32)
        self.assertEqual(rows["Wp/sms/Katja_Gauriloff"]["wikidata_id"], "Q138849357")
        executed_sql, executed_params = fake_cursor.execute.call_args.args
        self.assertIn("COALESCE(pp.pp_value, cl.cl_sortkey_prefix)", executed_sql)
        self.assertIn("LEFT JOIN categorylinks AS cl", executed_sql)
        self.assertEqual(executed_params[0], "wikibase_item")
        self.assertEqual(
            executed_params[1],
            service_source._INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE,
        )

    def test_build_record_uses_incubator_root_site_url_for_is_part_of(self) -> None:
        site_context = service_source._SiteContext(
            site="incubatorwiki",
            domain="incubator.wikimedia.org",
            dbname="incubatorwiki",
            lang_code="en",
            site_url="https://incubator.wikimedia.org/wiki/Wp/sms/",
            wiki_group="wikimedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        record = service_source._build_record(
            site_context,
            {
                "page_id": 11,
                "namespace": 0,
                "db_title": "Wp/sms/Katja_Gauriloff",
                "wikidata_id": "Q138849357",
            },
        )

        assert record is not None
        self.assertEqual(record["site_url"], "https://incubator.wikimedia.org/")
        self.assertEqual(record["wiki_group"], "wikipedia")
        self.assertEqual(record["lang_code"], "sms")

    def test_build_record_keeps_page_without_wikidata_id(self) -> None:
        site_context = service_source._SiteContext(
            site="enwiki",
            domain="en.wikipedia.org",
            dbname="enwiki",
            lang_code="en",
            site_url="https://en.wikipedia.org/",
            wiki_group="wikipedia",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 14: "Category"},
                namespace_aliases={0: ("",), 14: ("Category",)},
            ),
        )

        record = service_source._build_record(
            site_context,
            {
                "page_id": 41,
                "namespace": 0,
                "db_title": "No_qid_page",
                "wikidata_id": None,
            },
        )

        assert record is not None
        self.assertEqual(record["page_title"], "No_qid_page")
        self.assertNotIn("wikidata_id", record)
        self.assertNotIn("wikidata_entity", record)

    def test_build_record_adds_mediaitem_entity_for_commons_file_pages(self) -> None:
        site_context = service_source._SiteContext(
            site="commonswiki",
            domain="commons.wikimedia.org",
            dbname="commonswiki",
            lang_code="en",
            site_url="https://commons.wikimedia.org/",
            wiki_group="commons",
            siteinfo=newpages_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 6: "File", 14: "Category"},
                namespace_aliases={0: ("",), 6: ("File",), 14: ("Category",)},
            ),
        )

        record = service_source._build_record(
            site_context,
            {
                "page_id": 574781,
                "namespace": 6,
                "db_title": "Example.jpg",
                "wikidata_id": None,
            },
        )

        assert record is not None
        self.assertEqual(record["page_title"], "File:Example.jpg")
        self.assertNotIn("wikidata_id", record)
        self.assertEqual(
            record["wikidata_entity"],
            "https://commons.wikimedia.org/entity/M574781",
        )
