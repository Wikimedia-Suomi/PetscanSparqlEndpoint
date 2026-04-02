import json
import os
from datetime import datetime, timezone
from types import TracebackType
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from incubator import service_source


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


class IncubatorServiceSourceTests(SimpleTestCase):
    def test_incubator_lookup_backend_follows_global_wikidata_backend_setting(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            self.assertEqual(
                service_source.incubator_lookup_backend(),
                service_source.LOOKUP_BACKEND_TOOLFORGE_SQL,
            )

        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            self.assertEqual(
                service_source.incubator_lookup_backend(),
                service_source.LOOKUP_BACKEND_API,
            )

    def test_incubator_lookup_backend_uses_global_replica_flag_when_backend_is_not_explicit(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="", TOOLFORGE_USE_REPLICA=True):
            self.assertEqual(
                service_source.incubator_lookup_backend(),
                service_source.LOOKUP_BACKEND_TOOLFORGE_SQL,
            )

    def test_normalize_load_limit_supports_blank_and_positive_values(self) -> None:
        self.assertIsNone(service_source.normalize_load_limit(""))
        self.assertEqual(service_source.normalize_load_limit("25"), 25)

        with self.assertRaisesMessage(ValueError, "limit must be greater than zero."):
            service_source.normalize_load_limit("0")

    def test_normalize_page_latest_supports_blank_and_positive_values(self) -> None:
        self.assertIsNone(service_source.normalize_page_latest(""))
        self.assertEqual(service_source.normalize_page_latest("123456789"), 123456789)

        with self.assertRaisesMessage(ValueError, "page_latest must be greater than zero."):
            service_source.normalize_page_latest("0")

    def test_normalize_page_prefixes_supports_commas_and_ellipsis_suffix(self) -> None:
        self.assertEqual(
            service_source.normalize_page_prefixes(" Wt/sms/..., Wp/sms/Kat , , "),
            ["Wp/sms/Kat", "Wt/sms/"],
        )

    def test_available_incubator_namespace_options_uses_configured_setting(self) -> None:
        with self.settings(
            INCUBATOR_NAMESPACE_OPTIONS=(
                {"id": 0, "label": "Main"},
                {"id": 14, "label": "Category"},
            )
        ):
            self.assertEqual(
                service_source.available_incubator_namespace_options(),
                [
                    {"id": 0, "label": "Main"},
                    {"id": 14, "label": "Category"},
                ],
            )

    def test_normalize_namespaces_supports_commas_and_deduplicates(self) -> None:
        self.assertEqual(service_source.normalize_namespaces("14, 0, 999, 14"), [0, 14, 999])

    def test_normalize_namespaces_rejects_negative_namespace(self) -> None:
        with self.assertRaisesMessage(
            ValueError,
            "namespace must be zero or a positive integer.",
        ):
            service_source.normalize_namespaces("-1")

    def test_normalize_page_prefixes_rejects_more_than_ten_values(self) -> None:
        with self.assertRaisesMessage(
            ValueError,
            "page_prefix supports at most 10 comma-separated values.",
        ):
            service_source.normalize_page_prefixes(
                ",".join("Wp/sms/{}".format(index) for index in range(11))
            )

    def test_normalize_page_prefixes_rejects_values_longer_than_ten_characters(self) -> None:
        with self.assertRaisesMessage(
            ValueError,
            "page_prefix values must be at most 10 characters long.",
        ):
            service_source.normalize_page_prefixes("Wp/sms/1234")

    def test_fetch_incubator_records_via_api_stops_at_limit_and_normalizes_fields(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("incubator.service_source.urlopen") as urlopen_mock:
                urlopen_mock.return_value = _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "categorymembers": [
                                    {
                                        "pageid": 11,
                                        "title": "Wp/sms/Katja_Gauriloff",
                                        "sortkeyprefix": "Q138849357",
                                    },
                                    {
                                        "pageid": 12,
                                        "title": "Wp/sms/Vanha_sivu",
                                        "sortkeyprefix": "Q2",
                                    },
                                ]
                            },
                            "continue": {"cmcontinue": "page|123"},
                        }
                    ).encode("utf-8")
                )

                records, source_url = service_source.fetch_incubator_records(limit=2)

                self.assertEqual(source_url, service_source.build_incubator_category_url())
                self.assertEqual(
                    [record["page_title"] for record in records],
                    ["Wp/sms/Katja_Gauriloff", "Wp/sms/Vanha_sivu"],
                )
                self.assertNotIn("title", records[0])
                self.assertEqual(records[0]["wiki_group"], "wikipedia")
                self.assertEqual(records[0]["lang_code"], "sms")
                self.assertEqual(records[0]["page_name"], "Katja_Gauriloff")
                self.assertEqual(records[0]["page_label"], "Katja Gauriloff")
                self.assertNotIn("project_name", records[0])
                self.assertEqual(
                    records[0]["incubator_url"],
                    "https://incubator.wikimedia.org/wiki/Wp/sms/Katja_Gauriloff",
                )
                self.assertEqual(
                    records[0]["site_url"],
                    "https://incubator.wikimedia.org/wiki/Wp/sms/",
                )
                self.assertEqual(records[0]["wikidata_id"], "Q138849357")
                self.assertEqual(
                    records[0]["wikidata_entity"],
                    "http://www.wikidata.org/entity/Q138849357",
                )
                self.assertNotIn("namespace", records[0])
                self.assertNotIn("page_latest", records[0])

                request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
                self.assertIn("list=categorymembers", request_urls[0])
                self.assertIn("cmprop=ids%7Ctitle%7Csortkeyprefix", request_urls[0])
                self.assertNotIn("cmtype=page", request_urls[0])
                self.assertIn("cmlimit=2", request_urls[0])
                self.assertEqual(len(request_urls), 1)

    def test_fetch_incubator_records_via_api_stops_after_first_batch_when_limit_is_satisfied(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("incubator.service_source.urlopen") as urlopen_mock:
                urlopen_mock.return_value = _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "categorymembers": [
                                    {
                                        "pageid": 11,
                                        "title": "Wp/sms/Katja_Gauriloff",
                                        "sortkeyprefix": "Q138849357",
                                    },
                                    {
                                        "pageid": 13,
                                        "title": "Wp/sms/Uusi_sivu",
                                        "sortkeyprefix": "Q3",
                                    },
                                ]
                            },
                            "continue": {"cmcontinue": "page|999"},
                        }
                    ).encode("utf-8")
                )

                records, source_url = service_source.fetch_incubator_records(limit=2)

                self.assertEqual(source_url, service_source.build_incubator_category_url())
                self.assertEqual(len(records), 2)
                self.assertEqual(urlopen_mock.call_count, 1)
                self.assertEqual(
                    [record["page_title"] for record in records],
                    ["Wp/sms/Katja_Gauriloff", "Wp/sms/Uusi_sivu"],
                )

                request_url = urlopen_mock.call_args.args[0].full_url
                self.assertIn("list=categorymembers", request_url)
                self.assertIn("cmprop=ids%7Ctitle%7Csortkeyprefix", request_url)
                self.assertNotIn("cmtype=page", request_url)
                self.assertIn("cmlimit=2", request_url)

    def test_fetch_incubator_records_via_api_recentchanges_uses_timestamp_sorting(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("incubator.service_source._recentchanges_cutoff") as cutoff_mock:
                with patch("incubator.service_source.urlopen") as urlopen_mock:
                    cutoff_mock.return_value = datetime(2026, 3, 1, tzinfo=timezone.utc)
                    urlopen_mock.return_value = _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "categorymembers": [
                                        {
                                            "pageid": 11,
                                            "title": "Wp/sms/Katja_Gauriloff",
                                            "sortkeyprefix": "Q138849357",
                                            "timestamp": "2026-03-25T12:00:00Z",
                                        },
                                        {
                                            "pageid": 13,
                                            "title": "Wp/sms/Uusi_sivu",
                                            "sortkeyprefix": "Q3",
                                            "timestamp": "2026-03-20T09:30:00Z",
                                        },
                                    ]
                                },
                                "continue": {"cmcontinue": "page|999"},
                            }
                        ).encode("utf-8")
                    )

                    records, source_url = service_source.fetch_incubator_records(
                        limit=2,
                        namespaces=[0, 14],
                        recentchanges_only=True,
                    )

                    self.assertEqual(source_url, service_source.build_incubator_category_url())
                    self.assertEqual(
                        [record["page_title"] for record in records],
                        ["Wp/sms/Katja_Gauriloff", "Wp/sms/Uusi_sivu"],
                    )
                    request_url = urlopen_mock.call_args.args[0].full_url
                    self.assertIn("cmprop=ids%7Ctitle%7Csortkeyprefix%7Ctimestamp", request_url)
                    self.assertIn("cmnamespace=0%7C14", request_url)
                    self.assertIn("cmsort=timestamp", request_url)
                    self.assertIn("cmdir=desc", request_url)

    def test_fetch_incubator_records_via_api_recentchanges_stops_at_30_day_cutoff(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("incubator.service_source._recentchanges_cutoff") as cutoff_mock:
                with patch("incubator.service_source.urlopen") as urlopen_mock:
                    cutoff_mock.return_value = datetime(2026, 3, 1, tzinfo=timezone.utc)
                    urlopen_mock.side_effect = [
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "categorymembers": [
                                            {
                                                "pageid": 11,
                                                "title": "Wp/sms/Katja_Gauriloff",
                                                "sortkeyprefix": "Q138849357",
                                                "timestamp": "2026-03-25T12:00:00Z",
                                            },
                                            {
                                                "pageid": 13,
                                                "title": "Wp/sms/Uusi_sivu",
                                                "sortkeyprefix": "Q3",
                                                "timestamp": "2026-03-15T09:30:00Z",
                                            },
                                        ]
                                    },
                                    "continue": {"cmcontinue": "page|999"},
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "categorymembers": [
                                            {
                                                "pageid": 15,
                                                "title": "Wp/sms/Liian_vanha",
                                                "sortkeyprefix": "Q5",
                                                "timestamp": "2026-02-01T00:00:00Z",
                                            }
                                        ]
                                    },
                                    "continue": {"cmcontinue": "page|1000"},
                                }
                            ).encode("utf-8")
                        ),
                    ]

                    records, source_url = service_source.fetch_incubator_records(
                        recentchanges_only=True
                    )

                    self.assertEqual(source_url, service_source.build_incubator_category_url())
                    self.assertEqual(
                        [record["page_title"] for record in records],
                        ["Wp/sms/Katja_Gauriloff", "Wp/sms/Uusi_sivu"],
                    )
                    self.assertEqual(urlopen_mock.call_count, 2)

    def test_fetch_incubator_records_via_api_stops_after_consecutive_empty_batches(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch.object(
                service_source,
                "_MAX_INCUBATOR_API_CONSECUTIVE_EMPTY_BATCHES",
                2,
            ):
                with patch("incubator.service_source.urlopen") as urlopen_mock:
                    urlopen_mock.side_effect = [
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {"categorymembers": []},
                                    "continue": {"cmcontinue": "page|one"},
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {"categorymembers": []},
                                    "continue": {"cmcontinue": "page|two"},
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "categorymembers": [
                                            {
                                                "pageid": 11,
                                                "title": "Wp/sms/Katja_Gauriloff",
                                                "sortkeyprefix": "Q138849357",
                                            }
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                    ]

                    records, source_url = service_source.fetch_incubator_records(
                        limit=10,
                        namespaces=[4],
                    )

                    self.assertEqual(source_url, service_source.build_incubator_category_url())
                    self.assertEqual(records, [])
                    self.assertEqual(urlopen_mock.call_count, 2)

    def test_fetch_incubator_records_via_api_stops_on_repeated_continuation_token(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("incubator.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {"categorymembers": []},
                                "continue": {"cmcontinue": "page|same"},
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {"categorymembers": []},
                                "continue": {"cmcontinue": "page|same"},
                            }
                        ).encode("utf-8")
                    ),
                ]

                records, source_url = service_source.fetch_incubator_records(limit=10)

                self.assertEqual(source_url, service_source.build_incubator_category_url())
                self.assertEqual(records, [])
                self.assertEqual(urlopen_mock.call_count, 2)

    def test_fetch_incubator_records_rejects_page_latest_filter_in_api_mode(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with self.assertRaisesMessage(
                ValueError,
                "page_latest filter is only available when using the Toolforge replica backend.",
            ):
                service_source.fetch_incubator_records(page_latest=123456789)

    def test_fetch_incubator_records_rejects_page_prefix_filter_in_api_mode(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with self.assertRaisesMessage(
                ValueError,
                "page_prefix filter is only available when using the Toolforge replica backend.",
            ):
                service_source.fetch_incubator_records(page_prefixes=["Wp/sms/"])

    def test_fetch_incubator_records_via_replica_uses_configured_cnf(self) -> None:
        with self.settings(
            WIKIDATA_LOOKUP_BACKEND="toolforge_sql",
            TOOLFORGE_REPLICA_CNF="$HOME/replica.my.cnf",
        ):
            with patch("incubator.service_source.pymysql") as pymysql_mock:
                cursor = MagicMock()
                cursor.fetchall.return_value = [
                    (b"Wp/sms/Uusi_sivu", "Q3"),
                    (b"Wp/sms/Katja_Gauriloff", "Q138849357"),
                ]

                connection = MagicMock()
                cursor_cm = MagicMock()
                cursor_cm.__enter__.return_value = cursor
                cursor_cm.__exit__.return_value = None
                connection.cursor.return_value = cursor_cm
                pymysql_mock.connect.return_value = connection

                records, source_url = service_source.fetch_incubator_records(
                    limit=2,
                    namespaces=[0, 14],
                    page_latest=123456789,
                    page_prefixes=["Wt/fi_", "Wp/sms/..."],
                )

                self.assertEqual(source_url, service_source.build_incubator_category_url())
                self.assertEqual(
                    [record["page_title"] for record in records],
                    ["Wp/sms/Uusi_sivu", "Wp/sms/Katja_Gauriloff"],
                )
                self.assertNotIn("title", records[0])
                self.assertNotIn("namespace", records[0])
                self.assertEqual(records[0]["wikidata_id"], "Q3")
                self.assertEqual(records[1]["wikidata_id"], "Q138849357")

                connect_kwargs = pymysql_mock.connect.call_args.kwargs
                self.assertEqual(connect_kwargs.get("host"), "incubatorwiki.web.db.svc.wikimedia.cloud")
                self.assertEqual(connect_kwargs.get("database"), "incubatorwiki_p")
                self.assertEqual(
                    connect_kwargs.get("read_default_file"),
                    os.path.expanduser(os.path.expandvars("$HOME/replica.my.cnf")),
                )

                sql, params = cursor.execute.call_args.args
                self.assertIn("FROM page AS p", sql)
                self.assertIn("p.page_namespace IN (%s, %s)", sql)
                self.assertIn("p.page_latest >= %s", sql)
                self.assertIn("p.page_title LIKE %s ESCAPE '\\\\'", sql)
                self.assertIn("OR p.page_title LIKE %s ESCAPE '\\\\'", sql)
                self.assertIn("cl.cl_sortkey_prefix", sql)
                self.assertIn("LIMIT %s", sql)
                self.assertEqual(
                    params,
                    [
                        "Maintenance:Wikidata_interwiki_links",
                        0,
                        14,
                        123456789,
                        "Wp/sms/%",
                        "Wt/fi\\_%",
                        2,
                    ],
                )

    def test_fetch_incubator_records_via_recentchanges_replica_uses_recentchanges_table(self) -> None:
        with self.settings(
            WIKIDATA_LOOKUP_BACKEND="toolforge_sql",
            TOOLFORGE_REPLICA_CNF="$HOME/replica.my.cnf",
        ):
            with patch("incubator.service_source.pymysql") as pymysql_mock:
                cursor = MagicMock()
                cursor.fetchall.return_value = [
                    (b"Wp/sms/Uusi_sivu", "Q3"),
                    (b"Wp/sms/Katja_Gauriloff", "Q138849357"),
                ]

                connection = MagicMock()
                cursor_cm = MagicMock()
                cursor_cm.__enter__.return_value = cursor
                cursor_cm.__exit__.return_value = None
                connection.cursor.return_value = cursor_cm
                pymysql_mock.connect.return_value = connection

                records, source_url = service_source.fetch_incubator_records(
                    limit=2,
                    namespaces=[0, 14],
                    page_latest=123456789,
                    page_prefixes=["Wt/fi_", "Wp/sms/..."],
                    recentchanges_only=True,
                )

                self.assertEqual(source_url, service_source.build_incubator_category_url())
                self.assertEqual(
                    [record["page_title"] for record in records],
                    ["Wp/sms/Uusi_sivu", "Wp/sms/Katja_Gauriloff"],
                )

                sql, params = cursor.execute.call_args.args
                self.assertIn("SELECT latest_rc.rc_title, cl.cl_sortkey_prefix", sql)
                self.assertIn("FROM (SELECT rc.rc_cur_id, MAX(rc.rc_id) AS latest_rc_id", sql)
                self.assertIn("GROUP BY rc.rc_cur_id", sql)
                self.assertIn("JOIN recentchanges AS latest_rc", sql)
                self.assertIn("latest_rc.rc_id = latest_per_page.latest_rc_id", sql)
                self.assertNotIn("JOIN page AS p", sql)
                self.assertIn("rc.rc_source = %s", sql)
                self.assertIn("rc.rc_log_type = %s", sql)
                self.assertIn("OR (rc.rc_source = %s AND rc.rc_log_type = %s)", sql)
                self.assertIn("latest_rc.rc_namespace IN (%s, %s)", sql)
                self.assertNotIn("rc.rc_namespace = 0", sql)
                self.assertIn("AND latest_rc.rc_this_oldid >= %s", sql)
                self.assertIn("latest_rc.rc_title LIKE %s ESCAPE '\\\\'", sql)
                self.assertIn("OR latest_rc.rc_title LIKE %s ESCAPE '\\\\'", sql)
                self.assertIn("ORDER BY latest_per_page.latest_rc_id DESC", sql)
                self.assertIn("LIMIT %s", sql)
                self.assertEqual(
                    params,
                    [
                        "mw.edit",
                        "mw.log",
                        "move",
                        "Maintenance:Wikidata_interwiki_links",
                        0,
                        14,
                        123456789,
                        "Wp/sms/%",
                        "Wt/fi\\_%",
                        2,
                    ],
                )
