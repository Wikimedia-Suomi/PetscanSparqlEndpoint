import json
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from newpages import service_source


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


def _fake_connection(rows: list[tuple[object, ...]]) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchall.return_value = rows

    connection = MagicMock()
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = None
    connection.cursor.return_value = cursor_cm
    return connection


def _fake_connection_batches(row_batches: list[list[tuple[object, ...]]]) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchall.side_effect = row_batches

    connection = MagicMock()
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = None
    connection.cursor.return_value = cursor_cm
    return connection


class NewpagesServiceSourceTests(SimpleTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._original_siteinfo_snapshot_path = service_source._SITEINFO_SNAPSHOT_PATH
        test_snapshot_path = Path(__file__).resolve().with_name("_missing_siteinfo_snapshot.json")
        if test_snapshot_path.exists():
            test_snapshot_path.unlink()
        service_source._SITEINFO_SNAPSHOT_PATH = test_snapshot_path
        service_source._known_wikis_by_domain.cache_clear()
        service_source._known_wiki_domains_by_token.cache_clear()
        service_source._user_list_source_domain_for_prefix.cache_clear()
        service_source._siteinfo_snapshot_by_domain.cache_clear()
        service_source._siteinfo_for_domain.cache_clear()
        service_source._centralauth_user_summary.cache_clear()
        service_source._active_user_wiki_dbnames_for_user.cache_clear()
        service_source._quote_page_path.cache_clear()

    def tearDown(self) -> None:
        service_source._SITEINFO_SNAPSHOT_PATH = self._original_siteinfo_snapshot_path
        service_source._known_wiki_domains_by_token.cache_clear()
        service_source._siteinfo_snapshot_by_domain.cache_clear()
        service_source._siteinfo_for_domain.cache_clear()
        super().tearDown()

    def test_newpages_lookup_backend_follows_explicit_setting(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            self.assertEqual(
                service_source.newpages_lookup_backend(),
                service_source.LOOKUP_BACKEND_TOOLFORGE_SQL,
            )

        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            self.assertEqual(
                service_source.newpages_lookup_backend(),
                service_source.LOOKUP_BACKEND_API,
            )

    def test_newpages_lookup_backend_uses_replica_flag_when_backend_is_not_explicit(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="", TOOLFORGE_USE_REPLICA=True):
            self.assertEqual(
                service_source.newpages_lookup_backend(),
                service_source.LOOKUP_BACKEND_TOOLFORGE_SQL,
            )

    def test_normalize_timestamp_supports_prefix_padding(self) -> None:
        self.assertIsNone(service_source.normalize_timestamp(""))
        self.assertEqual(service_source.normalize_timestamp("202604"), "20260400000000")
        self.assertEqual(service_source.normalize_timestamp("2026040301"), "20260403010000")

        with self.assertRaisesMessage(
            ValueError,
            "timestamp must use YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHH, YYYYMMDDHHMM, or YYYYMMDDHHMMSS.",
        ):
            service_source.normalize_timestamp("20260")

    def test_recent_user_activity_threshold_uses_60_day_floor(self) -> None:
        fake_now = datetime(2026, 4, 6, 0, 0, 0, tzinfo=timezone.utc)
        with patch("newpages.service_source.datetime") as datetime_mock:
            datetime_mock.now.return_value = fake_now

            self.assertEqual(service_source._recent_user_activity_threshold(None), "20260205000000")
            self.assertEqual(
                service_source._recent_user_activity_threshold("20260201000000"),
                "20260205000000",
            )
            self.assertEqual(
                service_source._recent_user_activity_threshold("20260301000000"),
                "20260301000000",
            )

    def test_filter_user_names_for_recent_activity_sql_uses_actor_revision(self) -> None:
        descriptor = service_source._WikiDescriptor(
            domain="fi.wikipedia.org",
            dbname="fiwiki",
            lang_code="fi",
            wiki_group="wikipedia",
            site_url="https://fi.wikipedia.org",
        )
        with patch("newpages.service_source.pymysql") as pymysql_mock:
            connection = _fake_connection([("Alice A",)])
            pymysql_mock.connect.return_value = connection

            user_names = service_source._filter_user_names_for_recent_activity_sql(
                descriptor,
                ["Alice A", "Bob B"],
                "20260401000000",
            )

        self.assertEqual(user_names, ["Alice A"])
        sql, params = connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("FROM actor_revision AS a", sql)
        self.assertNotIn("FROM actor AS a ", sql)
        self.assertIn("JOIN revision_userindex AS r", sql)
        self.assertIn("a.actor_user = u.user_id", sql)
        self.assertEqual(params, ["Alice A", "Bob B", "20260401000000"])

    def test_siteinfo_for_domain_prefers_local_snapshot(self) -> None:
        snapshot_siteinfo = service_source._SiteInfo(
            article_path="/w/$1",
            lang_code="fi",
            namespace_names={0: "", 2: "Käyttäjä"},
            namespace_aliases={2: ("Käyttäjä", "User")},
        )
        with patch(
            "newpages.service_source._siteinfo_snapshot_by_domain",
            return_value={"fi.wikipedia.org": snapshot_siteinfo},
        ):
            with patch("newpages.service_source._fetch_siteinfo_from_api") as fetch_siteinfo_mock:
                siteinfo = service_source._siteinfo_for_domain("fi.wikipedia.org")

        self.assertEqual(siteinfo, snapshot_siteinfo)
        fetch_siteinfo_mock.assert_not_called()

    def test_siteinfo_for_domain_falls_back_to_api_when_snapshot_missing(self) -> None:
        api_siteinfo = service_source._SiteInfo(
            article_path="/wiki/$1",
            lang_code="sv",
            namespace_names={0: "", 2: "Användare"},
            namespace_aliases={2: ("Användare", "User")},
        )
        with patch("newpages.service_source._siteinfo_snapshot_by_domain", return_value={}):
            with patch("newpages.service_source._fetch_siteinfo_from_api", return_value=api_siteinfo) as fetch_siteinfo_mock:
                siteinfo = service_source._siteinfo_for_domain("sv.wikipedia.org")

        self.assertEqual(siteinfo, api_siteinfo)
        fetch_siteinfo_mock.assert_called_once_with("sv.wikipedia.org")

    def test_fetch_newpage_records_rejects_include_edited_pages_without_user_list_page(self) -> None:
        with self.assertRaisesMessage(ValueError, "include_edited_pages requires user_list_page."):
            service_source.fetch_newpage_records(
                wiki_domains=["fi.wikipedia.org"],
                timestamp="20260401000000",
                include_edited_pages=True,
            )

    def test_fetch_newpage_records_rejects_include_edited_pages_without_recent_enough_timestamp(self) -> None:
        with self.assertRaisesMessage(
            ValueError,
            "timestamp must be within the last 60 days when include_edited_pages is enabled.",
        ):
            service_source.fetch_newpage_records(
                wiki_domains=["fi.wikipedia.org"],
                timestamp="20240101000000",
                user_list_page=":w:fi:Wikipedia:Users",
                include_edited_pages=True,
            )

    def test_fetch_newpage_records_accepts_partial_timestamp_in_include_edited_pages_mode(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source._centralauth_user_exists", return_value=True):
                with patch("newpages.service_source.urlopen") as urlopen_mock:
                    urlopen_mock.side_effect = [
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "sitematrix": {
                                        "count": 1,
                                        "0": {
                                            "code": "fi",
                                            "site": [
                                                {
                                                    "url": "https://fi.wikipedia.org",
                                                    "dbname": "fiwiki",
                                                    "code": "wiki",
                                                }
                                            ],
                                        },
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                        "namespaces": {
                                            "0": {"*": ""},
                                            "2": {"*": "Käyttäjä", "canonical": "User"},
                                        },
                                        "namespacealiases": [{"id": 2, "*": "User"}],
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "pages": [
                                            {
                                                "pageid": 901,
                                                "links": [{"title": "Käyttäjä:Alice_A"}],
                                                "iwlinks": [],
                                            }
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "usercontribs": []
                                    }
                                }
                            ).encode("utf-8")
                        ),
                    ]

                    records, source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        timestamp="202604",
                        user_list_page=":w:fi:Wikipedia:Users",
                        include_edited_pages=True,
                    )

        self.assertEqual(records, [])
        self.assertEqual(source_url, "https://fi.wikipedia.org/wiki/Special:Contributions")
        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("ucend=2026-04-01T00%3A00%3A00Z", request_urls[3])

    def test_normalize_wikis_supports_commas_and_deduplicates(self) -> None:
        self.assertEqual(
            service_source.normalize_wikis(" fi.wikipedia.org,sv.wikipedia.org, fi.wikipedia.org "),
            ["fi", "sv"],
        )

    def test_normalize_wikis_accepts_short_forms_and_defaults_bare_codes_to_wikipedia(self) -> None:
        self.assertEqual(
            service_source.normalize_wikis("w:fi,w:se,b:fi,incubator,commons,meta,wikidata,smn,olo"),
            ["b:fi", "commons", "fi", "incubator", "meta", "olo", "se", "smn", "wikidata"],
        )

    def test_normalize_wikis_expands_wildcards_via_sitematrix(self) -> None:
        with patch("newpages.service_source.urlopen") as urlopen_mock:
            urlopen_mock.return_value = _FakeHttpResponse(
                json.dumps(
                    {
                        "sitematrix": {
                            "count": 4,
                            "0": {
                                "code": "fi",
                                "site": [
                                    {
                                        "url": "https://fi.wikipedia.org",
                                        "dbname": "fiwiki",
                                        "code": "wiki",
                                    },
                                    {
                                        "url": "https://fi.wiktionary.org",
                                        "dbname": "fiwiktionary",
                                        "code": "wiktionary",
                                    },
                                ],
                            },
                            "1": {
                                "code": "sv",
                                "site": [
                                    {
                                        "url": "https://sv.wikipedia.org",
                                        "dbname": "svwiki",
                                        "code": "wiki",
                                    }
                                ],
                            },
                            "specials": [
                                {
                                    "url": "https://meta.wikimedia.org",
                                    "dbname": "metawiki",
                                    "code": "meta",
                                },
                                {
                                    "url": "https://abstract.wikipedia.org",
                                    "dbname": "abstractwiki",
                                    "code": "abstract",
                                },
                                {
                                    "url": "https://test.wikipedia.org",
                                    "dbname": "testwiki",
                                    "code": "testwiki",
                                },
                            ],
                        }
                    }
                ).encode("utf-8")
            )

            self.assertEqual(
                service_source.normalize_wikis("*.wikipedia.org, fi.wikipedia.org"),
                ["fi", "sv"],
            )

    def test_selected_wiki_descriptors_resolves_legacy_short_wikipedia_token_via_sitematrix(self) -> None:
        descriptor = service_source._WikiDescriptor(
            domain="zh-yue.wikipedia.org",
            dbname="zh_yuewiki",
            lang_code="yue",
            wiki_group="wikipedia",
            site_url="https://zh-yue.wikipedia.org/",
            site_code="wiki",
        )
        with patch("newpages.service_source._known_wikis_by_domain", return_value={"zh-yue.wikipedia.org": descriptor}):
            service_source._known_wiki_domains_by_token.cache_clear()
            selected = service_source._selected_wiki_descriptors(["yue"])

        self.assertEqual(selected, [descriptor])

    def test_normalize_wikis_rejects_unknown_wildcard(self) -> None:
        with patch("newpages.service_source.urlopen") as urlopen_mock:
            urlopen_mock.return_value = _FakeHttpResponse(
                json.dumps(
                    {
                        "sitematrix": {
                            "count": 1,
                            "0": {
                                "code": "fi",
                                "site": [
                                    {
                                        "url": "https://fi.wikipedia.org",
                                        "dbname": "fiwiki",
                                        "code": "wiki",
                                    }
                                ],
                            },
                        }
                    }
                ).encode("utf-8")
            )

            with self.assertRaisesMessage(ValueError, "Unknown wiki wildcard: *.wikivoyage.org."):
                service_source.normalize_wikis("*.wikivoyage.org")

    def test_normalize_user_list_page_accepts_interwiki_and_direct_url(self) -> None:
        with patch("newpages.service_source.urlopen") as urlopen_mock:
            urlopen_mock.return_value = _FakeHttpResponse(
                json.dumps(
                    {
                        "sitematrix": {
                            "count": 2,
                            "0": {
                                "code": "fi",
                                "site": [
                                    {
                                        "url": "https://fi.wikipedia.org",
                                        "dbname": "fiwiki",
                                        "code": "wiki",
                                    }
                                ],
                            },
                            "specials": [
                                {
                                    "url": "https://species.wikimedia.org",
                                    "dbname": "specieswiki",
                                    "code": "species",
                                }
                            ],
                        }
                    }
                ).encode("utf-8")
            )

            self.assertEqual(
                service_source.normalize_user_list_page(
                    ":w:fi:Wikipedia:Viikon kilpailu/Viikon kilpailu 2026-15"
                ),
                ":w:fi:Wikipedia:Viikon_kilpailu/Viikon_kilpailu_2026-15",
            )
            self.assertEqual(
                service_source.normalize_user_list_page(
                    "https://fi.wikipedia.org/wiki/Wikipedia:Viikon_kilpailu/Viikon_kilpailu_2026-15"
                ),
                ":w:fi:Wikipedia:Viikon_kilpailu/Viikon_kilpailu_2026-15",
            )
            self.assertEqual(
                service_source.normalize_user_list_page(":species:Village_pump"),
                ":species:Village_pump",
            )
            self.assertEqual(
                service_source.normalize_user_list_page("https://species.wikimedia.org/wiki/Village_pump"),
                ":species:Village_pump",
            )

    def test_fetch_newpage_records_rejects_unknown_wiki_domain_via_sitematrix(self) -> None:
        with patch("newpages.service_source.urlopen") as urlopen_mock:
            urlopen_mock.return_value = _FakeHttpResponse(
                json.dumps(
                    {
                        "sitematrix": {
                            "count": 1,
                            "0": {
                                "code": "fi",
                                "site": [
                                    {
                                        "url": "https://fi.wikipedia.org",
                                        "dbname": "fiwiki",
                                        "code": "wiki",
                                    }
                                ],
                            },
                        }
                    }
                ).encode("utf-8")
            )

            with self.assertRaisesMessage(ValueError, "Unknown wiki domain: example.org."):
                service_source.fetch_newpage_records(wiki_domains=["example.org"])

    def test_fetch_newpage_records_rejects_unsupported_wiki_domain_via_sitematrix(self) -> None:
        with patch("newpages.service_source.urlopen") as urlopen_mock:
            urlopen_mock.return_value = _FakeHttpResponse(
                json.dumps(
                    {
                        "sitematrix": {
                            "count": 1,
                            "specials": [
                                {
                                    "url": "https://species.wikimedia.org",
                                    "dbname": "specieswiki",
                                    "code": "species",
                                }
                            ],
                        }
                    }
                ).encode("utf-8")
            )

            with self.assertRaisesMessage(
                ValueError,
                "Unsupported wiki domain: species.wikimedia.org. Supported projects are: "
                "Wikipedia, Wiktionary, Wikibooks, Wikinews, Wikiquote, Wikisource, "
                "Wikiversity, Wikivoyage, Wikidata, Commons, Incubator, Meta-Wiki.",
            ):
                service_source.fetch_newpage_records(wiki_domains=["species.wikimedia.org"])

    def test_fetch_user_names_for_page_collects_local_and_interwiki_user_links(self) -> None:
        with patch("newpages.service_source._centralauth_user_exists", return_value=True):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {"2": {"*": "Käyttäjä", "canonical": "User"}},
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 900,
                                            "links": [
                                                {"title": "Käyttäjä:Alice_A/sandbox"},
                                                {"title": "User:Bob_B"},
                                            ],
                                            "iwlinks": [
                                                {"url": "https://sv.wikipedia.org/wiki/Anv%C3%A4ndare:Charlie_C/common.js"},
                                                {"url": "https://commons.wikimedia.org/wiki/User:Delta_D"},
                                            ],
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 3,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wikipedia",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wikipedia",
                                            }
                                        ],
                                    },
                                    "specials": [
                                        {
                                            "url": "https://commons.wikimedia.org",
                                            "dbname": "commonswiki",
                                            "code": "commons",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {"2": {"*": "Användare", "canonical": "User"}},
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {"2": {"*": "User", "canonical": "User"}},
                                    "namespacealiases": [],
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                ref = service_source._resolve_user_list_page(":w:fi:Wikipedia:Users")
                self.assertIsNotNone(ref)
                user_names = service_source._fetch_user_names_for_page(ref) if ref is not None else []

        self.assertEqual(user_names, ["Alice A", "Bob B", "Charlie C", "Delta D"])

    def test_fetch_user_names_for_page_keeps_only_centralauth_users(self) -> None:
        with patch(
            "newpages.service_source._centralauth_user_exists",
            side_effect=lambda user_name: user_name in {"Alice A", "Charlie C"},
        ):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {"2": {"*": "Käyttäjä", "canonical": "User"}},
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 900,
                                            "links": [
                                                {"title": "Käyttäjä:Alice_A"},
                                                {"title": "User:Bob_B"},
                                            ],
                                            "iwlinks": [
                                                {"url": "https://sv.wikipedia.org/wiki/Anv%C3%A4ndare:Charlie_C"},
                                                {"url": "https://commons.wikimedia.org/wiki/User:Delta_D"},
                                            ],
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 3,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wikipedia",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wikipedia",
                                            }
                                        ],
                                    },
                                    "specials": [
                                        {
                                            "url": "https://commons.wikimedia.org",
                                            "dbname": "commonswiki",
                                            "code": "commons",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {"2": {"*": "Användare", "canonical": "User"}},
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {"2": {"*": "User", "canonical": "User"}},
                                    "namespacealiases": [],
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                ref = service_source._resolve_user_list_page(":w:fi:Wikipedia:Users")
                self.assertIsNotNone(ref)
                user_names = service_source._fetch_user_names_for_page(ref) if ref is not None else []

        self.assertEqual(user_names, ["Alice A", "Charlie C"])

    def test_fetch_user_names_for_page_follows_continue_and_deduplicates_across_batches(self) -> None:
        with patch("newpages.service_source._centralauth_user_exists", return_value=True):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {"2": {"*": "Käyttäjä", "canonical": "User"}},
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 900,
                                            "links": [{"title": "Käyttäjä:Alice_A"}],
                                            "iwlinks": [],
                                        }
                                    ]
                                },
                                "continue": {
                                    "continue": "||",
                                    "plcontinue": "900|2|User:Bob_B",
                                    "iwcontinue": "900|0|sv:User:Charlie_C",
                                },
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 900,
                                            "links": [
                                                {"title": "User:Bob_B/subpage"},
                                                {"title": "Käyttäjä:Alice_A"},
                                            ],
                                            "iwlinks": [
                                                {"url": "https://sv.wikipedia.org/wiki/Anv%C3%A4ndare:Charlie_C/common.js"},
                                                {"url": "https://commons.wikimedia.org/wiki/User:Delta_D/subpage"},
                                            ],
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 3,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wikipedia",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wikipedia",
                                            }
                                        ],
                                    },
                                    "specials": [
                                        {
                                            "url": "https://commons.wikimedia.org",
                                            "dbname": "commonswiki",
                                            "code": "commons",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {"2": {"*": "Användare", "canonical": "User"}},
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {"2": {"*": "User", "canonical": "User"}},
                                    "namespacealiases": [],
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                ref = service_source._resolve_user_list_page(":w:fi:Wikipedia:Users")
                self.assertIsNotNone(ref)
                user_names = service_source._fetch_user_names_for_page(ref) if ref is not None else []

        self.assertEqual(user_names, ["Alice A", "Bob B", "Charlie C", "Delta D"])
        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("prop=links%7Ciwlinks", request_urls[1])
        self.assertIn("plcontinue=900%7C2%7CUser%3ABob_B", request_urls[2])
        self.assertIn("iwcontinue=900%7C0%7Csv%3AUser%3ACharlie_C", request_urls[2])

    def test_fetch_user_names_for_page_sql_accepts_tuple_fetchall_results(self) -> None:
        descriptor_by_domain = {
            "fi.wikipedia.org": service_source._WikiDescriptor(
                domain="fi.wikipedia.org",
                dbname="fiwiki",
                lang_code="fi",
                wiki_group="wikipedia",
                site_url="https://fi.wikipedia.org",
            )
        }
        siteinfo = service_source._SiteInfo(
            article_path="/wiki/$1",
            lang_code="fi",
            namespace_names={0: "", 2: "Käyttäjä", 4: "Wikipedia"},
            namespace_aliases={2: ("Käyttäjä", "User"), 4: ("Project", "Wikipedia")},
        )
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            ((1900518, None, None),),
            ((b"Alice_A",),),
            tuple(),
        ]
        connection = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None
        connection.cursor.return_value = cursor_cm
        ref = service_source._UserListPageRef(
            domain="fi.wikipedia.org",
            page_title="Wikipedia:Viikon_kilpailu/Viikon_kilpailu_2026-15",
            canonical_ref=":w:fi:Wikipedia:Viikon_kilpailu/Viikon_kilpailu_2026-15",
        )

        with patch("newpages.service_source._known_wikis_by_domain", return_value=descriptor_by_domain):
            with patch("newpages.service_source._siteinfo_for_domain", return_value=siteinfo):
                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    pymysql_mock.connect.return_value = connection

                    user_names = service_source._fetch_user_names_for_page_sql(ref)

        self.assertEqual(user_names, ["Alice A"])

    def test_active_user_wiki_dbnames_for_user_filters_to_accounts_with_edits(self) -> None:
        with patch("newpages.service_source.urlopen") as urlopen_mock:
            urlopen_mock.return_value = _FakeHttpResponse(
                json.dumps(
                    {
                        "query": {
                            "globaluserinfo": {
                                "merged": [
                                    {"wiki": "fiwiki", "editcount": 12},
                                    {"wiki": "svwiki", "editcount": 0},
                                    {"wiki": "commonswiki"},
                                    {"wiki": "dewiki", "editcount": "7"},
                                ]
                            }
                        }
                    }
                ).encode("utf-8")
            )

            active_dbnames = service_source._active_user_wiki_dbnames_for_user("Alice A")

        self.assertEqual(active_dbnames, ("commonswiki", "dewiki", "fiwiki"))
        request_url = urlopen_mock.call_args.args[0].full_url
        self.assertIn("meta=globaluserinfo", request_url)
        self.assertIn("guiprop=merged", request_url)
        self.assertIn("guiuser=Alice+A", request_url)

    def test_fetch_newpage_records_via_api_for_meta_is_allowed(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "specials": [
                                        {
                                            "url": "https://meta.wikimedia.org",
                                            "dbname": "metawiki",
                                            "code": "meta",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 801,
                                            "ns": 0,
                                            "title": "Movement_Charter",
                                            "timestamp": "2026-04-05T07:08:09Z",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 801,
                                            "pageprops": {"wikibase_item": "Q42"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                records, source_url = service_source.fetch_newpage_records(
                    wiki_domains=["meta.wikimedia.org"],
                )

        self.assertEqual(source_url, "https://meta.wikimedia.org/wiki/Special:Log/create")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Movement_Charter")
        self.assertEqual(records[0]["page_url"], "https://meta.wikimedia.org/wiki/Movement_Charter")
        self.assertEqual(records[0]["site_url"], "https://meta.wikimedia.org/")
        self.assertEqual(records[0]["lang_code"], "en")
        self.assertEqual(records[0]["wikidata_id"], "Q42")

    def test_fetch_newpage_records_uses_default_api_backend_for_multiwiki_merge_and_limit(self) -> None:
        self.assertEqual(
            service_source.newpages_lookup_backend(),
            service_source.LOOKUP_BACKEND_API,
        )

        with patch("newpages.service_source.urlopen") as urlopen_mock:
            urlopen_mock.side_effect = [
                _FakeHttpResponse(
                    json.dumps(
                        {
                            "sitematrix": {
                                "count": 2,
                                "0": {
                                    "code": "fi",
                                    "site": [
                                        {
                                            "url": "https://fi.wikipedia.org",
                                            "dbname": "fiwiki",
                                            "code": "wiki",
                                        }
                                    ],
                                },
                                "1": {
                                    "code": "sv",
                                    "site": [
                                        {
                                            "url": "https://sv.wikipedia.org",
                                            "dbname": "svwiki",
                                            "code": "wiki",
                                        }
                                    ],
                                },
                            }
                        }
                    ).encode("utf-8")
                ),
                _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                "namespaces": {
                                    "0": {"*": ""},
                                },
                            }
                        }
                    ).encode("utf-8")
                ),
                _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "logevents": [
                                    {
                                        "pageid": 101,
                                        "ns": 0,
                                        "title": "Turku",
                                        "timestamp": "2026-04-04T02:03:04Z",
                                    },
                                    {
                                        "pageid": 102,
                                        "ns": 0,
                                        "title": "Tampere",
                                        "timestamp": "2026-04-04T01:02:03Z",
                                    },
                                ]
                            }
                        }
                    ).encode("utf-8")
                ),
                _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "pages": [
                                    {
                                        "pageid": 101,
                                        "pageprops": {"wikibase_item": "Q1757"},
                                    },
                                    {
                                        "pageid": 102,
                                        "pageprops": {"wikibase_item": "Q40840"},
                                    },
                                ]
                            }
                        }
                    ).encode("utf-8")
                ),
                _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                "namespaces": {
                                    "0": {"*": ""},
                                },
                            }
                        }
                    ).encode("utf-8")
                ),
                _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "logevents": [
                                    {
                                        "pageid": 201,
                                        "ns": 0,
                                        "title": "Stockholm",
                                        "timestamp": "2026-04-05T04:03:02Z",
                                    }
                                ]
                            }
                        }
                    ).encode("utf-8")
                ),
                _FakeHttpResponse(
                    json.dumps(
                        {
                            "query": {
                                "pages": [
                                    {
                                        "pageid": 201,
                                        "pageprops": {"wikibase_item": "Q1754"},
                                    }
                                ]
                            }
                        }
                    ).encode("utf-8")
                ),
            ]

            with patch("newpages.service_source.pymysql") as pymysql_mock:
                records, source_url = service_source.fetch_newpage_records(
                    limit=2,
                    wiki_domains=["fi.wikipedia.org", "sv.wikipedia.org"],
                )

        self.assertEqual(source_url, service_source.SITEMATRIX_SOURCE_URL)
        self.assertEqual(len(records), 2)
        self.assertEqual([record["wiki_domain"] for record in records], ["sv.wikipedia.org", "fi.wikipedia.org"])
        self.assertEqual([record["page_title"] for record in records], ["Stockholm", "Turku"])
        self.assertEqual(records[0]["created_timestamp"], "2026-04-05T04:03:02Z")
        self.assertEqual(records[0]["site_url"], "https://sv.wikipedia.org/")
        self.assertEqual(records[1]["created_timestamp"], "2026-04-04T02:03:04Z")
        self.assertEqual(records[1]["site_url"], "https://fi.wikipedia.org/")
        pymysql_mock.connect.assert_not_called()

    def test_fetch_newpage_records_via_api_uses_page_creation_log_and_filters_timestamp(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 2,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 123,
                                            "ns": 0,
                                            "title": "Turku",
                                            "timestamp": "2026-04-04T02:03:04Z",
                                        },
                                        {
                                            "pageid": 124,
                                            "ns": 0,
                                            "title": "Vanha_sivu",
                                            "timestamp": "2026-04-03T01:02:03Z",
                                        },
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 123,
                                            "pageprops": {"wikibase_item": "Q1757"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    records, source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        timestamp="20260404000000",
                    )

        self.assertEqual(source_url, "https://fi.wikipedia.org/wiki/Special:Log/create")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Turku")
        self.assertEqual(records[0]["page_url"], "https://fi.wikipedia.org/wiki/Turku")
        self.assertEqual(records[0]["created_timestamp"], "2026-04-04T02:03:04Z")
        self.assertEqual(records[0]["wikidata_id"], "Q1757")
        self.assertEqual(records[0]["site_url"], "https://fi.wikipedia.org/")
        pymysql_mock.connect.assert_not_called()

        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("list=logevents", request_urls[2])
        self.assertIn("letype=create", request_urls[2])
        self.assertIn("leprop=title%7Ctimestamp%7Cids%7Cuser", request_urls[2])
        self.assertIn("ledir=older", request_urls[2])
        self.assertIn("lelimit=100", request_urls[2])
        self.assertIn("prop=pageprops", request_urls[3])
        self.assertIn("ppprop=wikibase_item", request_urls[3])
        self.assertIn("pageids=123", request_urls[3])

    def test_fetch_newpage_records_via_api_filters_by_user_list_page_with_local_and_interwiki_users(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source._centralauth_user_exists", return_value=True):
                with patch("newpages.service_source.urlopen") as urlopen_mock:
                    urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 2,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "2": {"*": "Käyttäjä", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 901,
                                            "links": [{"title": "Käyttäjä:Alice_A/sandbox"}],
                                            "iwlinks": [
                                                {"url": "https://sv.wikipedia.org/wiki/Anv%C3%A4ndare:Charlie_C/common.js"}
                                            ],
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {
                                        "2": {"*": "Användare", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 123,
                                            "ns": 0,
                                            "title": "Alice_Page",
                                            "timestamp": "2026-04-05T02:03:04Z",
                                            "user": "Alice A",
                                        },
                                        {
                                            "pageid": 125,
                                            "ns": 0,
                                            "title": "Charlie_Page",
                                            "timestamp": "2026-04-05T01:33:04Z",
                                            "user": "Charlie C",
                                        },
                                        {
                                            "pageid": 124,
                                            "ns": 0,
                                            "title": "Bob_Page",
                                            "timestamp": "2026-04-05T01:03:04Z",
                                            "user": "Bob B",
                                        },
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 123,
                                            "pageprops": {"wikibase_item": "Q1757"},
                                        },
                                        {
                                            "pageid": 125,
                                            "pageprops": {"wikibase_item": "Q1234"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    ]

                    records, source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        user_list_page=":w:fi:Wikipedia:Users",
                    )

        self.assertEqual(source_url, "https://fi.wikipedia.org/wiki/Special:Log/create")
        self.assertEqual([record["page_title"] for record in records], ["Alice_Page", "Charlie_Page"])
        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("prop=links%7Ciwlinks", request_urls[2])
        self.assertIn("plnamespace=2", request_urls[2])
        self.assertIn("iwprop=url", request_urls[2])
        self.assertIn("leprop=title%7Ctimestamp%7Cids%7Cuser", request_urls[4])
        self.assertIn("pageids=123%7C125", request_urls[5])

    def test_fetch_newpage_records_via_api_can_include_edited_pages_for_user_list_page(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source._centralauth_user_exists", return_value=True):
                with patch("newpages.service_source.urlopen") as urlopen_mock:
                    urlopen_mock.side_effect = [
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "sitematrix": {
                                        "count": 1,
                                        "0": {
                                            "code": "fi",
                                            "site": [
                                                {
                                                    "url": "https://fi.wikipedia.org",
                                                    "dbname": "fiwiki",
                                                    "code": "wiki",
                                                }
                                            ],
                                        },
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                        "namespaces": {
                                            "0": {"*": ""},
                                            "2": {"*": "Käyttäjä", "canonical": "User"},
                                        },
                                        "namespacealiases": [{"id": 2, "*": "User"}],
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "pages": [
                                            {
                                                "pageid": 901,
                                                "links": [{"title": "Käyttäjä:Alice_A"}],
                                                "iwlinks": [],
                                            }
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "usercontribs": [
                                            {
                                                "pageid": 123,
                                                "ns": 0,
                                                "title": "Turku",
                                                "timestamp": "2026-04-05T03:02:01Z",
                                            },
                                            {
                                                "pageid": 123,
                                                "ns": 0,
                                                "title": "Turku",
                                                "timestamp": "2026-04-05T02:02:01Z",
                                            },
                                            {
                                                "pageid": 124,
                                                "ns": 0,
                                                "title": "Ilman_qidta",
                                                "timestamp": "2026-04-04T01:00:00Z",
                                            },
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "pages": [
                                            {
                                                "pageid": 123,
                                                "pageprops": {"wikibase_item": "Q1757"},
                                            },
                                            {
                                                "pageid": 124,
                                            },
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                    ]

                    records, source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        timestamp="20260401",
                        user_list_page=":w:fi:Wikipedia:Users",
                        include_edited_pages=True,
                    )

        self.assertEqual(source_url, "https://fi.wikipedia.org/wiki/Special:Contributions")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Turku")
        self.assertEqual(records[0]["current_timestamp"], "2026-04-05T03:02:01Z")
        self.assertNotIn("created_timestamp", records[0])
        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("list=usercontribs", request_urls[3])
        self.assertIn("ucuser=Alice+A", request_urls[3])
        self.assertIn("ucend=2026-04-01T00%3A00%3A00Z", request_urls[3])
        self.assertIn("prop=pageprops", request_urls[4])

    def test_fetch_newpage_records_via_api_user_list_page_skips_wikis_without_user_activity(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source._centralauth_user_exists", return_value=True):
                with patch("newpages.service_source.urlopen") as urlopen_mock:
                    urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 10,
                                    "0": {
                                        "code": "de",
                                        "site": [
                                            {
                                                "url": "https://de.wikipedia.org",
                                                "dbname": "dewiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "2": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "3": {
                                        "code": "en",
                                        "site": [
                                            {
                                                "url": "https://en.wikipedia.org",
                                                "dbname": "enwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "4": {
                                        "code": "fr",
                                        "site": [
                                            {
                                                "url": "https://fr.wikipedia.org",
                                                "dbname": "frwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "5": {
                                        "code": "es",
                                        "site": [
                                            {
                                                "url": "https://es.wikipedia.org",
                                                "dbname": "eswiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "6": {
                                        "code": "it",
                                        "site": [
                                            {
                                                "url": "https://it.wikipedia.org",
                                                "dbname": "itwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "7": {
                                        "code": "nl",
                                        "site": [
                                            {
                                                "url": "https://nl.wikipedia.org",
                                                "dbname": "nlwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "8": {
                                        "code": "pl",
                                        "site": [
                                            {
                                                "url": "https://pl.wikipedia.org",
                                                "dbname": "plwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "9": {
                                        "code": "cs",
                                        "site": [
                                            {
                                                "url": "https://cs.wikipedia.org",
                                                "dbname": "cswiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "2": {"*": "Käyttäjä", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 901,
                                            "links": [{"title": "Käyttäjä:Alice_A/sandbox"}],
                                            "iwlinks": [
                                                {"url": "https://sv.wikipedia.org/wiki/Anv%C3%A4ndare:Charlie_C/common.js"}
                                            ],
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {
                                        "2": {"*": "Användare", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 123,
                                            "ns": 0,
                                            "title": "Alice_Page",
                                            "timestamp": "2026-04-05T02:03:04Z",
                                            "user": "Alice A",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 123,
                                            "pageprops": {"wikibase_item": "Q1757"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 125,
                                            "ns": 0,
                                            "title": "Charlie_Page",
                                            "timestamp": "2026-04-05T01:33:04Z",
                                            "user": "Charlie C",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 125,
                                            "pageprops": {"wikibase_item": "Q1234"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    ]

                    with patch("newpages.service_source._active_user_wiki_dbnames_for_user") as active_wikis_mock:
                        active_wikis_mock.side_effect = [("fiwiki",), ("svwiki",)]
                        records, source_url = service_source.fetch_newpage_records(
                            wiki_domains=[
                                "cs.wikipedia.org",
                                "de.wikipedia.org",
                                "en.wikipedia.org",
                                "es.wikipedia.org",
                                "fi.wikipedia.org",
                                "fr.wikipedia.org",
                                "it.wikipedia.org",
                                "nl.wikipedia.org",
                                "pl.wikipedia.org",
                                "sv.wikipedia.org",
                            ],
                            user_list_page=":w:fi:Wikipedia:Users",
                        )

        self.assertEqual(source_url, service_source.SITEMATRIX_SOURCE_URL)
        self.assertEqual([record["wiki_domain"] for record in records], ["fi.wikipedia.org", "sv.wikipedia.org"])
        self.assertEqual(active_wikis_mock.call_count, 2)
        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertFalse(any("de.wikipedia.org" in request_url for request_url in request_urls))

    def test_fetch_newpage_records_via_api_small_user_list_keeps_explicit_incubator_wiki(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source._centralauth_user_exists", return_value=True):
                with patch("newpages.service_source.urlopen") as urlopen_mock:
                    urlopen_mock.side_effect = [
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "sitematrix": {
                                        "count": 2,
                                        "0": {
                                            "code": "fi",
                                            "site": [
                                                {
                                                    "url": "https://fi.wikipedia.org",
                                                    "dbname": "fiwiki",
                                                    "code": "wiki",
                                                }
                                            ],
                                        },
                                        "specials": [
                                            {
                                                "url": "https://incubator.wikimedia.org",
                                                "dbname": "incubatorwiki",
                                                "code": "incubator",
                                            }
                                        ],
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                        "namespaces": {
                                            "0": {"*": ""},
                                            "2": {"*": "Käyttäjä", "canonical": "User"},
                                        },
                                        "namespacealiases": [{"id": 2, "*": "User"}],
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "pages": [
                                            {
                                                "pageid": 901,
                                                "links": [{"title": "Käyttäjä:Alice_A"}],
                                                "iwlinks": [],
                                            }
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "logevents": []
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                        "namespaces": {
                                            "0": {"*": ""},
                                        },
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "logevents": [
                                            {
                                                "pageid": 701,
                                                "ns": 0,
                                                "title": "Wp/sms/Katja_Gauriloff",
                                                "timestamp": "2026-04-04T02:03:04Z",
                                                "user": "Alice A",
                                            }
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "pages": [
                                            {
                                                "pageid": 701,
                                            }
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                        _FakeHttpResponse(
                            json.dumps(
                                {
                                    "query": {
                                        "pages": [
                                            {
                                                "pageid": 701,
                                                "categories": [
                                                    {"sortkeyprefix": "Q2288381"},
                                                ],
                                            }
                                        ]
                                    }
                                }
                            ).encode("utf-8")
                        ),
                    ]

                    with patch("newpages.service_source._active_user_wiki_dbnames_for_user", return_value=("fiwiki",)):
                        records, _source_url = service_source.fetch_newpage_records(
                            wiki_domains=["fi.wikipedia.org", "incubator.wikimedia.org"],
                            timestamp="20260331",
                            user_list_page=":w:fi:Wikipedia:Users",
                            limit=1000,
                        )

        self.assertEqual([record["page_title"] for record in records], ["Wp/sms/Katja_Gauriloff"])
        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertTrue(any("incubator.wikimedia.org" in request_url and "list=logevents" in request_url for request_url in request_urls))

    def test_fetch_newpage_records_via_api_pages_until_limit_after_qid_filtering(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 2,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 123,
                                            "ns": 0,
                                            "title": "Ilman_qidta",
                                            "timestamp": "2026-04-05T03:02:01Z",
                                        }
                                    ]
                                },
                                "continue": {"lecontinue": "next|123"},
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 123,
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 124,
                                            "ns": 0,
                                            "title": "Turku",
                                            "timestamp": "2026-04-05T03:00:00Z",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 124,
                                            "pageprops": {"wikibase_item": "Q1757"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                records, source_url = service_source.fetch_newpage_records(
                    limit=1,
                    wiki_domains=["fi.wikipedia.org"],
                )

        self.assertEqual(source_url, "https://fi.wikipedia.org/wiki/Special:Log/create")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Turku")
        self.assertEqual(records[0]["wikidata_id"], "Q1757")

        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("lelimit=1", request_urls[2])
        self.assertIn("lelimit=1", request_urls[4])
        self.assertIn("lecontinue=next%7C123", request_urls[4])

    def test_fetch_newpage_records_via_api_with_timestamp_and_under_ten_wikis_scans_past_default_cap(self) -> None:
        first_batch_page_ids = list(range(1000, 1100))
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": page_id,
                                            "ns": 0,
                                            "title": "Page_{}".format(page_id),
                                            "timestamp": "2026-04-05T03:02:01Z",
                                        }
                                        for page_id in first_batch_page_ids
                                    ]
                                },
                                "continue": {"lecontinue": "next|100"},
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": page_id,
                                            "pageprops": {"wikibase_item": "Q{}".format(page_id)},
                                        }
                                        for page_id in first_batch_page_ids[:50]
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": page_id,
                                            "pageprops": {"wikibase_item": "Q{}".format(page_id)},
                                        }
                                        for page_id in first_batch_page_ids[50:]
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 2000,
                                            "ns": 0,
                                            "title": "Page_2000",
                                            "timestamp": "2026-04-05T03:01:00Z",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 2000,
                                            "pageprops": {"wikibase_item": "Q2000"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                records, source_url = service_source.fetch_newpage_records(
                    wiki_domains=["fi.wikipedia.org"],
                    timestamp="20260405000000",
                )

        self.assertEqual(source_url, "https://fi.wikipedia.org/wiki/Special:Log/create")
        self.assertEqual(len(records), 101)
        self.assertEqual(records[0]["page_title"], "Page_1000")
        self.assertEqual(records[-1]["page_title"], "Page_2000")

        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("lelimit=100", request_urls[2])
        self.assertIn("lelimit=100", request_urls[5])
        self.assertIn("lecontinue=next%7C100", request_urls[5])

    def test_fetch_newpage_records_via_api_normalizes_localized_namespace_titles(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "0": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "14": {"*": "Kategori"},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 321,
                                            "ns": 14,
                                            "title": "Kategori:Exempel",
                                            "timestamp": "2026-04-05T05:06:07Z",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 321,
                                            "pageprops": {"wikibase_item": "Q42"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                records, source_url = service_source.fetch_newpage_records(
                    wiki_domains=["sv.wikipedia.org"],
                )

        self.assertEqual(source_url, "https://sv.wikipedia.org/wiki/Special:Log/create")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Kategori:Exempel")
        self.assertEqual(records[0]["page_label"], "Kategori:Exempel")
        self.assertEqual(records[0]["page_url"], "https://sv.wikipedia.org/wiki/Kategori:Exempel")
        self.assertEqual(records[0]["lang_code"], "sv")
        self.assertEqual(records[0]["wikidata_entity"], "http://www.wikidata.org/entity/Q42")

    def test_fetch_newpage_records_via_api_for_commons_excludes_file_namespace(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "specials": [
                                        {
                                            "url": "https://commons.wikimedia.org",
                                            "dbname": "commonswiki",
                                            "code": "commons",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {
                                        "6": {"*": "File"},
                                        "14": {"*": "Category"},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 555,
                                            "ns": 6,
                                            "title": "File:Example.jpg",
                                            "timestamp": "2026-04-04T06:07:08Z",
                                        },
                                        {
                                            "pageid": 556,
                                            "ns": 14,
                                            "title": "Category:Example_category",
                                            "timestamp": "2026-04-04T05:07:08Z",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 556,
                                            "pageprops": {"wikibase_item": "Q42"},
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                records, _source_url = service_source.fetch_newpage_records(
                    wiki_domains=["commons.wikimedia.org"],
                )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Category:Example_category")
        self.assertEqual(
            records[0]["page_url"],
            "https://commons.wikimedia.org/wiki/Category:Example_category",
        )
        logevents_url = urlopen_mock.call_args_list[2].args[0].full_url
        self.assertIn("list=logevents", logevents_url)
        self.assertNotIn("lenamespace=6", logevents_url)
        self.assertIn("lelimit=100", logevents_url)
        pageprops_url = urlopen_mock.call_args_list[3].args[0].full_url
        self.assertIn("pageids=556", pageprops_url)
        self.assertNotIn("555", pageprops_url)

    def test_fetch_newpage_records_via_api_for_incubator_uses_category_sortkey_qids(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "specials": [
                                        {
                                            "url": "https://incubator.wikimedia.org",
                                            "dbname": "incubatorwiki",
                                            "code": "incubator",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "logevents": [
                                        {
                                            "pageid": 701,
                                            "ns": 0,
                                            "title": "Wp/sms/Uusi_sivu",
                                            "timestamp": "2026-04-05T07:08:09Z",
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 701,
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "pages": [
                                        {
                                            "pageid": 701,
                                            "categories": [
                                                {
                                                    "title": "Category:Maintenance:Wikidata interwiki links",
                                                    "sortkeyprefix": "Q123",
                                                }
                                            ],
                                        }
                                    ]
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                records, source_url = service_source.fetch_newpage_records(
                    wiki_domains=["incubator.wikimedia.org"],
                )

        self.assertEqual(source_url, "https://incubator.wikimedia.org/wiki/Special:Log/create")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Wp/sms/Uusi_sivu")
        self.assertEqual(
            records[0]["page_url"],
            "https://incubator.wikimedia.org/wiki/Wp/sms/Uusi_sivu",
        )
        self.assertEqual(records[0]["lang_code"], "sms")
        self.assertEqual(records[0]["site_url"], "https://incubator.wikimedia.org/wiki/Wp/sms/")
        self.assertEqual(records[0]["wiki_group"], "wikipedia")
        self.assertEqual(records[0]["wikidata_id"], "Q123")

        request_urls = [call.args[0].full_url for call in urlopen_mock.call_args_list]
        self.assertIn("prop=pageprops", request_urls[3])
        self.assertIn("prop=categories", request_urls[4])
        self.assertIn(
            "clcategories=Category%3AMaintenance%3AWikidata_interwiki_links",
            request_urls[4],
        )
        self.assertIn("clprop=sortkey", request_urls[4])

    def test_fetch_newpage_records_via_replica_merges_multiple_wikis(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 2,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "14": {"*": "Luokka"},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "14": {"*": "Kategori"},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    fi_connection = _fake_connection(
                        [
                            (123, b"Turku", 0, "Q1757", "20260403010203"),
                        ]
                    )
                    sv_connection = _fake_connection(
                        [
                            (124, b"Esimerkki", 14, "Q42", "20260404020304"),
                        ]
                    )
                    pymysql_mock.connect.side_effect = [fi_connection, sv_connection]

                    records, source_url = service_source.fetch_newpage_records(
                        limit=2,
                        wiki_domains=["fi.wikipedia.org", "sv.wikipedia.org"],
                        timestamp="202604",
                    )

        self.assertEqual(source_url, service_source.SITEMATRIX_SOURCE_URL)
        self.assertEqual([record["wiki_domain"] for record in records], ["sv.wikipedia.org", "fi.wikipedia.org"])
        self.assertEqual(records[0]["page_title"], "Kategori:Esimerkki")
        self.assertEqual(records[0]["page_url"], "https://sv.wikipedia.org/wiki/Kategori:Esimerkki")
        self.assertEqual(records[0]["created_timestamp"], "2026-04-04T02:03:04Z")
        self.assertEqual(records[0]["lang_code"], "sv")
        self.assertEqual(records[1]["page_title"], "Turku")
        self.assertEqual(records[1]["page_url"], "https://fi.wikipedia.org/wiki/Turku")
        self.assertEqual(records[1]["site_url"], "https://fi.wikipedia.org/")
        self.assertEqual(records[1]["wikidata_entity"], "http://www.wikidata.org/entity/Q1757")

        fi_connect_kwargs = pymysql_mock.connect.call_args_list[0].kwargs
        self.assertEqual(fi_connect_kwargs.get("host"), "fiwiki.web.db.svc.wikimedia.cloud")
        self.assertEqual(fi_connect_kwargs.get("database"), "fiwiki_p")

        fi_sql, fi_params = fi_connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("FROM recentchanges_userindex AS rc", fi_sql)
        self.assertIn("JOIN page AS p ON p.page_id = rc.rc_cur_id", fi_sql)
        self.assertIn("JOIN page_props AS pp", fi_sql)
        self.assertIn("SELECT rc.rc_cur_id AS page_id, p.page_title, p.page_namespace", fi_sql)
        self.assertIn("rc.rc_source = %s", fi_sql)
        self.assertIn("rc.rc_timestamp >= %s", fi_sql)
        self.assertIn("ORDER BY rc.rc_timestamp DESC", fi_sql)
        self.assertIn("LIMIT %s", fi_sql)
        self.assertEqual(fi_params, ["wikibase_item", "mw.new", "20260400000000", 2])

    def test_fetch_newpage_records_via_replica_uses_current_page_namespace_and_title(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "14": {"*": "Luokka"},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    fi_connection = _fake_connection(
                        [
                            (123, b"Nykyinen_luokka", 14, "Q1757", "20260403010203"),
                        ]
                    )
                    pymysql_mock.connect.return_value = fi_connection

                    records, _source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        timestamp="202604",
                    )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["namespace"], 14)
        self.assertEqual(records[0]["page_title"], "Luokka:Nykyinen_luokka")
        self.assertEqual(records[0]["page_url"], "https://fi.wikipedia.org/wiki/Luokka:Nykyinen_luokka")

        fi_sql, _fi_params = fi_connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("JOIN page AS p ON p.page_id = rc.rc_cur_id", fi_sql)
        self.assertIn("SELECT rc.rc_cur_id AS page_id, p.page_title, p.page_namespace", fi_sql)

    def test_fetch_newpage_records_without_limit_uses_default_sql_cap(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 2,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    fi_connection = _fake_connection(
                        [
                            (123, b"Turku", 0, "Q1757", "20260403010203"),
                        ]
                    )
                    pymysql_mock.connect.return_value = fi_connection

                    records, _source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        timestamp="202604",
                    )

        self.assertEqual(len(records), 1)
        fi_sql, fi_params = fi_connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("FROM recentchanges_userindex AS rc", fi_sql)
        self.assertIn("LIMIT %s", fi_sql)
        self.assertEqual(fi_params, ["wikibase_item", "mw.new", "20260400000000", 50000])

    def test_fetch_newpage_records_via_replica_filters_by_user_list_page_with_local_and_interwiki_users(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 2,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "2": {"*": "Käyttäjä", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {
                                        "2": {"*": "Användare", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    user_list_connection = _fake_connection_batches(
                        [
                            [(901, None, None)],
                            [(b"Alice_A/sandbox",)],
                            [("sv", "Användare:Charlie_C/common.js")],
                        ]
                    )
                    centralauth_connection = _fake_connection(
                        [
                            ("Alice A", "fiwiki"),
                            ("Charlie C", "fiwiki"),
                            ("Charlie C", "svwiki"),
                        ]
                    )
                    fi_connection = _fake_connection(
                        [
                            (123, b"Turku", 0, "Q1757", "20260403010203"),
                        ]
                    )
                    pymysql_mock.connect.side_effect = [
                        user_list_connection,
                        centralauth_connection,
                        fi_connection,
                    ]

                    records, _source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        timestamp="202604",
                        user_list_page=":w:fi:Wikipedia:Users",
                    )

        self.assertEqual(len(records), 1)
        fi_sql, fi_params = fi_connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("FROM recentchanges_userindex AS rc", fi_sql)
        self.assertIn("JOIN actor_recentchanges AS a ON rc.rc_actor = a.actor_id", fi_sql)
        self.assertIn("a.actor_name IN (%s, %s)", fi_sql)
        self.assertEqual(
            fi_params,
            [
                "wikibase_item",
                "mw.new",
                "Alice A",
                "Charlie C",
                "20260400000000",
                50000,
            ],
        )

    def test_fetch_newpage_records_via_replica_can_include_edited_pages_for_user_list_page(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "0": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "2": {"*": "Käyttäjä", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    user_list_connection = _fake_connection_batches(
                        [
                            [(901, None, None)],
                            [(b"Alice_A",)],
                            [],
                        ]
                    )
                    centralauth_connection = _fake_connection(
                        [
                            ("Alice A", "fiwiki"),
                        ]
                    )
                    fi_activity_connection = _fake_connection(
                        [
                            ("Alice A",),
                        ]
                    )
                    fi_connection = _fake_connection(
                        [
                            (123, b"Turku", 0, "Q1757", "20260403010203"),
                        ]
                    )
                    pymysql_mock.connect.side_effect = [
                        user_list_connection,
                        centralauth_connection,
                        fi_activity_connection,
                        fi_connection,
                    ]

                    records, source_url = service_source.fetch_newpage_records(
                        wiki_domains=["fi.wikipedia.org"],
                        timestamp="202604",
                        user_list_page=":w:fi:Wikipedia:Users",
                        include_edited_pages=True,
                    )

        self.assertEqual(source_url, "https://fi.wikipedia.org/wiki/Special:Contributions")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Turku")
        self.assertEqual(records[0]["current_timestamp"], "2026-04-03T01:02:03Z")
        self.assertNotIn("created_timestamp", records[0])
        fi_sql, fi_params = fi_connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("FROM actor_revision AS a", fi_sql)
        self.assertIn("JOIN revision_userindex AS rev ON rev.rev_actor = a.actor_id", fi_sql)
        self.assertIn("MAX(rev.rev_timestamp) AS matched_timestamp", fi_sql)
        self.assertIn("a.actor_name IN (%s)", fi_sql)
        self.assertIn("rev.rev_timestamp >= %s", fi_sql)
        self.assertIn("GROUP BY p.page_id, p.page_title, p.page_namespace, pp.pp_value", fi_sql)
        self.assertNotIn("rc.rc_source", fi_sql)
        self.assertEqual(
            fi_params,
            [
                "wikibase_item",
                "Alice A",
                "20260400000000",
                50000,
            ],
        )

    def test_fetch_newpage_records_via_replica_user_list_page_limits_to_wikis_with_registered_users(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 10,
                                    "0": {
                                        "code": "de",
                                        "site": [
                                            {
                                                "url": "https://de.wikipedia.org",
                                                "dbname": "dewiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "1": {
                                        "code": "fi",
                                        "site": [
                                            {
                                                "url": "https://fi.wikipedia.org",
                                                "dbname": "fiwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "2": {
                                        "code": "sv",
                                        "site": [
                                            {
                                                "url": "https://sv.wikipedia.org",
                                                "dbname": "svwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "3": {
                                        "code": "en",
                                        "site": [
                                            {
                                                "url": "https://en.wikipedia.org",
                                                "dbname": "enwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "4": {
                                        "code": "fr",
                                        "site": [
                                            {
                                                "url": "https://fr.wikipedia.org",
                                                "dbname": "frwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "5": {
                                        "code": "es",
                                        "site": [
                                            {
                                                "url": "https://es.wikipedia.org",
                                                "dbname": "eswiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "6": {
                                        "code": "it",
                                        "site": [
                                            {
                                                "url": "https://it.wikipedia.org",
                                                "dbname": "itwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "7": {
                                        "code": "nl",
                                        "site": [
                                            {
                                                "url": "https://nl.wikipedia.org",
                                                "dbname": "nlwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "8": {
                                        "code": "pl",
                                        "site": [
                                            {
                                                "url": "https://pl.wikipedia.org",
                                                "dbname": "plwiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                    "9": {
                                        "code": "cs",
                                        "site": [
                                            {
                                                "url": "https://cs.wikipedia.org",
                                                "dbname": "cswiki",
                                                "code": "wiki",
                                            }
                                        ],
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "fi"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                        "2": {"*": "Käyttäjä", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "sv"},
                                    "namespaces": {
                                        "2": {"*": "Användare", "canonical": "User"},
                                    },
                                    "namespacealiases": [{"id": 2, "*": "User"}],
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    user_list_connection = _fake_connection_batches(
                        [
                            [(901, None, None)],
                            [(b"Alice_A/sandbox",)],
                            [("sv", "Användare:Charlie_C/common.js")],
                        ]
                    )
                    centralauth_connection = _fake_connection(
                        [
                            ("Alice A", "fiwiki"),
                            ("Charlie C", "svwiki"),
                        ]
                    )
                    fi_connection = _fake_connection(
                        [
                            (123, b"Turku", 0, "Q1757", "20260403010203"),
                        ]
                    )
                    sv_connection = _fake_connection(
                        [
                            (321, b"Esimerkki", 0, "Q42", "20260404020304"),
                        ]
                    )
                    pymysql_mock.connect.side_effect = [
                        user_list_connection,
                        centralauth_connection,
                        fi_connection,
                        sv_connection,
                    ]

                    records, _source_url = service_source.fetch_newpage_records(
                        wiki_domains=[
                            "cs.wikipedia.org",
                            "de.wikipedia.org",
                            "en.wikipedia.org",
                            "es.wikipedia.org",
                            "fi.wikipedia.org",
                            "fr.wikipedia.org",
                            "it.wikipedia.org",
                            "nl.wikipedia.org",
                            "pl.wikipedia.org",
                            "sv.wikipedia.org",
                        ],
                        timestamp="202604",
                        user_list_page=":w:fi:Wikipedia:Users",
                    )

        self.assertEqual([record["wiki_domain"] for record in records], ["sv.wikipedia.org", "fi.wikipedia.org"])
        self.assertEqual(len(pymysql_mock.connect.call_args_list), 4)
        self.assertEqual(
            [call.kwargs.get("database") for call in pymysql_mock.connect.call_args_list],
            [
                "fiwiki_p",
                "centralauth_p",
                "fiwiki_p",
                "svwiki_p",
            ],
        )

    def test_fetch_newpage_records_for_commons_excludes_file_namespace(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "specials": [
                                        {
                                            "url": "https://commons.wikimedia.org",
                                            "dbname": "commonswiki",
                                            "code": "commons",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {
                                        "6": {"*": "File"},
                                        "14": {"*": "Category"},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    commons_connection = _fake_connection(
                        [
                            (125, b"Example_category", 14, "Q42", "20260404020304"),
                        ]
                    )
                    pymysql_mock.connect.return_value = commons_connection

                    records, _source_url = service_source.fetch_newpage_records(
                        wiki_domains=["commons.wikimedia.org"],
                        timestamp="202604",
                    )

        self.assertEqual(records[0]["page_title"], "Category:Example_category")
        commons_sql, commons_params = commons_connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("FROM recentchanges_userindex AS rc", commons_sql)
        self.assertIn("JOIN page AS p ON p.page_id = rc.rc_cur_id", commons_sql)
        self.assertIn("SELECT rc.rc_cur_id AS page_id, p.page_title, p.page_namespace", commons_sql)
        self.assertIn("rc.rc_namespace <> %s", commons_sql)
        self.assertEqual(commons_params, ["wikibase_item", "mw.new", 6, "20260400000000", 50000])

    def test_fetch_newpage_records_for_incubator_uses_category_sortkey_qids_in_sql(self) -> None:
        with self.settings(WIKIDATA_LOOKUP_BACKEND="toolforge_sql"):
            with patch("newpages.service_source.urlopen") as urlopen_mock:
                urlopen_mock.side_effect = [
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "sitematrix": {
                                    "count": 1,
                                    "specials": [
                                        {
                                            "url": "https://incubator.wikimedia.org",
                                            "dbname": "incubatorwiki",
                                            "code": "incubator",
                                        }
                                    ],
                                }
                            }
                        ).encode("utf-8")
                    ),
                    _FakeHttpResponse(
                        json.dumps(
                            {
                                "query": {
                                    "general": {"articlepath": "/wiki/$1", "lang": "en"},
                                    "namespaces": {
                                        "0": {"*": ""},
                                    },
                                }
                            }
                        ).encode("utf-8")
                    ),
                ]

                with patch("newpages.service_source.pymysql") as pymysql_mock:
                    incubator_connection = _fake_connection_batches(
                        [
                            [],
                            [
                                (702, b"Wp/sms/Uusi_sivu", 0, "Q123", "20260405070809"),
                            ],
                        ]
                    )
                    pymysql_mock.connect.return_value = incubator_connection

                    records, _source_url = service_source.fetch_newpage_records(
                        wiki_domains=["incubator.wikimedia.org"],
                        timestamp="202604",
                    )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["page_title"], "Wp/sms/Uusi_sivu")
        self.assertEqual(records[0]["lang_code"], "sms")
        self.assertEqual(records[0]["site_url"], "https://incubator.wikimedia.org/wiki/Wp/sms/")
        self.assertEqual(records[0]["wiki_group"], "wikipedia")
        self.assertEqual(records[0]["wikidata_id"], "Q123")

        execute_calls = incubator_connection.cursor.return_value.__enter__.return_value.execute.call_args_list
        self.assertEqual(len(execute_calls), 2)

        primary_sql, primary_params = execute_calls[0].args
        self.assertIn("FROM recentchanges_userindex AS rc", primary_sql)
        self.assertIn("JOIN page AS p ON p.page_id = rc.rc_cur_id", primary_sql)
        self.assertIn("SELECT rc.rc_cur_id AS page_id, p.page_title, p.page_namespace, pp.pp_value", primary_sql)
        self.assertIn("JOIN page_props AS pp", primary_sql)
        self.assertNotIn("JOIN linktarget AS lt", primary_sql)
        self.assertEqual(
            primary_params,
            [
                "wikibase_item",
                "mw.new",
                "20260400000000",
                50000,
            ],
        )

        fallback_sql, fallback_params = execute_calls[1].args
        self.assertIn("FROM recentchanges_userindex AS rc", fallback_sql)
        self.assertIn("JOIN page AS p ON p.page_id = rc.rc_cur_id", fallback_sql)
        self.assertIn("LEFT JOIN page_props AS pp", fallback_sql)
        self.assertIn("JOIN linktarget AS lt", fallback_sql)
        self.assertIn("JOIN categorylinks AS cl", fallback_sql)
        self.assertIn("cl.cl_sortkey_prefix AS qid", fallback_sql)
        self.assertIn("pp.pp_value IS NULL", fallback_sql)
        self.assertIn("cl.cl_sortkey_prefix IS NOT NULL", fallback_sql)
        self.assertEqual(
            fallback_params,
            [
                "wikibase_item",
                "Maintenance:Wikidata_interwiki_links",
                "mw.new",
                "20260400000000",
                50000,
            ],
        )
