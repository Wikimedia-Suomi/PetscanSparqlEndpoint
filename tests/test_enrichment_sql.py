import os
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from petscan import enrichment_sql
from petscan.service_errors import GilLinkEnrichmentError


class EnrichmentSqlTests(SimpleTestCase):
    @patch("petscan.enrichment_sql.pymysql")
    def test_commons_category_lookup_uses_links_and_core_replica_hosts(
        self,
        pymysql_mock,
    ):
        links_cursor = MagicMock()
        links_cursor.fetchall.return_value = [
            (0, b"Example", b"Featured_pictures", 501),
            (0, b"Example", b"Featured_pictures", 501),
            (0, b"Example", b"Quality_images", 502),
        ]
        links_connection = MagicMock()
        links_cursor_cm = MagicMock()
        links_cursor_cm.__enter__.return_value = links_cursor
        links_cursor_cm.__exit__.return_value = None
        links_connection.cursor.return_value = links_cursor_cm

        core_cursor = MagicMock()
        core_cursor.fetchall.return_value = [
            (501, b"wikibase_item", b"Q123"),
            (501, b"hiddencat", b""),
        ]
        core_connection = MagicMock()
        core_cursor_cm = MagicMock()
        core_cursor_cm.__enter__.return_value = core_cursor
        core_cursor_cm.__exit__.return_value = None
        core_connection.cursor.return_value = core_cursor_cm
        pymysql_mock.connect.side_effect = [links_connection, core_connection]

        resolved = enrichment_sql.fetch_page_categories_with_wikidata_sql(
            "commonswiki",
            [(0, "Example", "Example")],
            timeout_seconds=5,
            replica_cnf="$HOME/replica.my.cnf",
        )

        self.assertEqual(
            resolved,
            {
                "Example": [
                    {
                        "title": "Category:Featured_pictures",
                        "hiddencat": True,
                        "wikidata_id": "Q123",
                    },
                    {
                        "title": "Category:Quality_images",
                        "hiddencat": False,
                        "wikidata_id": None,
                    },
                ]
            },
        )
        self.assertEqual(
            [call.kwargs["host"] for call in pymysql_mock.connect.call_args_list],
            [
                "links.commonswiki.web.db.svc.wikimedia.cloud",
                "commonswiki.web.db.svc.wikimedia.cloud",
            ],
        )
        category_sql = links_cursor.execute.call_args.args[0]
        category_property_sql = core_cursor.execute.call_args_list[0].args[0]
        self.assertIn("JOIN categorylinks AS cl", category_sql)
        self.assertIn("JOIN linktarget AS lt", category_sql)
        self.assertIn("LEFT JOIN page AS category_page", category_sql)
        self.assertNotIn("sortkey", category_sql)
        self.assertNotIn("page_props", category_sql)
        self.assertIn("FROM page_props AS pp", category_property_sql)
        self.assertIn("pp.pp_page IN", category_property_sql)
        self.assertIn("hiddencat", core_cursor.execute.call_args_list[0].args[1])
        self.assertIn(501, core_cursor.execute.call_args_list[0].args[1])
        self.assertIn(502, core_cursor.execute.call_args_list[0].args[1])
        self.assertNotIn("defaultsort", category_property_sql)
        self.assertNotIn("categorylinks", category_property_sql)
        links_connection.close.assert_called_once()
        core_connection.close.assert_called_once()

    @patch("petscan.enrichment_sql.pymysql")
    def test_item_category_lookup_uses_source_and_category_page_ids(self, pymysql_mock):
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [
                (42, 701),
                (42, 702),
            ],
            [
                (701, b"Featured_pictures", 501),
                (702, b"Redlink_category", None),
            ],
            [
                (501, b"wikibase_item", b"Q123"),
                (501, b"hiddencat", b""),
            ],
        ]
        connection = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None
        connection.cursor.return_value = cursor_cm
        pymysql_mock.connect.return_value = connection

        resolved = enrichment_sql.fetch_page_categories_with_wikidata_sql(
            "enwiki",
            [(0, "Example", "Example")],
            timeout_seconds=5,
            page_ids_by_title={"Example": 42},
        )

        self.assertEqual(
            resolved,
            {
                "Example": [
                    {
                        "title": "Category:Featured_pictures",
                        "hiddencat": True,
                        "wikidata_id": "Q123",
                    },
                    {
                        "title": "Category:Redlink_category",
                        "hiddencat": False,
                        "wikidata_id": None,
                    },
                ]
            },
        )
        category_links_sql, category_links_params = cursor.execute.call_args_list[0].args
        category_metadata_sql, category_metadata_params = cursor.execute.call_args_list[1].args
        property_sql, property_params = cursor.execute.call_args_list[2].args
        self.assertIn("SELECT cl.cl_from, cl.cl_target_id", category_links_sql)
        self.assertNotIn("JOIN linktarget", category_links_sql)
        self.assertEqual(category_links_params, [42])
        self.assertIn("lt.lt_id IN", category_metadata_sql)
        self.assertIn("LEFT JOIN page AS category_page", category_metadata_sql)
        self.assertEqual(category_metadata_params, [701, 702])
        self.assertIn("pp.pp_page IN", property_sql)
        self.assertEqual(property_params, ["wikibase_item", "hiddencat", 501])

    @patch("petscan.enrichment_sql.pymysql")
    def test_item_category_lookup_fetches_all_source_page_ids_at_once(
        self,
        pymysql_mock,
    ):
        page_count = enrichment_sql._GIL_CATEGORY_SQL_BATCH_SIZE + 1
        targets = [
            (0, "Example_{}".format(page_id), "Example_{}".format(page_id))
            for page_id in range(1, page_count + 1)
        ]
        page_ids_by_title = {
            "Example_{}".format(page_id): page_id
            for page_id in range(1, page_count + 1)
        }

        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [(1, 701), (page_count, 701)],
            [(701, b"Shared_category", 501)],
            [],
        ]
        connection = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None
        connection.cursor.return_value = cursor_cm
        pymysql_mock.connect.return_value = connection

        resolved = enrichment_sql.fetch_page_categories_with_wikidata_sql(
            "enwiki",
            targets,
            timeout_seconds=5,
            page_ids_by_title=page_ids_by_title,
        )

        category_links_calls = [
            call
            for call in cursor.execute.call_args_list
            if "FROM categorylinks AS cl" in call.args[0]
        ]
        self.assertEqual(len(category_links_calls), 1)
        self.assertEqual(
            category_links_calls[0].args[1],
            list(range(1, page_count + 1)),
        )
        category_metadata_calls = [
            call
            for call in cursor.execute.call_args_list
            if "FROM linktarget AS lt" in call.args[0]
        ]
        self.assertEqual(len(category_metadata_calls), 1)
        self.assertEqual(category_metadata_calls[0].args[1], [701])
        self.assertEqual(
            resolved["Example_1"],
            [
                {
                    "title": "Category:Shared_category",
                    "hiddencat": False,
                    "wikidata_id": None,
                }
            ],
        )
        self.assertEqual(resolved["Example_2"], [])
        self.assertEqual(resolved["Example_{}".format(page_count)], resolved["Example_1"])

    @patch("petscan.enrichment_sql.pymysql")
    def test_fetch_global_user_registrations_uses_centralauth_replica(self, pymysql_mock):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (b"New uploader", b"20260315100000"),
            (b"No registration", None),
        ]

        connection = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None
        connection.cursor.return_value = cursor_cm
        pymysql_mock.connect.return_value = connection

        resolved = enrichment_sql.fetch_global_user_registrations_sql(
            ["New uploader", "No registration"],
            timeout_seconds=5,
            replica_cnf="$HOME/replica.my.cnf",
        )

        self.assertEqual(resolved, {"New uploader": "20260315100000"})
        connect_kwargs = pymysql_mock.connect.call_args.kwargs
        self.assertEqual(
            connect_kwargs["host"],
            "centralauth.web.db.svc.wikimedia.cloud",
        )
        self.assertEqual(connect_kwargs["database"], "centralauth_p")
        self.assertEqual(
            connect_kwargs["read_default_file"],
            os.path.expanduser(os.path.expandvars("$HOME/replica.my.cnf")),
        )
        sql, params = cursor.execute.call_args.args
        self.assertIn("FROM globaluser", sql)
        self.assertIn("gu_registration", sql)
        self.assertEqual(params, ["New uploader", "No registration"])
        connection.close.assert_called_once()

    @patch("petscan.enrichment_sql.pymysql")
    def test_fetch_wikibase_items_raises_on_sql_error(self, pymysql_mock):
        pymysql_mock.connect.side_effect = RuntimeError("db down")

        with self.assertRaisesMessage(
            GilLinkEnrichmentError,
            "Wikibase enrichment SQL query failed for site enwiki",
        ) as captured:
            enrichment_sql.fetch_wikibase_items_for_site_sql(
                "enwiki",
                [(0, "Albert_Einstein", "Albert_Einstein")],
                timeout_seconds=5,
            )

        self.assertEqual(
            captured.exception.public_message,
            "Failed to enrich linked pages from the replica database.",
        )

    @patch("petscan.enrichment_sql.pymysql")
    def test_fetch_wikibase_items_allows_successful_empty_sql_response(self, pymysql_mock):
        cursor = MagicMock()
        cursor.fetchall.return_value = []

        connection = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None
        connection.cursor.return_value = cursor_cm
        pymysql_mock.connect.return_value = connection

        resolved = enrichment_sql.fetch_wikibase_items_for_site_sql(
            "enwiki",
            [(0, "Albert_Einstein", "Albert_Einstein")],
            timeout_seconds=5,
        )

        self.assertEqual(resolved, {})

    @patch.dict(os.environ, {"TOOLFORGE_REPLICA_CNF": "$HOME/replica.my.cnf"}, clear=False)
    @patch("petscan.enrichment_sql.pymysql")
    def test_fetch_wikibase_items_uses_site_specific_host_and_cnf_only(self, pymysql_mock):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (0, "Albert_Einstein", 736, "Q937", 886543, "20260314235959")
        ]

        connection = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None
        connection.cursor.return_value = cursor_cm
        pymysql_mock.connect.return_value = connection

        resolved = enrichment_sql.fetch_wikibase_items_for_site_sql(
            "fiwiki",
            [(0, "Albert_Einstein", "Albert_Einstein")],
            timeout_seconds=5,
            replica_cnf=os.environ["TOOLFORGE_REPLICA_CNF"],
        )

        self.assertEqual(
            resolved,
            {
                "Albert_Einstein": {
                    "wikidata_id": "Q937",
                    "page_len": 886543,
                    "rev_timestamp": "20260314235959",
                    "page_id": 736,
                }
            },
        )
        connect_kwargs = pymysql_mock.connect.call_args.kwargs
        self.assertEqual(connect_kwargs.get("host"), "fiwiki.web.db.svc.wikimedia.cloud")
        self.assertEqual(connect_kwargs.get("database"), "fiwiki_p")
        self.assertEqual(
            connect_kwargs.get("read_default_file"),
            os.path.expanduser(os.path.expandvars(os.environ["TOOLFORGE_REPLICA_CNF"])),
        )
        self.assertNotIn("user", connect_kwargs)
        self.assertNotIn("password", connect_kwargs)

    @patch.dict(os.environ, {"TOOLFORGE_REPLICA_CNF": "$HOME/replica.my.cnf"}, clear=False)
    @patch("petscan.enrichment_sql.pymysql")
    def test_fetch_wikibase_items_ignores_invalid_site_token(self, pymysql_mock):
        resolved = enrichment_sql.fetch_wikibase_items_for_site_sql(
            "fiwiki.bad/host",
            [(0, "Albert_Einstein", "Albert_Einstein")],
            timeout_seconds=5,
            replica_cnf=os.environ["TOOLFORGE_REPLICA_CNF"],
        )

        self.assertEqual(resolved, {})
        pymysql_mock.connect.assert_not_called()

    @patch("petscan.enrichment_sql.pymysql")
    def test_fetch_wikibase_items_decodes_binary_page_titles_from_sql(self, pymysql_mock):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (0, b"S\xc3\xa3o_Paulo", 1, "Q174", 1523411, b"20200101112233"),
            (0, b"\xc5\x81\xc3\xb3d\xc5\xba", 2, "Q580", 889221, b"20210102112233"),
            (0, b"Beyonc\xc3\xa9", 3, "Q36153", 55320, b"20220103112233"),
        ]

        connection = MagicMock()
        cursor_cm = MagicMock()
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None
        connection.cursor.return_value = cursor_cm
        pymysql_mock.connect.return_value = connection

        resolved = enrichment_sql.fetch_wikibase_items_for_site_sql(
            "enwiki",
            [
                (0, "São_Paulo", "São_Paulo"),
                (0, "Łódź", "Łódź"),
                (0, "Beyoncé", "Beyoncé"),
            ],
            timeout_seconds=5,
        )

        self.assertEqual(
            resolved,
            {
                "São_Paulo": {
                    "wikidata_id": "Q174",
                    "page_len": 1523411,
                    "rev_timestamp": "20200101112233",
                    "page_id": 1,
                },
                "Łódź": {
                    "wikidata_id": "Q580",
                    "page_len": 889221,
                    "rev_timestamp": "20210102112233",
                    "page_id": 2,
                },
                "Beyoncé": {
                    "wikidata_id": "Q36153",
                    "page_len": 55320,
                    "rev_timestamp": "20220103112233",
                    "page_id": 3,
                },
            },
        )
