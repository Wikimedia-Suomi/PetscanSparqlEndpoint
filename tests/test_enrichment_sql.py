import os
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from petscan import enrichment_sql


class EnrichmentSqlTests(SimpleTestCase):
    @patch.dict(os.environ, {"TOOLFORGE_REPLICA_CNF": "$HOME/replica.my.cnf"}, clear=False)
    @patch("petscan.enrichment_sql.pymysql")
    def test_fetch_wikibase_items_uses_site_specific_host_and_cnf_only(self, pymysql_mock):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(0, "Albert_Einstein", "Q937", 886543, "20260314235959")]

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
            (0, b"S\xc3\xa3o_Paulo", "Q174", 1523411, b"20200101112233"),
            (0, b"\xc5\x81\xc3\xb3d\xc5\xba", "Q580", 889221, b"20210102112233"),
            (0, b"Beyonc\xc3\xa9", "Q36153", 55320, b"20220103112233"),
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
                },
                "Łódź": {
                    "wikidata_id": "Q580",
                    "page_len": 889221,
                    "rev_timestamp": "20210102112233",
                },
                "Beyoncé": {
                    "wikidata_id": "Q36153",
                    "page_len": 55320,
                    "rev_timestamp": "20220103112233",
                },
            },
        )
