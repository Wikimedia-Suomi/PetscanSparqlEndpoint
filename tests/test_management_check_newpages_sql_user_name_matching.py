import io
from typing import Any
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase

from newpages import service_source
from newpages.management.commands import check_newpages_sql_user_name_matching as command


def _fake_connection(row_batches: list[list[tuple[object, ...]]]) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchall.side_effect = row_batches

    connection = MagicMock()
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = None
    connection.cursor.return_value = cursor_cm
    return connection


class CheckNewpagesSqlUserNameMatchingCommandTests(SimpleTestCase):
    @patch("newpages.management.commands.check_newpages_sql_user_name_matching._load_user_name_match_rows")
    @patch("newpages.management.commands.check_newpages_sql_user_name_matching._resolve_target_user_names")
    def test_command_succeeds_when_exact_sql_matches_exist(
        self,
        resolve_target_user_names_mock: Any,
        load_user_name_match_rows_mock: Any,
    ) -> None:
        resolve_target_user_names_mock.return_value = ["Alice A", "Charlie C"]
        load_user_name_match_rows_mock.return_value = (
            "20260306000000",
            [
                command._SqlUserNameMatchRow(
                    wiki_domain="fi.wikipedia.org",
                    user_name="Alice A",
                    actor_exact_values=("Alice A",),
                    rc_exact_values=("Alice A",),
                    rc_exact_hits=2,
                    rc_exact_latest="20260405010203",
                ),
                command._SqlUserNameMatchRow(
                    wiki_domain="fi.wikipedia.org",
                    user_name="Charlie C",
                    actor_exact_values=("Charlie C",),
                    rc_exact_values=("Charlie C",),
                    rc_exact_hits=1,
                    rc_exact_latest="20260404020304",
                ),
            ],
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        call_command(
            "check_newpages_sql_user_name_matching",
            "--wiki",
            "fi.wikipedia.org",
            "--user",
            "Alice A",
            stdout=stdout,
            stderr=stderr,
        )

        resolve_target_user_names_mock.assert_called_once_with(["Alice A"], "")
        load_user_name_match_rows_mock.assert_called_once_with(["fi.wikipedia.org"], ["Alice A", "Charlie C"], 30)
        self.assertIn("target_wikis=1", stdout.getvalue())
        self.assertIn("target_users=2", stdout.getvalue())
        self.assertIn("wiki=fi.wikipedia.org user=Alice A status=match", stdout.getvalue())
        self.assertIn("actor_exact:      yes raw=Alice A", stdout.getvalue())
        self.assertIn("rc_exact:         hits=2 latest=20260405010203 raw=Alice A", stdout.getvalue())
        self.assertIn(
            "SQL user-name matching passed for 2 user/wiki checks.",
            stdout.getvalue(),
        )
        self.assertEqual(stderr.getvalue(), "")

    @patch("newpages.management.commands.check_newpages_sql_user_name_matching._load_user_name_match_rows")
    @patch("newpages.management.commands.check_newpages_sql_user_name_matching._resolve_target_user_names")
    def test_command_fails_when_no_exact_match_exists(
        self,
        resolve_target_user_names_mock: Any,
        load_user_name_match_rows_mock: Any,
    ) -> None:
        resolve_target_user_names_mock.return_value = ["Alice A"]
        load_user_name_match_rows_mock.return_value = (
            "20260306000000",
            [
                command._SqlUserNameMatchRow(
                    wiki_domain="fi.wikipedia.org",
                    user_name="Alice A",
                    actor_exact_values=(),
                    rc_exact_values=(),
                    rc_exact_hits=0,
                    rc_exact_latest="",
                )
            ],
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        with self.assertRaises(CommandError) as context:
            call_command(
                "check_newpages_sql_user_name_matching",
                "--wiki",
                "fi.wikipedia.org",
                "--user",
                "Alice A",
                stdout=stdout,
                stderr=stderr,
            )

        self.assertIn("SQL user-name matching failed for 1 user/wiki checks.", str(context.exception))
        self.assertIn("status=not_found", stdout.getvalue())
        self.assertIn(
            "wiki=fi.wikipedia.org user=Alice A status=not_found actor_exact_raw=- rc_exact_hits=0",
            stderr.getvalue(),
        )

    @patch("newpages.management.commands.check_newpages_sql_user_name_matching._timestamp_days_ago")
    @patch("newpages.management.commands.check_newpages_sql_user_name_matching.source._selected_wiki_descriptors")
    @patch("newpages.management.commands.check_newpages_sql_user_name_matching.source.pymysql")
    def test_load_user_name_match_rows_queries_exact_actor_and_actor_recentchanges(
        self,
        pymysql_mock: Any,
        selected_wiki_descriptors_mock: Any,
        timestamp_days_ago_mock: Any,
    ) -> None:
        timestamp_days_ago_mock.return_value = "20260306000000"
        selected_wiki_descriptors_mock.return_value = [
            service_source._WikiDescriptor(
                domain="fi.wikipedia.org",
                dbname="fiwiki",
                lang_code="fi",
                wiki_group="wikipedia",
                site_url="https://fi.wikipedia.org/",
            )
        ]
        connection = _fake_connection(
            [
                [("Alice A",)],
                [("Alice A", 2, "20260405010203")],
            ]
        )
        pymysql_mock.connect.return_value = connection

        threshold_timestamp, rows = command._load_user_name_match_rows(
            ["fi.wikipedia.org"],
            ["Alice A"],
            days=30,
        )

        self.assertEqual(threshold_timestamp, "20260306000000")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].wiki_domain, "fi.wikipedia.org")
        self.assertEqual(rows[0].user_name, "Alice A")
        self.assertEqual(rows[0].actor_exact_values, ("Alice A",))
        self.assertEqual(rows[0].rc_exact_values, ("Alice A",))
        self.assertEqual(rows[0].rc_exact_hits, 2)
        self.assertEqual(command._row_status(rows[0]), "match")

        cursor = connection.cursor.return_value.__enter__.return_value
        self.assertEqual(cursor.execute.call_count, 2)
        actor_exact_sql, actor_exact_params = cursor.execute.call_args_list[0].args
        rc_exact_sql, rc_exact_params = cursor.execute.call_args_list[1].args

        self.assertIn("SELECT actor_name FROM actor", actor_exact_sql)
        self.assertEqual(actor_exact_params, ["Alice A"])
        self.assertIn("FROM recentchanges_userindex AS rc", rc_exact_sql)
        self.assertIn("JOIN actor_recentchanges AS a ON rc.rc_actor = a.actor_id", rc_exact_sql)
        self.assertIn("SELECT a.actor_name, COUNT(*), MAX(rc.rc_timestamp)", rc_exact_sql)
        self.assertEqual(rc_exact_params, ["20260306000000", "Alice A"])

    def test_resolve_target_user_names_merges_cli_and_user_list_page_users(self) -> None:
        with patch("newpages.management.commands.check_newpages_sql_user_name_matching.source.normalize_user_list_page") as normalize_mock:
            with patch("newpages.management.commands.check_newpages_sql_user_name_matching.source._resolve_user_list_page") as resolve_mock:
                with patch("newpages.management.commands.check_newpages_sql_user_name_matching.source._fetch_user_names_for_page") as fetch_mock:
                    normalize_mock.return_value = ":w:fi:Wikipedia:Users"
                    resolve_mock.return_value = object()
                    fetch_mock.return_value = ["Charlie C", "Alice A"]

                    user_names = command._resolve_target_user_names(
                        ["Alice_A,Bob B"],
                        ":w:fi:Wikipedia:Users",
                    )

        self.assertEqual(user_names, ["Alice A", "Bob B", "Charlie C"])
