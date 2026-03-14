import io
import os
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, override_settings


def _build_connection_mock() -> MagicMock:
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)

    connection = MagicMock()
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = None
    connection.cursor.return_value = cursor_cm
    return connection


class CheckReplicaConnectionsCommandTests(SimpleTestCase):
    @override_settings(TOOLFORGE_REPLICA_CNF="$HOME/replica.my.cnf")
    @patch.dict(os.environ, {"HOME": "/home/toolforge"}, clear=False)
    @patch("petscan.management.commands.check_replica_connections.enrichment_sql.pymysql")
    def test_command_checks_all_expected_replica_hosts(self, pymysql_mock):
        pymysql_mock.connect.side_effect = [
            _build_connection_mock(),
            _build_connection_mock(),
            _build_connection_mock(),
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()

        call_command("check_replica_connections", stdout=stdout, stderr=stderr)

        self.assertEqual(pymysql_mock.connect.call_count, 3)
        connect_calls = pymysql_mock.connect.call_args_list
        self.assertEqual(
            [call.kwargs["host"] for call in connect_calls],
            [
                "fiwiki.web.db.svc.wikimedia.cloud",
                "wikidatawiki.web.db.svc.wikimedia.cloud",
                "commonswiki.web.db.svc.wikimedia.cloud",
            ],
        )
        self.assertEqual(
            [call.kwargs["database"] for call in connect_calls],
            ["fiwiki_p", "wikidatawiki_p", "commonswiki_p"],
        )
        self.assertTrue(all("user" not in call.kwargs for call in connect_calls))
        self.assertTrue(all("password" not in call.kwargs for call in connect_calls))
        self.assertTrue(
            all(
                call.kwargs["read_default_file"] == "/home/toolforge/replica.my.cnf"
                for call in connect_calls
            )
        )

        self.assertIn("Replica connectivity check passed for all sites.", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    @override_settings(TOOLFORGE_REPLICA_CNF="")
    @patch("petscan.management.commands.check_replica_connections.enrichment_sql.pymysql")
    def test_command_requires_replica_cnf_setting(self, pymysql_mock):
        with self.assertRaises(CommandError) as ctx:
            call_command("check_replica_connections")

        self.assertIn("TOOLFORGE_REPLICA_CNF is required", str(ctx.exception))
        pymysql_mock.connect.assert_not_called()

    @override_settings(TOOLFORGE_REPLICA_CNF="$HOME/replica.my.cnf")
    @patch.dict(os.environ, {"HOME": "/home/toolforge"}, clear=False)
    @patch("petscan.management.commands.check_replica_connections.enrichment_sql.pymysql")
    def test_command_fails_if_any_replica_connection_fails(self, pymysql_mock):
        pymysql_mock.connect.side_effect = [
            _build_connection_mock(),
            RuntimeError("boom"),
            _build_connection_mock(),
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()

        with self.assertRaises(CommandError) as ctx:
            call_command("check_replica_connections", stdout=stdout, stderr=stderr)

        self.assertIn("wikidatawiki", str(ctx.exception))
        self.assertIn("[FAIL] site=wikidatawiki", stderr.getvalue())
        self.assertEqual(pymysql_mock.connect.call_count, 3)
