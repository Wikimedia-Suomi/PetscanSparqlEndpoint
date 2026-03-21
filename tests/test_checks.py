from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.test import SimpleTestCase, override_settings

from petscan import checks


class RunserverDebugChecksTests(SimpleTestCase):
    def test_returns_error_when_runserver_uses_debug_off(self) -> None:
        with override_settings(DEBUG=False):
            with patch.object(checks.sys, "argv", ["manage.py", "runserver"]):
                messages = checks.error_if_runserver_without_debug(None)

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].id, "petscan.E001")
        self.assertEqual(
            messages[0].msg,
            "DJANGO_DEBUG must be enabled when using manage.py runserver.",
        )
        self.assertIn("DJANGO_DEBUG=1", messages[0].hint)

    def test_check_command_fails_for_runserver_when_debug_is_disabled(self) -> None:
        with override_settings(DEBUG=False):
            with patch.object(checks.sys, "argv", ["manage.py", "runserver"]):
                with self.assertRaises(SystemCheckError) as raised:
                    call_command("check")

        self.assertIn("petscan.E001", str(raised.exception))
        self.assertIn("DJANGO_DEBUG=1", str(raised.exception))

    def test_does_not_error_when_debug_is_enabled(self) -> None:
        with override_settings(DEBUG=True):
            with patch.object(checks.sys, "argv", ["manage.py", "runserver"]):
                messages = checks.error_if_runserver_without_debug(None)

        self.assertEqual(messages, [])

    def test_does_not_error_for_other_management_commands(self) -> None:
        with override_settings(DEBUG=False):
            with patch.object(checks.sys, "argv", ["manage.py", "test"]):
                messages = checks.error_if_runserver_without_debug(None)

        self.assertEqual(messages, [])
