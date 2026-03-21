from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from petscan import checks


class RunserverDebugChecksTests(SimpleTestCase):
    def test_returns_warning_when_runserver_uses_debug_off(self) -> None:
        with override_settings(DEBUG=False):
            with patch.object(checks.sys, "argv", ["manage.py", "runserver"]):
                messages = checks.warn_if_runserver_without_debug(None)

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].id, "petscan.W001")
        self.assertEqual(
            messages[0].msg,
            "DJANGO_DEBUG should be enabled when using manage.py runserver.",
        )
        self.assertIn("DJANGO_DEBUG=1", messages[0].hint)

    def test_does_not_warn_when_debug_is_enabled(self) -> None:
        with override_settings(DEBUG=True):
            with patch.object(checks.sys, "argv", ["manage.py", "runserver"]):
                messages = checks.warn_if_runserver_without_debug(None)

        self.assertEqual(messages, [])

    def test_does_not_warn_for_other_management_commands(self) -> None:
        with override_settings(DEBUG=False):
            with patch.object(checks.sys, "argv", ["manage.py", "test"]):
                messages = checks.warn_if_runserver_without_debug(None)

        self.assertEqual(messages, [])
