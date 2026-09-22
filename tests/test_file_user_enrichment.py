from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from petscan import file_user_enrichment


class FileUserEnrichmentTests(SimpleTestCase):
    def test_file_user_name_falls_back_to_extended_data_metadata(self) -> None:
        self.assertEqual(
            file_user_enrichment.file_user_name(
                {"metadata": {"img_user_text": "example_Uploader"}}
            ),
            "Example Uploader",
        )

    @override_settings(  # type: ignore[untyped-decorator]
        PETSCAN_USER_REGISTRATION_LOOKUP_BACKEND="api",
        CENTRALAUTH_API_ENDPOINT="https://meta.wikimedia.org/w/api.php",
    )
    @patch("petscan.file_user_enrichment.enrichment_api.fetch_global_user_registrations_api")
    def test_api_backend_returns_normalized_global_user_registrations(
        self,
        api_fetch_mock: Any,
    ) -> None:
        api_fetch_mock.return_value = {
            "New uploader": "2026-01-16T12:00:00Z",
            "Old uploader": "20240101000000",
        }
        records = [
            {"img_user_text": "New_uploader"},
            {"img_user_text": "Old uploader"},
            {"img_user_text": "Missing uploader"},
            {"img_user_text": "New uploader"},
        ]

        result = file_user_enrichment.build_img_user_registration_by_name(records)

        self.assertEqual(
            result,
            {
                "New uploader": "2026-01-16T12:00:00Z",
                "Old uploader": "2024-01-01T00:00:00Z",
            },
        )
        api_fetch_mock.assert_called_once_with(
            "https://meta.wikimedia.org/w/api.php",
            ["New uploader", "Old uploader", "Missing uploader"],
            user_agent=file_user_enrichment.HTTP_USER_AGENT,
            timeout_seconds=120,
        )
        self.assertEqual(
            file_user_enrichment.img_user_registration_for_record(records[0], result),
            "2026-01-16T12:00:00Z",
        )
        self.assertIsNone(
            file_user_enrichment.img_user_registration_for_record(records[2], result)
        )

    @patch("petscan.file_user_enrichment._fetch_registrations")
    def test_only_first_username_character_is_case_insensitive(
        self,
        fetch_registrations_mock: Any,
    ) -> None:
        fetch_registrations_mock.return_value = {
            "CaseUser": "20200101000000",
            "CASEUSER": "20210101000000",
        }
        records = [
            {"img_user_text": "caseUser"},
            {"img_user_text": "CaseUser"},
            {"img_user_text": "CASEUSER"},
        ]

        result = file_user_enrichment.build_img_user_registration_by_name(records)

        fetch_registrations_mock.assert_called_once_with(["CaseUser", "CASEUSER"])
        self.assertEqual(
            result,
            {
                "CaseUser": "2020-01-01T00:00:00Z",
                "CASEUSER": "2021-01-01T00:00:00Z",
            },
        )
        self.assertEqual(
            file_user_enrichment.img_user_registration_for_record(records[0], result),
            "2020-01-01T00:00:00Z",
        )
        self.assertEqual(
            file_user_enrichment.img_user_registration_for_record(records[2], result),
            "2021-01-01T00:00:00Z",
        )

    @override_settings(  # type: ignore[untyped-decorator]
        PETSCAN_USER_REGISTRATION_LOOKUP_BACKEND="toolforge_sql"
    )
    @patch("petscan.file_user_enrichment.enrichment_sql.fetch_global_user_registrations_sql")
    def test_toolforge_backend_uses_centralauth_sql(self, sql_fetch_mock: Any) -> None:
        sql_fetch_mock.return_value = {"Uploader": "20260201000000"}

        result = file_user_enrichment.build_img_user_registration_by_name(
            [{"img_user_text": "Uploader"}]
        )

        self.assertEqual(result, {"Uploader": "2026-02-01T00:00:00Z"})
        sql_fetch_mock.assert_called_once_with(
            ["Uploader"],
            timeout_seconds=120,
            replica_cnf="",
        )
