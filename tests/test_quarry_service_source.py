import json
from typing import Any
from unittest.mock import patch

from quarry import service_source
from tests.service_test_support import ServiceTestCase


class QuarryServiceSourceTests(ServiceTestCase):
    def test_extract_qrun_id_from_quarry_html_script_block(self) -> None:
        html = """
        <html>
          <script>
            var vars = {"can_edit": false, "published": true, "qrun_id": 1084251, "query_id": 103479};
          </script>
        </html>
        """

        self.assertEqual(service_source.extract_qrun_id(html), 1084251)

    def test_extract_query_db_name_from_query_db_input(self) -> None:
        html = """
        <div class="query-db-parent">
          <input id="query-db" placeholder="Enter the db name here..." value="fiwiki_p" />
        </div>
        """

        self.assertEqual(service_source.extract_query_db_name(html), "fiwiki_p")

    def test_extract_qrun_id_raises_when_missing(self) -> None:
        with self.assertRaisesMessage(
            service_source.PetscanServiceError,
            "Could not locate qrun_id in Quarry query page.",
        ):
            service_source.extract_qrun_id("<html><body>No vars block here.</body></html>")

    @patch("quarry.service_source.fetch_quarry_query_html")
    @patch("quarry.service_source._bundled_quarry_example_for_query_id", return_value=None)
    def test_resolve_quarry_run_includes_query_db_name(
        self,
        _bundled_example_mock: Any,
        fetch_quarry_query_html_mock: Any,
    ) -> None:
        fetch_quarry_query_html_mock.return_value = (
            """
            <html>
              <script>
                var vars = {"qrun_id": 1084251, "query_id": 103479};
              </script>
              <div class="query-db-parent">
                <input id="query-db" value="fiwiki_p" />
              </div>
            </html>
            """,
            "https://quarry.wmcloud.org/query/103479",
        )

        resolved = service_source.resolve_quarry_run(103479)

        self.assertEqual(resolved["qrun_id"], 1084251)
        self.assertEqual(resolved["query_db"], "fiwiki_p")

    @patch("quarry.service_source.fetch_quarry_query_html")
    @patch("quarry.service_source._bundled_quarry_example_for_query_id")
    def test_resolve_quarry_run_prefers_bundled_example_when_available(
        self,
        bundled_example_mock: Any,
        fetch_quarry_query_html_mock: Any,
    ) -> None:
        bundled_example_mock.return_value = {
            "quarry_id": 103479,
            "qrun_id": 1084300,
            "query_db": "fiwiki_p",
            "file_name": "quarry-103479-run-1084300.json.gz",
        }

        resolved = service_source.resolve_quarry_run(103479)

        self.assertEqual(resolved["qrun_id"], 1084300)
        self.assertEqual(resolved["query_db"], "fiwiki_p")
        self.assertEqual(resolved["query_url"], "https://quarry.wmcloud.org/query/103479")
        self.assertEqual(resolved["json_url"], "https://quarry.wmcloud.org/run/1084300/output/0/json")
        fetch_quarry_query_html_mock.assert_not_called()

    def test_extract_records_maps_headers_and_applies_limit(self) -> None:
        payload = {
            "meta": {"run_id": 1084251, "query_id": 103479},
            "headers": ["rc_title", "rc_namespace", "wikibase_item"],
            "rows": [
                ["Marvel_Cinematic_Universe:_Toinen_vaihe", 0, "Q51963356"],
                ["Aleksios_II_Komnenos", 0, "Q41849"],
            ],
        }

        self.assertEqual(
            service_source.extract_records(payload, limit=1),
            [
                {
                    "rc_title": "Marvel_Cinematic_Universe:_Toinen_vaihe",
                    "rc_namespace": 0,
                    "wikibase_item": "Q51963356",
                }
            ],
        )

    def test_extract_records_deduplicates_duplicate_headers(self) -> None:
        payload = {
            "headers": ["title", "title", ""],
            "rows": [["first", "second", "third"]],
        }

        self.assertEqual(
            service_source.extract_records(payload),
            [{"title": "first", "title_2": "second", "column_3": "third"}],
        )

    def test_extract_records_normalizes_headers_to_ascii_word_chars(self) -> None:
        payload = {
            "headers": ["Title (fi) / 2024", "äää", "a---b", "a___b", "title!"],
            "rows": [["first", "second", "third", "fourth", "fifth"]],
        }

        self.assertEqual(
            service_source.extract_records(payload),
            [
                {
                    "Title_fi_2024": "first",
                    "_": "second",
                    "a_b": "third",
                    "a_b_2": "fourth",
                    "title_": "fifth",
                }
            ],
        )

    def test_extract_records_normalizes_mapping_row_keys_to_ascii_word_chars(self) -> None:
        payload = {
            "headers": ["ignored"],
            "rows": [
                {
                    "Name (fi)": "Turku",
                    "a---b": 1,
                    "a___b": 2,
                }
            ],
        }

        self.assertEqual(
            service_source.extract_records(payload),
            [{"Name_fi_": "Turku", "a_b": 1, "a_b_2": 2}],
        )

    def test_normalize_load_limit_supports_blank_and_positive_values(self) -> None:
        self.assertIsNone(service_source.normalize_load_limit(""))
        self.assertEqual(service_source.normalize_load_limit("25"), 25)

        with self.assertRaisesMessage(ValueError, "limit must be greater than zero."):
            service_source.normalize_load_limit("0")

    @patch("quarry.service_source.urlopen")
    def test_fetch_quarry_json_returns_payload_and_source_url(self, urlopen_mock: Any) -> None:
        response = urlopen_mock.return_value.__enter__.return_value
        response.read.return_value = json.dumps(
            {
                "meta": {"run_id": 1084251, "query_id": 103479},
                "headers": ["rc_title"],
                "rows": [["Example_title"]],
            }
        ).encode("utf-8")

        payload, source_url = service_source.fetch_quarry_json(1084251)

        self.assertEqual(payload["meta"]["query_id"], 103479)
        self.assertEqual(source_url, "https://quarry.wmcloud.org/run/1084251/output/0/json")

        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.full_url, source_url)
        self.assertEqual(dict(request.header_items())["Accept"], "application/json")
        self.assertEqual(dict(request.header_items())["User-agent"], service_source.HTTP_USER_AGENT)

    @patch("quarry.service_source.urlopen")
    @patch("quarry.service_source._load_bundled_quarry_example_payload")
    @patch("quarry.service_source._bundled_quarry_example_for_qrun_id")
    def test_fetch_quarry_json_prefers_bundled_example_when_available(
        self,
        bundled_example_mock: Any,
        load_payload_mock: Any,
        urlopen_mock: Any,
    ) -> None:
        bundled_example_mock.return_value = {
            "quarry_id": 103479,
            "qrun_id": 1084300,
            "query_db": "fiwiki_p",
            "file_name": "quarry-103479-run-1084300.json.gz",
        }
        load_payload_mock.return_value = {
            "meta": {"run_id": 1084300, "query_id": 103479},
            "headers": ["rc_title"],
            "rows": [["Example_title"]],
        }

        payload, source_url = service_source.fetch_quarry_json(1084300)

        self.assertEqual(payload["meta"]["query_id"], 103479)
        self.assertEqual(source_url, "https://quarry.wmcloud.org/run/1084300/output/0/json")
        urlopen_mock.assert_not_called()
