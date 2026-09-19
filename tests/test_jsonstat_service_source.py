import json
from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase

from jsonstat import service_source as source
from petscan.service_errors import PetscanServiceError
from tests.service_test_support import ServiceTestCase

EXAMPLE_URL = "https://pxdata.stat.fi/PxWeb/sq/552d1f53-bdab-472b-a8e7-68b5b8c37cda"


def _small_payload() -> dict[str, Any]:
    return {
        "version": "2.0",
        "class": "dataset",
        "id": ["area", "year"],
        "size": [2, 2],
        "dimension": {
            "area": {
                "label": "Area",
                "category": {
                    "index": ["FI", "SE"],
                    "label": {"FI": "Finland", "SE": "Sweden"},
                    "coordinates": {"FI": [25.0, 64.0]},
                },
            },
            "year": {
                "label": "Year",
                "category": {
                    "index": {"2024": 0, "2025": 1},
                    "label": {"2024": "2024", "2025": "2025"},
                },
            },
        },
        "value": [1, None, 3.5, 4],
        "status": {"1": "."},
    }


class JsonstatSourceTests(ServiceTestCase):
    def test_extract_records_from_statistics_finland_example(self) -> None:
        payload = self._load_payload("jsonstat-552d1f53.json")

        records = source.extract_records(payload)

        self.assertEqual(len(records), 7)
        self.assertEqual(
            records[0],
            {
                "timeperiod_y": "1990",
                "timeperiod_y_label": "1990",
                "ikaryhma_10_20180101": "SSS",
                "ikaryhma_10_20180101_label": "Total",
                "contentscode": "akuo_lkm",
                "contentscode_label": "Participants in adult education and training, number",
                "contentscode_note": [
                    "Participants in adult education and training, population aged 18 to 64.\r\n"
                    "Adult education refers to training arranged and organised specifically for "
                    "adults. Includes studying abroad excl. the year 1990. Until 2017, only such "
                    "training is included where participation was at least six hours."
                ],
                "contentscode_unit_base": "number",
                "contentscode_unit_decimals": 0,
                "value": 1526457,
            },
        )
        self.assertEqual(records[-1]["timeperiod_y"], "2022")
        self.assertEqual(records[-1]["value"], 1452840)

    def test_extract_records_uses_jsonstat_row_major_order_and_status(self) -> None:
        records = source.extract_records(_small_payload())

        self.assertEqual(
            [(row["area"], row["year"], row["value"]) for row in records],
            [
                ("FI", "2024", 1),
                ("FI", "2025", None),
                ("SE", "2024", 3.5),
                ("SE", "2025", 4),
            ],
        )
        self.assertEqual(records[0]["area_longitude"], 25.0)
        self.assertEqual(records[0]["area_latitude"], 64.0)
        self.assertEqual(records[1]["status"], ".")
        self.assertNotIn("status", records[0])

    def test_extract_records_supports_sparse_values_and_constant_status(self) -> None:
        payload = _small_payload()
        payload["value"] = {"0": 10, "3": 40}
        payload["status"] = "estimated"

        records = source.extract_records(payload)

        self.assertEqual([row["value"] for row in records], [10, None, None, 40])
        self.assertEqual([row["status"] for row in records], ["estimated"] * 4)

    def test_extract_records_supports_constant_dimension_without_index(self) -> None:
        payload = {
            "version": "2.0",
            "class": "dataset",
            "id": ["metric"],
            "size": [1],
            "dimension": {
                "metric": {
                    "category": {
                        "label": {"population": "Population"},
                        "unit": {"population": {"label": "persons", "decimals": 0}},
                    }
                }
            },
            "value": [123],
        }

        self.assertEqual(
            source.extract_records(payload),
            [
                {
                    "metric": "population",
                    "metric_label": "Population",
                    "metric_unit_decimals": 0,
                    "metric_unit_label": "persons",
                    "value": 123,
                }
            ],
        )

    def test_extract_records_normalizes_dimension_field_names_and_avoids_reserved_names(
        self,
    ) -> None:
        payload = {
            "version": "2.0",
            "class": "dataset",
            "id": ["value", "Ää / region"],
            "size": [1, 1],
            "dimension": {
                "value": {"category": {"index": ["metric"]}},
                "Ää / region": {"category": {"index": ["north"]}},
            },
            "value": [1],
        }

        self.assertEqual(
            source.extract_records(payload)[0],
            {
                "value_2": "metric",
                "value_2_label": "metric",
                "region": "north",
                "region_label": "north",
                "value": 1,
            },
        )

    def test_extract_records_rejects_invalid_shape(self) -> None:
        payload = _small_payload()
        payload["value"] = [1]
        with self.assertRaisesMessage(
            PetscanServiceError,
            "JSON-stat value array length does not match dataset size.",
        ):
            source.extract_records(payload)

        payload = _small_payload()
        payload["version"] = "1.0"
        with self.assertRaisesMessage(PetscanServiceError, "must use version 2.0"):
            source.extract_records(payload)

    def test_extract_records_enforces_cell_limit(self) -> None:
        with self.settings(JSONSTAT_MAX_CELLS=3):
            with self.assertRaisesMessage(
                PetscanServiceError, "configured maximum is 3"
            ) as captured:
                source.extract_records(_small_payload())
        self.assertEqual(
            captured.exception.public_message, "The JSON-stat dataset contains too many cells."
        )

    def test_source_token_round_trip_uses_normalized_url(self) -> None:
        token = source.encode_source_token(" HTTPS://PXDATA.STAT.FI/PxWeb/sq/example ")
        self.assertEqual(
            source.decode_source_token(token), "https://pxdata.stat.fi/PxWeb/sq/example"
        )

    def test_source_url_allows_configured_domain_and_its_subdomains(self) -> None:
        with self.settings(JSONSTAT_ALLOWED_SOURCE_DOMAINS=("stat.fi",)):
            self.assertEqual(
                source.normalize_source_url("https://stat.fi/data.json"),
                "https://stat.fi/data.json",
            )
            self.assertEqual(
                source.normalize_source_url("https://pxdata.stat.fi/data.json"),
                "https://pxdata.stat.fi/data.json",
            )

    def test_source_url_rejects_hosts_outside_configured_domains(self) -> None:
        with self.settings(JSONSTAT_ALLOWED_SOURCE_DOMAINS=("stat.fi",)):
            for value in (
                "https://example.org/data.json",
                "https://evilstat.fi/data.json",
                "https://stat.fi.example.com/data.json",
            ):
                with self.subTest(value=value):
                    with self.assertRaisesMessage(
                        ValueError,
                        "source host is not allowed. Allowed domains: stat.fi.",
                    ):
                        source.normalize_source_url(value)

    def test_source_url_uses_overridden_domain_allowlist(self) -> None:
        with self.settings(JSONSTAT_ALLOWED_SOURCE_DOMAINS=("example.org", "data.example.net")):
            self.assertEqual(
                source.allowed_source_domains(),
                ("example.org", "data.example.net"),
            )
            self.assertEqual(
                source.normalize_source_url("https://api.example.org/data.json"),
                "https://api.example.org/data.json",
            )
            with self.assertRaisesMessage(
                ValueError, "Allowed domains: example.org, data.example.net"
            ):
                source.normalize_source_url(EXAMPLE_URL)

    def test_normalize_source_url_rejects_unsafe_urls(self) -> None:
        for value in (
            "http://example.org/data.json",
            "https://localhost/data.json",
            "https://user:password@example.org/data.json",
            "https://example.org/data.json#fragment",
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    source.normalize_source_url(value)

    @patch("jsonstat.service_source.socket.getaddrinfo")
    def test_public_host_check_rejects_private_ip(self, getaddrinfo_mock: Any) -> None:
        getaddrinfo_mock.return_value = [
            (2, 1, 6, "", ("127.0.0.1", 443)),
        ]

        with self.assertRaisesMessage(PetscanServiceError, "non-public IP address") as captured:
            source._ensure_public_host("https://example.org/data.json")
        self.assertEqual(
            captured.exception.public_message, "The JSON-stat source URL must use a public host."
        )

    @patch("jsonstat.service_source._ensure_public_host")
    @patch("jsonstat.service_source.build_opener")
    def test_fetch_jsonstat_json_returns_payload_and_final_url(
        self,
        build_opener_mock: Any,
        _ensure_public_host_mock: Any,
    ) -> None:
        response = build_opener_mock.return_value.open.return_value.__enter__.return_value
        response.headers = {"Content-Length": "123"}
        response.read.return_value = json.dumps(_small_payload()).encode("utf-8")
        response.geturl.return_value = EXAMPLE_URL

        payload, final_url = source.fetch_jsonstat_json(EXAMPLE_URL)

        self.assertEqual(payload["class"], "dataset")
        self.assertEqual(final_url, EXAMPLE_URL)
        request = build_opener_mock.return_value.open.call_args.args[0]
        self.assertEqual(request.full_url, EXAMPLE_URL)
        self.assertIn("application/json-stat+json", dict(request.header_items())["Accept"])

    @patch("jsonstat.service_source._ensure_public_host")
    @patch("jsonstat.service_source.build_opener")
    def test_fetch_jsonstat_json_sanitizes_transport_error(
        self,
        build_opener_mock: Any,
        _ensure_public_host_mock: Any,
    ) -> None:
        build_opener_mock.return_value.open.side_effect = OSError("connection refused")

        with self.assertRaisesMessage(PetscanServiceError, "connection refused") as captured:
            source.fetch_jsonstat_json(EXAMPLE_URL)
        self.assertEqual(
            captured.exception.public_message,
            "Failed to load JSON-stat data from the upstream service.",
        )


class JsonstatSourceSimpleTests(SimpleTestCase):
    def test_decode_source_token_rejects_non_base64url_text(self) -> None:
        with self.assertRaisesMessage(ValueError, "source token is invalid"):
            source.decode_source_token("not valid!")
