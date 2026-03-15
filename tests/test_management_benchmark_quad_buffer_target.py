import io
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase


class BenchmarkQuadBufferTargetCommandTests(SimpleTestCase):
    @patch("petscan.management.commands.benchmark_quad_buffer_target.store_builder.build_store")
    @patch("petscan.management.commands.benchmark_quad_buffer_target.source.extract_records")
    @patch("petscan.management.commands.benchmark_quad_buffer_target.source.fetch_petscan_json")
    def test_command_prefetches_source_once_by_default(
        self,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_store_mock,
    ):
        records = [{"id": 1, "title": "Example"}]
        fetch_petscan_json_mock.return_value = (
            {"payload": True},
            "https://petscan.example/?psid=43641756",
        )
        extract_records_mock.return_value = records

        stdout = io.StringIO()
        stderr = io.StringIO()

        call_command(
            "benchmark_quad_buffer_target",
            "--petscan-url",
            "https://petscan.wmcloud.org/?psid=43641756&output_limit=10",
            "--candidates",
            "10,20",
            "--runs",
            "1",
            "--warmup",
            "0",
            "--backend",
            "auto",
            stdout=stdout,
            stderr=stderr,
        )

        self.assertEqual(fetch_petscan_json_mock.call_count, 1)
        self.assertEqual(extract_records_mock.call_count, 1)
        self.assertEqual(build_store_mock.call_count, 2)
        self.assertIn("best_quad_buffer_target=", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    @patch("petscan.management.commands.benchmark_quad_buffer_target.store_builder.build_store")
    @patch("petscan.management.commands.benchmark_quad_buffer_target.source.extract_records")
    @patch("petscan.management.commands.benchmark_quad_buffer_target.source.fetch_petscan_json")
    def test_command_refetches_source_for_each_iteration_when_requested(
        self,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_store_mock,
    ):
        records = [{"id": 1, "title": "Example"}]
        fetch_petscan_json_mock.return_value = (
            {"payload": True},
            "https://petscan.example/?psid=43641756",
        )
        extract_records_mock.return_value = records

        stdout = io.StringIO()
        stderr = io.StringIO()

        call_command(
            "benchmark_quad_buffer_target",
            "--psid",
            "43641756",
            "--candidates",
            "10,20",
            "--runs",
            "2",
            "--warmup",
            "1",
            "--refresh-source-each-run",
            "--backend",
            "auto",
            stdout=stdout,
            stderr=stderr,
        )

        expected_iterations = 2 * (2 + 1)
        self.assertEqual(fetch_petscan_json_mock.call_count, expected_iterations)
        self.assertEqual(extract_records_mock.call_count, expected_iterations)
        self.assertEqual(build_store_mock.call_count, expected_iterations)
        self.assertEqual(stderr.getvalue(), "")

    def test_command_rejects_non_positive_candidate_values(self):
        with self.assertRaises(CommandError) as context:
            call_command(
                "benchmark_quad_buffer_target",
                "--psid",
                "43641756",
                "--candidates",
                "10,0,20",
                "--backend",
                "auto",
            )

        self.assertIn("--candidates values must be greater than zero", str(context.exception))
