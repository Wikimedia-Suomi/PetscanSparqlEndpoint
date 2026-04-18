import io
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase


class BenchmarkExampleDatasetsCommandTests(SimpleTestCase):
    def _write_spec(self, path: Path) -> None:
        path.write_text(
            json.dumps(
                {
                    "datasets": [
                        {
                            "name": "alpha",
                            "kind": "petscan",
                            "file_name": "petscan-43641756.json.gz",
                            "source_id": 43641756,
                            "expected_records": 2638,
                            "use_fake_api": True,
                        },
                        {
                            "name": "beta",
                            "kind": "quarry",
                            "file_name": "quarry-103479-run-1084300.json.gz",
                            "source_id": 103479,
                            "store_id": 103479,
                            "query_db": "fiwiki_p",
                            "expected_records": 100000,
                            "use_fake_api": False,
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )

    @patch("petscan.management.commands.benchmark_example_datasets._collect_environment_info")
    @patch("petscan.management.commands.benchmark_example_datasets._benchmark_dataset")
    @patch("petscan.management.commands.benchmark_example_datasets._now_utc")
    def test_command_writes_result_latest_and_history(
        self,
        now_utc_mock,
        benchmark_dataset_mock,
        collect_environment_info_mock,
    ):
        now_utc_mock.return_value = datetime(2026, 4, 18, 10, 20, 30, tzinfo=timezone.utc)
        collect_environment_info_mock.return_value = {
            "pyoxigraph_version": "0.5.6",
            "git_commit": "abc123",
            "git_dirty": True,
        }
        benchmark_dataset_mock.side_effect = [
            {
                "name": "alpha",
                "records": 2638,
                "median_build_store_ms": 600.0,
                "mean_build_store_ms": 610.0,
            },
            {
                "name": "beta",
                "records": 100000,
                "median_build_store_ms": 4500.0,
                "mean_build_store_ms": 4525.0,
            },
        ]

        stdout = io.StringIO()
        stderr = io.StringIO()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            spec_path = tmp_path / "offline_store_build_datasets.json"
            results_dir = tmp_path / "results"
            latest_path = results_dir / "latest.json"
            history_path = results_dir / "history.jsonl"
            self._write_spec(spec_path)

            with (
                patch(
                    "petscan.management.commands.benchmark_example_datasets.DATASET_SPEC_PATH",
                    spec_path,
                ),
                patch("petscan.management.commands.benchmark_example_datasets.RESULTS_DIR", results_dir),
                patch(
                    "petscan.management.commands.benchmark_example_datasets.LATEST_RESULT_PATH",
                    latest_path,
                ),
                patch("petscan.management.commands.benchmark_example_datasets.HISTORY_PATH", history_path),
            ):
                call_command(
                    "benchmark_example_datasets",
                    "--runs",
                    "2",
                    "--warmup",
                    "1",
                    "--label",
                    "PyOxigraph 0.5.6",
                    stdout=stdout,
                    stderr=stderr,
                )

            result_files = list(results_dir.glob("store-build-benchmark-*.json"))
            self.assertEqual(len(result_files), 1)
            result_payload = json.loads(result_files[0].read_text(encoding="utf-8"))
            latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
            history_lines = history_path.read_text(encoding="utf-8").splitlines()
            history_entry = json.loads(history_lines[0])

        self.assertEqual(result_payload["label"], "PyOxigraph 0.5.6")
        self.assertEqual(result_payload["datasets_selected"], ["alpha", "beta"])
        self.assertEqual(len(result_payload["datasets"]), 2)
        self.assertEqual(result_payload, latest_payload)
        self.assertEqual(history_entry["datasets"]["alpha"]["median_build_store_ms"], 600.0)
        self.assertEqual(history_entry["pyoxigraph_version"], "0.5.6")
        self.assertIn("result_output=", stdout.getvalue())
        self.assertIn("history_output=", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    @patch("petscan.management.commands.benchmark_example_datasets._collect_environment_info")
    @patch("petscan.management.commands.benchmark_example_datasets._benchmark_dataset")
    def test_command_filters_selected_dataset_names(
        self,
        benchmark_dataset_mock,
        collect_environment_info_mock,
    ):
        collect_environment_info_mock.return_value = {}
        benchmark_dataset_mock.return_value = {
            "name": "beta",
            "records": 100000,
            "median_build_store_ms": 4500.0,
            "mean_build_store_ms": 4525.0,
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            spec_path = tmp_path / "offline_store_build_datasets.json"
            results_dir = tmp_path / "results"
            self._write_spec(spec_path)

            with (
                patch(
                    "petscan.management.commands.benchmark_example_datasets.DATASET_SPEC_PATH",
                    spec_path,
                ),
                patch("petscan.management.commands.benchmark_example_datasets.RESULTS_DIR", results_dir),
                patch(
                    "petscan.management.commands.benchmark_example_datasets.LATEST_RESULT_PATH",
                    results_dir / "latest.json",
                ),
                patch(
                    "petscan.management.commands.benchmark_example_datasets.HISTORY_PATH",
                    results_dir / "history.jsonl",
                ),
            ):
                call_command(
                    "benchmark_example_datasets",
                    "--datasets",
                    "beta",
                    "--output",
                    str(tmp_path / "result.json"),
                )

        self.assertEqual(benchmark_dataset_mock.call_count, 1)
        call_args = benchmark_dataset_mock.call_args[0][0]
        self.assertEqual(call_args["name"], "beta")

    def test_command_rejects_unknown_dataset_names(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            spec_path = tmp_path / "offline_store_build_datasets.json"
            self._write_spec(spec_path)

            with (
                patch(
                    "petscan.management.commands.benchmark_example_datasets.DATASET_SPEC_PATH",
                    spec_path,
                ),
                self.assertRaises(CommandError) as context,
            ):
                call_command(
                    "benchmark_example_datasets",
                    "--datasets",
                    "missing",
                )

        self.assertIn("Unknown dataset name", str(context.exception))
