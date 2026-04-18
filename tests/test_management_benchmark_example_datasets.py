import io
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase
from pyoxigraph import Literal, NamedNode


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
                "write_strategy": "bulk_extend",
                "median_build_store_ms": 600.0,
                "mean_build_store_ms": 610.0,
            },
            {
                "name": "beta",
                "records": 100000,
                "write_strategy": "bulk_extend",
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
        self.assertEqual(result_payload["write_strategy"], "bulk_extend")
        self.assertEqual(len(result_payload["datasets"]), 2)
        self.assertEqual(result_payload, latest_payload)
        self.assertEqual(history_entry["datasets"]["alpha"]["median_build_store_ms"], 600.0)
        self.assertEqual(history_entry["write_strategy"], "bulk_extend")
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
            "write_strategy": "bulk_extend",
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
        call_args = benchmark_dataset_mock.call_args
        self.assertEqual(call_args.args[0]["name"], "beta")
        self.assertEqual(call_args.kwargs["strategy"], "bulk_extend")

    @patch("petscan.management.commands.benchmark_example_datasets._collect_environment_info")
    @patch("petscan.management.commands.benchmark_example_datasets._benchmark_dataset")
    def test_command_passes_bulk_load_strategy_to_benchmark(
        self,
        benchmark_dataset_mock,
        collect_environment_info_mock,
    ):
        collect_environment_info_mock.return_value = {}
        benchmark_dataset_mock.return_value = {
            "name": "alpha",
            "records": 2638,
            "write_strategy": "bulk_load_nquads",
            "median_build_store_ms": 590.0,
            "mean_build_store_ms": 595.0,
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
                    "alpha",
                    "--strategy",
                    "bulk_load_nquads",
                    "--output",
                    str(tmp_path / "result.json"),
                )

        self.assertEqual(benchmark_dataset_mock.call_count, 1)
        call_args = benchmark_dataset_mock.call_args
        self.assertEqual(call_args.args[0]["name"], "alpha")
        self.assertEqual(call_args.kwargs["strategy"], "bulk_load_nquads")

    @patch("petscan.management.commands.benchmark_example_datasets._collect_environment_info")
    @patch("petscan.management.commands.benchmark_example_datasets._benchmark_dataset")
    def test_command_passes_bulk_load_direct_file_strategy_to_benchmark(
        self,
        benchmark_dataset_mock,
        collect_environment_info_mock,
    ):
        collect_environment_info_mock.return_value = {}
        benchmark_dataset_mock.return_value = {
            "name": "alpha",
            "records": 2638,
            "write_strategy": "bulk_load_nquads_direct_file",
            "median_build_store_ms": 570.0,
            "mean_build_store_ms": 575.0,
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
                    "alpha",
                    "--strategy",
                    "bulk_load_nquads_direct_file",
                    "--output",
                    str(tmp_path / "result.json"),
                )

        self.assertEqual(benchmark_dataset_mock.call_count, 1)
        call_args = benchmark_dataset_mock.call_args
        self.assertEqual(call_args.args[0]["name"], "alpha")
        self.assertEqual(call_args.kwargs["strategy"], "bulk_load_nquads_direct_file")

    @patch("petscan.management.commands.benchmark_example_datasets._collect_environment_info")
    @patch("petscan.management.commands.benchmark_example_datasets._benchmark_dataset")
    def test_command_passes_bulk_load_gzip_stream_strategy_to_benchmark(
        self,
        benchmark_dataset_mock,
        collect_environment_info_mock,
    ):
        collect_environment_info_mock.return_value = {}
        benchmark_dataset_mock.return_value = {
            "name": "alpha",
            "records": 2638,
            "write_strategy": "bulk_load_nquads_gzip_stream",
            "median_build_store_ms": 610.0,
            "mean_build_store_ms": 615.0,
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
                    "alpha",
                    "--strategy",
                    "bulk_load_nquads_gzip_stream",
                    "--output",
                    str(tmp_path / "result.json"),
                )

        self.assertEqual(benchmark_dataset_mock.call_count, 1)
        call_args = benchmark_dataset_mock.call_args
        self.assertEqual(call_args.args[0]["name"], "alpha")
        self.assertEqual(call_args.kwargs["strategy"], "bulk_load_nquads_gzip_stream")

    @patch("petscan.management.commands.benchmark_example_datasets._collect_environment_info")
    @patch("petscan.management.commands.benchmark_example_datasets._benchmark_dataset")
    def test_command_passes_bulk_load_stream_strategy_to_benchmark(
        self,
        benchmark_dataset_mock,
        collect_environment_info_mock,
    ):
        collect_environment_info_mock.return_value = {}
        benchmark_dataset_mock.return_value = {
            "name": "alpha",
            "records": 2638,
            "write_strategy": "bulk_load_nquads_stream",
            "median_build_store_ms": 580.0,
            "mean_build_store_ms": 585.0,
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
                    "alpha",
                    "--strategy",
                    "bulk_load_nquads_stream",
                    "--output",
                    str(tmp_path / "result.json"),
                )

        self.assertEqual(benchmark_dataset_mock.call_count, 1)
        call_args = benchmark_dataset_mock.call_args
        self.assertEqual(call_args.args[0]["name"], "alpha")
        self.assertEqual(call_args.kwargs["strategy"], "bulk_load_nquads_stream")

    def test_flush_nquads_lines_to_binary_stream_writes_expected_nquads(self):
        from petscan.management.commands import benchmark_example_datasets as mod

        sink = io.BytesIO()
        writer = io.BufferedWriter(sink)
        nquads_line_buffer = [
            "<https://example.test/s> <https://example.test/p> \"alpha\" .\n",
            "<https://example.test/x> <https://example.test/y> <https://example.test/z> .\n",
        ]

        bytes_written = mod._flush_nquads_lines_to_binary_stream(writer, nquads_line_buffer)
        writer.flush()
        payload = sink.getvalue()

        self.assertEqual(
            payload,
            (
                b"<https://example.test/s> <https://example.test/p> \"alpha\" .\n"
                b"<https://example.test/x> <https://example.test/y> <https://example.test/z> .\n"
            ),
        )
        self.assertEqual(bytes_written, len(payload))
        self.assertEqual(nquads_line_buffer, [])

    def test_flush_nquads_lines_to_text_stream_writes_expected_nquads(self):
        from petscan.management.commands import benchmark_example_datasets as mod

        sink = io.StringIO()
        nquads_line_buffer = [
            "<https://example.test/s> <https://example.test/p> \"alpha\" .\n",
            "<https://example.test/x> <https://example.test/y> <https://example.test/z> .\n",
        ]

        bytes_written = mod._flush_nquads_lines_to_text_stream(sink, nquads_line_buffer)
        payload = sink.getvalue()

        self.assertEqual(
            payload,
            (
                "<https://example.test/s> <https://example.test/p> \"alpha\" .\n"
                "<https://example.test/x> <https://example.test/y> <https://example.test/z> .\n"
            ),
        )
        self.assertEqual(bytes_written, len(payload.encode("utf-8")))
        self.assertEqual(nquads_line_buffer, [])

    def test_write_petscan_record_nquads_lines_matches_quad_serialization(self):
        from petscan import service_rdf as rdf
        from petscan import service_store_builder as store_builder
        from petscan.management.commands import benchmark_example_datasets as mod

        row = {
            "id": 123,
            "title": "Example page",
            "size": 42,
            "rev_timestamp": "20240102030405",
            "gil": "https://fi.wikipedia.org/wiki/Example_page",
        }
        resolved_gil_links = [("https://fi.wikipedia.org/wiki/Example_page", "Q123")]
        loaded_at = "2026-04-18T11:15:00+00:00"
        context = store_builder._RecordWriteContext(
            predicates=store_builder._build_store_predicates(),
            psid=77,
            gil_link_enrichment_map={},
            xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
            psid_literal=Literal("77", datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            loaded_at_literal=Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )

        quad_buffer = []
        expected_kinds, expected_counts = store_builder._write_record_quads(
            index=0,
            row=row,
            context=context,
            resolved_gil_links=resolved_gil_links,
            quad_buffer=quad_buffer,
        )
        expected_text = "".join("{} .\n".format(quad) for quad in quad_buffer)

        line_buffer = []
        actual_kinds, actual_counts, chars_written = mod._write_petscan_record_nquads_lines(
            index=0,
            row=row,
            context=context,
            resolved_gil_links=resolved_gil_links,
            nquads_line_buffer=line_buffer,
            predicate_text_cache={},
            iri_text_cache={},
        )

        self.assertEqual("".join(line_buffer), expected_text)
        self.assertEqual(actual_kinds, expected_kinds)
        self.assertEqual(actual_counts, expected_counts)
        self.assertEqual(chars_written, len(expected_text))

    def test_write_quarry_record_nquads_lines_matches_quad_serialization(self):
        from petscan import service_rdf as rdf
        from petscan.management.commands import benchmark_example_datasets as mod
        from quarry import service_store_builder as store_builder

        source_row = {
            "pageid": 123,
            "title": "Example page",
            "namespace": 0,
            "wiki": "fiwiki",
            "rev_timestamp": "20240102030405",
            "size": 42,
        }
        prepared_row = store_builder._records_with_derived_uris([source_row], "fiwiki_p")[0]
        row_plan = store_builder._row_write_plans([prepared_row])[0]
        loaded_at = "2026-04-18T11:15:00+00:00"
        context = store_builder._RecordWriteContext(
            predicates=store_builder._build_store_predicates(),
            quarry_id=103479,
            row_subject_base="{}/{}#".format(store_builder._QUARRY_ROW_BASE, 103479),
            gil_link_enrichment_map={},
            xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
            quarry_id_literal=Literal("103479", datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            loaded_at_literal=Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )

        quad_buffer = []
        structure_accumulator = rdf.StructureAccumulator()
        store_builder._write_record_quads(
            index=0,
            row=prepared_row,
            row_plan=row_plan,
            context=context,
            resolved_gil_links=[],
            quad_buffer=quad_buffer,
            structure_accumulator=structure_accumulator,
        )
        expected_text = "".join("{} .\n".format(quad) for quad in quad_buffer)

        line_buffer = []
        actual_kinds, actual_counts, chars_written = mod._write_quarry_record_nquads_lines(
            index=0,
            row=prepared_row,
            row_plan=row_plan,
            context=context,
            resolved_gil_links=[],
            nquads_line_buffer=line_buffer,
            predicate_text_cache={},
            iri_text_cache={},
        )

        self.assertEqual("".join(line_buffer), expected_text)
        self.assertEqual(chars_written, len(expected_text))
        self.assertTrue(actual_kinds)
        self.assertTrue(actual_counts)

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
