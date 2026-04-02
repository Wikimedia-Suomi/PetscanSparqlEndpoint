import io
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase

from petscan import endpoint_snapshot_regression as regression


class CheckEndpointSnapshotRegressionCommandTests(SimpleTestCase):
    def test_write_mode_writes_selected_snapshot_files(self) -> None:
        case = regression.SnapshotCase(
            name="petscan-test",
            kind="petscan",
            service_id=1,
            source_file="petscan-test.json.gz",
            output_file="petscan-test.nt.gz",
        )
        stdout = io.StringIO()

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_dir = Path(tmp_dir)
            with patch(
                "petscan.management.commands.check_endpoint_snapshot_regression.regression.iter_snapshot_cases",
                return_value=(case,),
            ), patch(
                "petscan.management.commands.check_endpoint_snapshot_regression.regression.write_case_snapshot",
                return_value=regression.SnapshotResult(
                    case=case,
                    snapshot_path=snapshot_dir / case.output_file,
                    triple_count=12,
                    sha256="abc123",
                    byte_count=456,
                ),
            ) as write_case_snapshot_mock:
                call_command(
                    "check_endpoint_snapshot_regression",
                    "--write",
                    "--snapshot-dir",
                    str(snapshot_dir),
                    stdout=stdout,
                )

        write_case_snapshot_mock.assert_called_once()
        self.assertIn("WROTE petscan-test", stdout.getvalue())

    def test_verify_mode_reports_ok_for_matching_snapshot(self) -> None:
        case = regression.SnapshotCase(
            name="petscan-test",
            kind="petscan",
            service_id=1,
            source_file="petscan-test.json.gz",
            output_file="petscan-test.nt.gz",
        )
        stdout = io.StringIO()

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_dir = Path(tmp_dir)
            with patch(
                "petscan.management.commands.check_endpoint_snapshot_regression.regression.iter_snapshot_cases",
                return_value=(case,),
            ), patch(
                "petscan.management.commands.check_endpoint_snapshot_regression.regression.verify_case_snapshot",
                return_value=regression.SnapshotResult(
                    case=case,
                    snapshot_path=snapshot_dir / case.output_file,
                    triple_count=12,
                    sha256="abc123",
                    byte_count=456,
                ),
            ) as verify_case_snapshot_mock:
                call_command(
                    "check_endpoint_snapshot_regression",
                    "--snapshot-dir",
                    str(snapshot_dir),
                    stdout=stdout,
                )

        verify_case_snapshot_mock.assert_called_once()
        self.assertIn("OK petscan-test", stdout.getvalue())

    def test_verify_mode_raises_when_snapshot_differs(self) -> None:
        case = regression.SnapshotCase(
            name="petscan-test",
            kind="petscan",
            service_id=1,
            source_file="petscan-test.json.gz",
            output_file="petscan-test.nt.gz",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_dir = Path(tmp_dir)
            with patch(
                "petscan.management.commands.check_endpoint_snapshot_regression.regression.iter_snapshot_cases",
                return_value=(case,),
            ), patch(
                "petscan.management.commands.check_endpoint_snapshot_regression.regression.verify_case_snapshot",
                side_effect=regression.SnapshotMismatchError("petscan-test snapshot mismatch"),
            ):
                with self.assertRaises(CommandError) as context:
                    call_command(
                        "check_endpoint_snapshot_regression",
                        "--snapshot-dir",
                        str(snapshot_dir),
                    )

        self.assertIn("snapshot mismatch", str(context.exception))
