from argparse import ArgumentParser
from pathlib import Path
from typing import Any, List

from django.core.management.base import BaseCommand, CommandError

from petscan import endpoint_snapshot_regression as regression


class Command(BaseCommand):  # type: ignore[misc]
    help = (
        "Verify or update offline SPARQL endpoint-output snapshots built from the bundled "
        "PetScan and Quarry source JSON examples."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--write",
            action="store_true",
            help="Write/update stored endpoint-output snapshots instead of only verifying them.",
        )
        parser.add_argument(
            "--case",
            action="append",
            choices=sorted(regression.SNAPSHOT_CASES_BY_NAME.keys()),
            help="Limit the run to one or more named snapshot cases.",
        )
        parser.add_argument(
            "--snapshot-dir",
            default="",
            help="Optional override for the endpoint snapshot output directory.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        selected_cases = regression.iter_snapshot_cases(options.get("case"))
        snapshot_dir_raw = str(options.get("snapshot_dir") or "").strip()
        snapshot_dir = Path(snapshot_dir_raw) if snapshot_dir_raw else regression.endpoint_snapshots_dir()
        write_mode = bool(options.get("write"))

        failures: List[str] = []
        for case in selected_cases:
            try:
                if write_mode:
                    result = regression.write_case_snapshot(case, snapshot_dir=snapshot_dir)
                    self.stdout.write(
                        "WROTE {name} triples={triples} sha256={sha} path={path}".format(
                            name=case.name,
                            triples=result.triple_count,
                            sha=result.sha256,
                            path=result.snapshot_path,
                        )
                    )
                else:
                    result = regression.verify_case_snapshot(case, snapshot_dir=snapshot_dir)
                    self.stdout.write(
                        "OK {name} triples={triples} sha256={sha}".format(
                            name=case.name,
                            triples=result.triple_count,
                            sha=result.sha256,
                        )
                    )
            except FileNotFoundError as exc:
                failures.append("{} ({})".format(case.name, exc))
            except regression.SnapshotMismatchError as exc:
                failures.append(str(exc))

        if failures:
            raise CommandError(
                "Endpoint snapshot regression check failed:\n{}".format("\n".join(failures))
            )
