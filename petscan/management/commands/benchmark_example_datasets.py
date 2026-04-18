import gzip
import hashlib
import json
import platform
import re
import subprocess
import sys
from argparse import ArgumentParser
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from statistics import mean, median
from tempfile import TemporaryDirectory
from time import perf_counter
from types import ModuleType
from typing import Any, Dict, List, Mapping, Optional, Sequence
from unittest.mock import patch
from urllib.parse import urlparse

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from petscan import service_links
from petscan import service_source as petscan_source
from petscan import service_store as shared_store
from petscan import service_store_builder as petscan_store_builder
from quarry import service_source as quarry_source
from quarry import service_store_builder as quarry_store_builder

pyoxigraph: Optional[ModuleType]
try:
    import pyoxigraph
except ImportError:  # pragma: no cover - dependency check at runtime
    pyoxigraph = None

DATASET_SPEC_PATH = Path(settings.BASE_DIR) / "data" / "benchmarks" / "offline_store_build_datasets.json"
EXAMPLES_DIR = Path(settings.BASE_DIR) / "data" / "examples"
RESULTS_DIR = Path(settings.BASE_DIR) / "data" / "benchmarks" / "results"
LATEST_RESULT_PATH = RESULTS_DIR / "latest.json"
HISTORY_PATH = RESULTS_DIR / "history.jsonl"
_KIND_PETSCAN = "petscan"
_KIND_QUARRY = "quarry"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return slug.strip("-")


def _git_output(*args: str) -> Optional[str]:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=str(settings.BASE_DIR),
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
    except Exception:
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return value or None


def _package_version(package_name: str) -> Optional[str]:
    try:
        return metadata.version(package_name)
    except metadata.PackageNotFoundError:
        return None


def _collect_environment_info() -> Dict[str, Any]:
    git_status = _git_output("status", "--short")
    return {
        "python_version": sys.version.split()[0],
        "django_version": _package_version("Django"),
        "pyoxigraph_version": _package_version("pyoxigraph"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "hostname": platform.node() or None,
        "git_commit": _git_output("rev-parse", "HEAD"),
        "git_branch": _git_output("branch", "--show-current"),
        "git_dirty": bool(git_status),
    }


def _fake_enrichment_payload(site: str, title: str) -> Dict[str, Any] | None:
    seed = hashlib.blake2b(
        "{}|{}".format(site, title).encode("utf-8"),
        digest_size=16,
        person=b"gil-parity-seed",
    ).digest()
    selector = seed[0] % 4
    qid = "Q{}".format(1 + (int.from_bytes(seed[1:5], "big") % 90_000_000))
    page_len = 100 + (int.from_bytes(seed[5:9], "big") % 900_000)
    timestamp = "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}".format(
        2020 + (seed[9] % 7),
        1 + (seed[10] % 12),
        1 + (seed[11] % 28),
        seed[12] % 24,
        seed[13] % 60,
        seed[14] % 60,
    )

    if selector == 0:
        return {"wikidata_id": qid, "page_len": None, "rev_timestamp": None}
    if selector == 1:
        return {"wikidata_id": None, "page_len": page_len, "rev_timestamp": timestamp}
    if selector == 2:
        return {"wikidata_id": qid, "page_len": page_len, "rev_timestamp": timestamp}
    return None


def _fake_enrichment_fetch(api_url: str, titles: list[str], **_kwargs: Any) -> Dict[str, Dict[str, Any]]:
    site = urlparse(api_url).netloc.lower()
    resolved: Dict[str, Dict[str, Any]] = {}
    for title in titles:
        payload = _fake_enrichment_payload(site, title)
        if payload is not None:
            resolved[title] = payload
    return resolved


def _unexpected_enrichment_fetch(
    api_url: str, titles: list[str], **_kwargs: Any
) -> Dict[str, Dict[str, Any]]:
    raise AssertionError(
        "Unexpected enrichment request during offline benchmark: api_url={} titles={}".format(
            api_url,
            titles[:5],
        )
    )


def _load_dataset_specs(spec_path: Path) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CommandError("Dataset spec file does not exist: {}".format(spec_path)) from exc
    except json.JSONDecodeError as exc:
        raise CommandError("Dataset spec file is not valid JSON: {}".format(spec_path)) from exc

    datasets = payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise CommandError("Dataset spec file must contain a non-empty datasets list.")

    normalized_specs: List[Dict[str, Any]] = []
    seen_names = set()
    for item in datasets:
        if not isinstance(item, dict):
            raise CommandError("Every dataset spec entry must be an object.")

        name = str(item.get("name", "")).strip()
        if not name:
            raise CommandError("Each dataset spec must define a non-empty name.")
        if name in seen_names:
            raise CommandError("Dataset names must be unique: {}".format(name))
        seen_names.add(name)

        kind = str(item.get("kind", "")).strip().lower()
        if kind not in {_KIND_PETSCAN, _KIND_QUARRY}:
            raise CommandError("Dataset {} has unsupported kind {}.".format(name, kind))

        file_name = str(item.get("file_name", "")).strip()
        if not file_name:
            raise CommandError("Dataset {} must define file_name.".format(name))

        source_id = _coerce_positive_int(
            item.get("source_id"),
            dataset_name=name,
            field_name="source_id",
        )
        expected_records = _coerce_positive_int(
            item.get("expected_records"),
            dataset_name=name,
            field_name="expected_records",
        )
        store_id = _coerce_positive_int(
            item.get("store_id", source_id),
            dataset_name=name,
            field_name="store_id",
        )

        use_fake_api = bool(item.get("use_fake_api", False))
        query_db_raw = item.get("query_db")
        query_db = str(query_db_raw).strip() if query_db_raw is not None else None
        if kind == _KIND_QUARRY and not query_db:
            raise CommandError("Dataset {} must define query_db for quarry input.".format(name))

        normalized_specs.append(
            {
                "name": name,
                "kind": kind,
                "file_name": file_name,
                "source_id": source_id,
                "store_id": store_id,
                "query_db": query_db or None,
                "expected_records": expected_records,
                "use_fake_api": use_fake_api,
            }
        )

    return normalized_specs


def _coerce_positive_int(value: Any, *, dataset_name: str, field_name: str) -> int:
    try:
        normalized = int(value)
    except Exception as exc:
        raise CommandError(
            "Dataset {} must define integer {}.".format(dataset_name, field_name)
        ) from exc
    if normalized <= 0:
        raise CommandError(
            "Dataset {} {} must be greater than zero.".format(dataset_name, field_name)
        )
    return normalized


def _select_dataset_specs(specs: Sequence[Mapping[str, Any]], raw_selection: str) -> List[Dict[str, Any]]:
    text = str(raw_selection or "").strip()
    if not text or text.lower() == "all":
        return [dict(spec) for spec in specs]

    requested_names = [token.strip() for token in text.split(",") if token.strip()]
    if not requested_names:
        raise CommandError("--datasets must be 'all' or a comma-separated list of dataset names.")

    spec_by_name = {str(spec["name"]): dict(spec) for spec in specs}
    missing = [name for name in requested_names if name not in spec_by_name]
    if missing:
        raise CommandError("Unknown dataset name(s): {}".format(", ".join(sorted(set(missing)))))

    return [spec_by_name[name] for name in requested_names]


def _source_url_for_spec(spec: Mapping[str, Any]) -> str:
    return "bundled://data/examples/{}".format(spec["file_name"])


def _load_records_for_spec(spec: Mapping[str, Any]) -> Dict[str, Any]:
    payload_path = EXAMPLES_DIR / str(spec["file_name"])
    started_at = perf_counter()
    with gzip.open(payload_path, mode="rt", encoding="utf-8") as payload_file:
        payload = json.load(payload_file)
    payload_load_ms = (perf_counter() - started_at) * 1000.0

    started_at = perf_counter()
    if spec["kind"] == _KIND_PETSCAN:
        records = petscan_source.extract_records(payload)
    else:
        records = quarry_source.extract_records(payload)
    extract_records_ms = (perf_counter() - started_at) * 1000.0

    if len(records) != int(spec["expected_records"]):
        raise CommandError(
            "Dataset {} expected {} records but loaded {}.".format(
                spec["name"],
                spec["expected_records"],
                len(records),
            )
        )

    return {
        "payload_load_ms": payload_load_ms,
        "extract_records_ms": extract_records_ms,
        "records": records,
    }


def _measure_build_store_ms(spec: Mapping[str, Any], records: Sequence[Mapping[str, Any]]) -> float:
    with TemporaryDirectory(prefix="offline-benchmark-{}-".format(spec["name"])) as temp_dir:
        temp_root = Path(temp_dir)

        def _store_path(value_id: int) -> Path:
            return temp_root / str(value_id)

        def _meta_path(value_id: int) -> Path:
            return _store_path(value_id) / "meta.json"

        fetch_side_effect = _fake_enrichment_fetch if bool(spec["use_fake_api"]) else _unexpected_enrichment_fetch
        source_url = _source_url_for_spec(spec)

        if spec["kind"] == _KIND_PETSCAN:
            with (
                patch.object(service_links, "wikidata_lookup_backend", return_value="api"),
                patch.object(
                    service_links,
                    "fetch_wikibase_items_for_site_api",
                    side_effect=fetch_side_effect,
                ),
                patch.object(shared_store, "store_path", side_effect=_store_path),
                patch.object(shared_store, "meta_path", side_effect=_meta_path),
            ):
                started_at = perf_counter()
                petscan_store_builder.build_store(int(spec["source_id"]), records, source_url)
                return (perf_counter() - started_at) * 1000.0

        with (
            patch.object(service_links, "wikidata_lookup_backend", return_value="api"),
            patch.object(
                service_links,
                "fetch_wikibase_items_for_site_api",
                side_effect=fetch_side_effect,
            ),
            patch.object(shared_store, "store_path", side_effect=_store_path),
            patch.object(shared_store, "meta_path", side_effect=_meta_path),
        ):
            started_at = perf_counter()
            quarry_store_builder.build_store(
                int(spec["store_id"]),
                int(spec["source_id"]),
                records,
                source_url,
                query_db=spec["query_db"],
            )
            return (perf_counter() - started_at) * 1000.0


def _benchmark_dataset(spec: Mapping[str, Any], runs: int, warmup: int) -> Dict[str, Any]:
    loaded = _load_records_for_spec(spec)
    records = loaded["records"]
    durations: List[float] = []
    measured_runs: List[Dict[str, Any]] = []

    total_runs = warmup + runs
    for iteration_index in range(total_runs):
        elapsed_ms = _measure_build_store_ms(spec, records)
        is_warmup = iteration_index < warmup
        run_payload = {
            "run_index": iteration_index + 1,
            "warmup": is_warmup,
            "build_store_ms": elapsed_ms,
        }
        if not is_warmup:
            durations.append(elapsed_ms)
            measured_runs.append(run_payload)

    return {
        "name": spec["name"],
        "kind": spec["kind"],
        "file_name": spec["file_name"],
        "source_id": spec["source_id"],
        "store_id": spec["store_id"],
        "query_db": spec["query_db"],
        "records": len(records),
        "use_fake_api": bool(spec["use_fake_api"]),
        "payload_load_ms": loaded["payload_load_ms"],
        "extract_records_ms": loaded["extract_records_ms"],
        "warmup_runs": warmup,
        "measured_runs": runs,
        "median_build_store_ms": median(durations),
        "mean_build_store_ms": mean(durations),
        "min_build_store_ms": min(durations),
        "max_build_store_ms": max(durations),
        "runs": measured_runs,
    }


def _default_output_path(label: Optional[str]) -> Path:
    timestamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    label_slug = _slugify(label or "")
    suffix = "-{}".format(label_slug) if label_slug else ""
    return RESULTS_DIR / "store-build-benchmark-{}{}.json".format(timestamp, suffix)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(Path(settings.BASE_DIR)))
    except ValueError:
        return str(path)


def _history_entry(report: Mapping[str, Any], output_path: Path) -> Dict[str, Any]:
    datasets_summary = {}
    for dataset in report["datasets"]:
        datasets_summary[str(dataset["name"])] = {
            "records": dataset["records"],
            "median_build_store_ms": dataset["median_build_store_ms"],
            "mean_build_store_ms": dataset["mean_build_store_ms"],
        }
    environment = report.get("environment", {})
    return {
        "schema_version": report.get("schema_version", 1),
        "created_at": report["created_at"],
        "label": report.get("label"),
        "output_file": _display_path(output_path),
        "pyoxigraph_version": environment.get("pyoxigraph_version"),
        "git_commit": environment.get("git_commit"),
        "git_dirty": environment.get("git_dirty"),
        "datasets": datasets_summary,
    }


def _write_report_files(report: Mapping[str, Any], output_path: Path) -> None:
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload, encoding="utf-8")

    LATEST_RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    LATEST_RESULT_PATH.write_text(payload, encoding="utf-8")

    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_PATH.open("a", encoding="utf-8") as history_file:
        history_file.write(json.dumps(_history_entry(report, output_path), sort_keys=True) + "\n")


class Command(BaseCommand):  # type: ignore[misc]
    help = (
        "Benchmark bundled offline PetScan and Quarry example datasets and save "
        "machine-readable results for later comparison."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--datasets",
            default="all",
            help="Dataset names to run (comma-separated) or 'all' (default).",
        )
        parser.add_argument(
            "--runs",
            type=int,
            default=2,
            help="Measured runs per dataset (default: 2).",
        )
        parser.add_argument(
            "--warmup",
            type=int,
            default=1,
            help="Warmup runs per dataset (default: 1).",
        )
        parser.add_argument(
            "--label",
            default="",
            help="Optional label stored in the result file name and metadata.",
        )
        parser.add_argument(
            "--output",
            default="",
            help="Optional JSON output path. Defaults to data/benchmarks/results/<timestamp>.json.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if pyoxigraph is None:
            raise CommandError("pyoxigraph is required for benchmark_example_datasets.")

        runs = int(options["runs"])
        warmup = int(options["warmup"])
        if runs <= 0:
            raise CommandError("--runs must be greater than zero.")
        if warmup < 0:
            raise CommandError("--warmup must be zero or greater.")

        label = str(options["label"] or "").strip() or None
        output_raw = str(options["output"] or "").strip()
        output_path = Path(output_raw) if output_raw else _default_output_path(label)

        specs = _load_dataset_specs(DATASET_SPEC_PATH)
        selected_specs = _select_dataset_specs(specs, str(options["datasets"] or "all"))
        selected_names = [str(spec["name"]) for spec in selected_specs]

        self.stdout.write(
            "Running offline benchmark for datasets={} runs={} warmup={}".format(
                selected_names,
                runs,
                warmup,
            )
        )

        benchmark_results = []
        for spec in selected_specs:
            self.stdout.write(
                "Benchmarking dataset {} ({})...".format(spec["name"], spec["file_name"])
            )
            dataset_result = _benchmark_dataset(spec, runs=runs, warmup=warmup)
            benchmark_results.append(dataset_result)
            self.stdout.write(
                "dataset={} records={} median_build_store_ms={:.1f} mean_build_store_ms={:.1f}".format(
                    dataset_result["name"],
                    dataset_result["records"],
                    dataset_result["median_build_store_ms"],
                    dataset_result["mean_build_store_ms"],
                )
            )

        report = {
            "schema_version": 1,
            "created_at": _now_utc().replace(microsecond=0).isoformat(),
            "label": label,
            "datasets_selected": selected_names,
            "runs": runs,
            "warmup": warmup,
            "environment": _collect_environment_info(),
            "datasets": benchmark_results,
        }

        _write_report_files(report, output_path)

        self.stdout.write(self.style.SUCCESS("Saved benchmark report."))
        self.stdout.write("result_output={}".format(output_path))
        self.stdout.write("latest_output={}".format(LATEST_RESULT_PATH))
        self.stdout.write("history_output={}".format(HISTORY_PATH))
