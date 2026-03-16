import json
from contextlib import nullcontext
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Tuple
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from django.core.management.base import BaseCommand, CommandError

from petscan import enrichment_sql
from petscan import service_source as source
from petscan import service_store_builder as store_builder

DEFAULT_PETSCAN_URL = "https://petscan.wmcloud.org/?psid=43641756"
DEFAULT_CANDIDATES = (20_000, 50_000, 100_000, 200_000, 500_000)
_PETSCAN_RESERVED_QUERY_PARAMS = {"psid", "format", "query", "refresh"}


def _extract_psid_and_params_from_url(petscan_url: str) -> Tuple[int, Dict[str, List[str]]]:
    parsed = urlparse(str(petscan_url or "").strip())
    query_pairs = parse_qs(parsed.query, keep_blank_values=False)

    psid_values = [value.strip() for value in query_pairs.get("psid", []) if str(value).strip()]
    if not psid_values:
        raise CommandError("The given PetScan URL must include ?psid=<number>.")

    try:
        psid = int(psid_values[-1])
    except Exception as exc:
        raise CommandError("psid in PetScan URL must be an integer.") from exc
    if psid <= 0:
        raise CommandError("psid must be greater than zero.")

    forwarded: Dict[str, List[str]] = {}
    for key, values in query_pairs.items():
        if key.lower() in _PETSCAN_RESERVED_QUERY_PARAMS:
            continue
        normalized_values = [str(value).strip() for value in values if str(value).strip()]
        if normalized_values:
            forwarded[key] = normalized_values

    return psid, forwarded


def _parse_candidates(raw_value: str) -> List[int]:
    values: List[int] = []
    for chunk in str(raw_value or "").split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            value = int(token)
        except Exception as exc:
            raise CommandError("--candidates must contain comma-separated integers.") from exc
        if value <= 0:
            raise CommandError("--candidates values must be greater than zero.")
        values.append(value)

    unique_values = sorted(set(values))
    if not unique_values:
        raise CommandError("--candidates must include at least one value.")
    return unique_values


def _load_records(
    psid: int,
    petscan_params: Mapping[str, Any],
) -> Tuple[List[Dict[str, Any]], str, float, float]:
    started_at = perf_counter()
    payload, source_url = source.fetch_petscan_json(psid, petscan_params=petscan_params)
    fetch_ms = (perf_counter() - started_at) * 1000.0

    started_at = perf_counter()
    records = source.extract_records(payload)
    extract_ms = (perf_counter() - started_at) * 1000.0

    if not records:
        raise CommandError("PetScan returned zero rows for psid {}.".format(psid))

    return records, source_url, fetch_ms, extract_ms


class Command(BaseCommand):
    help = (
        "Benchmark _QUAD_BUFFER_TARGET values with the real PetScan import path "
        "(including SQL/API enrichment backend)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--petscan-url",
            default=DEFAULT_PETSCAN_URL,
            help="PetScan URL with psid and optional extra query params.",
        )
        parser.add_argument(
            "--psid",
            type=int,
            default=None,
            help="Override psid from --petscan-url.",
        )
        parser.add_argument(
            "--store-psid",
            type=int,
            default=None,
            help="Store psid used for build target (default: same as source psid).",
        )
        parser.add_argument(
            "--candidates",
            default=",".join(str(value) for value in DEFAULT_CANDIDATES),
            help="Comma-separated _QUAD_BUFFER_TARGET candidates.",
        )
        parser.add_argument(
            "--runs",
            type=int,
            default=3,
            help="Measured runs per candidate (default: 3).",
        )
        parser.add_argument(
            "--warmup",
            type=int,
            default=1,
            help="Warmup runs per candidate (default: 1).",
        )
        parser.add_argument(
            "--refresh-source-each-run",
            action="store_true",
            help="Refetch PetScan payload before every run.",
        )
        parser.add_argument(
            "--backend",
            choices=["toolforge_sql", "api", "auto"],
            default="toolforge_sql",
            help="Force enrichment backend used during benchmark (default: toolforge_sql).",
        )

    def handle(self, *args, **options):
        source_psid_from_url, petscan_params = _extract_psid_and_params_from_url(
            options["petscan_url"]
        )
        source_psid = int(options["psid"]) if options["psid"] is not None else source_psid_from_url
        if source_psid <= 0:
            raise CommandError("--psid must be greater than zero.")

        store_psid = (
            int(options["store_psid"]) if options["store_psid"] is not None else source_psid
        )
        if store_psid <= 0:
            raise CommandError("--store-psid must be greater than zero.")

        runs = int(options["runs"])
        warmup = int(options["warmup"])
        if runs <= 0:
            raise CommandError("--runs must be greater than zero.")
        if warmup < 0:
            raise CommandError("--warmup must be zero or greater.")

        refresh_source_each_run = bool(options["refresh_source_each_run"])
        backend = str(options["backend"] or "").strip().lower()
        candidates = _parse_candidates(options["candidates"])

        if backend == "toolforge_sql" and enrichment_sql.pymysql is None:
            raise CommandError("PyMySQL is required when --backend=toolforge_sql.")

        self.stdout.write(
            "Benchmark settings: source_psid={} store_psid={} backend={} runs={} warmup={} "
            "refresh_source_each_run={} candidates={}".format(
                source_psid,
                store_psid,
                backend,
                runs,
                warmup,
                refresh_source_each_run,
                candidates,
            )
        )

        preloaded_records: Optional[List[Dict[str, Any]]] = None
        preloaded_source_url = ""
        prefetch_fetch_ms = 0.0
        prefetch_extract_ms = 0.0

        if not refresh_source_each_run:
            self.stdout.write("Prefetching PetScan data once before benchmark...")
            (
                preloaded_records,
                preloaded_source_url,
                prefetch_fetch_ms,
                prefetch_extract_ms,
            ) = _load_records(source_psid, petscan_params)
            self.stdout.write(
                "Prefetch done: records={} fetch_ms={:.1f} extract_ms={:.1f}".format(
                    len(preloaded_records),
                    prefetch_fetch_ms,
                    prefetch_extract_ms,
                )
            )

        backend_context = (
            patch("petscan.service_links.wikidata_lookup_backend", return_value=backend)
            if backend != "auto"
            else nullcontext()
        )

        report_candidates: List[Dict[str, Any]] = []
        original_quad_buffer_target = store_builder._QUAD_BUFFER_TARGET

        try:
            with backend_context:
                for candidate in candidates:
                    store_builder._QUAD_BUFFER_TARGET = candidate
                    measured_runs: List[Dict[str, Any]] = []
                    total_iterations = warmup + runs

                    self.stdout.write(
                        "Running candidate _QUAD_BUFFER_TARGET={} (warmup={}, runs={})...".format(
                            candidate,
                            warmup,
                            runs,
                        )
                    )

                    for iteration_index in range(total_iterations):
                        is_warmup = iteration_index < warmup
                        if refresh_source_each_run:
                            records, source_url, fetch_ms, extract_ms = _load_records(
                                source_psid,
                                petscan_params,
                            )
                        else:
                            records = preloaded_records or []
                            source_url = preloaded_source_url
                            fetch_ms = 0.0
                            extract_ms = 0.0

                        started_at = perf_counter()
                        store_builder.build_store(
                            store_psid,
                            records,
                            source_url,
                            source_params=petscan_params,
                        )
                        build_store_ms = (perf_counter() - started_at) * 1000.0

                        run_result = {
                            "run_index": iteration_index + 1,
                            "warmup": is_warmup,
                            "records": len(records),
                            "source_fetch_ms": fetch_ms,
                            "source_extract_ms": extract_ms,
                            "build_store_ms": build_store_ms,
                            "total_ms": fetch_ms + extract_ms + build_store_ms,
                        }
                        if not is_warmup:
                            measured_runs.append(run_result)

                    avg_total_ms = sum(run["total_ms"] for run in measured_runs) / len(
                        measured_runs
                    )
                    avg_build_store_ms = (
                        sum(run["build_store_ms"] for run in measured_runs) / len(measured_runs)
                    )
                    avg_source_fetch_ms = (
                        sum(run["source_fetch_ms"] for run in measured_runs) / len(measured_runs)
                    )
                    avg_source_extract_ms = (
                        sum(run["source_extract_ms"] for run in measured_runs) / len(measured_runs)
                    )
                    min_total_ms = min(run["total_ms"] for run in measured_runs)
                    max_total_ms = max(run["total_ms"] for run in measured_runs)

                    candidate_report = {
                        "quad_buffer_target": candidate,
                        "avg_total_ms": avg_total_ms,
                        "avg_build_store_ms": avg_build_store_ms,
                        "avg_source_fetch_ms": avg_source_fetch_ms,
                        "avg_source_extract_ms": avg_source_extract_ms,
                        "min_total_ms": min_total_ms,
                        "max_total_ms": max_total_ms,
                        "measured_runs": measured_runs,
                    }
                    report_candidates.append(candidate_report)

                    self.stdout.write(
                        "candidate={} avg_total_ms={:.1f} avg_build_store_ms={:.1f} "
                        "avg_source_fetch_ms={:.1f} avg_source_extract_ms={:.1f}".format(
                            candidate,
                            avg_total_ms,
                            avg_build_store_ms,
                            avg_source_fetch_ms,
                            avg_source_extract_ms,
                        )
                    )
        finally:
            store_builder._QUAD_BUFFER_TARGET = original_quad_buffer_target

        best = min(report_candidates, key=lambda candidate: candidate["avg_total_ms"])
        self.stdout.write(
            self.style.SUCCESS(
                "best_quad_buffer_target={} avg_total_ms={:.1f}".format(
                    best["quad_buffer_target"],
                    best["avg_total_ms"],
                )
            )
        )

        report = {
            "source_psid": source_psid,
            "store_psid": store_psid,
            "source_params": petscan_params,
            "backend": backend,
            "refresh_source_each_run": refresh_source_each_run,
            "runs": runs,
            "warmup": warmup,
            "prefetch_fetch_ms": prefetch_fetch_ms,
            "prefetch_extract_ms": prefetch_extract_ms,
            "candidates": report_candidates,
            "best_quad_buffer_target": best["quad_buffer_target"],
            "best_avg_total_ms": best["avg_total_ms"],
        }
        self.stdout.write(json.dumps(report, indent=2, sort_keys=True))
