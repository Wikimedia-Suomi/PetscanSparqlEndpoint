import gzip
import hashlib
import io
import json
import os
import platform
import re
import subprocess
import sys
import threading
from argparse import ArgumentParser
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from statistics import mean, median
from tempfile import TemporaryDirectory
from time import perf_counter
from types import ModuleType
from typing import Any, Dict, List, Mapping, Optional, Sequence, TextIO
from unittest.mock import patch
from urllib.parse import urlparse

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from petscan import service_links
from petscan import service_rdf as shared_rdf
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
_STRATEGY_BULK_EXTEND = "bulk_extend"
_STRATEGY_BULK_LOAD_NQUADS = "bulk_load_nquads"
_STRATEGY_BULK_LOAD_NQUADS_DIRECT_FILE = "bulk_load_nquads_direct_file"
_STRATEGY_BULK_LOAD_NQUADS_GZIP_STREAM = "bulk_load_nquads_gzip_stream"
_STRATEGY_BULK_LOAD_NQUADS_STREAM = "bulk_load_nquads_stream"
_NQUADS_STREAM_CHUNK_TARGET_CHARS = 4 * 1024 * 1024
_WRITE_STRATEGIES = (
    _STRATEGY_BULK_EXTEND,
    _STRATEGY_BULK_LOAD_NQUADS,
    _STRATEGY_BULK_LOAD_NQUADS_DIRECT_FILE,
    _STRATEGY_BULK_LOAD_NQUADS_GZIP_STREAM,
    _STRATEGY_BULK_LOAD_NQUADS_STREAM,
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _require_pyoxigraph() -> ModuleType:
    if pyoxigraph is None:
        raise CommandError("pyoxigraph is required for benchmark_example_datasets.")
    return pyoxigraph


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


def _flush_quads_to_nquads_file(nquads_file: TextIO, quad_buffer: List[Any]) -> None:
    if not quad_buffer:
        return
    nquads_file.writelines("{} .\n".format(quad) for quad in quad_buffer)
    quad_buffer.clear()


def _append_nquads_line(
    nquads_line_buffer: List[str],
    subject_text: str,
    predicate_text: str,
    object_text: str,
) -> int:
    line = subject_text + " " + predicate_text + " " + object_text + " .\n"
    nquads_line_buffer.append(line)
    return len(line)


def _flush_nquads_lines_to_binary_stream(
    binary_stream: io.BufferedWriter,
    nquads_line_buffer: List[str],
) -> int:
    if not nquads_line_buffer:
        return 0
    payload = "".join(nquads_line_buffer).encode("utf-8")
    binary_stream.write(payload)
    nquads_line_buffer.clear()
    return len(payload)


def _flush_nquads_lines_to_text_stream(
    text_stream: TextIO,
    nquads_line_buffer: List[str],
) -> int:
    if not nquads_line_buffer:
        return 0
    payload = "".join(nquads_line_buffer)
    text_stream.write(payload)
    nquads_line_buffer.clear()
    return len(payload.encode("utf-8"))


def _predicate_text_for_key(predicate_text_cache: Dict[str, str], key: str) -> str:
    predicate_text = predicate_text_cache.get(key)
    if predicate_text is None:
        predicate_text = str(shared_rdf.predicate_for(key))
        predicate_text_cache[key] = predicate_text
    return predicate_text


def _quarry_predicate_text_for_key(predicate_text_cache: Dict[str, str], key: str) -> str:
    predicate_text = predicate_text_cache.get(key)
    if predicate_text is None:
        predicate_text = str(quarry_store_builder._quarry_predicate_for(key))
        predicate_text_cache[key] = predicate_text
    return predicate_text


def _iri_text(iri_text_cache: Dict[str, str], iri: str) -> str:
    iri_text = iri_text_cache.get(iri)
    if iri_text is None:
        iri_text = str(_require_pyoxigraph().NamedNode(iri))
        iri_text_cache[iri] = iri_text
    return iri_text


def _nquads_object_text_for_typed_value(
    value: Any,
    sparql_type: str,
    *,
    iri_text_cache: Dict[str, str],
) -> str:
    if sparql_type == shared_rdf.SPARQL_IRI_TYPE:
        return _iri_text(iri_text_cache, str(value))
    return str(shared_rdf.object_term_for_typed_value(value, sparql_type))


def _nquads_object_text_for_kind_bits(
    value: Any,
    kind_bits: int,
    *,
    iri_text_cache: Dict[str, str],
) -> str:
    if kind_bits == quarry_store_builder._ROW_FIELD_KIND_IRI_BIT:
        return _iri_text(iri_text_cache, str(value))
    if kind_bits == quarry_store_builder._ROW_FIELD_KIND_DATETIME_BIT:
        return str(shared_rdf.object_term_for_typed_value(value, "xsd:dateTime"))
    return str(shared_rdf.literal_for(value))


def _write_petscan_record_nquads_lines(
    index: int,
    row: Mapping[str, Any],
    context: Any,
    resolved_gil_links: Sequence[tuple[str, Optional[str]]],
    nquads_line_buffer: List[str],
    *,
    predicate_text_cache: Dict[str, str],
    iri_text_cache: Dict[str, str],
) -> tuple[Dict[str, int], Dict[str, int], int]:
    row_field_kinds: Dict[str, int] = {}
    row_field_value_counts: Dict[str, int] = {}

    def _track_field_kind(key: str, kind: str) -> None:
        shared_rdf._track_row_field_kind(row_field_kinds, key, kind)
        shared_rdf._track_row_field_value_count(row_field_value_counts, key)

    predicates = context.predicates
    subject_text = str(shared_rdf.item_subject(context.psid, row, index))
    gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links] if "gil" in row else None
    chars_written = 0

    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.rdf_type),
        str(predicates.page_class),
    )
    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.psid),
        str(context.psid_literal),
    )
    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.position),
        str(shared_rdf.literal_for(index)),
    )
    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.loaded_at),
        str(context.loaded_at_literal),
    )

    for key, raw_value in shared_rdf.iter_scalar_fields(row, gil_links=gil_link_uris):
        value, sparql_type = shared_rdf._normalize_scalar_field_value_and_type(key, raw_value)
        _track_field_kind(key, sparql_type)
        predicate_text = _predicate_text_for_key(predicate_text_cache, key)
        object_text = _nquads_object_text_for_typed_value(
            value,
            sparql_type,
            iri_text_cache=iri_text_cache,
        )
        chars_written += _append_nquads_line(
            nquads_line_buffer,
            subject_text,
            predicate_text,
            object_text,
        )

    gil_link_predicate_text = _predicate_text_for_key(predicate_text_cache, "gil_link")
    for link_uri, qid in resolved_gil_links:
        link_text = _iri_text(iri_text_cache, link_uri)
        for key, value, sparql_type in shared_rdf.iter_typed_gil_link_fields(
            link_uri,
            qid,
            gil_link_enrichment_map=context.gil_link_enrichment_map,
        ):
            _track_field_kind(key, sparql_type)
            if key == "gil_link":
                predicate_text = gil_link_predicate_text
                quad_subject_text = subject_text
                object_text = link_text
            else:
                predicate_text = _predicate_text_for_key(predicate_text_cache, key)
                quad_subject_text = link_text
                object_text = _nquads_object_text_for_typed_value(
                    value,
                    sparql_type,
                    iri_text_cache=iri_text_cache,
                )
            chars_written += _append_nquads_line(
                nquads_line_buffer,
                quad_subject_text,
                predicate_text,
                object_text,
            )

    return row_field_kinds, row_field_value_counts, chars_written


def _write_quarry_record_nquads_lines(
    index: int,
    row: Mapping[str, Any],
    row_plan: Any,
    context: Any,
    resolved_gil_links: Sequence[tuple[str, Optional[str]]],
    nquads_line_buffer: List[str],
    *,
    predicate_text_cache: Dict[str, str],
    iri_text_cache: Dict[str, str],
) -> tuple[Dict[str, int], Dict[str, int], int]:
    row_field_kinds: Dict[str, int] = {}
    row_field_value_counts: Dict[str, int] = {}

    predicates = context.predicates
    track_field_kind = shared_rdf._track_row_field_kind
    track_field_kind_bits = shared_rdf._track_row_field_kind_bits
    track_field_value_count = shared_rdf._track_row_field_value_count
    subject_text = _iri_text(iri_text_cache, "{}{}".format(context.row_subject_base, index + 1))
    gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links] if row_plan.has_gil else None
    chars_written = 0

    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.rdf_type),
        str(predicates.page_class),
    )
    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.psid),
        str(context.quarry_id_literal),
    )
    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.position),
        str(shared_rdf.literal_for(index)),
    )
    chars_written += _append_nquads_line(
        nquads_line_buffer,
        subject_text,
        str(predicates.loaded_at),
        str(context.loaded_at_literal),
    )

    if row_plan.use_fast_scalar_path:
        row_get = row.get
        for field in row_plan.scalar_fields:
            raw_value = row_get(field.key)
            if raw_value is None:
                continue
            if isinstance(raw_value, quarry_store_builder._SCALAR_VALUE_TYPES):
                scalar_value = raw_value
            elif isinstance(raw_value, list):
                flattened_list = quarry_store_builder._normalize_fast_scalar_list(raw_value)
                if flattened_list is None:
                    continue
                scalar_value = flattened_list
            else:
                continue

            value, kind_bits = quarry_store_builder._normalize_planned_quarry_scalar_value_and_kind(
                field,
                scalar_value,
            )
            track_field_kind_bits(row_field_kinds, field.key, kind_bits)
            track_field_value_count(row_field_value_counts, field.key)
            predicate_text = _quarry_predicate_text_for_key(predicate_text_cache, field.key)
            object_text = _nquads_object_text_for_kind_bits(
                value,
                kind_bits,
                iri_text_cache=iri_text_cache,
            )
            chars_written += _append_nquads_line(
                nquads_line_buffer,
                subject_text,
                predicate_text,
                object_text,
            )
    else:
        for key, raw_value in shared_rdf.iter_scalar_fields(row, gil_links=gil_link_uris):
            value, sparql_type = quarry_store_builder._normalize_quarry_scalar_value_and_type(key, raw_value)
            track_field_kind(row_field_kinds, key, sparql_type)
            track_field_value_count(row_field_value_counts, key)
            predicate_text = _quarry_predicate_text_for_key(predicate_text_cache, key)
            object_text = _nquads_object_text_for_typed_value(
                value,
                sparql_type,
                iri_text_cache=iri_text_cache,
            )
            chars_written += _append_nquads_line(
                nquads_line_buffer,
                subject_text,
                predicate_text,
                object_text,
            )

    for link_uri, qid in resolved_gil_links:
        link_text = _iri_text(iri_text_cache, link_uri)
        for key, value, sparql_type in shared_rdf.iter_typed_gil_link_fields(
            link_uri,
            qid,
            gil_link_enrichment_map=context.gil_link_enrichment_map,
        ):
            track_field_kind(row_field_kinds, key, sparql_type)
            track_field_value_count(row_field_value_counts, key)
            if key == "gil_link":
                predicate_text = str(predicates.gil_link)
                quad_subject_text = subject_text
                object_text = link_text
            else:
                predicate_text = _quarry_predicate_text_for_key(predicate_text_cache, key)
                quad_subject_text = link_text
                object_text = _nquads_object_text_for_typed_value(
                    value,
                    sparql_type,
                    iri_text_cache=iri_text_cache,
                )
            chars_written += _append_nquads_line(
                nquads_line_buffer,
                quad_subject_text,
                predicate_text,
                object_text,
            )

    return row_field_kinds, row_field_value_counts, chars_written


def _measure_bulk_extend_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
) -> Dict[str, Any]:
    started_at = perf_counter()
    if spec["kind"] == _KIND_PETSCAN:
        petscan_store_builder.build_store(int(spec["source_id"]), records, source_url)
    else:
        quarry_store_builder.build_store(
            int(spec["store_id"]),
            int(spec["source_id"]),
            records,
            source_url,
            query_db=spec["query_db"],
        )
    return {"build_store_ms": (perf_counter() - started_at) * 1000.0}


def _measure_petscan_bulk_load_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = petscan_store_builder._reset_store_directory(int(spec["source_id"]))
    store_class = petscan_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    nquads_path = temp_root / "{}-bulk-load.nq".format(spec["name"])
    total_started_at = perf_counter()
    try:
        preload_started_at = perf_counter()
        predicates = petscan_store_builder._build_store_predicates()
        gil_link_result = service_links.build_gil_link_enrichment(records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = shared_rdf.StructureAccumulator()
        write_context = petscan_store_builder._RecordWriteContext(
            predicates=predicates,
            psid=int(spec["source_id"]),
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            psid_literal=px.Literal(
                str(spec["source_id"]),
                datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=px.Literal(
                loaded_at,
                datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
            ),
        )
        quad_buffer: List[Any] = []

        with nquads_path.open("w", encoding="utf-8") as nquads_file:
            for index, row in enumerate(records):
                resolved_gil_links = resolved_gil_links_by_row[index]
                row_field_kinds, row_field_value_counts = petscan_store_builder._write_record_quads(
                    index=index,
                    row=row,
                    context=write_context,
                    resolved_gil_links=resolved_gil_links,
                    quad_buffer=quad_buffer,
                )
                structure_accumulator.add_row_field_kinds(
                    row_field_kinds,
                    row_field_value_counts=row_field_value_counts,
                )
                if len(quad_buffer) >= petscan_store_builder._QUAD_BUFFER_TARGET:
                    _flush_quads_to_nquads_file(nquads_file, quad_buffer)
            _flush_quads_to_nquads_file(nquads_file, quad_buffer)

        preload_ms = (perf_counter() - preload_started_at) * 1000.0

        bulk_load_started_at = perf_counter()
        store_instance.bulk_load(
            path=str(nquads_path),
            format=px.RdfFormat.N_QUADS,
        )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0

        optimize_flush_started_at = perf_counter()
        petscan_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        meta = petscan_store_builder._build_store_meta(
            psid=int(spec["source_id"]),
            records=records,
            source_url=source_url,
            source_params=None,
            loaded_at=loaded_at,
            structure=structure_accumulator.build_summary(row_count=len(records)),
        )
        petscan_store_builder._persist_store_meta(int(spec["source_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "preload_ms": preload_ms,
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": nquads_path.stat().st_size,
        }
    finally:
        store_instance = None


def _measure_petscan_bulk_load_direct_file_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = petscan_store_builder._reset_store_directory(int(spec["source_id"]))
    store_class = petscan_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    nquads_path = temp_root / "{}-bulk-load-direct-file.nq".format(spec["name"])
    total_started_at = perf_counter()
    try:
        file_write_started_at = perf_counter()
        predicates = petscan_store_builder._build_store_predicates()
        gil_link_result = service_links.build_gil_link_enrichment(records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = shared_rdf.StructureAccumulator()
        write_context = petscan_store_builder._RecordWriteContext(
            predicates=predicates,
            psid=int(spec["source_id"]),
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            psid_literal=px.Literal(
                str(spec["source_id"]),
                datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=px.Literal(
                loaded_at,
                datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
            ),
        )
        nquads_line_buffer: List[str] = []
        predicate_text_cache: Dict[str, str] = {}
        iri_text_cache: Dict[str, str] = {}
        bytes_written = 0
        buffered_chars = 0

        with nquads_path.open("w", encoding="utf-8") as nquads_file:
            for index, row in enumerate(records):
                resolved_gil_links = resolved_gil_links_by_row[index]
                row_field_kinds, row_field_value_counts, row_chars = _write_petscan_record_nquads_lines(
                    index=index,
                    row=row,
                    context=write_context,
                    resolved_gil_links=resolved_gil_links,
                    nquads_line_buffer=nquads_line_buffer,
                    predicate_text_cache=predicate_text_cache,
                    iri_text_cache=iri_text_cache,
                )
                structure_accumulator.add_row_field_kinds(
                    row_field_kinds,
                    row_field_value_counts=row_field_value_counts,
                )
                buffered_chars += row_chars
                if buffered_chars >= _NQUADS_STREAM_CHUNK_TARGET_CHARS:
                    bytes_written += _flush_nquads_lines_to_text_stream(
                        nquads_file,
                        nquads_line_buffer,
                    )
                    buffered_chars = 0
            bytes_written += _flush_nquads_lines_to_text_stream(
                nquads_file,
                nquads_line_buffer,
            )

        file_write_ms = (perf_counter() - file_write_started_at) * 1000.0

        bulk_load_started_at = perf_counter()
        store_instance.bulk_load(
            path=str(nquads_path),
            format=px.RdfFormat.N_QUADS,
        )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0

        optimize_flush_started_at = perf_counter()
        petscan_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        meta = petscan_store_builder._build_store_meta(
            psid=int(spec["source_id"]),
            records=records,
            source_url=source_url,
            source_params=None,
            loaded_at=loaded_at,
            structure=structure_accumulator.build_summary(row_count=len(records)),
        )
        petscan_store_builder._persist_store_meta(int(spec["source_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "file_write_ms": file_write_ms,
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": bytes_written,
        }
    finally:
        store_instance = None


def _measure_quarry_bulk_load_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = quarry_store_builder._reset_store_directory(int(spec["store_id"]))
    store_class = quarry_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    nquads_path = temp_root / "{}-bulk-load.nq".format(spec["name"])
    total_started_at = perf_counter()
    try:
        preload_started_at = perf_counter()
        prepared_records = quarry_store_builder._records_with_derived_uris(records, spec["query_db"])
        row_write_plans = quarry_store_builder._row_write_plans(prepared_records)
        predicates = quarry_store_builder._build_store_predicates()
        gil_link_result = service_links.build_gil_link_enrichment(prepared_records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = shared_rdf.StructureAccumulator()
        write_context = quarry_store_builder._RecordWriteContext(
            predicates=predicates,
            quarry_id=int(spec["source_id"]),
            row_subject_base="{}/{}#".format(quarry_store_builder._QUARRY_ROW_BASE, spec["source_id"]),
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            quarry_id_literal=px.Literal(
                str(spec["source_id"]),
                datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=px.Literal(
                loaded_at,
                datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
            ),
        )
        quad_buffer: List[Any] = []

        with nquads_path.open("w", encoding="utf-8") as nquads_file:
            for index, row in enumerate(prepared_records):
                quarry_store_builder._write_record_quads(
                    index=index,
                    row=row,
                    row_plan=row_write_plans[index],
                    context=write_context,
                    resolved_gil_links=resolved_gil_links_by_row[index],
                    quad_buffer=quad_buffer,
                    structure_accumulator=structure_accumulator,
                )
                if len(quad_buffer) >= quarry_store_builder._QUAD_BUFFER_TARGET:
                    _flush_quads_to_nquads_file(nquads_file, quad_buffer)
            _flush_quads_to_nquads_file(nquads_file, quad_buffer)

        preload_ms = (perf_counter() - preload_started_at) * 1000.0

        bulk_load_started_at = perf_counter()
        store_instance.bulk_load(
            path=str(nquads_path),
            format=px.RdfFormat.N_QUADS,
        )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0

        optimize_flush_started_at = perf_counter()
        quarry_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        summary = quarry_store_builder._quarry_structure_summary(
            structure_accumulator.build_summary(row_count=len(prepared_records))
        )
        meta = quarry_store_builder._build_store_meta(
            store_id=int(spec["store_id"]),
            records=prepared_records,
            source_url=source_url,
            source_params=None,
            loaded_at=loaded_at,
            structure=summary,
        )
        quarry_store_builder._persist_store_meta(int(spec["store_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "preload_ms": preload_ms,
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": nquads_path.stat().st_size,
        }
    finally:
        store_instance = None


def _measure_quarry_bulk_load_direct_file_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = quarry_store_builder._reset_store_directory(int(spec["store_id"]))
    store_class = quarry_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    nquads_path = temp_root / "{}-bulk-load-direct-file.nq".format(spec["name"])
    total_started_at = perf_counter()
    try:
        file_write_started_at = perf_counter()
        prepared_records = quarry_store_builder._records_with_derived_uris(records, spec["query_db"])
        row_write_plans = quarry_store_builder._row_write_plans(prepared_records)
        predicates = quarry_store_builder._build_store_predicates()
        gil_link_result = service_links.build_gil_link_enrichment(prepared_records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = shared_rdf.StructureAccumulator()
        write_context = quarry_store_builder._RecordWriteContext(
            predicates=predicates,
            quarry_id=int(spec["source_id"]),
            row_subject_base="{}/{}#".format(quarry_store_builder._QUARRY_ROW_BASE, spec["source_id"]),
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            quarry_id_literal=px.Literal(
                str(spec["source_id"]),
                datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=px.Literal(
                loaded_at,
                datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
            ),
        )
        nquads_line_buffer: List[str] = []
        predicate_text_cache: Dict[str, str] = {}
        iri_text_cache: Dict[str, str] = {}
        bytes_written = 0
        buffered_chars = 0

        with nquads_path.open("w", encoding="utf-8") as nquads_file:
            for index, row in enumerate(prepared_records):
                row_field_kinds, row_field_value_counts, row_chars = _write_quarry_record_nquads_lines(
                    index=index,
                    row=row,
                    row_plan=row_write_plans[index],
                    context=write_context,
                    resolved_gil_links=resolved_gil_links_by_row[index],
                    nquads_line_buffer=nquads_line_buffer,
                    predicate_text_cache=predicate_text_cache,
                    iri_text_cache=iri_text_cache,
                )
                structure_accumulator.add_row_field_kinds(
                    row_field_kinds,
                    row_field_value_counts=row_field_value_counts,
                )
                buffered_chars += row_chars
                if buffered_chars >= _NQUADS_STREAM_CHUNK_TARGET_CHARS:
                    bytes_written += _flush_nquads_lines_to_text_stream(
                        nquads_file,
                        nquads_line_buffer,
                    )
                    buffered_chars = 0
            bytes_written += _flush_nquads_lines_to_text_stream(
                nquads_file,
                nquads_line_buffer,
            )

        file_write_ms = (perf_counter() - file_write_started_at) * 1000.0

        bulk_load_started_at = perf_counter()
        store_instance.bulk_load(
            path=str(nquads_path),
            format=px.RdfFormat.N_QUADS,
        )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0

        optimize_flush_started_at = perf_counter()
        quarry_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        summary = quarry_store_builder._quarry_structure_summary(
            structure_accumulator.build_summary(row_count=len(prepared_records))
        )
        meta = quarry_store_builder._build_store_meta(
            store_id=int(spec["store_id"]),
            records=prepared_records,
            source_url=source_url,
            source_params=None,
            loaded_at=loaded_at,
            structure=summary,
        )
        quarry_store_builder._persist_store_meta(int(spec["store_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "file_write_ms": file_write_ms,
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": bytes_written,
        }
    finally:
        store_instance = None


def _measure_petscan_bulk_load_gzip_stream_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = petscan_store_builder._reset_store_directory(int(spec["source_id"]))
    store_class = petscan_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    nquads_gzip_path = temp_root / "{}-bulk-load.nq.gz".format(spec["name"])
    total_started_at = perf_counter()
    try:
        preload_started_at = perf_counter()
        predicates = petscan_store_builder._build_store_predicates()
        gil_link_result = service_links.build_gil_link_enrichment(records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = shared_rdf.StructureAccumulator()
        write_context = petscan_store_builder._RecordWriteContext(
            predicates=predicates,
            psid=int(spec["source_id"]),
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            psid_literal=px.Literal(
                str(spec["source_id"]),
                datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=px.Literal(
                loaded_at,
                datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
            ),
        )
        quad_buffer: List[Any] = []

        with gzip.open(nquads_gzip_path, "wt", encoding="utf-8") as nquads_gzip_file:
            for index, row in enumerate(records):
                resolved_gil_links = resolved_gil_links_by_row[index]
                row_field_kinds, row_field_value_counts = petscan_store_builder._write_record_quads(
                    index=index,
                    row=row,
                    context=write_context,
                    resolved_gil_links=resolved_gil_links,
                    quad_buffer=quad_buffer,
                )
                structure_accumulator.add_row_field_kinds(
                    row_field_kinds,
                    row_field_value_counts=row_field_value_counts,
                )
                if len(quad_buffer) >= petscan_store_builder._QUAD_BUFFER_TARGET:
                    _flush_quads_to_nquads_file(nquads_gzip_file, quad_buffer)
            _flush_quads_to_nquads_file(nquads_gzip_file, quad_buffer)

        preload_ms = (perf_counter() - preload_started_at) * 1000.0

        bulk_load_started_at = perf_counter()
        with gzip.open(nquads_gzip_path, "rb") as nquads_gzip_stream:
            store_instance.bulk_load(
                input=nquads_gzip_stream,
                format=px.RdfFormat.N_QUADS,
            )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0

        optimize_flush_started_at = perf_counter()
        petscan_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        meta = petscan_store_builder._build_store_meta(
            psid=int(spec["source_id"]),
            records=records,
            source_url=source_url,
            source_params=None,
            loaded_at=loaded_at,
            structure=structure_accumulator.build_summary(row_count=len(records)),
        )
        petscan_store_builder._persist_store_meta(int(spec["source_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "preload_ms": preload_ms,
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": nquads_gzip_path.stat().st_size,
        }
    finally:
        store_instance = None


def _measure_quarry_bulk_load_gzip_stream_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = quarry_store_builder._reset_store_directory(int(spec["store_id"]))
    store_class = quarry_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    nquads_gzip_path = temp_root / "{}-bulk-load.nq.gz".format(spec["name"])
    total_started_at = perf_counter()
    try:
        preload_started_at = perf_counter()
        prepared_records = quarry_store_builder._records_with_derived_uris(records, spec["query_db"])
        row_write_plans = quarry_store_builder._row_write_plans(prepared_records)
        predicates = quarry_store_builder._build_store_predicates()
        gil_link_result = service_links.build_gil_link_enrichment(prepared_records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = shared_rdf.StructureAccumulator()
        write_context = quarry_store_builder._RecordWriteContext(
            predicates=predicates,
            quarry_id=int(spec["source_id"]),
            row_subject_base="{}/{}#".format(quarry_store_builder._QUARRY_ROW_BASE, spec["source_id"]),
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            quarry_id_literal=px.Literal(
                str(spec["source_id"]),
                datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
            ),
            loaded_at_literal=px.Literal(
                loaded_at,
                datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
            ),
        )
        quad_buffer: List[Any] = []

        with gzip.open(nquads_gzip_path, "wt", encoding="utf-8") as nquads_gzip_file:
            for index, row in enumerate(prepared_records):
                quarry_store_builder._write_record_quads(
                    index=index,
                    row=row,
                    row_plan=row_write_plans[index],
                    context=write_context,
                    resolved_gil_links=resolved_gil_links_by_row[index],
                    quad_buffer=quad_buffer,
                    structure_accumulator=structure_accumulator,
                )
                if len(quad_buffer) >= quarry_store_builder._QUAD_BUFFER_TARGET:
                    _flush_quads_to_nquads_file(nquads_gzip_file, quad_buffer)
            _flush_quads_to_nquads_file(nquads_gzip_file, quad_buffer)

        preload_ms = (perf_counter() - preload_started_at) * 1000.0

        bulk_load_started_at = perf_counter()
        with gzip.open(nquads_gzip_path, "rb") as nquads_gzip_stream:
            store_instance.bulk_load(
                input=nquads_gzip_stream,
                format=px.RdfFormat.N_QUADS,
            )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0

        optimize_flush_started_at = perf_counter()
        quarry_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        summary = quarry_store_builder._quarry_structure_summary(
            structure_accumulator.build_summary(row_count=len(prepared_records))
        )
        meta = quarry_store_builder._build_store_meta(
            store_id=int(spec["store_id"]),
            records=prepared_records,
            source_url=source_url,
            source_params=None,
            loaded_at=loaded_at,
            structure=summary,
        )
        quarry_store_builder._persist_store_meta(int(spec["store_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "preload_ms": preload_ms,
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": nquads_gzip_path.stat().st_size,
        }
    finally:
        store_instance = None


def _measure_petscan_bulk_load_stream_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = petscan_store_builder._reset_store_directory(int(spec["source_id"]))
    store_class = petscan_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    total_started_at = perf_counter()
    writer_metrics: Dict[str, Any] = {}
    writer_error: List[BaseException] = []
    read_fd, write_fd = os.pipe()

    def _writer() -> None:
        try:
            write_started_at = perf_counter()
            predicates = petscan_store_builder._build_store_predicates()
            gil_link_result = service_links.build_gil_link_enrichment(records)
            resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
            gil_link_enrichment_map = gil_link_result.enrichment_by_link
            loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            structure_accumulator = shared_rdf.StructureAccumulator()
            write_context = petscan_store_builder._RecordWriteContext(
                predicates=predicates,
                psid=int(spec["source_id"]),
                gil_link_enrichment_map=gil_link_enrichment_map,
                xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
                psid_literal=px.Literal(
                    str(spec["source_id"]),
                    datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
                ),
                loaded_at_literal=px.Literal(
                    loaded_at,
                    datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
                ),
            )
            nquads_line_buffer: List[str] = []
            predicate_text_cache: Dict[str, str] = {}
            iri_text_cache: Dict[str, str] = {}
            bytes_written = 0
            buffered_chars = 0

            with os.fdopen(write_fd, "wb", buffering=0, closefd=True) as raw_writer:
                with io.BufferedWriter(raw_writer, buffer_size=1024 * 1024) as buffered_writer:
                    for index, row in enumerate(records):
                        resolved_gil_links = resolved_gil_links_by_row[index]
                        row_field_kinds, row_field_value_counts, row_chars = _write_petscan_record_nquads_lines(
                            index=index,
                            row=row,
                            context=write_context,
                            resolved_gil_links=resolved_gil_links,
                            nquads_line_buffer=nquads_line_buffer,
                            predicate_text_cache=predicate_text_cache,
                            iri_text_cache=iri_text_cache,
                        )
                        structure_accumulator.add_row_field_kinds(
                            row_field_kinds,
                            row_field_value_counts=row_field_value_counts,
                        )
                        buffered_chars += row_chars
                        if buffered_chars >= _NQUADS_STREAM_CHUNK_TARGET_CHARS:
                            bytes_written += _flush_nquads_lines_to_binary_stream(
                                buffered_writer,
                                nquads_line_buffer,
                            )
                            buffered_chars = 0
                    bytes_written += _flush_nquads_lines_to_binary_stream(
                        buffered_writer,
                        nquads_line_buffer,
                    )
                    buffered_writer.flush()

            writer_metrics.update(
                {
                    "stream_write_ms": (perf_counter() - write_started_at) * 1000.0,
                    "nquads_bytes": bytes_written,
                    "loaded_at": loaded_at,
                    "structure": structure_accumulator.build_summary(row_count=len(records)),
                }
            )
        except BaseException as exc:  # pragma: no cover - benchmark worker failure path
            try:
                os.close(write_fd)
            except OSError:
                pass
            writer_error.append(exc)

    writer_thread = threading.Thread(target=_writer, name="petscan-bulk-load-stream-writer")
    writer_thread.start()
    try:
        bulk_load_started_at = perf_counter()
        with os.fdopen(read_fd, "rb", closefd=True) as reader:
            store_instance.bulk_load(
                input=reader,
                format=px.RdfFormat.N_QUADS,
            )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0
        writer_thread.join()
        if writer_error:
            raise writer_error[0]

        optimize_flush_started_at = perf_counter()
        petscan_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        meta = petscan_store_builder._build_store_meta(
            psid=int(spec["source_id"]),
            records=records,
            source_url=source_url,
            source_params=None,
            loaded_at=str(writer_metrics["loaded_at"]),
            structure=writer_metrics["structure"],
        )
        petscan_store_builder._persist_store_meta(int(spec["source_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "stream_write_ms": float(writer_metrics["stream_write_ms"]),
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": int(writer_metrics["nquads_bytes"]),
        }
    finally:
        writer_thread.join()
        store_instance = None


def _measure_quarry_bulk_load_stream_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
) -> Dict[str, Any]:
    px = _require_pyoxigraph()
    store_path = quarry_store_builder._reset_store_directory(int(spec["store_id"]))
    store_class = quarry_store_builder._require_store_class()
    store_instance = store_class(str(store_path))
    total_started_at = perf_counter()
    writer_metrics: Dict[str, Any] = {}
    writer_error: List[BaseException] = []
    read_fd, write_fd = os.pipe()

    def _writer() -> None:
        try:
            write_started_at = perf_counter()
            prepared_records = quarry_store_builder._records_with_derived_uris(records, spec["query_db"])
            row_write_plans = quarry_store_builder._row_write_plans(prepared_records)
            predicates = quarry_store_builder._build_store_predicates()
            gil_link_result = service_links.build_gil_link_enrichment(prepared_records)
            resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
            gil_link_enrichment_map = gil_link_result.enrichment_by_link
            loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            structure_accumulator = shared_rdf.StructureAccumulator()
            write_context = quarry_store_builder._RecordWriteContext(
                predicates=predicates,
                quarry_id=int(spec["source_id"]),
                row_subject_base="{}/{}#".format(quarry_store_builder._QUARRY_ROW_BASE, spec["source_id"]),
                gil_link_enrichment_map=gil_link_enrichment_map,
                xsd_integer_type=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
                quarry_id_literal=px.Literal(
                    str(spec["source_id"]),
                    datatype=px.NamedNode(shared_rdf.XSD_INTEGER_IRI),
                ),
                loaded_at_literal=px.Literal(
                    loaded_at,
                    datatype=px.NamedNode(shared_rdf.XSD_DATE_TIME_IRI),
                ),
            )
            nquads_line_buffer: List[str] = []
            predicate_text_cache: Dict[str, str] = {}
            iri_text_cache: Dict[str, str] = {}
            bytes_written = 0
            buffered_chars = 0

            with os.fdopen(write_fd, "wb", buffering=0, closefd=True) as raw_writer:
                with io.BufferedWriter(raw_writer, buffer_size=1024 * 1024) as buffered_writer:
                    for index, row in enumerate(prepared_records):
                        row_field_kinds, row_field_value_counts, row_chars = _write_quarry_record_nquads_lines(
                            index=index,
                            row=row,
                            row_plan=row_write_plans[index],
                            context=write_context,
                            resolved_gil_links=resolved_gil_links_by_row[index],
                            nquads_line_buffer=nquads_line_buffer,
                            predicate_text_cache=predicate_text_cache,
                            iri_text_cache=iri_text_cache,
                        )
                        structure_accumulator.add_row_field_kinds(
                            row_field_kinds,
                            row_field_value_counts=row_field_value_counts,
                        )
                        buffered_chars += row_chars
                        if buffered_chars >= _NQUADS_STREAM_CHUNK_TARGET_CHARS:
                            bytes_written += _flush_nquads_lines_to_binary_stream(
                                buffered_writer,
                                nquads_line_buffer,
                            )
                            buffered_chars = 0
                    bytes_written += _flush_nquads_lines_to_binary_stream(
                        buffered_writer,
                        nquads_line_buffer,
                    )
                    buffered_writer.flush()

            writer_metrics.update(
                {
                    "stream_write_ms": (perf_counter() - write_started_at) * 1000.0,
                    "nquads_bytes": bytes_written,
                    "loaded_at": loaded_at,
                    "records": prepared_records,
                    "structure": quarry_store_builder._quarry_structure_summary(
                        structure_accumulator.build_summary(row_count=len(prepared_records))
                    ),
                }
            )
        except BaseException as exc:  # pragma: no cover - benchmark worker failure path
            try:
                os.close(write_fd)
            except OSError:
                pass
            writer_error.append(exc)

    writer_thread = threading.Thread(target=_writer, name="quarry-bulk-load-stream-writer")
    writer_thread.start()
    try:
        bulk_load_started_at = perf_counter()
        with os.fdopen(read_fd, "rb", closefd=True) as reader:
            store_instance.bulk_load(
                input=reader,
                format=px.RdfFormat.N_QUADS,
            )
        bulk_load_ms = (perf_counter() - bulk_load_started_at) * 1000.0
        writer_thread.join()
        if writer_error:
            raise writer_error[0]

        optimize_flush_started_at = perf_counter()
        quarry_store_builder._optimize_store(store_instance)
        store_instance.flush()
        optimize_flush_ms = (perf_counter() - optimize_flush_started_at) * 1000.0

        meta = quarry_store_builder._build_store_meta(
            store_id=int(spec["store_id"]),
            records=writer_metrics["records"],
            source_url=source_url,
            source_params=None,
            loaded_at=str(writer_metrics["loaded_at"]),
            structure=writer_metrics["structure"],
        )
        quarry_store_builder._persist_store_meta(int(spec["store_id"]), meta)

        return {
            "build_store_ms": (perf_counter() - total_started_at) * 1000.0,
            "stream_write_ms": float(writer_metrics["stream_write_ms"]),
            "bulk_load_ms": bulk_load_ms,
            "optimize_flush_ms": optimize_flush_ms,
            "nquads_bytes": int(writer_metrics["nquads_bytes"]),
        }
    finally:
        writer_thread.join()
        store_instance = None


def _measure_bulk_load_nquads_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    if spec["kind"] == _KIND_PETSCAN:
        return _measure_petscan_bulk_load_result(spec, records, source_url=source_url, temp_root=temp_root)
    return _measure_quarry_bulk_load_result(spec, records, source_url=source_url, temp_root=temp_root)


def _measure_bulk_load_nquads_direct_file_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    if spec["kind"] == _KIND_PETSCAN:
        return _measure_petscan_bulk_load_direct_file_result(
            spec,
            records,
            source_url=source_url,
            temp_root=temp_root,
        )
    return _measure_quarry_bulk_load_direct_file_result(
        spec,
        records,
        source_url=source_url,
        temp_root=temp_root,
    )


def _measure_bulk_load_nquads_stream_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
) -> Dict[str, Any]:
    if spec["kind"] == _KIND_PETSCAN:
        return _measure_petscan_bulk_load_stream_result(
            spec,
            records,
            source_url=source_url,
        )
    return _measure_quarry_bulk_load_stream_result(
        spec,
        records,
        source_url=source_url,
    )


def _measure_bulk_load_nquads_gzip_stream_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    source_url: str,
    temp_root: Path,
) -> Dict[str, Any]:
    if spec["kind"] == _KIND_PETSCAN:
        return _measure_petscan_bulk_load_gzip_stream_result(
            spec,
            records,
            source_url=source_url,
            temp_root=temp_root,
        )
    return _measure_quarry_bulk_load_gzip_stream_result(
        spec,
        records,
        source_url=source_url,
        temp_root=temp_root,
    )


def _measure_build_store_result(
    spec: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    strategy: str,
) -> Dict[str, Any]:
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
                if strategy == _STRATEGY_BULK_EXTEND:
                    return _measure_bulk_extend_result(spec, records, source_url=source_url)
                if strategy == _STRATEGY_BULK_LOAD_NQUADS:
                    return _measure_bulk_load_nquads_result(
                        spec,
                        records,
                        source_url=source_url,
                        temp_root=temp_root,
                    )
                if strategy == _STRATEGY_BULK_LOAD_NQUADS_DIRECT_FILE:
                    return _measure_bulk_load_nquads_direct_file_result(
                        spec,
                        records,
                        source_url=source_url,
                        temp_root=temp_root,
                    )
                if strategy == _STRATEGY_BULK_LOAD_NQUADS_STREAM:
                    return _measure_bulk_load_nquads_stream_result(
                        spec,
                        records,
                        source_url=source_url,
                    )
                if strategy == _STRATEGY_BULK_LOAD_NQUADS_GZIP_STREAM:
                    return _measure_bulk_load_nquads_gzip_stream_result(
                        spec,
                        records,
                        source_url=source_url,
                        temp_root=temp_root,
                    )
                raise CommandError("Unsupported write strategy: {}".format(strategy))

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
            if strategy == _STRATEGY_BULK_EXTEND:
                return _measure_bulk_extend_result(spec, records, source_url=source_url)
            if strategy == _STRATEGY_BULK_LOAD_NQUADS:
                return _measure_bulk_load_nquads_result(
                    spec,
                    records,
                    source_url=source_url,
                    temp_root=temp_root,
                )
            if strategy == _STRATEGY_BULK_LOAD_NQUADS_DIRECT_FILE:
                return _measure_bulk_load_nquads_direct_file_result(
                    spec,
                    records,
                    source_url=source_url,
                    temp_root=temp_root,
                )
            if strategy == _STRATEGY_BULK_LOAD_NQUADS_STREAM:
                return _measure_bulk_load_nquads_stream_result(
                    spec,
                    records,
                    source_url=source_url,
                )
            if strategy == _STRATEGY_BULK_LOAD_NQUADS_GZIP_STREAM:
                return _measure_bulk_load_nquads_gzip_stream_result(
                    spec,
                    records,
                    source_url=source_url,
                    temp_root=temp_root,
                )
            raise CommandError("Unsupported write strategy: {}".format(strategy))


def _metric_summaries_from_runs(runs: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, float]]:
    metric_names = sorted(
        {
            str(key)
            for run in runs
            for key in run.keys()
            if key not in {"run_index", "warmup", "build_store_ms"}
        }
    )
    summaries: Dict[str, Dict[str, float]] = {}
    for metric_name in metric_names:
        values = [float(run[metric_name]) for run in runs if metric_name in run]
        if not values:
            continue
        summaries[metric_name] = {
            "median": median(values),
            "mean": mean(values),
            "min": min(values),
            "max": max(values),
        }
    return summaries


def _benchmark_dataset(
    spec: Mapping[str, Any],
    runs: int,
    warmup: int,
    *,
    strategy: str,
) -> Dict[str, Any]:
    loaded = _load_records_for_spec(spec)
    records = loaded["records"]
    durations: List[float] = []
    measured_runs: List[Dict[str, Any]] = []

    total_runs = warmup + runs
    for iteration_index in range(total_runs):
        build_result = _measure_build_store_result(spec, records, strategy=strategy)
        elapsed_ms = float(build_result["build_store_ms"])
        is_warmup = iteration_index < warmup
        run_payload = {
            "run_index": iteration_index + 1,
            "warmup": is_warmup,
            "build_store_ms": elapsed_ms,
        }
        for key, value in build_result.items():
            if key != "build_store_ms":
                run_payload[key] = value
        if not is_warmup:
            durations.append(elapsed_ms)
            measured_runs.append(run_payload)

    result = {
        "name": spec["name"],
        "kind": spec["kind"],
        "file_name": spec["file_name"],
        "source_id": spec["source_id"],
        "store_id": spec["store_id"],
        "query_db": spec["query_db"],
        "records": len(records),
        "use_fake_api": bool(spec["use_fake_api"]),
        "write_strategy": strategy,
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
    metric_summaries = _metric_summaries_from_runs(measured_runs)
    if metric_summaries:
        result["metric_summaries"] = metric_summaries
    return result


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
        "write_strategy": report.get("write_strategy"),
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
        parser.add_argument(
            "--strategy",
            choices=_WRITE_STRATEGIES,
            default=_STRATEGY_BULK_EXTEND,
            help=(
                "Store write strategy to benchmark. "
                "Use 'bulk_extend' for the current path, 'bulk_load_nquads' for plain N-Quads bulk_load via temp file, "
                "'bulk_load_nquads_direct_file' for direct N-Quads line serialization via temp file, "
                "'bulk_load_nquads_stream' for direct uncompressed streaming input without a temp file, "
                "or 'bulk_load_nquads_gzip_stream' for gzip.open(...) streaming input."
            ),
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

        strategy = str(options["strategy"] or _STRATEGY_BULK_EXTEND)
        label = str(options["label"] or "").strip() or None
        output_raw = str(options["output"] or "").strip()
        output_path = Path(output_raw) if output_raw else _default_output_path(label)

        specs = _load_dataset_specs(DATASET_SPEC_PATH)
        selected_specs = _select_dataset_specs(specs, str(options["datasets"] or "all"))
        selected_names = [str(spec["name"]) for spec in selected_specs]

        self.stdout.write(
            "Running offline benchmark for datasets={} runs={} warmup={} strategy={}".format(
                selected_names,
                runs,
                warmup,
                strategy,
            )
        )

        benchmark_results = []
        for spec in selected_specs:
            self.stdout.write(
                "Benchmarking dataset {} ({})...".format(spec["name"], spec["file_name"])
            )
            dataset_result = _benchmark_dataset(spec, runs=runs, warmup=warmup, strategy=strategy)
            benchmark_results.append(dataset_result)
            self.stdout.write(
                "dataset={} strategy={} records={} median_build_store_ms={:.1f} mean_build_store_ms={:.1f}".format(
                    dataset_result["name"],
                    dataset_result["write_strategy"],
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
            "write_strategy": strategy,
            "environment": _collect_environment_info(),
            "datasets": benchmark_results,
        }

        _write_report_files(report, output_path)

        self.stdout.write(self.style.SUCCESS("Saved benchmark report."))
        self.stdout.write("result_output={}".format(output_path))
        self.stdout.write("latest_output={}".format(LATEST_RESULT_PATH))
        self.stdout.write("history_output={}".format(HISTORY_PATH))
