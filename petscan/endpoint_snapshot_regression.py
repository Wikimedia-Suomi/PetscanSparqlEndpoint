"""Offline endpoint-output snapshot generation and verification helpers."""

from __future__ import annotations

import gzip
import hashlib
import json
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Literal, Optional, Sequence, Tuple
from unittest.mock import patch
from urllib.parse import urlparse

from django.conf import settings
from django.test.utils import override_settings

from petscan import service as petscan_service
from petscan import service_links
from petscan import service_source as petscan_source
from quarry import service as quarry_service
from quarry import service_source as quarry_source

ALL_VALUES_GRAPH_QUERY = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
FIXED_LOADED_AT = datetime(2026, 3, 21, tzinfo=timezone.utc)
EXAMPLES_DIRNAME = "examples"
ENDPOINT_SNAPSHOTS_DIRNAME = "endpoint_snapshots"
_DEFAULT_QUERY_DB_SITEINFO = {
    "fiwiki_p": {
        "domain": "fi.wikipedia.org",
        "article_path": "/wiki/$1",
        "namespace_names": {
            0: "",
            6: "Tiedosto",
            14: "Luokka",
        },
        "interwiki_urls": {
            "commons": "https://commons.wikimedia.org/wiki/$1",
        },
    }
}


class SnapshotMismatchError(Exception):
    """Raised when a regenerated endpoint snapshot differs from the stored baseline."""


@dataclass(frozen=True)
class SnapshotCase:
    name: str
    kind: Literal["petscan", "quarry"]
    service_id: int
    source_file: str
    output_file: str
    qrun_id: Optional[int] = None
    query_db: Optional[str] = None
    limit: Optional[int] = None


@dataclass(frozen=True)
class SnapshotResult:
    case: SnapshotCase
    snapshot_path: Path
    triple_count: int
    sha256: str
    byte_count: int


class _FixedDateTime:
    @staticmethod
    def now(_tz: Optional[timezone] = None) -> datetime:
        return FIXED_LOADED_AT


SNAPSHOT_CASES: Tuple[SnapshotCase, ...] = (
    SnapshotCase(
        name="petscan-43641756",
        kind="petscan",
        service_id=43641756,
        source_file="petscan-43641756.json.gz",
        output_file="petscan-43641756.nt.gz",
    ),
    SnapshotCase(
        name="petscan-43642782",
        kind="petscan",
        service_id=43642782,
        source_file="petscan-43642782.json.gz",
        output_file="petscan-43642782.nt.gz",
    ),
    SnapshotCase(
        name="petscan-43706364",
        kind="petscan",
        service_id=43706364,
        source_file="petscan-43706364.json.gz",
        output_file="petscan-43706364.nt.gz",
    ),
    SnapshotCase(
        name="quarry-103479-run-1084300",
        kind="quarry",
        service_id=103479,
        source_file="quarry-103479-run-1084300.json.gz",
        output_file="quarry-103479-run-1084300.nt.gz",
        qrun_id=1084300,
        query_db="fiwiki_p",
    ),
    SnapshotCase(
        name="quarry-103514-run-1084648",
        kind="quarry",
        service_id=103514,
        source_file="quarry-103514-run-1084648.json.gz",
        output_file="quarry-103514-run-1084648.nt.gz",
        qrun_id=1084648,
        query_db="fiwiki_p",
    ),
)
SNAPSHOT_CASES_BY_NAME = {case.name: case for case in SNAPSHOT_CASES}


def examples_dir() -> Path:
    return Path(settings.BASE_DIR) / "data" / EXAMPLES_DIRNAME


def endpoint_snapshots_dir() -> Path:
    return Path(settings.BASE_DIR) / "data" / ENDPOINT_SNAPSHOTS_DIRNAME


def iter_snapshot_cases(selected_names: Optional[Sequence[str]] = None) -> Tuple[SnapshotCase, ...]:
    if not selected_names:
        return SNAPSHOT_CASES

    selected: list[SnapshotCase] = []
    seen = set()
    for raw_name in selected_names:
        name = str(raw_name).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        selected.append(SNAPSHOT_CASES_BY_NAME[name])
    return tuple(selected)


def example_payload_path(case: SnapshotCase) -> Path:
    return examples_dir() / case.source_file


def snapshot_path(case: SnapshotCase, snapshot_dir: Optional[Path] = None) -> Path:
    base_dir = snapshot_dir if snapshot_dir is not None else endpoint_snapshots_dir()
    return base_dir / case.output_file


def _load_json_payload(path: Path) -> Dict[str, Any]:
    if path.suffix == ".gz":
        with gzip.open(path, mode="rt", encoding="utf-8") as payload_file:
            payload = json.load(payload_file)
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(payload, dict):
        raise ValueError("Snapshot payload must be a JSON object: {}".format(path))
    return payload


def read_snapshot_text(path: Path) -> str:
    if path.suffix == ".gz":
        with gzip.open(path, mode="rt", encoding="utf-8") as payload_file:
            return payload_file.read()
    return path.read_text(encoding="utf-8")


def write_snapshot_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".gz":
        with path.open("wb") as raw_file:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_file, mtime=0) as gz_file:
                gz_file.write(text.encode("utf-8"))
        return
    path.write_text(text, encoding="utf-8")


def _canonicalize_ntriples(payload: str) -> str:
    lines = [line.rstrip() for line in str(payload or "").splitlines() if line.strip()]
    lines.sort()
    if not lines:
        return ""
    return "".join("{}\n".format(line) for line in lines)


def _build_snapshot_result(case: SnapshotCase, path: Path, payload: str) -> SnapshotResult:
    normalized_payload = str(payload)
    return SnapshotResult(
        case=case,
        snapshot_path=path,
        triple_count=sum(1 for line in normalized_payload.splitlines() if line.strip()),
        sha256=hashlib.sha256(normalized_payload.encode("utf-8")).hexdigest(),
        byte_count=len(normalized_payload.encode("utf-8")),
    )


def _fake_enrichment_payload(site: str, title: str) -> Optional[Dict[str, Any]]:
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


def _fake_enrichment_fetch(
    api_url: str,
    titles: Sequence[str],
    **_kwargs: Any,
) -> Dict[str, Dict[str, Any]]:
    site = urlparse(api_url).netloc.lower()
    resolved: Dict[str, Dict[str, Any]] = {}
    for title in titles:
        payload = _fake_enrichment_payload(site, title)
        if payload is not None:
            resolved[title] = payload
    return resolved


def _offline_siteinfo_for_query_db(query_db: str) -> Optional[Dict[str, Any]]:
    return _DEFAULT_QUERY_DB_SITEINFO.get(str(query_db or "").strip().lower())


def _execute_petscan_case(case: SnapshotCase, query: str) -> str:
    payload = _load_json_payload(example_payload_path(case))
    source_url = petscan_source.build_petscan_url(case.service_id)

    with ExitStack() as stack:
        temp_dir = stack.enter_context(TemporaryDirectory(prefix="endpoint-snapshot-petscan-"))
        stack.enter_context(override_settings(OXIGRAPH_BASE_DIR=temp_dir))
        stack.enter_context(
            patch(
                "petscan.service_source.fetch_petscan_json",
                return_value=(payload, source_url),
            )
        )
        stack.enter_context(
            patch(
                "petscan.service_links.wikidata_lookup_backend",
                return_value=service_links.LOOKUP_BACKEND_API,
            )
        )
        stack.enter_context(
            patch(
                "petscan.service_links.fetch_wikibase_items_for_site_api",
                side_effect=_fake_enrichment_fetch,
            )
        )
        stack.enter_context(patch("petscan.service_store_builder.datetime", _FixedDateTime))

        execution = petscan_service.execute_query(case.service_id, query, refresh=True, petscan_params=None)

    if execution["result_format"] != "n-triples":
        raise ValueError("Expected n-triples result format for {}.".format(case.name))
    return _canonicalize_ntriples(execution["ntriples"])


def _execute_quarry_case(case: SnapshotCase, query: str) -> str:
    if case.qrun_id is None:
        raise ValueError("Quarry snapshot case {} is missing qrun_id.".format(case.name))

    payload = _load_json_payload(example_payload_path(case))
    resolution = {
        "quarry_id": case.service_id,
        "qrun_id": case.qrun_id,
        "query_db": case.query_db,
        "query_url": quarry_source.build_quarry_query_url(case.service_id),
        "json_url": quarry_source.build_quarry_json_url(case.qrun_id),
    }

    with ExitStack() as stack:
        temp_dir = stack.enter_context(TemporaryDirectory(prefix="endpoint-snapshot-quarry-"))
        stack.enter_context(override_settings(OXIGRAPH_BASE_DIR=temp_dir))
        stack.enter_context(
            patch(
                "quarry.service_source.resolve_quarry_run",
                return_value=resolution,
            )
        )
        stack.enter_context(
            patch(
                "quarry.service_source.fetch_quarry_json",
                return_value=(payload, resolution["json_url"]),
            )
        )
        stack.enter_context(
            patch(
                "quarry.service_uri_derivation._siteinfo_for_query_db",
                side_effect=_offline_siteinfo_for_query_db,
            )
        )
        stack.enter_context(
            patch(
                "petscan.service_links.wikidata_lookup_backend",
                return_value=service_links.LOOKUP_BACKEND_API,
            )
        )
        stack.enter_context(
            patch(
                "petscan.service_links.fetch_wikibase_items_for_site_api",
                side_effect=_fake_enrichment_fetch,
            )
        )
        stack.enter_context(patch("quarry.service_store_builder.datetime", _FixedDateTime))

        execution = quarry_service.execute_query(case.service_id, query, refresh=True, limit=case.limit)

    if execution["result_format"] != "n-triples":
        raise ValueError("Expected n-triples result format for {}.".format(case.name))
    return _canonicalize_ntriples(execution["ntriples"])


def render_case_snapshot(case: SnapshotCase, query: str = ALL_VALUES_GRAPH_QUERY) -> str:
    if case.kind == "petscan":
        return _execute_petscan_case(case, query)
    if case.kind == "quarry":
        return _execute_quarry_case(case, query)
    raise ValueError("Unsupported snapshot case kind: {}".format(case.kind))


def write_case_snapshot(
    case: SnapshotCase,
    *,
    snapshot_dir: Optional[Path] = None,
    query: str = ALL_VALUES_GRAPH_QUERY,
) -> SnapshotResult:
    payload = render_case_snapshot(case, query=query)
    path = snapshot_path(case, snapshot_dir=snapshot_dir)
    write_snapshot_text(path, payload)
    return _build_snapshot_result(case, path, payload)


def verify_case_snapshot(
    case: SnapshotCase,
    *,
    snapshot_dir: Optional[Path] = None,
    query: str = ALL_VALUES_GRAPH_QUERY,
) -> SnapshotResult:
    path = snapshot_path(case, snapshot_dir=snapshot_dir)
    expected_payload = read_snapshot_text(path)
    actual_payload = render_case_snapshot(case, query=query)
    if actual_payload != expected_payload:
        expected_result = _build_snapshot_result(case, path, expected_payload)
        actual_result = _build_snapshot_result(case, path, actual_payload)
        raise SnapshotMismatchError(
            "{} snapshot mismatch: expected sha256={} got sha256={}".format(
                case.name,
                expected_result.sha256,
                actual_result.sha256,
            )
        )
    return _build_snapshot_result(case, path, actual_payload)
