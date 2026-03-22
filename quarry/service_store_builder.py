"""Oxigraph store construction from Quarry rows."""

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from petscan import service_links as links
from petscan import service_rdf as rdf
from petscan import service_source as source
from petscan import service_store as store
from petscan.service_errors import PetscanServiceError
from petscan.service_types import StoreMeta, StoreMetaModel, StructureField, StructureSummary

from . import service_uri_derivation as uri_derivation

__all__ = ["build_store"]
_QUAD_BUFFER_TARGET = 4_000_000
_QUARRY_ROW_BASE = "https://quarry.wmcloud.org/query"
QUARRY_PREDICATE_BASE = "https://quarry.wmcloud.org/ontology/"
_COMPACT_TIMESTAMP_RE = re.compile(r"^20\d{12}$")
_WIKIDATA_QID_RE = re.compile(r"^Q\d+$")
_COMMONS_MID_RE = re.compile(r"^M\d+$")
_ABSOLUTE_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://\S+$")
_ALLOWED_EXTERNAL_URI_SCHEMES = frozenset({"http", "https"})

try:
    from pyoxigraph import Literal, NamedNode, Quad, Store
except ImportError:  # pragma: no cover - dependency check at runtime
    Literal = None  # type: ignore[misc,assignment]
    NamedNode = None  # type: ignore[misc,assignment]
    Quad = None  # type: ignore[misc,assignment]
    Store = None  # type: ignore[misc,assignment]


@dataclass(frozen=True)
class _StorePredicates:
    page_class: Any
    rdf_type: Any
    psid: Any
    position: Any
    loaded_at: Any


@dataclass(frozen=True)
class _RecordWriteContext:
    predicates: _StorePredicates
    quarry_id: int
    gil_link_enrichment_map: Mapping[str, Mapping[str, Any]]
    xsd_integer_type: Any
    quarry_id_literal: Any
    loaded_at_literal: Any


def _reset_store_directory(store_id: int) -> Path:
    store_path = store.store_path(store_id)
    if store_path.exists():
        shutil.rmtree(store_path)
    store_path.mkdir(parents=True, exist_ok=True)
    return store_path


def _require_store_class() -> Any:
    if Store is None:
        raise PetscanServiceError(
            "pyoxigraph is not installed. Install dependencies from requirements.txt first."
        )
    return Store


def _build_store_predicates() -> _StorePredicates:
    return _StorePredicates(
        page_class=NamedNode(QUARRY_PREDICATE_BASE + "Page"),
        rdf_type=NamedNode(rdf.RDF_TYPE_IRI),
        psid=NamedNode(QUARRY_PREDICATE_BASE + "psid"),
        position=NamedNode(QUARRY_PREDICATE_BASE + "position"),
        loaded_at=NamedNode(QUARRY_PREDICATE_BASE + "loadedAt"),
    )


def _quarry_predicate_for(key: str) -> Any:
    return NamedNode(QUARRY_PREDICATE_BASE + rdf._field_name(key))


def _quarry_structure_summary(summary: StructureSummary) -> StructureSummary:
    fields: List[StructureField] = []
    for field in summary["fields"]:
        updated_field: StructureField = {
            "source_key": field["source_key"],
            "predicate": QUARRY_PREDICATE_BASE + rdf._field_name(field["source_key"]),
            "present_in_rows": field["present_in_rows"],
            "primary_type": field["primary_type"],
            "observed_types": list(field["observed_types"]),
        }
        fields.append(updated_field)
    return {
        "row_count": summary["row_count"],
        "field_count": summary["field_count"],
        "fields": fields,
    }


def _row_subject(quarry_id: int, index: int) -> Any:
    return NamedNode("{}{}#{}".format(_QUARRY_ROW_BASE, "/" + str(quarry_id), index + 1))


def _normalize_quarry_scalar_value_and_type(key: str, value: Any) -> Tuple[Any, str]:
    normalized_value, sparql_type = rdf._normalize_scalar_field_value_and_type(key, value)

    if sparql_type != "xsd:string" or not isinstance(normalized_value, str):
        return normalized_value, sparql_type

    stripped = normalized_value.strip()
    if not stripped:
        return normalized_value, sparql_type

    if _ABSOLUTE_URI_RE.fullmatch(stripped):
        parsed_uri = urlsplit(stripped)
        if parsed_uri.scheme.lower() in _ALLOWED_EXTERNAL_URI_SCHEMES and parsed_uri.netloc:
            return stripped, rdf.SPARQL_IRI_TYPE

    if key.endswith("_uri") and (stripped.startswith("http://") or stripped.startswith("https://")):
        return stripped, rdf.SPARQL_IRI_TYPE

    if _COMPACT_TIMESTAMP_RE.fullmatch(stripped):
        normalized_datetime = rdf.normalize_datetime_xsd(stripped)
        if normalized_datetime is not None:
            return normalized_datetime, "xsd:dateTime"

    if _WIKIDATA_QID_RE.fullmatch(stripped):
        return "http://www.wikidata.org/entity/{}".format(stripped), rdf.SPARQL_IRI_TYPE

    if _COMMONS_MID_RE.fullmatch(stripped):
        return "https://commons.wikimedia.org/entity/{}".format(stripped), rdf.SPARQL_IRI_TYPE

    return normalized_value, sparql_type


def _write_record_quads(
    index: int,
    row: Mapping[str, Any],
    context: _RecordWriteContext,
    resolved_gil_links: Sequence[Tuple[str, Optional[str]]],
    quad_buffer: List[Any],
) -> Dict[str, int]:
    row_field_kinds: Dict[str, int] = {}

    def _track_field_kind(key: str, kind: str) -> None:
        rdf._track_row_field_kind(row_field_kinds, key, kind)

    predicates = context.predicates
    append_quad = quad_buffer.append
    literal_ctor = Literal
    object_term_for_typed_value = rdf.object_term_for_typed_value
    predicate_for = _quarry_predicate_for
    quad_ctor = Quad
    subject = _row_subject(context.quarry_id, index)
    gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links] if "gil" in row else None
    gil_link_predicate = predicate_for("gil_link")
    append_quad(quad_ctor(subject, predicates.rdf_type, predicates.page_class))
    append_quad(quad_ctor(subject, predicates.psid, context.quarry_id_literal))
    append_quad(
        quad_ctor(
            subject,
            predicates.position,
            literal_ctor(str(index), datatype=context.xsd_integer_type),
        )
    )
    append_quad(quad_ctor(subject, predicates.loaded_at, context.loaded_at_literal))
    for key, raw_value in rdf.iter_scalar_fields(row, gil_links=gil_link_uris):
        value, sparql_type = _normalize_quarry_scalar_value_and_type(key, raw_value)
        _track_field_kind(key, sparql_type)
        append_quad(
            quad_ctor(
                subject,
                predicate_for(key),
                object_term_for_typed_value(value, sparql_type),
            )
        )

    for link_uri, qid in resolved_gil_links:
        link_node = NamedNode(link_uri)
        for key, value, sparql_type in rdf.iter_typed_gil_link_fields(
            link_uri,
            qid,
            gil_link_enrichment_map=context.gil_link_enrichment_map,
        ):
            _track_field_kind(key, sparql_type)
            quad_subject = subject if key == "gil_link" else link_node
            quad_object = link_node if key == "gil_link" else object_term_for_typed_value(value, sparql_type)
            append_quad(
                quad_ctor(
                    quad_subject,
                    gil_link_predicate if key == "gil_link" else predicate_for(key),
                    quad_object,
                )
            )
    return row_field_kinds


def _flush_quads(store_instance: Any, quad_buffer: List[Any]) -> None:
    if not quad_buffer:
        return
    store_instance.bulk_extend(quad_buffer)
    quad_buffer.clear()


def _optimize_store(store_instance: Any) -> None:
    store_instance.optimize()


def _build_store_meta(
    store_id: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]],
    loaded_at: str,
    structure: StructureSummary,
) -> StoreMeta:
    meta_model = StoreMetaModel(
        psid=store_id,
        records=len(records),
        source_url=source_url,
        source_params=source.normalize_petscan_params(source_params),
        loaded_at=loaded_at,
        structure=structure,
    )
    return meta_model.to_dict()


def _persist_store_meta(store_id: int, meta: StoreMeta) -> None:
    store.meta_path(store_id).write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _records_with_derived_uris(
    records: Sequence[Mapping[str, Any]],
    query_db: Optional[str],
) -> List[Dict[str, Any]]:
    prepared_records = []  # type: List[Dict[str, Any]]
    for row in records:
        prepared = dict(row)
        for key, value in uri_derivation.derive_uri_fields(prepared, query_db).items():
            prepared.setdefault(key, value)
        prepared_records.append(prepared)
    return prepared_records


def build_store(
    store_id: int,
    quarry_id: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]] = None,
    query_db: Optional[str] = None,
) -> StoreMeta:
    store_path = _reset_store_directory(store_id)
    store_class = _require_store_class()
    store_instance = store_class(str(store_path))
    try:
        prepared_records = _records_with_derived_uris(records, query_db)
        predicates = _build_store_predicates()
        gil_link_result = links.build_gil_link_enrichment(prepared_records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = rdf.StructureAccumulator()
        write_context = _RecordWriteContext(
            predicates=predicates,
            quarry_id=quarry_id,
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
            quarry_id_literal=Literal(str(quarry_id), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            loaded_at_literal=Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )
        quad_buffer: List[Any] = []

        for index, row in enumerate(prepared_records):
            resolved_gil_links = resolved_gil_links_by_row[index]
            row_field_kinds = _write_record_quads(
                index=index,
                row=row,
                context=write_context,
                resolved_gil_links=resolved_gil_links,
                quad_buffer=quad_buffer,
            )
            structure_accumulator.add_row_field_kinds(row_field_kinds)
            if len(quad_buffer) >= _QUAD_BUFFER_TARGET:
                _flush_quads(store_instance, quad_buffer)

        _flush_quads(store_instance, quad_buffer)
        _optimize_store(store_instance)
        store_instance.flush()

        summary = _quarry_structure_summary(structure_accumulator.build_summary(row_count=len(prepared_records)))
        meta = _build_store_meta(
            store_id=store_id,
            records=prepared_records,
            source_url=source_url,
            source_params=source_params,
            loaded_at=loaded_at,
            structure=summary,
        )
        _persist_store_meta(store_id, meta)
        return meta
    finally:
        store_instance = None
