"""Oxigraph store construction from Quarry rows."""

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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
    gil_link: Any


@dataclass(frozen=True)
class _RecordWriteContext:
    predicates: _StorePredicates
    quarry_id: int
    row_subject_base: str
    gil_link_enrichment_map: Mapping[str, Mapping[str, Any]]
    xsd_integer_type: Any
    quarry_id_literal: Any
    loaded_at_literal: Any


@dataclass(frozen=True)
class _ScalarFieldPlan:
    key: str
    predicate: Any
    normalizer_mode: int


@dataclass(frozen=True)
class _RowWritePlan:
    scalar_fields: Tuple[_ScalarFieldPlan, ...]
    use_fast_scalar_path: bool
    has_gil: bool


_FAST_PATH_EXCLUDED_KEYS = frozenset({"metadata", "gil", "wikidata_id", "qid", "q", "wikidata"})
_SCALAR_VALUE_TYPES = (str, int, float, bool)
_ROW_FIELD_KIND_IRI_BIT = rdf._ROW_FIELD_KIND_IRI_BIT
_ROW_FIELD_KIND_STRING_BIT = rdf._ROW_FIELD_KIND_STRING_BIT
_ROW_FIELD_KIND_INTEGER_BIT = rdf._ROW_FIELD_KIND_INTEGER_BIT
_ROW_FIELD_KIND_DOUBLE_BIT = rdf._ROW_FIELD_KIND_DOUBLE_BIT
_ROW_FIELD_KIND_BOOLEAN_BIT = rdf._ROW_FIELD_KIND_BOOLEAN_BIT
_ROW_FIELD_KIND_DATETIME_BIT = rdf._ROW_FIELD_KIND_DATETIME_BIT

_NORMALIZER_MODE_GENERIC = 0
_NORMALIZER_MODE_BOOL = 1
_NORMALIZER_MODE_INT = 2
_NORMALIZER_MODE_FLOAT = 3
_NORMALIZER_MODE_DATETIME_KEY = 4
_NORMALIZER_MODE_HTTP_IRI = 5
_NORMALIZER_MODE_COMPACT_DATETIME = 6
_NORMALIZER_MODE_QID = 7
_NORMALIZER_MODE_COMMONS_MID = 8


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
        gil_link=_quarry_predicate_for("gil_link"),
    )


@lru_cache(maxsize=256)
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


def _build_row_write_plan(row: Mapping[str, Any]) -> _RowWritePlan:
    scalar_fields = tuple(
        _ScalarFieldPlan(
            key=str(key),
            predicate=_quarry_predicate_for(str(key)),
            normalizer_mode=_classify_scalar_field_normalizer_mode(str(key), row.get(key)),
        )
        for key in row.keys()
    )
    key_set = {field.key for field in scalar_fields}
    return _RowWritePlan(
        scalar_fields=scalar_fields,
        use_fast_scalar_path=not bool(key_set & _FAST_PATH_EXCLUDED_KEYS),
        has_gil="gil" in key_set,
    )


def _normalize_fast_scalar_list(value: list[Any]) -> Optional[str]:
    scalar_values = []  # type: List[str]
    for item in value:
        if not isinstance(item, _SCALAR_VALUE_TYPES):
            continue
        text = str(item).strip()
        if text:
            scalar_values.append(text)
    if not scalar_values:
        return None
    return "; ".join(scalar_values)


def _classify_scalar_field_normalizer_mode(key: str, sample_value: Any) -> int:
    if key in rdf._XSD_DATETIME_SCALAR_FIELDS:
        return _NORMALIZER_MODE_DATETIME_KEY
    if isinstance(sample_value, bool):
        return _NORMALIZER_MODE_BOOL
    if isinstance(sample_value, int):
        return _NORMALIZER_MODE_INT
    if isinstance(sample_value, float):
        return _NORMALIZER_MODE_FLOAT
    if isinstance(sample_value, list):
        sample_value = _normalize_fast_scalar_list(sample_value)
    if not isinstance(sample_value, str):
        return _NORMALIZER_MODE_GENERIC

    stripped = sample_value.strip()
    if not stripped:
        return _NORMALIZER_MODE_GENERIC
    if stripped.startswith("http://") or stripped.startswith("https://"):
        scheme_end = stripped.find("://")
        authority = stripped[scheme_end + 3 :]
        if authority and not authority.startswith("/") and not any(char.isspace() for char in stripped):
            return _NORMALIZER_MODE_HTTP_IRI
    if len(stripped) == 14 and stripped.startswith("20") and stripped.isdigit():
        return _NORMALIZER_MODE_COMPACT_DATETIME
    if len(stripped) > 1 and stripped[0] in {"Q", "q"} and stripped[1:].isdigit():
        return _NORMALIZER_MODE_QID
    if len(stripped) > 1 and stripped[0] == "M" and stripped[1:].isdigit():
        return _NORMALIZER_MODE_COMMONS_MID
    return _NORMALIZER_MODE_GENERIC


def _normalize_quarry_scalar_value_and_type(key: str, value: Any) -> Tuple[Any, str]:
    if isinstance(value, bool):
        return value, "xsd:boolean"
    if isinstance(value, int):
        return value, "xsd:integer"
    if isinstance(value, float):
        return value, "xsd:double"

    if key in rdf._XSD_DATETIME_SCALAR_FIELDS:
        normalized_datetime = rdf.normalize_datetime_xsd(value)
        if normalized_datetime is not None:
            return normalized_datetime, "xsd:dateTime"

    if not isinstance(value, str):
        return value, "xsd:string"

    stripped = value.strip()
    if not stripped:
        return value, "xsd:string"

    if stripped.startswith("http://") or stripped.startswith("https://"):
        scheme_end = stripped.find("://")
        authority = stripped[scheme_end + 3 :]
        if authority and not authority.startswith("/") and not any(char.isspace() for char in stripped):
            return stripped, rdf.SPARQL_IRI_TYPE

    if len(stripped) == 14 and stripped.startswith("20") and stripped.isdigit():
        normalized_datetime = rdf.normalize_datetime_xsd(stripped)
        if normalized_datetime is not None:
            return normalized_datetime, "xsd:dateTime"

    if len(stripped) > 1 and stripped[0] in {"Q", "q"} and stripped[1:].isdigit():
        return "http://www.wikidata.org/entity/Q{}".format(stripped[1:]), rdf.SPARQL_IRI_TYPE

    if len(stripped) > 1 and stripped[0] == "M" and stripped[1:].isdigit():
        return "https://commons.wikimedia.org/entity/{}".format(stripped), rdf.SPARQL_IRI_TYPE

    return value, "xsd:string"


def _normalize_quarry_scalar_value_and_kind(key: str, value: Any) -> Tuple[Any, int]:
    normalized_value, sparql_type = _normalize_quarry_scalar_value_and_type(key, value)
    return normalized_value, rdf._ROW_FIELD_KIND_BIT_BY_NAME[sparql_type]


def _normalize_planned_quarry_scalar_value_and_kind(field: _ScalarFieldPlan, value: Any) -> Tuple[Any, int]:
    mode = field.normalizer_mode

    if mode == _NORMALIZER_MODE_BOOL:
        if isinstance(value, bool):
            return value, _ROW_FIELD_KIND_BOOLEAN_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if mode == _NORMALIZER_MODE_INT:
        if isinstance(value, int) and not isinstance(value, bool):
            return value, _ROW_FIELD_KIND_INTEGER_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if mode == _NORMALIZER_MODE_FLOAT:
        if isinstance(value, float):
            return value, _ROW_FIELD_KIND_DOUBLE_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if mode == _NORMALIZER_MODE_DATETIME_KEY:
        normalized_datetime = rdf.normalize_datetime_xsd(value)
        if normalized_datetime is not None:
            return normalized_datetime, _ROW_FIELD_KIND_DATETIME_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if not isinstance(value, str):
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if mode == _NORMALIZER_MODE_HTTP_IRI:
        stripped = value.strip()
        if stripped.startswith("http://") or stripped.startswith("https://"):
            scheme_end = stripped.find("://")
            authority = stripped[scheme_end + 3 :]
            if authority and not authority.startswith("/") and not any(char.isspace() for char in stripped):
                return stripped, _ROW_FIELD_KIND_IRI_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if mode == _NORMALIZER_MODE_COMPACT_DATETIME:
        stripped = value.strip()
        if len(stripped) == 14 and stripped.startswith("20") and stripped.isdigit():
            normalized_datetime = rdf.normalize_datetime_xsd(stripped)
            if normalized_datetime is not None:
                return normalized_datetime, _ROW_FIELD_KIND_DATETIME_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if mode == _NORMALIZER_MODE_QID:
        stripped = value.strip()
        if len(stripped) > 1 and stripped[0] in {"Q", "q"} and stripped[1:].isdigit():
            return "http://www.wikidata.org/entity/Q{}".format(stripped[1:]), _ROW_FIELD_KIND_IRI_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    if mode == _NORMALIZER_MODE_COMMONS_MID:
        stripped = value.strip()
        if len(stripped) > 1 and stripped[0] == "M" and stripped[1:].isdigit():
            return "https://commons.wikimedia.org/entity/{}".format(stripped), _ROW_FIELD_KIND_IRI_BIT
        return _normalize_quarry_scalar_value_and_kind(field.key, value)

    return _normalize_quarry_scalar_value_and_kind(field.key, value)


def _write_record_quads(
    index: int,
    row: Mapping[str, Any],
    row_plan: _RowWritePlan,
    context: _RecordWriteContext,
    resolved_gil_links: Sequence[Tuple[str, Optional[str]]],
    quad_buffer: List[Any],
    structure_accumulator: rdf.StructureAccumulator,
) -> None:
    row_field_kinds: Optional[Dict[str, int]] = None

    predicates = context.predicates
    track_field_kind = rdf._track_row_field_kind
    append_quad = quad_buffer.append
    literal_ctor = Literal
    literal_for = rdf.literal_for
    named_node_ctor = NamedNode
    quad_ctor = Quad
    xsd_date_time_type = rdf._XSD_DATE_TIME_NODE
    track_structure_field_kind_bits = structure_accumulator.add_row_field_kind_bits
    sparql_iri_type = rdf.SPARQL_IRI_TYPE
    subject = named_node_ctor("{}{}".format(context.row_subject_base, index + 1))
    gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links] if row_plan.has_gil else None
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

    if row_plan.use_fast_scalar_path:
        row_get = row.get
        for field in row_plan.scalar_fields:
            raw_value = row_get(field.key)
            if raw_value is None:
                continue
            if isinstance(raw_value, _SCALAR_VALUE_TYPES):
                scalar_value = raw_value
            elif isinstance(raw_value, list):
                flattened_list = _normalize_fast_scalar_list(raw_value)
                if flattened_list is None:
                    continue
                scalar_value = flattened_list
            else:
                continue

            value, kind_bits = _normalize_planned_quarry_scalar_value_and_kind(field, scalar_value)
            track_structure_field_kind_bits(field.key, kind_bits)
            if kind_bits == _ROW_FIELD_KIND_IRI_BIT:
                object_term = named_node_ctor(str(value))
            elif kind_bits == _ROW_FIELD_KIND_DATETIME_BIT:
                object_term = literal_ctor(str(value), datatype=xsd_date_time_type)
            else:
                object_term = literal_for(value)
            append_quad(quad_ctor(subject, field.predicate, object_term))
    else:
        row_field_kinds = {}
        for key, raw_value in rdf.iter_scalar_fields(row, gil_links=gil_link_uris):
            value, sparql_type = _normalize_quarry_scalar_value_and_type(key, raw_value)
            track_field_kind(row_field_kinds, key, sparql_type)
            if sparql_type == sparql_iri_type:
                object_term = named_node_ctor(str(value))
            elif sparql_type == "xsd:dateTime":
                object_term = literal_ctor(str(value), datatype=xsd_date_time_type)
            else:
                object_term = literal_for(value)
            append_quad(
                quad_ctor(
                    subject,
                    _quarry_predicate_for(key),
                    object_term,
                )
            )

    if resolved_gil_links:
        if row_field_kinds is None:
            row_field_kinds = {}

    for link_uri, qid in resolved_gil_links:
        link_node = named_node_ctor(link_uri)
        for key, value, sparql_type in rdf.iter_typed_gil_link_fields(
            link_uri,
            qid,
            gil_link_enrichment_map=context.gil_link_enrichment_map,
        ):
            track_field_kind(row_field_kinds, key, sparql_type)
            quad_subject = subject if key == "gil_link" else link_node
            if key == "gil_link":
                quad_object = link_node
                predicate = predicates.gil_link
            else:
                predicate = _quarry_predicate_for(key)
                if sparql_type == sparql_iri_type:
                    quad_object = named_node_ctor(str(value))
                elif sparql_type == "xsd:dateTime":
                    quad_object = literal_ctor(str(value), datatype=xsd_date_time_type)
                else:
                    quad_object = literal_for(value)
            append_quad(
                quad_ctor(
                    quad_subject,
                    predicate,
                    quad_object,
                )
            )
    if row_field_kinds:
        structure_accumulator.add_row_field_kinds(row_field_kinds)


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
    derivation_cache = {}  # type: Dict[Tuple[str, ...], Any]
    for row in records:
        prepared = dict(row)
        row_keys = tuple(str(key).strip() for key in prepared.keys() if str(key).strip())
        if row_keys not in derivation_cache:
            derivation_cache[row_keys] = uri_derivation.build_uri_field_deriver(query_db, row_keys)
        deriver = derivation_cache[row_keys]
        derived = deriver(prepared) if deriver is not None else {}
        for key, value in derived.items():
            prepared.setdefault(key, value)
        prepared_records.append(prepared)
    return prepared_records


def _row_write_plans(records: Sequence[Mapping[str, Any]]) -> List[_RowWritePlan]:
    plan_cache = {}  # type: Dict[Tuple[str, ...], _RowWritePlan]
    plans = []  # type: List[_RowWritePlan]
    for row in records:
        row_keys = tuple(str(key) for key in row.keys())
        plan = plan_cache.get(row_keys)
        if plan is None:
            plan = _build_row_write_plan(row)
            plan_cache[row_keys] = plan
        plans.append(plan)
    return plans


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
        row_write_plans = _row_write_plans(prepared_records)
        predicates = _build_store_predicates()
        gil_link_result = links.build_gil_link_enrichment(prepared_records)
        resolved_gil_links_by_row = gil_link_result.resolved_links_by_row
        gil_link_enrichment_map = gil_link_result.enrichment_by_link
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = rdf.StructureAccumulator()
        write_context = _RecordWriteContext(
            predicates=predicates,
            quarry_id=quarry_id,
            row_subject_base="{}/{}#".format(_QUARRY_ROW_BASE, quarry_id),
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
            quarry_id_literal=Literal(str(quarry_id), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            loaded_at_literal=Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )
        quad_buffer: List[Any] = []

        for index, row in enumerate(prepared_records):
            resolved_gil_links = resolved_gil_links_by_row[index]
            _write_record_quads(
                index=index,
                row=row,
                row_plan=row_write_plans[index],
                context=write_context,
                resolved_gil_links=resolved_gil_links,
                quad_buffer=quad_buffer,
                structure_accumulator=structure_accumulator,
            )
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
