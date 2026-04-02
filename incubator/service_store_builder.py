"""Oxigraph store construction from Incubator rows."""

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from petscan import service_rdf as rdf
from petscan import service_store as store
from petscan.service_errors import PetscanServiceError
from petscan.service_types import StoreMeta, StoreMetaModel, StructureField, StructureSummary

from . import service_source as source

__all__ = ["INCUBATOR_PREDICATE_BASE", "build_store"]

INCUBATOR_PREDICATE_BASE = "https://incubator.wikimedia.org/ontology/"
_SCHEMA_ARTICLE_IRI = "http://schema.org/Article"
_SCHEMA_ABOUT_IRI = "http://schema.org/about"
_SCHEMA_IN_LANGUAGE_IRI = "http://schema.org/inLanguage"
_SCHEMA_NAME_IRI = "http://schema.org/name"
_SCHEMA_IS_PART_OF_IRI = "http://schema.org/isPartOf"
_WIKIBASE_WIKI_GROUP_IRI = "http://wikiba.se/ontology#wikiGroup"
_QUAD_BUFFER_TARGET = 4_000_000
_IRI_FIELD_KEYS = frozenset({"incubator_url", "site_url", "wikidata_entity"})
_LANGUAGE_TAG_RE = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
_STANDARD_PREDICATE_BY_SOURCE_KEY = {
    "wikidata_entity": _SCHEMA_ABOUT_IRI,
    "lang_code": _SCHEMA_IN_LANGUAGE_IRI,
    "page_label": _SCHEMA_NAME_IRI,
    "site_url": _SCHEMA_IS_PART_OF_IRI,
    "wiki_group": _WIKIBASE_WIKI_GROUP_IRI,
}
_STANDARD_ONLY_FIELD_KEYS = frozenset(_STANDARD_PREDICATE_BY_SOURCE_KEY.keys())
_NON_RDF_FIELD_KEYS = frozenset({"page_name"})

try:
    from pyoxigraph import Literal, NamedNode, Quad, Store
except ImportError:  # pragma: no cover - dependency check at runtime
    Literal = None  # type: ignore[misc,assignment]
    NamedNode = None  # type: ignore[misc,assignment]
    Quad = None  # type: ignore[misc,assignment]
    Store = None  # type: ignore[misc,assignment]


@dataclass(frozen=True)
class _StorePredicates:
    rdf_type: Any
    position: Any
    loaded_at: Any
    schema_article: Any
    schema_about: Any
    schema_in_language: Any
    schema_name: Any
    schema_is_part_of: Any
    wikibase_wiki_group: Any


@dataclass(frozen=True)
class _RecordWriteContext:
    predicates: _StorePredicates
    loaded_at_literal: Any
    xsd_integer_type: Any


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
        rdf_type=NamedNode(rdf.RDF_TYPE_IRI),
        position=NamedNode(INCUBATOR_PREDICATE_BASE + "position"),
        loaded_at=NamedNode(INCUBATOR_PREDICATE_BASE + "loadedAt"),
        schema_article=NamedNode(_SCHEMA_ARTICLE_IRI),
        schema_about=NamedNode(_SCHEMA_ABOUT_IRI),
        schema_in_language=NamedNode(_SCHEMA_IN_LANGUAGE_IRI),
        schema_name=NamedNode(_SCHEMA_NAME_IRI),
        schema_is_part_of=NamedNode(_SCHEMA_IS_PART_OF_IRI),
        wikibase_wiki_group=NamedNode(_WIKIBASE_WIKI_GROUP_IRI),
    )


@lru_cache(maxsize=256)
def _incubator_predicate_for(key: str) -> Any:
    return NamedNode(INCUBATOR_PREDICATE_BASE + rdf._field_name(key))


def _incubator_structure_summary(summary: StructureSummary) -> StructureSummary:
    fields: List[StructureField] = []
    for field in summary["fields"]:
        source_key = field["source_key"]
        if source_key in _NON_RDF_FIELD_KEYS:
            continue
        updated_field: StructureField = {
            "source_key": source_key,
            "predicate": _STANDARD_PREDICATE_BY_SOURCE_KEY.get(
                source_key,
                INCUBATOR_PREDICATE_BASE + rdf._field_name(source_key),
            ),
            "present_in_rows": field["present_in_rows"],
            "primary_type": field["primary_type"],
            "observed_types": list(field["observed_types"]),
        }
        if "row_side_cardinality" in field:
            updated_field["row_side_cardinality"] = field["row_side_cardinality"]
        fields.append(updated_field)
    return {
        "row_count": summary["row_count"],
        "field_count": len(fields),
        "fields": fields,
    }


def _normalize_scalar_value_and_type(key: str, value: Any) -> Tuple[Any, str]:
    if isinstance(value, bool):
        return value, "xsd:boolean"
    if isinstance(value, int) and not isinstance(value, bool):
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

    if key in _IRI_FIELD_KEYS and (stripped.startswith("http://") or stripped.startswith("https://")):
        return stripped, rdf.SPARQL_IRI_TYPE

    return value, "xsd:string"


def _schema_name_literal(page_label: Any, lang_code: Any) -> Any:
    text = str(page_label or "").strip()
    language = str(lang_code or "").strip()
    if not text:
        return None
    if language and _LANGUAGE_TAG_RE.fullmatch(language):
        return Literal(text, language=language)
    return Literal(text)


def _flush_quads(store_instance: Any, quad_buffer: List[Any]) -> None:
    if not quad_buffer:
        return
    store_instance.bulk_extend(quad_buffer)
    quad_buffer.clear()


def _optimize_store(store_instance: Any) -> None:
    store_instance.optimize()


def _write_scalar_record_quads(
    subject: Any,
    row: Mapping[str, Any],
    quad_buffer: List[Any],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    row_field_kinds: Dict[str, int] = {}
    row_field_value_counts: Dict[str, int] = {}
    append_quad = quad_buffer.append

    for key, raw_value in row.items():
        normalized_key = str(key)
        if normalized_key in _NON_RDF_FIELD_KEYS:
            continue
        normalized_value, sparql_type = _normalize_scalar_value_and_type(normalized_key, raw_value)
        rdf._track_row_field_kind(row_field_kinds, normalized_key, sparql_type)
        rdf._track_row_field_value_count(row_field_value_counts, str(key))
        if normalized_key in _STANDARD_ONLY_FIELD_KEYS:
            continue
        append_quad(
            Quad(
                subject,
                _incubator_predicate_for(normalized_key),
                rdf.object_term_for_typed_value(normalized_value, sparql_type),
            )
        )

    return row_field_kinds, row_field_value_counts


def _write_record_quads(
    index: int,
    row: Mapping[str, Any],
    context: _RecordWriteContext,
    quad_buffer: List[Any],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    incubator_url = str(row.get("incubator_url", "") or "").strip()
    if not incubator_url:
        raise PetscanServiceError("Incubator record is missing incubator_url.")

    subject = NamedNode(incubator_url)
    predicates = context.predicates
    append_quad = quad_buffer.append

    append_quad(Quad(subject, predicates.rdf_type, predicates.schema_article))
    append_quad(
        Quad(
            subject,
            predicates.position,
            Literal(str(index + 1), datatype=context.xsd_integer_type),
        )
    )
    append_quad(Quad(subject, predicates.loaded_at, context.loaded_at_literal))

    row_field_kinds, row_field_value_counts = _write_scalar_record_quads(subject, row, quad_buffer)

    wikidata_entity = str(row.get("wikidata_entity", "") or "").strip()
    if wikidata_entity:
        append_quad(Quad(subject, predicates.schema_about, NamedNode(wikidata_entity)))

    lang_code = str(row.get("lang_code", "") or "").strip()
    if lang_code:
        append_quad(Quad(subject, predicates.schema_in_language, Literal(lang_code)))

    schema_name_literal = _schema_name_literal(row.get("page_label"), row.get("lang_code"))
    if schema_name_literal is not None:
        append_quad(Quad(subject, predicates.schema_name, schema_name_literal))

    site_url = str(row.get("site_url", "") or "").strip()
    if site_url:
        site_node = NamedNode(site_url)
        append_quad(Quad(subject, predicates.schema_is_part_of, site_node))

        wiki_group = str(row.get("wiki_group", "") or "").strip()
        if wiki_group:
            append_quad(Quad(site_node, predicates.wikibase_wiki_group, Literal(wiki_group)))

    return row_field_kinds, row_field_value_counts


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
        source_params=source.normalize_source_params(source_params),
        loaded_at=loaded_at,
        structure=structure,
    )
    return meta_model.to_dict()


def _persist_store_meta(store_id: int, meta: StoreMeta) -> None:
    store.meta_path(store_id).write_text(json.dumps(meta, indent=2), encoding="utf-8")


def build_store(
    store_id: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]] = None,
) -> StoreMeta:
    store_path = _reset_store_directory(store_id)
    store_class = _require_store_class()
    store_instance = store_class(str(store_path))

    try:
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        predicates = _build_store_predicates()
        structure_accumulator = rdf.StructureAccumulator()
        write_context = _RecordWriteContext(
            predicates=predicates,
            loaded_at_literal=Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
            xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
        )
        quad_buffer: List[Any] = []

        for index, row in enumerate(records):
            row_field_kinds, row_field_value_counts = _write_record_quads(
                index=index,
                row=row,
                context=write_context,
                quad_buffer=quad_buffer,
            )
            structure_accumulator.add_row_field_kinds(
                row_field_kinds,
                row_field_value_counts=row_field_value_counts,
            )
            if len(quad_buffer) >= _QUAD_BUFFER_TARGET:
                _flush_quads(store_instance, quad_buffer)

        _flush_quads(store_instance, quad_buffer)
        _optimize_store(store_instance)
        store_instance.flush()

        meta = _build_store_meta(
            store_id=store_id,
            records=records,
            source_url=source_url,
            source_params=source_params,
            loaded_at=loaded_at,
            structure=_incubator_structure_summary(
                structure_accumulator.build_summary(row_count=len(records))
            ),
        )
        _persist_store_meta(store_id, meta)
        return meta
    finally:
        store_instance = None
