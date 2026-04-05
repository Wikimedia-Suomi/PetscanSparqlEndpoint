"""Oxigraph store construction from new-page rows."""

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Mapping, Optional, Sequence, cast
from typing import Literal as LiteralType

from petscan import service_rdf as rdf
from petscan import service_store as store
from petscan.service_errors import PetscanServiceError
from petscan.service_types import StoreMeta, StoreMetaModel, StructureField, StructureSummary

from . import service_source as source

__all__ = ["NEWPAGES_PREDICATE_BASE", "build_store"]

NEWPAGES_PREDICATE_BASE = "https://sparqlbridge.toolforge.org/newpages/ontology/"
_SCHEMA_ARTICLE_IRI = "http://schema.org/Article"
_SCHEMA_ABOUT_IRI = "http://schema.org/about"
_SCHEMA_IN_LANGUAGE_IRI = "http://schema.org/inLanguage"
_SCHEMA_NAME_IRI = "http://schema.org/name"
_SCHEMA_IS_PART_OF_IRI = "http://schema.org/isPartOf"
_WIKIBASE_WIKI_GROUP_IRI = "http://wikiba.se/ontology#wikiGroup"
_QUAD_BUFFER_TARGET = 4_000_000
_LANGUAGE_TAG_RE = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
_STANDARD_PREDICATE_BY_SOURCE_KEY = {
    "wikidata_entity": _SCHEMA_ABOUT_IRI,
    "lang_code": _SCHEMA_IN_LANGUAGE_IRI,
    "page_label": _SCHEMA_NAME_IRI,
    "site_url": _SCHEMA_IS_PART_OF_IRI,
    "wiki_group": _WIKIBASE_WIKI_GROUP_IRI,
}
_NEWPAGES_FIELD_ORDER = (
    "created_timestamp",
    "current_timestamp",
    "lang_code",
    "namespace",
    "page_id",
    "page_title",
    "page_label",
    "site_url",
    "wiki_domain",
    "wiki_dbname",
    "wiki_group",
    "wikidata_entity",
    "wikidata_id",
)
_NEWPAGES_FIELD_TYPE_BY_KEY = {
    "created_timestamp": "xsd:dateTime",
    "current_timestamp": "xsd:dateTime",
    "lang_code": "xsd:string",
    "namespace": "xsd:integer",
    "page_id": "xsd:integer",
    "page_title": "xsd:string",
    "page_label": "xsd:string",
    "site_url": rdf.SPARQL_IRI_TYPE,
    "wiki_domain": "xsd:string",
    "wiki_dbname": "xsd:string",
    "wiki_group": "xsd:string",
    "wikidata_entity": rdf.SPARQL_IRI_TYPE,
    "wikidata_id": "xsd:string",
}

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
    schema_article: Any
    schema_about: Any
    schema_in_language: Any
    schema_name: Any
    schema_is_part_of: Any
    wikibase_wiki_group: Any
    created_timestamp: Any
    current_timestamp: Any
    namespace: Any
    page_id: Any
    page_title: Any
    wiki_domain: Any
    wiki_dbname: Any
    wikidata_id: Any
    xsd_date_time_type: Any
    xsd_integer_type: Any


@dataclass(frozen=True)
class _RecordWriteContext:
    predicates: _StorePredicates


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
        schema_article=NamedNode(_SCHEMA_ARTICLE_IRI),
        schema_about=NamedNode(_SCHEMA_ABOUT_IRI),
        schema_in_language=NamedNode(_SCHEMA_IN_LANGUAGE_IRI),
        schema_name=NamedNode(_SCHEMA_NAME_IRI),
        schema_is_part_of=NamedNode(_SCHEMA_IS_PART_OF_IRI),
        wikibase_wiki_group=NamedNode(_WIKIBASE_WIKI_GROUP_IRI),
        created_timestamp=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("created_timestamp")),
        current_timestamp=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("current_timestamp")),
        namespace=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("namespace")),
        page_id=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("page_id")),
        page_title=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("page_title")),
        wiki_domain=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("wiki_domain")),
        wiki_dbname=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("wiki_dbname")),
        wikidata_id=NamedNode(NEWPAGES_PREDICATE_BASE + rdf._field_name("wikidata_id")),
        xsd_date_time_type=NamedNode(rdf.XSD_DATE_TIME_IRI),
        xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
    )


def _build_structure_summary(records: Sequence[Mapping[str, Any]]) -> StructureSummary:
    row_count = len(records)
    present_counts = {key: 0 for key in _NEWPAGES_FIELD_ORDER}

    for row in records:
        for key in _NEWPAGES_FIELD_ORDER:
            value = row.get(key)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            present_counts[key] += 1

    fields: List[StructureField] = []
    for source_key in _NEWPAGES_FIELD_ORDER:
        present_in_rows = present_counts[source_key]
        if present_in_rows <= 0:
            continue
        sparql_type = _NEWPAGES_FIELD_TYPE_BY_KEY[source_key]
        fields.append(
            {
                "source_key": source_key,
                "predicate": _STANDARD_PREDICATE_BY_SOURCE_KEY.get(
                    source_key,
                    NEWPAGES_PREDICATE_BASE + rdf._field_name(source_key),
                ),
                "present_in_rows": present_in_rows,
                "primary_type": sparql_type,
                "observed_types": [sparql_type],
                "row_side_cardinality": cast(LiteralType["1", "M"], rdf.ROW_SIDE_CARDINALITY_ONE),
            }
        )

    return {
        "row_count": row_count,
        "field_count": len(fields),
        "fields": fields,
    }


def _schema_name_literal(page_label: Any, lang_code: Any) -> Any:
    text = str(page_label or "").strip()
    language = str(lang_code or "").strip()
    if not text:
        return None
    if language and _LANGUAGE_TAG_RE.fullmatch(language):
        return Literal(text, language=language)
    return Literal(text)


def _append_integer_quad(
    subject: Any,
    predicate: Any,
    value: Any,
    xsd_integer_type: Any,
    quad_buffer: List[Any],
) -> None:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return
    quad_buffer.append(Quad(subject, predicate, Literal(str(normalized), datatype=xsd_integer_type)))


def _flush_quads(store_instance: Any, quad_buffer: List[Any]) -> None:
    if not quad_buffer:
        return
    store_instance.bulk_extend(quad_buffer)
    quad_buffer.clear()


def _optimize_store(store_instance: Any) -> None:
    store_instance.optimize()


def _write_record_quads(
    row: Mapping[str, Any],
    context: _RecordWriteContext,
    quad_buffer: List[Any],
) -> None:
    page_url = str(row.get("page_url", "") or "").strip()
    if not page_url:
        raise PetscanServiceError("New-page record is missing page_url.")

    subject = NamedNode(page_url)
    predicates = context.predicates
    append_quad = quad_buffer.append

    append_quad(Quad(subject, predicates.rdf_type, predicates.schema_article))
    _append_integer_quad(subject, predicates.page_id, row.get("page_id"), predicates.xsd_integer_type, quad_buffer)
    _append_integer_quad(subject, predicates.namespace, row.get("namespace"), predicates.xsd_integer_type, quad_buffer)

    page_title = str(row.get("page_title", "") or "").strip()
    if page_title:
        append_quad(Quad(subject, predicates.page_title, Literal(page_title)))

    wiki_domain = str(row.get("wiki_domain", "") or "").strip()
    if wiki_domain:
        append_quad(Quad(subject, predicates.wiki_domain, Literal(wiki_domain)))

    wiki_dbname = str(row.get("wiki_dbname", "") or "").strip()
    if wiki_dbname:
        append_quad(Quad(subject, predicates.wiki_dbname, Literal(wiki_dbname)))

    wikidata_id = str(row.get("wikidata_id", "") or "").strip()
    if wikidata_id:
        append_quad(Quad(subject, predicates.wikidata_id, Literal(wikidata_id)))

    created_timestamp = rdf.normalize_datetime_xsd(row.get("created_timestamp"))
    if created_timestamp is not None:
        append_quad(
            Quad(
                subject,
                predicates.created_timestamp,
                Literal(created_timestamp, datatype=predicates.xsd_date_time_type),
            )
        )

    current_timestamp = rdf.normalize_datetime_xsd(row.get("current_timestamp"))
    if current_timestamp is not None:
        append_quad(
            Quad(
                subject,
                predicates.current_timestamp,
                Literal(current_timestamp, datatype=predicates.xsd_date_time_type),
            )
        )

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
        write_context = _RecordWriteContext(
            predicates=predicates,
        )
        quad_buffer: List[Any] = []

        for row in records:
            _write_record_quads(
                row=row,
                context=write_context,
                quad_buffer=quad_buffer,
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
            structure=_build_structure_summary(records),
        )
        _persist_store_meta(store_id, meta)
        return meta
    finally:
        store_instance = None
