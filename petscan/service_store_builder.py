"""Oxigraph store construction from PetScan records."""

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from . import service_links as links
from . import service_rdf as rdf
from . import service_source as source
from . import service_store as store
from .service_errors import PetscanServiceError
from .service_types import StoreMeta, StoreMetaModel, StructureSummary

__all__ = ["build_store"]
_QUAD_BUFFER_TARGET = 1_000_000
_ROW_XSD_DATETIME_FIELDS = frozenset({"img_timestamp", "touched"})

try:
    from pyoxigraph import DefaultGraph, Literal, NamedNode, Quad, Store
except ImportError:  # pragma: no cover - dependency check at runtime
    DefaultGraph = None  # type: ignore[misc,assignment]
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
    gil_link_wikidata_id: Any
    gil_link_wikidata_entity: Any
    gil_link_page_len: Any
    gil_link_rev_timestamp: Any


@dataclass(frozen=True)
class _RecordWriteContext:
    predicates: _StorePredicates
    psid: int
    loaded_at: str
    gil_link_enrichment_map: Mapping[str, Mapping[str, Any]]
    default_graph: Any
    xsd_integer_type: Any
    xsd_date_time_type: Any
    psid_literal: Any
    loaded_at_literal: Any


def _reset_store_directory(psid: int) -> Path:
    store_path = store.store_path(psid)
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
        page_class=NamedNode(rdf.PREDICATE_BASE + "Page"),
        rdf_type=NamedNode(rdf.RDF_TYPE_IRI),
        psid=NamedNode(rdf.PREDICATE_BASE + "psid"),
        position=NamedNode(rdf.PREDICATE_BASE + "position"),
        loaded_at=NamedNode(rdf.PREDICATE_BASE + "loadedAt"),
        gil_link=NamedNode(rdf.PREDICATE_BASE + "gil_link"),
        gil_link_wikidata_id=NamedNode(rdf.PREDICATE_BASE + "gil_link_wikidata_id"),
        gil_link_wikidata_entity=NamedNode(rdf.PREDICATE_BASE + "gil_link_wikidata_entity"),
        gil_link_page_len=NamedNode(rdf.PREDICATE_BASE + "gil_link_page_len"),
        gil_link_rev_timestamp=NamedNode(rdf.PREDICATE_BASE + "gil_link_rev_timestamp"),
    )


def _write_record_quads(
    index: int,
    row: Mapping[str, Any],
    context: _RecordWriteContext,
    resolved_gil_links: Sequence[Tuple[str, Optional[str]]],
) -> Tuple[Dict[str, Set[str]], List[Any]]:
    row_field_kinds: Dict[str, Set[str]] = {}
    row_quads: List[Any] = []

    def _track_field_kind(key: str, value: Any, kind_override: Optional[str] = None) -> None:
        kind = kind_override if kind_override is not None else rdf.sparql_type_for_value(value)
        kinds = row_field_kinds.get(key)
        if kinds is None:
            row_field_kinds[key] = {kind}
        else:
            kinds.add(kind)

    predicates = context.predicates
    subject = rdf.item_subject(context.psid, row, index)
    gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links]
    row_quads.append(Quad(subject, predicates.rdf_type, predicates.page_class, context.default_graph))
    row_quads.append(
        Quad(
            subject,
            predicates.psid,
            context.psid_literal,
            context.default_graph,
        )
    )
    row_quads.append(
        Quad(
            subject,
            predicates.position,
            Literal(str(index), datatype=context.xsd_integer_type),
            context.default_graph,
        )
    )
    row_quads.append(
        Quad(
            subject,
            predicates.loaded_at,
            context.loaded_at_literal,
            context.default_graph,
        )
    )
    for key, value in rdf.iter_scalar_fields(row, gil_links=gil_link_uris):
        _track_field_kind(key, value, kind_override=rdf.sparql_type_for_scalar_field(key, value))
        predicate = rdf.predicate_for(key)
        literal = _literal_for_scalar_field(key=key, value=value, context=context)
        row_quads.append(Quad(subject, predicate, literal, context.default_graph))

    for link_uri, qid in resolved_gil_links:
        _track_field_kind("gil_link", link_uri, kind_override=rdf.SPARQL_IRI_TYPE)
        link_node = NamedNode(link_uri)
        row_quads.append(Quad(subject, predicates.gil_link, link_node, context.default_graph))

        enrichment = context.gil_link_enrichment_map.get(link_uri)
        page_len = None
        rev_timestamp = None
        if isinstance(enrichment, dict):
            raw_page_len = enrichment.get("page_len")
            try:
                page_len = int(raw_page_len) if raw_page_len is not None else None
            except Exception:
                page_len = None
            if page_len is not None and page_len < 0:
                page_len = None

            raw_rev_timestamp = enrichment.get("rev_timestamp")
            if isinstance(raw_rev_timestamp, str):
                normalized_timestamp = raw_rev_timestamp.strip()
                if normalized_timestamp:
                    rev_timestamp = normalized_timestamp

        if page_len is not None:
            _track_field_kind("gil_link_page_len", page_len, kind_override="xsd:integer")
            row_quads.append(
                Quad(
                    link_node,
                    predicates.gil_link_page_len,
                    Literal(str(page_len), datatype=context.xsd_integer_type),
                    context.default_graph,
                )
            )

        if rev_timestamp is not None:
            _track_field_kind("gil_link_rev_timestamp", rev_timestamp, kind_override="xsd:dateTime")
            row_quads.append(
                Quad(
                    link_node,
                    predicates.gil_link_rev_timestamp,
                    Literal(rev_timestamp, datatype=context.xsd_date_time_type),
                    context.default_graph,
                )
            )

        if qid is not None:
            _track_field_kind("gil_link_wikidata_id", qid, kind_override="xsd:string")
            entity_iri = "http://www.wikidata.org/entity/{}".format(qid)
            _track_field_kind("gil_link_wikidata_entity", entity_iri, kind_override=rdf.SPARQL_IRI_TYPE)
            row_quads.append(
                Quad(
                    link_node,
                    predicates.gil_link_wikidata_id,
                    Literal(qid),
                    context.default_graph,
                )
            )
            row_quads.append(
                Quad(
                    link_node,
                    predicates.gil_link_wikidata_entity,
                    NamedNode(entity_iri),
                    context.default_graph,
                )
            )
    return row_field_kinds, row_quads


def _literal_for_scalar_field(key: str, value: Any, context: _RecordWriteContext) -> Any:
    if key in _ROW_XSD_DATETIME_FIELDS:
        normalized_datetime = rdf.normalize_datetime_xsd(value)
        if normalized_datetime is not None:
            return Literal(normalized_datetime, datatype=context.xsd_date_time_type)
    return rdf.literal_for(value)


def _flush_quads(store_instance: Any, quad_buffer: Sequence[Any]) -> None:
    if not quad_buffer:
        return
    store_instance.bulk_extend(quad_buffer)


def _optimize_store(store_instance: Any) -> None:
    store_instance.optimize()


def _build_store_meta(
    psid: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]],
    loaded_at: str,
    structure: StructureSummary,
) -> StoreMeta:
    meta_model = StoreMetaModel(
        psid=psid,
        records=len(records),
        source_url=source_url,
        source_params=source.normalize_petscan_params(source_params),
        loaded_at=loaded_at,
        structure=structure,
    )
    return meta_model.to_dict()


def _persist_store_meta(psid: int, meta: StoreMeta) -> None:
    store.meta_path(psid).write_text(json.dumps(meta, indent=2), encoding="utf-8")


def build_store(
    psid: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]] = None,
) -> StoreMeta:
    store_path = _reset_store_directory(psid)
    store_class = _require_store_class()
    store_instance = store_class(str(store_path))
    try:
        predicates = _build_store_predicates()
        resolved_gil_links_by_row: List[List[Tuple[str, Optional[str]]]] = []
        gil_link_enrichment_map = links.build_gil_link_enrichment_map(
            records,
            resolved_links_by_row_out=resolved_gil_links_by_row,
        )
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = rdf.StructureAccumulator()
        write_context = _RecordWriteContext(
            predicates=predicates,
            psid=psid,
            loaded_at=loaded_at,
            gil_link_enrichment_map=gil_link_enrichment_map,
            default_graph=DefaultGraph(),
            xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
            xsd_date_time_type=NamedNode(rdf.XSD_DATE_TIME_IRI),
            psid_literal=Literal(str(psid), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            loaded_at_literal=Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )
        quad_buffer: List[Any] = []

        for index, row in enumerate(records):
            resolved_gil_links = resolved_gil_links_by_row[index]
            row_field_kinds, row_quads = _write_record_quads(
                index=index,
                row=row,
                context=write_context,
                resolved_gil_links=resolved_gil_links,
            )
            structure_accumulator.add_row_field_kinds(row_field_kinds)
            quad_buffer.extend(row_quads)
            if len(quad_buffer) >= _QUAD_BUFFER_TARGET:
                _flush_quads(store_instance, quad_buffer)
                quad_buffer = []

        _flush_quads(store_instance, quad_buffer)
        _optimize_store(store_instance)
        store_instance.flush()

        meta = _build_store_meta(
            psid=psid,
            records=records,
            source_url=source_url,
            source_params=source_params,
            loaded_at=loaded_at,
            structure=structure_accumulator.build_summary(row_count=len(records)),
        )
        _persist_store_meta(psid, meta)
        return meta
    finally:
        store_instance = None
