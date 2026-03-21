"""Oxigraph store construction from PetScan records."""

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from . import service_links as links
from . import service_rdf as rdf
from . import service_source as source
from . import service_store as store
from .service_errors import PetscanServiceError
from .service_types import StoreMeta, StoreMetaModel, StructureSummary

__all__ = ["build_store"]
_QUAD_BUFFER_TARGET = 4_000_000

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
    psid: int
    gil_link_enrichment_map: Mapping[str, Mapping[str, Any]]
    xsd_integer_type: Any
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
    )


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
    predicate_for = rdf.predicate_for
    quad_ctor = Quad
    subject = rdf.item_subject(context.psid, row, index)
    gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links] if "gil" in row else None
    gil_link_predicate = predicate_for("gil_link")
    append_quad(quad_ctor(subject, predicates.rdf_type, predicates.page_class))
    append_quad(
        quad_ctor(
            subject,
            predicates.psid,
            context.psid_literal,
        )
    )
    append_quad(
        quad_ctor(
            subject,
            predicates.position,
            literal_ctor(str(index), datatype=context.xsd_integer_type),
        )
    )
    append_quad(
        quad_ctor(
            subject,
            predicates.loaded_at,
            context.loaded_at_literal,
        )
    )
    rdf.append_scalar_field_quads(
        subject=subject,
        record=row,
        quad_buffer=quad_buffer,
        row_field_kinds=row_field_kinds,
        gil_links=gil_link_uris,
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
            quad_object = (
                link_node
                if key == "gil_link"
                else object_term_for_typed_value(value, sparql_type)
            )
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
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=NamedNode(rdf.XSD_INTEGER_IRI),
            psid_literal=Literal(str(psid), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            loaded_at_literal=Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )
        quad_buffer: List[Any] = []

        for index, row in enumerate(records):
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
