"""Oxigraph store construction from PetScan records."""

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from . import service_links as links
from . import service_rdf as rdf
from . import service_source as source
from . import service_store as store
from .service_errors import PetscanServiceError
from .service_types import StoreMeta, StoreMetaModel, StructureSummary

__all__ = ["build_store"]

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


@dataclass(frozen=True)
class _RecordWriteContext:
    predicates: _StorePredicates
    psid: int
    loaded_at: str
    gil_link_wikidata_map: Mapping[str, str]


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
    )


def _write_record_quads(
    store_instance: Any,
    index: int,
    row: Mapping[str, Any],
    context: _RecordWriteContext,
) -> Dict[str, List[Any]]:
    row_fields: Dict[str, List[Any]] = {}
    predicates = context.predicates
    subject = rdf.item_subject(context.psid, row, index)
    resolved_gil_links = links.resolve_gil_links(
        row,
        gil_link_wikidata_map=context.gil_link_wikidata_map,
    )
    gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links]
    store_instance.add(Quad(subject, predicates.rdf_type, predicates.page_class, DefaultGraph()))
    store_instance.add(
        Quad(
            subject,
            predicates.psid,
            Literal(str(context.psid), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            DefaultGraph(),
        )
    )
    store_instance.add(
        Quad(
            subject,
            predicates.position,
            Literal(str(index), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            DefaultGraph(),
        )
    )
    store_instance.add(
        Quad(
            subject,
            predicates.loaded_at,
            Literal(context.loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
            DefaultGraph(),
        )
    )
    for key, value in rdf.iter_scalar_fields(row, gil_links=gil_link_uris):
        row_fields.setdefault(key, []).append(value)
        predicate = rdf.predicate_for(key)
        literal = rdf.literal_for(value)
        store_instance.add(Quad(subject, predicate, literal, DefaultGraph()))

    for link_uri, qid in resolved_gil_links:
        row_fields.setdefault("gil_link", []).append(link_uri)
        link_node = NamedNode(link_uri)
        store_instance.add(Quad(subject, predicates.gil_link, link_node, DefaultGraph()))
        if qid is not None:
            row_fields.setdefault("gil_link_wikidata_id", []).append(qid)
            row_fields.setdefault("gil_link_wikidata_entity", []).append(
                "http://www.wikidata.org/entity/{}".format(qid)
            )
            store_instance.add(
                Quad(
                    link_node,
                    predicates.gil_link_wikidata_id,
                    Literal(qid),
                    DefaultGraph(),
                )
            )
            store_instance.add(
                Quad(
                    link_node,
                    predicates.gil_link_wikidata_entity,
                    NamedNode("http://www.wikidata.org/entity/{}".format(qid)),
                    DefaultGraph(),
                )
            )
    return row_fields


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
    predicates = _build_store_predicates()
    gil_link_wikidata_map = links.build_gil_link_wikidata_map(records)
    loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    structure_accumulator = rdf.StructureAccumulator()
    write_context = _RecordWriteContext(
        predicates=predicates,
        psid=psid,
        loaded_at=loaded_at,
        gil_link_wikidata_map=gil_link_wikidata_map,
    )

    for index, row in enumerate(records):
        row_fields = _write_record_quads(
            store_instance=store_instance,
            index=index,
            row=row,
            context=write_context,
        )
        structure_accumulator.add_row_fields(row_fields)

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
