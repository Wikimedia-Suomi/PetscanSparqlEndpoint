"""Service-layer orchestration for PetScan ingestion and SPARQL execution.

Responsibilities:
- orchestrate PetScan ingestion and cache lifecycle
- enrich GIL links with Wikidata IDs
- build/load Oxigraph stores with metadata cache
- execute and serialize read-only SPARQL queries
"""

import json
import re
import shutil
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, cast
from urllib.parse import quote

from ._service_errors import PetscanServiceError
from ._service_links import (
    _LOOKUP_BACKEND_API,
    _LOOKUP_BACKEND_TOOLFORGE_SQL,
    _MAX_TITLES_PER_MEDIAWIKI_BATCH,
    _chunked,
    _direct_wikidata_qid_for_target,
    _extract_qid,
    _iter_gil_link_enrichment,
    _iter_gil_link_targets,
    _iter_gil_link_uris,
    _normalize_page_title,
    _normalize_qid,
    _site_to_mediawiki_api_url,
)
from ._service_links import (
    _fetch_wikibase_items_for_site_api as _links_fetch_wikibase_items_for_site_api,
)
from ._service_links import (
    _fetch_wikibase_items_for_site_sql as _links_fetch_wikibase_items_for_site_sql,
)
from ._service_links import (
    _gil_link_uri as _links_gil_link_uri,
)
from ._service_links import (
    _parse_gil_link_target as _links_parse_gil_link_target,
)
from ._service_links import (
    _wikidata_lookup_backend as _links_wikidata_lookup_backend,
)
from ._service_links import (
    pymysql as _links_pymysql,
)
from ._service_source import (
    _build_petscan_url as _source_build_petscan_url,
)
from ._service_source import (
    _extract_records,
    _fetch_petscan_json,
    _normalize_petscan_params,
)
from ._service_sparql import (
    _contains_service_clause,
    _query_type,
    _serialize_ask,
    _serialize_graph,
    _serialize_select,
)
from ._service_store import _get_psid_lock, _has_existing_store, _meta_path, _read_meta, _store_path
from ._service_types import (
    QueryExecution,
    QueryExecutionModel,
    StoreMeta,
    StoreMetaModel,
    StructureField,
    StructureSummary,
)

try:
    from pyoxigraph import BlankNode, DefaultGraph, Literal, NamedNode, Quad, Store
except ImportError:  # pragma: no cover - dependency check at runtime
    BlankNode = None  # type: ignore[misc,assignment]
    DefaultGraph = None  # type: ignore[misc,assignment]
    Literal = None  # type: ignore[misc,assignment]
    NamedNode = None  # type: ignore[misc,assignment]
    Quad = None  # type: ignore[misc,assignment]
    Store = None  # type: ignore[misc,assignment]

PREDICATE_BASE = "https://petscan.wmcloud.org/ontology/"
ITEM_BASE = "https://petscan.wmcloud.org/psid"
RDF_TYPE_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_INTEGER_IRI = "http://www.w3.org/2001/XMLSchema#integer"
XSD_DOUBLE_IRI = "http://www.w3.org/2001/XMLSchema#double"
XSD_BOOLEAN_IRI = "http://www.w3.org/2001/XMLSchema#boolean"
XSD_DATE_TIME_IRI = "http://www.w3.org/2001/XMLSchema#dateTime"

_FIELD_NAME_RE = re.compile(r"[^0-9A-Za-z_]+")
_FIELD_RENAMES = {
    "id": "page_id",
}

pymysql = _links_pymysql


def _ensure_oxigraph() -> None:
    if Store is None:
        raise PetscanServiceError(
            "pyoxigraph is not installed. Install dependencies from requirements.txt first."
        )


def _field_name(key: str) -> str:
    canonical = _FIELD_RENAMES.get(key, key)
    cleaned = _FIELD_NAME_RE.sub("_", canonical).strip("_")
    if not cleaned:
        cleaned = "field"
    if cleaned[0].isdigit():
        cleaned = "field_{}".format(cleaned)
    return cleaned


def _predicate_for(key: str):
    return NamedNode(PREDICATE_BASE + _field_name(key))


def _literal_for(value: Any):
    if isinstance(value, bool):
        return Literal("true" if value else "false", datatype=NamedNode(XSD_BOOLEAN_IRI))
    if isinstance(value, int):
        return Literal(str(value), datatype=NamedNode(XSD_INTEGER_IRI))
    if isinstance(value, float):
        return Literal(repr(value), datatype=NamedNode(XSD_DOUBLE_IRI))
    return Literal(str(value))


def _record_identifier(record: Mapping[str, Any], index: int) -> str:
    for key in ("id", "pageid", "title", "qid", "wikidata"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return str(index)


def _record_page_id(record: Mapping[str, Any]) -> Optional[int]:
    for key in ("id", "pageid"):
        value = record.get(key)
        if value is None:
            continue
        try:
            page_id = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if page_id > 0:
            return page_id
    return None


def _is_commons_file_record(record: Mapping[str, Any]) -> bool:
    wiki_value = str(record.get("wiki", "")).strip().lower()
    if wiki_value and wiki_value in {"commonswiki", "commons.wikimedia.org"}:
        return True

    namespace_value = record.get("namespace")
    try:
        namespace = int(namespace_value) if namespace_value is not None else None
    except Exception:
        namespace = None

    nstext = str(record.get("nstext", "")).strip().lower()
    has_image_metadata = any(str(key).startswith("img_") for key in record.keys())

    # PetScan media rows often omit explicit wiki while still representing Commons files.
    if namespace == 6 and nstext == "file" and has_image_metadata:
        return True

    return False


def _item_subject(psid: int, record: Mapping[str, Any], index: int):
    page_id = _record_page_id(record)
    if page_id is not None and _is_commons_file_record(record):
        return NamedNode("https://commons.wikimedia.org/entity/M{}".format(page_id))

    qid = _extract_qid(record)
    if qid is not None:
        return NamedNode("http://www.wikidata.org/entity/{}".format(qid))

    identifier = quote(_record_identifier(record, index), safe="")
    return NamedNode("{}/{}/item/{}".format(ITEM_BASE, psid, identifier))


def _build_petscan_url(psid: int, petscan_params: Optional[Mapping[str, Any]] = None) -> str:
    return _source_build_petscan_url(psid, petscan_params=petscan_params)


def _parse_gil_link_target(link: str) -> Optional[Tuple[str, int, str]]:
    return _links_parse_gil_link_target(link)


def _gil_link_uri(site: str, title: str) -> Optional[str]:
    return _links_gil_link_uri(site, title)


def _wikidata_lookup_backend() -> str:
    return _links_wikidata_lookup_backend()


def _fetch_wikibase_items_for_site_api(api_url: str, titles: Sequence[str]) -> Dict[str, str]:
    return _links_fetch_wikibase_items_for_site_api(api_url, titles)


def _fetch_wikibase_items_for_site_sql(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
) -> Dict[str, str]:
    return _links_fetch_wikibase_items_for_site_sql(site, targets)


def _fetch_wikibase_items_for_site(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
    backend: str,
) -> Dict[str, str]:
    if not targets:
        return {}

    if backend not in {_LOOKUP_BACKEND_API, _LOOKUP_BACKEND_TOOLFORGE_SQL}:
        backend = _LOOKUP_BACKEND_API

    if backend == _LOOKUP_BACKEND_TOOLFORGE_SQL:
        return _fetch_wikibase_items_for_site_sql(site, targets)

    api_url = _site_to_mediawiki_api_url(site)
    if api_url is None:
        return {}
    titles = sorted({_normalize_page_title(api_title) for _ns, api_title, _db in targets if api_title})
    resolved = {}  # type: Dict[str, str]
    for batch in _chunked(titles, _MAX_TITLES_PER_MEDIAWIKI_BATCH):
        batch_result = _fetch_wikibase_items_for_site_api(api_url, batch)
        resolved.update(batch_result)
    return resolved


def _build_gil_link_wikidata_map(records: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    link_targets = {}  # type: Dict[str, Tuple[str, int, str, str]]
    site_to_targets = {}  # type: Dict[str, set]
    link_to_qid = {}  # type: Dict[str, str]

    for row in records:
        for link_uri, site, namespace, api_title, db_title in _iter_gil_link_targets(row):
            link_targets[link_uri] = (site, namespace, api_title, db_title)
            direct_qid = _direct_wikidata_qid_for_target(site, namespace, api_title, db_title)
            if direct_qid is not None:
                link_to_qid[link_uri] = direct_qid
                continue
            site_to_targets.setdefault(site, set()).add((namespace, api_title, db_title))

    site_title_to_qid = {}  # type: Dict[Tuple[str, str], str]
    backend = _wikidata_lookup_backend()
    for site, targets in site_to_targets.items():
        ordered_targets = sorted(targets, key=lambda item: (item[0], item[1], item[2]))
        result = _fetch_wikibase_items_for_site(site, ordered_targets, backend=backend)
        for title, qid in result.items():
            normalized_title = _normalize_page_title(title)
            normalized_qid = _normalize_qid(qid)
            if normalized_title and normalized_qid:
                site_title_to_qid[(site, normalized_title)] = normalized_qid

    for link_uri, (site, _namespace, api_title, _db_title) in link_targets.items():
        if link_uri in link_to_qid:
            continue
        resolved_qid = site_title_to_qid.get((site, _normalize_page_title(api_title)))
        if resolved_qid is not None:
            link_to_qid[link_uri] = resolved_qid

    return link_to_qid


def _thumbnail_url(image_name: str, width: int = 320) -> str:
    normalized = image_name.strip().replace(" ", "_")
    encoded = quote(normalized, safe="_-().,:")
    return "https://commons.wikimedia.org/wiki/Special:FilePath/{}?width={}".format(
        encoded,
        width,
    )


def _parse_coordinates(value: Any) -> Optional[Tuple[float, float]]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    parts = re.split(r"\s*[/,;]\s*", text)
    if len(parts) < 2:
        return None
    try:
        lat = float(parts[0])
        lon = float(parts[1])
    except Exception:
        return None

    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def _iter_scalar_fields(
    record: Mapping[str, Any],
    gil_link_wikidata_map: Optional[Mapping[str, str]] = None,
) -> Iterable[Tuple[str, Any]]:
    qid = _extract_qid(record)
    if qid is not None:
        yield "qid", qid
        yield "wikidata_entity", "https://www.wikidata.org/entity/{}".format(qid)

    metadata = record.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}

    image_name = metadata_map.get("image")
    if isinstance(image_name, str) and image_name.strip():
        normalized_image_name = image_name.strip()
        yield "thumbnail_image", _thumbnail_url(normalized_image_name)
        yield "thumbnail_image_file", normalized_image_name

    coordinates_value = metadata_map.get("coordinates")
    parsed_coordinates = _parse_coordinates(coordinates_value)
    if parsed_coordinates is not None:
        lat, lon = parsed_coordinates
        yield "coordinates", str(coordinates_value).strip()
        yield "coordinate_lat", lat
        yield "coordinate_lon", lon

    for key, value in record.items():
        if value is None:
            continue

        if key == "gil" and isinstance(value, str):
            # Keep raw field at item-level; URI link relationships are emitted as dedicated quads.
            yield key, value
            gil_links = _iter_gil_link_uris(record)
            yield "gil_link_count", len(gil_links)
            continue

        if isinstance(value, (str, int, float, bool)):
            yield key, value
            continue
        if isinstance(value, list):
            scalar_values = [
                str(item)
                for item in value
                if isinstance(item, (str, int, float, bool)) and str(item).strip()
            ]
            if scalar_values:
                yield key, "; ".join(scalar_values)


def _value_kind(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return "other"


def _summarize_structure(
    records: Sequence[Mapping[str, Any]],
    gil_link_wikidata_map: Optional[Mapping[str, str]] = None,
) -> StructureSummary:
    field_info = {}  # type: Dict[str, Dict[str, Any]]

    for row in records:
        row_fields = {}  # type: Dict[str, List[Any]]
        for key, value in _iter_scalar_fields(row, gil_link_wikidata_map=gil_link_wikidata_map):
            row_fields.setdefault(key, []).append(value)
        for link_uri, qid in _iter_gil_link_enrichment(
            row,
            gil_link_wikidata_map=gil_link_wikidata_map,
        ):
            row_fields.setdefault("gil_link", []).append(link_uri)
            if qid is not None:
                row_fields.setdefault("gil_link_wikidata_id", []).append(qid)
                row_fields.setdefault("gil_link_wikidata_entity", []).append(
                    "http://www.wikidata.org/entity/{}".format(qid)
                )

        for key, values in row_fields.items():
            info = field_info.setdefault(
                key,
                {
                    "source_key": key,
                    "predicate": PREDICATE_BASE + _field_name(key),
                    "present_in_rows": 0,
                    "type_counts": {},
                },
            )
            info["present_in_rows"] += 1

            type_counts = info["type_counts"]
            for kind in sorted({_value_kind(v) for v in values}):
                type_counts[kind] = int(type_counts.get(kind, 0)) + 1

    fields: List[StructureField] = []
    for key in sorted(field_info.keys()):
        info = field_info[key]
        type_counts = info["type_counts"]
        observed_types = sorted(type_counts.keys())
        primary_type = max(
            observed_types,
            key=lambda kind: (int(type_counts.get(kind, 0)), kind),
        )
        fields.append(
            {
                "source_key": info["source_key"],
                "predicate": info["predicate"],
                "present_in_rows": info["present_in_rows"],
                "primary_type": primary_type,
                "observed_types": observed_types,
            }
        )

    return {
        "row_count": len(records),
        "field_count": len(fields),
        "fields": fields,
    }


def _build_store(
    psid: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]] = None,
) -> StoreMeta:
    store_path = _store_path(psid)
    if store_path.exists():
        shutil.rmtree(store_path)
    store_path.mkdir(parents=True, exist_ok=True)

    store = Store(str(store_path))

    page_class = NamedNode(PREDICATE_BASE + "Page")
    rdf_type = NamedNode(RDF_TYPE_IRI)
    psid_predicate = NamedNode(PREDICATE_BASE + "psid")
    position_predicate = NamedNode(PREDICATE_BASE + "position")
    loaded_at_predicate = NamedNode(PREDICATE_BASE + "loadedAt")
    gil_link_predicate = NamedNode(PREDICATE_BASE + "gil_link")
    gil_link_wikidata_id_predicate = NamedNode(PREDICATE_BASE + "gil_link_wikidata_id")
    gil_link_wikidata_entity_predicate = NamedNode(PREDICATE_BASE + "gil_link_wikidata_entity")
    gil_link_wikidata_map = _build_gil_link_wikidata_map(records)

    loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for index, row in enumerate(records):
        subject = _item_subject(psid, row, index)
        store.add(Quad(subject, rdf_type, page_class, DefaultGraph()))
        store.add(
            Quad(
                subject,
                psid_predicate,
                Literal(str(psid), datatype=NamedNode(XSD_INTEGER_IRI)),
                DefaultGraph(),
            )
        )
        store.add(
            Quad(
                subject,
                position_predicate,
                Literal(str(index), datatype=NamedNode(XSD_INTEGER_IRI)),
                DefaultGraph(),
            )
        )
        store.add(
            Quad(
                subject,
                loaded_at_predicate,
                Literal(loaded_at, datatype=NamedNode(XSD_DATE_TIME_IRI)),
                DefaultGraph(),
            )
        )

        for key, value in _iter_scalar_fields(row, gil_link_wikidata_map=gil_link_wikidata_map):
            predicate = _predicate_for(key)
            literal = _literal_for(value)
            store.add(Quad(subject, predicate, literal, DefaultGraph()))

        for link_uri, qid in _iter_gil_link_enrichment(
            row,
            gil_link_wikidata_map=gil_link_wikidata_map,
        ):
            link_node = NamedNode(link_uri)
            store.add(Quad(subject, gil_link_predicate, link_node, DefaultGraph()))
            if qid is not None:
                store.add(
                    Quad(
                        link_node,
                        gil_link_wikidata_id_predicate,
                        Literal(qid),
                        DefaultGraph(),
                    )
                )
                store.add(
                    Quad(
                        link_node,
                        gil_link_wikidata_entity_predicate,
                        NamedNode("http://www.wikidata.org/entity/{}".format(qid)),
                        DefaultGraph(),
                    )
                )

    meta_model = StoreMetaModel(
        psid=psid,
        records=len(records),
        source_url=source_url,
        source_params=_normalize_petscan_params(source_params),
        loaded_at=loaded_at,
        structure=_summarize_structure(records, gil_link_wikidata_map=gil_link_wikidata_map),
    )
    meta = meta_model.to_dict()
    _meta_path(psid).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta


def _meta_has_matching_source_params(meta: Mapping[str, Any], petscan_params: Mapping[str, Any]) -> bool:
    expected = _normalize_petscan_params(petscan_params)
    actual = _normalize_petscan_params(meta.get("source_params") if isinstance(meta, Mapping) else {})
    return expected == actual


def _meta_is_usable(meta: Mapping[str, Any], psid: int) -> bool:
    if not isinstance(meta, Mapping) or not meta:
        return False

    meta_psid = meta.get("psid")
    if not isinstance(meta_psid, int) or isinstance(meta_psid, bool) or meta_psid != psid:
        return False

    records = meta.get("records")
    if not isinstance(records, int) or isinstance(records, bool) or records < 0:
        return False

    source_url = meta.get("source_url")
    if not isinstance(source_url, str) or not source_url.strip():
        return False

    loaded_at = meta.get("loaded_at")
    if not isinstance(loaded_at, str) or not loaded_at.strip():
        return False

    source_params = meta.get("source_params", {})
    if not isinstance(source_params, Mapping):
        return False

    return True


def ensure_loaded(
    psid: int,
    refresh: bool = False,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> StoreMeta:
    _ensure_oxigraph()
    lock = _get_psid_lock(psid)
    normalized_params = _normalize_petscan_params(petscan_params)

    with lock:
        if not refresh and _has_existing_store(psid):
            meta = _read_meta(psid)
            if _meta_is_usable(meta, psid) and _meta_has_matching_source_params(meta, normalized_params):
                return cast(StoreMeta, meta)

        payload, source_url = _fetch_petscan_json(psid, petscan_params=normalized_params)
        records = _extract_records(payload)
        if not records:
            raise PetscanServiceError("PetScan returned zero rows for psid {}.".format(psid))

        return _build_store(psid, records, source_url, source_params=normalized_params)


def execute_query(
    psid: int,
    query: str,
    refresh: bool = False,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> QueryExecution:
    _ensure_oxigraph()
    if _contains_service_clause(query):
        raise ValueError("SERVICE clauses are not allowed in this endpoint.")

    meta = ensure_loaded(psid, refresh=refresh, petscan_params=petscan_params)

    store = Store(str(_store_path(psid)))
    qtype = _query_type(query)

    try:
        raw_result = store.query(query)
    except Exception as exc:
        raise PetscanServiceError("SPARQL query failed: {}".format(exc)) from exc

    if qtype == "SELECT":
        result = QueryExecutionModel(
            query_type=qtype,
            result_format="sparql-json",
            sparql_json=_serialize_select(raw_result),
            meta=meta,
        )
        return result.to_dict()

    if qtype == "ASK":
        result = QueryExecutionModel(
            query_type=qtype,
            result_format="sparql-json",
            sparql_json=_serialize_ask(raw_result),
            meta=meta,
        )
        return result.to_dict()

    result = QueryExecutionModel(
        query_type=qtype,
        result_format="n-triples",
        ntriples=_serialize_graph(raw_result),
        meta=meta,
    )
    return result.to_dict()
