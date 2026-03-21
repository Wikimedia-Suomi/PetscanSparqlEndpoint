"""RDF field shaping and summary helpers for PetScan records."""

import re
from collections.abc import Mapping as RuntimeMapping
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Collection, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

from . import service_links as links
from .service_types import StructureField, StructureSummary

try:
    from pyoxigraph import Literal, NamedNode
except ImportError:  # pragma: no cover - dependency check at runtime
    Literal = None  # type: ignore[misc,assignment]
    NamedNode = None  # type: ignore[misc,assignment]

PREDICATE_BASE = "https://petscan.wmcloud.org/ontology/"
ITEM_BASE = "https://petscan.wmcloud.org/psid"
RDF_TYPE_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_INTEGER_IRI = "http://www.w3.org/2001/XMLSchema#integer"
XSD_DOUBLE_IRI = "http://www.w3.org/2001/XMLSchema#double"
XSD_BOOLEAN_IRI = "http://www.w3.org/2001/XMLSchema#boolean"
XSD_DATE_TIME_IRI = "http://www.w3.org/2001/XMLSchema#dateTime"
SPARQL_IRI_TYPE = "iri"
_XSD_DATETIME_SCALAR_FIELDS = frozenset({"img_timestamp", "touched", "gil_link_rev_timestamp"})
__all__ = [
    "ITEM_BASE",
    "PREDICATE_BASE",
    "RDF_TYPE_IRI",
    "XSD_BOOLEAN_IRI",
    "XSD_DATE_TIME_IRI",
    "XSD_DOUBLE_IRI",
    "XSD_INTEGER_IRI",
    "item_subject",
    "iter_scalar_fields",
    "literal_for",
    "normalize_datetime_xsd",
    "predicate_for",
    "sparql_type_for_scalar_field",
    "sparql_type_for_value",
    "StructureAccumulator",
    "summarize_structure",
    "value_kind",
]

if NamedNode is not None:
    _XSD_INTEGER_NODE = NamedNode(XSD_INTEGER_IRI)
    _XSD_DOUBLE_NODE = NamedNode(XSD_DOUBLE_IRI)
    _XSD_BOOLEAN_NODE = NamedNode(XSD_BOOLEAN_IRI)
else:  # pragma: no cover - dependency check at runtime
    _XSD_INTEGER_NODE = None
    _XSD_DOUBLE_NODE = None
    _XSD_BOOLEAN_NODE = None

_FIELD_NAME_RE = re.compile(r"[^0-9A-Za-z_]+")
_FIELD_RENAMES = {
    "id": "page_id",
}


class StructureAccumulator:
    def __init__(self) -> None:
        self._field_info: Dict[str, Dict[str, Any]] = {}

    def add_row_field_kinds(self, row_field_kinds: Mapping[str, Collection[str]]) -> None:
        for key, kinds in row_field_kinds.items():
            if not kinds:
                continue
            info = self._field_info.setdefault(
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
            for kind in kinds:
                type_counts[kind] = int(type_counts.get(kind, 0)) + 1

    def add_row_fields(self, row_fields: Mapping[str, Sequence[Any]]) -> None:
        row_field_kinds: Dict[str, List[str]] = {}
        for key, values in row_fields.items():
            seen = set()
            for value in values:
                seen.add(sparql_type_for_scalar_field(key, value))
            if seen:
                row_field_kinds[key] = list(seen)
        self.add_row_field_kinds(row_field_kinds)

    def build_summary(self, row_count: int) -> StructureSummary:
        fields: List[StructureField] = []
        for key in sorted(self._field_info.keys()):
            info = self._field_info[key]
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
            "row_count": row_count,
            "field_count": len(fields),
            "fields": fields,
        }


@lru_cache(maxsize=512)
def _field_name(key: str) -> str:
    canonical = _FIELD_RENAMES.get(key, key)
    cleaned = _FIELD_NAME_RE.sub("_", canonical).strip("_")
    if not cleaned:
        cleaned = "field"
    if cleaned[0].isdigit():
        cleaned = "field_{}".format(cleaned)
    return cleaned


@lru_cache(maxsize=512)
def predicate_for(key: str) -> Any:
    return NamedNode(PREDICATE_BASE + _field_name(key))


def literal_for(value: Any) -> Any:
    if isinstance(value, bool):
        return Literal("true" if value else "false", datatype=_XSD_BOOLEAN_NODE)
    if isinstance(value, int):
        return Literal(str(value), datatype=_XSD_INTEGER_NODE)
    if isinstance(value, float):
        return Literal(repr(value), datatype=_XSD_DOUBLE_NODE)
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

    namespace_value = record.get("namespace")
    try:
        namespace = int(namespace_value) if namespace_value is not None else None
    except Exception:
        namespace = None

    nstext = str(record.get("nstext", "")).strip().lower()
    is_file_page = namespace == 6 or nstext == "file"
    if not is_file_page:
        return False

    if wiki_value:
        return wiki_value in {"commonswiki", "commons.wikimedia.org"}

    has_image_metadata = any(isinstance(key, str) and key.startswith("img_") for key in record)

    # PetScan media rows often omit explicit wiki while still representing Commons files.
    return has_image_metadata


def item_subject(psid: int, record: Mapping[str, Any], index: int) -> Any:
    page_id = _record_page_id(record)
    if page_id is not None and _is_commons_file_record(record):
        return NamedNode("https://commons.wikimedia.org/entity/M{}".format(page_id))

    qid = links.extract_qid(record)
    if qid is not None:
        # PetScan result rows come from a single source wiki. `gil_link` targets may
        # point to other wikis, but those are modeled separately and do not become
        # row subjects here. Within one wiki a Wikibase item/QID should belong to
        # only one page, so using the QID as the subject is not expected to merge
        # distinct PetScan result rows. Revisit this assumption if the input model
        # changes in the future.
        return NamedNode("http://www.wikidata.org/entity/{}".format(qid))

    identifier = quote(_record_identifier(record, index), safe="")
    return NamedNode("{}/{}/item/{}".format(ITEM_BASE, psid, identifier))


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


def iter_scalar_fields(
    record: Mapping[str, Any],
    gil_links: Optional[Sequence[str]] = None,
) -> Iterable[Tuple[str, Any]]:
    record_get = record.get
    metadata = record_get("metadata")
    metadata_map = metadata if isinstance(metadata, RuntimeMapping) else {}
    if (
        "wikidata_id" in record
        or "qid" in record
        or "q" in record
        or "wikidata" in record
        or "wikidata" in metadata_map
    ):
        qid = links.extract_qid(record)
        if qid is not None:
            yield "qid", qid
            yield "wikidata_entity", "https://www.wikidata.org/entity/{}".format(qid)

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
            resolved_gil_links = list(gil_links) if gil_links is not None else links.iter_gil_link_uris(record)
            yield "gil_link_count", len(resolved_gil_links)
            continue

        if isinstance(value, (str, int, float, bool)):
            yield key, value
            continue
        if isinstance(value, list):
            scalar_values = []
            for item in value:
                if not isinstance(item, (str, int, float, bool)):
                    continue
                text = str(item).strip()
                if text:
                    scalar_values.append(text)
            if scalar_values:
                yield key, "; ".join(scalar_values)


def value_kind(value: Any) -> str:
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


def sparql_type_for_value(value: Any) -> str:
    if isinstance(value, bool):
        return "xsd:boolean"
    if isinstance(value, int):
        return "xsd:integer"
    if isinstance(value, float):
        return "xsd:double"
    return "xsd:string"


def sparql_type_for_scalar_field(key: str, value: Any) -> str:
    if key in _XSD_DATETIME_SCALAR_FIELDS and normalize_datetime_xsd(value) is not None:
        return "xsd:dateTime"
    return sparql_type_for_value(value)


def _normalize_page_len(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        page_len = int(value)
    except Exception:
        return None
    if page_len < 0:
        return None
    return page_len


def normalize_datetime_xsd(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    if re.fullmatch(r"\d{14}", text):
        formatted = "{}-{}-{}T{}:{}:{}+00:00".format(
            text[0:4],
            text[4:6],
            text[6:8],
            text[8:10],
            text[10:12],
            text[12:14],
        )
    else:
        formatted = text[:-1] + "+00:00" if text.endswith("Z") else text

    try:
        parsed = datetime.fromisoformat(formatted)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    normalized = parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return normalized.replace("+00:00", "Z")


def summarize_structure(
    records: Sequence[Mapping[str, Any]],
    gil_link_enrichment_map: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> StructureSummary:
    accumulator = StructureAccumulator()

    for row in records:
        row_field_kinds: Dict[str, set[str]] = {}

        def _track_row_field_kind(key: str, kind: str) -> None:
            kinds = row_field_kinds.get(key)
            if kinds is None:
                row_field_kinds[key] = {kind}
            else:
                kinds.add(kind)

        resolved_gil_links = links.resolve_gil_links(
            row,
            gil_link_enrichment_map=gil_link_enrichment_map,
        )
        gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links]
        for key, value in iter_scalar_fields(row, gil_links=gil_link_uris):
            _track_row_field_kind(key, sparql_type_for_scalar_field(key, value))

        for link_uri, qid in resolved_gil_links:
            _track_row_field_kind("gil_link", SPARQL_IRI_TYPE)
            payload = gil_link_enrichment_map.get(link_uri) if gil_link_enrichment_map is not None else None
            if isinstance(payload, RuntimeMapping):
                page_len = _normalize_page_len(payload.get("page_len"))
                if page_len is not None:
                    _track_row_field_kind("gil_link_page_len", "xsd:integer")

                rev_timestamp = normalize_datetime_xsd(payload.get("rev_timestamp"))
                if rev_timestamp is not None:
                    _track_row_field_kind("gil_link_rev_timestamp", "xsd:dateTime")

            if qid is not None:
                _track_row_field_kind("gil_link_wikidata_id", "xsd:string")
                _track_row_field_kind("gil_link_wikidata_entity", SPARQL_IRI_TYPE)

        accumulator.add_row_field_kinds(row_field_kinds)

    return accumulator.build_summary(row_count=len(records))
