"""RDF field shaping and summary helpers for PetScan records."""

import re
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple
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
    "predicate_for",
    "summarize_structure",
]

_FIELD_NAME_RE = re.compile(r"[^0-9A-Za-z_]+")
_FIELD_RENAMES = {
    "id": "page_id",
}


def _field_name(key: str) -> str:
    canonical = _FIELD_RENAMES.get(key, key)
    cleaned = _FIELD_NAME_RE.sub("_", canonical).strip("_")
    if not cleaned:
        cleaned = "field"
    if cleaned[0].isdigit():
        cleaned = "field_{}".format(cleaned)
    return cleaned


def predicate_for(key: str):
    return NamedNode(PREDICATE_BASE + _field_name(key))


def literal_for(value: Any):
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


def item_subject(psid: int, record: Mapping[str, Any], index: int):
    page_id = _record_page_id(record)
    if page_id is not None and _is_commons_file_record(record):
        return NamedNode("https://commons.wikimedia.org/entity/M{}".format(page_id))

    qid = links.extract_qid(record)
    if qid is not None:
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
    gil_link_wikidata_map: Optional[Mapping[str, str]] = None,
) -> Iterable[Tuple[str, Any]]:
    qid = links.extract_qid(record)
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
            gil_links = links.iter_gil_link_uris(record)
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


def summarize_structure(
    records: Sequence[Mapping[str, Any]],
    gil_link_wikidata_map: Optional[Mapping[str, str]] = None,
) -> StructureSummary:
    field_info = {}  # type: dict[str, dict[str, Any]]

    for row in records:
        row_fields = {}  # type: dict[str, List[Any]]
        for key, value in iter_scalar_fields(row, gil_link_wikidata_map=gil_link_wikidata_map):
            row_fields.setdefault(key, []).append(value)
        for link_uri, qid in links.iter_gil_link_enrichment(
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
