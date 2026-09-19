"""Oxigraph store construction for W3C Data Cube and JSON-stat compatibility RDF."""

import json
import shutil
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, cast
from urllib.parse import quote

from petscan import service_rdf as rdf
from petscan import service_store as store
from petscan.service_errors import PetscanServiceError
from petscan.service_types import StoreMeta, StoreMetaModel, StructureField, StructureSummary

from . import service_classification as classification
from . import service_runtime_links as runtime_links_service
from . import service_source as source

__all__ = ["JSONSTAT_ONTOLOGY_BASE", "build_store"]

JSONSTAT_ONTOLOGY_BASE = "https://sparqlbridge.toolforge.org/ontology/jsonstat/"
_RESOURCE_BASE = "https://sparqlbridge.toolforge.org/jsonstat/dataset/"
_QUAD_BUFFER_TARGET = 1_000_000
_QB_BASE = "http://purl.org/linked-data/cube#"
_SKOS_BASE = "http://www.w3.org/2004/02/skos/core#"
_RDF_PROPERTY_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"
_RDFS_LABEL_IRI = "http://www.w3.org/2000/01/rdf-schema#label"
_RDFS_RANGE_IRI = "http://www.w3.org/2000/01/rdf-schema#range"
_RDFS_COMMENT_IRI = "http://www.w3.org/2000/01/rdf-schema#comment"
_RDFS_SEE_ALSO_IRI = "http://www.w3.org/2000/01/rdf-schema#seeAlso"
_DCT_ISSUED_IRI = "http://purl.org/dc/terms/issued"
_DCT_MODIFIED_IRI = "http://purl.org/dc/terms/modified"
_DCT_SOURCE_IRI = "http://purl.org/dc/terms/source"
_DCT_TITLE_IRI = "http://purl.org/dc/terms/title"
_SDMX_UNIT_MEASURE_IRI = "http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure"
_XSD_DATE_IRI = "http://www.w3.org/2001/XMLSchema#date"
_UNIT_KEYS = ("base", "decimals", "label", "position", "symbol")
_CLASSIFICATION_NOTE_PREDICATES = {
    "generalNote": "classificationGeneralNote",
    "includes": "classificationIncludes",
    "includesAlso": "classificationIncludesAlso",
    "excludes": "classificationExcludes",
}

try:
    from pyoxigraph import Literal, NamedNode, Quad, Store
except ImportError:  # pragma: no cover - dependency check at runtime
    Literal = None  # type: ignore[misc,assignment]
    NamedNode = None  # type: ignore[misc,assignment]
    Quad = None  # type: ignore[misc,assignment]
    Store = None  # type: ignore[misc,assignment]


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


@lru_cache(maxsize=1024)
def _jsonstat_predicate_for(key: str) -> Any:
    return NamedNode(JSONSTAT_ONTOLOGY_BASE + rdf._field_name(key))


def _jsonstat_structure_summary(summary: StructureSummary) -> StructureSummary:
    fields: List[StructureField] = []
    for field in summary["fields"]:
        updated_field: StructureField = {
            "source_key": field["source_key"],
            "predicate": JSONSTAT_ONTOLOGY_BASE + rdf._field_name(field["source_key"]),
            "present_in_rows": field["present_in_rows"],
            "primary_type": field["primary_type"],
            "observed_types": list(field["observed_types"]),
        }
        if "row_side_cardinality" in field:
            updated_field["row_side_cardinality"] = field["row_side_cardinality"]
        fields.append(updated_field)
    return {
        "row_count": summary["row_count"],
        "field_count": summary["field_count"],
        "fields": fields,
    }


def _dataset_subject(store_id: int) -> Any:
    return NamedNode("{}{}".format(_RESOURCE_BASE, store_id))


def _data_structure_subject(store_id: int) -> Any:
    return NamedNode("{}{}/structure".format(_RESOURCE_BASE, store_id))


def _component_specification_subject(store_id: int, kind: str, component_id: str) -> Any:
    return NamedNode(
        "{}{}/structure/{}/{}".format(
            _RESOURCE_BASE,
            store_id,
            quote(kind, safe=""),
            quote(component_id, safe=""),
        )
    )


def _observation_subject(store_id: int, position: int) -> Any:
    return NamedNode("{}{}/observation/{}".format(_RESOURCE_BASE, store_id, position + 1))


def _dimension_subject(store_id: int, dimension_id: str) -> Any:
    return NamedNode(
        "{}{}/dimension/{}".format(_RESOURCE_BASE, store_id, quote(dimension_id, safe=""))
    )


def _category_subject(store_id: int, dimension_id: str, category_id: str) -> Any:
    return NamedNode(
        "{}{}/dimension/{}/category/{}".format(
            _RESOURCE_BASE,
            store_id,
            quote(dimension_id, safe=""),
            quote(category_id, safe=""),
        )
    )


def _classification_subject(store_id: int, dimension_id: str) -> Any:
    return NamedNode(
        "{}{}/dimension/{}/classification".format(
            _RESOURCE_BASE,
            store_id,
            quote(dimension_id, safe=""),
        )
    )


def _dimension_property(field_name: str) -> Any:
    return _jsonstat_predicate_for("{}_concept".format(field_name))


def _attribute_field_names(
    payload: Mapping[str, Any],
    dimension_field_names: Mapping[str, str],
) -> List[str]:
    fields: List[str] = []
    if payload.get("status") is not None:
        fields.append("status")

    dimensions = payload.get("dimension")
    if not isinstance(dimensions, Mapping):
        return fields
    for dimension_id, field_name in dimension_field_names.items():
        dimension = dimensions.get(dimension_id)
        if not isinstance(dimension, Mapping):
            continue
        category = dimension.get("category")
        if not isinstance(category, Mapping):
            continue
        units = category.get("unit")
        if not isinstance(units, Mapping):
            continue
        present_unit_keys = {
            unit_key
            for unit in units.values()
            if isinstance(unit, Mapping)
            for unit_key in _UNIT_KEYS
            if unit.get(unit_key) is not None
            and not isinstance(unit.get(unit_key), (Mapping, list))
        }
        fields.extend(
            "{}_unit_{}".format(field_name, unit_key)
            for unit_key in _UNIT_KEYS
            if unit_key in present_unit_keys
        )
    return fields


def _append_component_specification_quads(
    store_id: int,
    dsd: Any,
    kind: str,
    component_id: str,
    component_property: Any,
    quad_buffer: List[Any],
    *,
    order: Optional[int] = None,
    required: Optional[bool] = None,
) -> None:
    component = _component_specification_subject(store_id, kind, component_id)
    append_quad = quad_buffer.append
    append_quad(
        Quad(component, NamedNode(rdf.RDF_TYPE_IRI), NamedNode(_QB_BASE + "ComponentSpecification"))
    )
    append_quad(Quad(dsd, NamedNode(_QB_BASE + "component"), component))
    append_quad(Quad(component, NamedNode(_QB_BASE + kind), component_property))
    append_quad(Quad(component, NamedNode(_QB_BASE + "componentProperty"), component_property))
    if order is not None:
        append_quad(Quad(component, NamedNode(_QB_BASE + "order"), rdf.literal_for(order)))
    if required is not None:
        append_quad(
            Quad(component, NamedNode(_QB_BASE + "componentRequired"), rdf.literal_for(required))
        )


def _append_localized_quads(
    subject: Any,
    predicate: Any,
    localized_values: Mapping[str, Sequence[str]],
    quad_buffer: List[Any],
) -> None:
    for language, values in localized_values.items():
        for value in values:
            if value.strip():
                quad_buffer.append(
                    Quad(subject, predicate, Literal(value.strip(), language=language))
                )


def _append_classification_scheme_quads(
    dimension_node: Any,
    store_id: int,
    enrichment: classification.ClassificationEnrichment,
    quad_buffer: List[Any],
) -> Any:
    rdf_type = NamedNode(rdf.RDF_TYPE_IRI)
    scheme = _classification_subject(store_id, enrichment.local_id)
    quad_buffer.append(Quad(dimension_node, _jsonstat_predicate_for("classification"), scheme))
    quad_buffer.append(Quad(scheme, rdf_type, NamedNode(_SKOS_BASE + "ConceptScheme")))
    quad_buffer.append(
        Quad(
            scheme,
            _jsonstat_predicate_for("classificationLocalId"),
            Literal(enrichment.local_id),
        )
    )
    api_resource = NamedNode(enrichment.api_url)
    quad_buffer.append(Quad(scheme, NamedNode(_RDFS_SEE_ALSO_IRI), api_resource))
    quad_buffer.append(Quad(scheme, _jsonstat_predicate_for("apiResource"), api_resource))
    _append_localized_quads(
        scheme,
        NamedNode(_SKOS_BASE + "prefLabel"),
        enrichment.labels,
        quad_buffer,
    )
    _append_localized_quads(
        scheme,
        NamedNode(_SKOS_BASE + "definition"),
        enrichment.descriptions,
        quad_buffer,
    )
    _append_localized_quads(
        scheme,
        _jsonstat_predicate_for("classificationPurpose"),
        enrichment.purposes,
        quad_buffer,
    )
    _append_localized_quads(
        scheme,
        _jsonstat_predicate_for("classificationDetailedDescription"),
        enrichment.detailed_descriptions,
        quad_buffer,
    )
    _append_localized_quads(
        scheme,
        _jsonstat_predicate_for("classificationInternationalRelationship"),
        enrichment.international_relationships,
        quad_buffer,
    )
    _append_localized_quads(
        scheme,
        _jsonstat_predicate_for("classificationSeriesLabel"),
        enrichment.series_labels,
        quad_buffer,
    )

    if enrichment.series_id is not None:
        quad_buffer.append(
            Quad(
                scheme,
                _jsonstat_predicate_for("classificationSeriesId"),
                Literal(enrichment.series_id),
            )
        )
    if enrichment.release_date is not None:
        quad_buffer.append(
            Quad(
                scheme,
                NamedNode(_DCT_ISSUED_IRI),
                Literal(enrichment.release_date, datatype=NamedNode(_XSD_DATE_IRI)),
            )
        )
    if enrichment.termination_date is not None:
        quad_buffer.append(
            Quad(
                scheme,
                _jsonstat_predicate_for("terminationDate"),
                Literal(enrichment.termination_date, datatype=NamedNode(_XSD_DATE_IRI)),
            )
        )
    if enrichment.modified_at is not None:
        quad_buffer.append(
            Quad(
                scheme,
                NamedNode(_DCT_MODIFIED_IRI),
                Literal(enrichment.modified_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
            )
        )
    for key, value in (
        ("internationalRecommendation", enrichment.international_recommendation),
        ("nationalRecommendation", enrichment.national_recommendation),
    ):
        if value is not None:
            quad_buffer.append(Quad(scheme, _jsonstat_predicate_for(key), rdf.literal_for(value)))
    return scheme


def _classification_level_literal(level: float) -> Any:
    if level.is_integer():
        return rdf.literal_for(int(level))
    return rdf.literal_for(level)


def _append_enriched_category_quads(
    store_id: int,
    enrichment: classification.ClassificationEnrichment,
    item: classification.ClassificationItemEnrichment,
    original_label: Any,
    scheme: Any,
    quad_buffer: List[Any],
) -> None:
    rdf_type = NamedNode(rdf.RDF_TYPE_IRI)
    category_node = _category_subject(store_id, enrichment.local_id, item.code)
    api_resource = NamedNode(item.api_url)
    quad_buffer.append(Quad(category_node, rdf_type, NamedNode(_SKOS_BASE + "Concept")))
    quad_buffer.append(Quad(category_node, NamedNode(_SKOS_BASE + "inScheme"), scheme))
    quad_buffer.append(Quad(category_node, NamedNode(_SKOS_BASE + "notation"), Literal(item.code)))
    quad_buffer.append(
        Quad(category_node, _jsonstat_predicate_for("classificationItem"), api_resource)
    )
    quad_buffer.append(Quad(category_node, NamedNode(_RDFS_SEE_ALSO_IRI), api_resource))
    quad_buffer.append(Quad(category_node, NamedNode(_SKOS_BASE + "exactMatch"), api_resource))
    for relation, target in item.external_links:
        quad_buffer.append(Quad(category_node, NamedNode(relation), NamedNode(target)))
    if original_label is not None and str(original_label).strip():
        quad_buffer.append(
            Quad(
                category_node,
                NamedNode(_SKOS_BASE + "prefLabel"),
                Literal(str(original_label).strip()),
            )
        )
    _append_localized_quads(
        category_node,
        NamedNode(_SKOS_BASE + "prefLabel"),
        item.labels,
        quad_buffer,
    )
    if item.level is not None:
        quad_buffer.append(
            Quad(
                category_node,
                _jsonstat_predicate_for("classificationLevel"),
                _classification_level_literal(item.level),
            )
        )
    if item.order is not None:
        quad_buffer.append(
            Quad(
                category_node,
                _jsonstat_predicate_for("classificationOrder"),
                rdf.literal_for(item.order),
            )
        )
    if item.parent_code is not None and item.parent_code != item.code:
        parent_node = _category_subject(store_id, enrichment.local_id, item.parent_code)
        parent_api_resource = NamedNode(
            classification.classification_item_api_url(
                enrichment.local_id,
                item.parent_code,
            )
        )
        quad_buffer.append(Quad(category_node, NamedNode(_SKOS_BASE + "broader"), parent_node))
        quad_buffer.append(
            Quad(
                category_node,
                _jsonstat_predicate_for("classificationParentCode"),
                Literal(item.parent_code),
            )
        )
        quad_buffer.append(Quad(parent_node, rdf_type, NamedNode(_SKOS_BASE + "Concept")))
        quad_buffer.append(Quad(parent_node, NamedNode(_SKOS_BASE + "inScheme"), scheme))
        quad_buffer.append(
            Quad(parent_node, NamedNode(_SKOS_BASE + "notation"), Literal(item.parent_code))
        )
        quad_buffer.append(Quad(parent_node, NamedNode(_RDFS_SEE_ALSO_IRI), parent_api_resource))
        quad_buffer.append(
            Quad(parent_node, NamedNode(_SKOS_BASE + "exactMatch"), parent_api_resource)
        )

    for note_key, localized_values in item.notes.items():
        predicate_name = _CLASSIFICATION_NOTE_PREDICATES.get(note_key)
        if predicate_name is None:
            continue
        _append_localized_quads(
            category_node,
            _jsonstat_predicate_for(predicate_name),
            localized_values,
            quad_buffer,
        )


def _statistics_id_from_payload(payload: Mapping[str, Any]) -> Optional[str]:
    extension = payload.get("extension")
    if not isinstance(extension, Mapping):
        return None
    px = extension.get("px")
    if not isinstance(px, Mapping):
        return None
    raw_identifier = px.get("subject-code")
    if not isinstance(raw_identifier, str) or not raw_identifier.strip():
        return None
    return raw_identifier.strip().lower()


def _append_statistics_link_quads(
    dataset: Any,
    payload: Mapping[str, Any],
    runtime_links: Optional[runtime_links_service.RuntimeLinks],
    quad_buffer: List[Any],
) -> None:
    if runtime_links is None:
        return
    statistics_id = _statistics_id_from_payload(payload)
    if statistics_id is None:
        return
    statistics = runtime_links.statistics.get(statistics_id)
    if statistics is None:
        return
    statistics_resource = NamedNode(statistics.url)
    quad_buffer.append(
        Quad(dataset, _jsonstat_predicate_for("statisticsId"), Literal(statistics.identifier))
    )
    quad_buffer.append(
        Quad(dataset, _jsonstat_predicate_for("statisticsResource"), statistics_resource)
    )
    quad_buffer.append(Quad(dataset, NamedNode(_RDFS_SEE_ALSO_IRI), statistics_resource))
    for external_link in statistics.external_links:
        relation = NamedNode(external_link.relation)
        for target in external_link.targets:
            quad_buffer.append(Quad(dataset, relation, NamedNode(target)))


def _append_mapped_unit_quads(
    category_node: Any,
    unit: Mapping[str, Any],
    runtime_links: Optional[runtime_links_service.RuntimeLinks],
    quad_buffer: List[Any],
) -> None:
    if runtime_links is None:
        return
    base = unit.get("base")
    if not isinstance(base, str):
        return
    mapping = runtime_links.units.get(base)
    if mapping is None:
        return
    quad_buffer.append(
        Quad(
            category_node,
            _jsonstat_predicate_for("unitMappingStatus"),
            Literal(mapping.mapping_status),
        )
    )
    for field_name in (
        "ucumCode",
        "unitMultiplier",
        "currencyCode",
        "denominatorUcumCode",
        "quantityKind",
        "referencePeriod",
        "referenceValue",
        "minimum",
        "maximum",
    ):
        value = mapping.model.get(field_name)
        if value is None or isinstance(value, (Mapping, list)):
            continue
        quad_buffer.append(
            Quad(
                category_node,
                _jsonstat_predicate_for(field_name),
                rdf.literal_for(value),
            )
        )
    concepts = mapping.model.get("fintoUcumConcepts")
    if isinstance(concepts, list):
        for concept in concepts:
            if not isinstance(concept, Mapping):
                continue
            uri = concept.get("uri")
            if isinstance(uri, str) and uri.startswith(("http://", "https://")):
                unit_concept = NamedNode(uri)
                quad_buffer.append(
                    Quad(
                        category_node,
                        _jsonstat_predicate_for("unitConcept"),
                        unit_concept,
                    )
                )
                if concept.get("code") == mapping.model.get("ucumCode"):
                    quad_buffer.append(
                        Quad(
                            category_node,
                            NamedNode(_SDMX_UNIT_MEASURE_IRI),
                            unit_concept,
                        )
                    )


def _append_dataset_metadata_quads(
    store_id: int,
    payload: Mapping[str, Any],
    source_url: str,
    loaded_at: str,
    dimension_field_names: Mapping[str, str],
    classification_enrichments: Mapping[str, classification.ClassificationEnrichment],
    runtime_links: Optional[runtime_links_service.RuntimeLinks],
    quad_buffer: List[Any],
) -> None:
    dataset = _dataset_subject(store_id)
    dsd = _data_structure_subject(store_id)
    rdf_type = NamedNode(rdf.RDF_TYPE_IRI)
    append_quad = quad_buffer.append
    append_quad(Quad(dataset, rdf_type, NamedNode(_QB_BASE + "DataSet")))
    append_quad(Quad(dataset, rdf_type, NamedNode(JSONSTAT_ONTOLOGY_BASE + "Dataset")))
    append_quad(Quad(dataset, NamedNode(_QB_BASE + "structure"), dsd))
    append_quad(Quad(dsd, rdf_type, NamedNode(_QB_BASE + "DataStructureDefinition")))
    append_quad(Quad(dataset, _jsonstat_predicate_for("sourceUrl"), NamedNode(source_url)))
    append_quad(Quad(dataset, NamedNode(_DCT_SOURCE_IRI), NamedNode(source_url)))
    _append_statistics_link_quads(dataset, payload, runtime_links, quad_buffer)
    append_quad(
        Quad(
            dataset,
            _jsonstat_predicate_for("loadedAt"),
            Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )
    )

    label = payload.get("label")
    if isinstance(label, str) and label.strip():
        normalized_label = Literal(label.strip())
        append_quad(Quad(dataset, _jsonstat_predicate_for("label"), normalized_label))
        append_quad(Quad(dataset, NamedNode(_RDFS_LABEL_IRI), normalized_label))
        append_quad(Quad(dataset, NamedNode(_DCT_TITLE_IRI), normalized_label))

    source_label = payload.get("source")
    if isinstance(source_label, str) and source_label.strip():
        append_quad(Quad(dataset, _jsonstat_predicate_for("source"), Literal(source_label.strip())))

    updated = rdf.normalize_datetime_xsd(payload.get("updated"))
    if updated is not None:
        updated_literal = Literal(updated, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI))
        append_quad(Quad(dataset, _jsonstat_predicate_for("updated"), updated_literal))
        append_quad(Quad(dataset, NamedNode(_DCT_MODIFIED_IRI), updated_literal))

    notes = payload.get("note")
    if isinstance(notes, list):
        for note in notes:
            text = str(note).strip()
            if text:
                append_quad(Quad(dataset, _jsonstat_predicate_for("note"), Literal(text)))
                append_quad(Quad(dataset, NamedNode(_RDFS_COMMENT_IRI), Literal(text)))

    dimension_ids = cast(List[str], payload.get("id", []))
    sizes = cast(List[int], payload.get("size", []))
    dimensions = cast(Mapping[str, Any], payload.get("dimension", {}))
    value_property = _jsonstat_predicate_for("value")
    append_quad(Quad(value_property, rdf_type, NamedNode(_RDF_PROPERTY_IRI)))
    append_quad(Quad(value_property, rdf_type, NamedNode(_QB_BASE + "MeasureProperty")))
    append_quad(Quad(value_property, NamedNode(_RDFS_LABEL_IRI), Literal("Observation value")))
    _append_component_specification_quads(
        store_id,
        dsd,
        "measure",
        "value",
        value_property,
        quad_buffer,
    )

    for attribute_field_name in _attribute_field_names(payload, dimension_field_names):
        attribute_property = _jsonstat_predicate_for(attribute_field_name)
        append_quad(Quad(attribute_property, rdf_type, NamedNode(_RDF_PROPERTY_IRI)))
        append_quad(Quad(attribute_property, rdf_type, NamedNode(_QB_BASE + "AttributeProperty")))
        append_quad(
            Quad(attribute_property, NamedNode(_RDFS_LABEL_IRI), Literal(attribute_field_name))
        )
        _append_component_specification_quads(
            store_id,
            dsd,
            "attribute",
            attribute_field_name,
            attribute_property,
            quad_buffer,
            required=False,
        )

    roles = payload.get("role")
    role_by_dimension: Dict[str, List[str]] = {}
    if isinstance(roles, Mapping):
        for role, raw_dimension_ids in roles.items():
            if not isinstance(raw_dimension_ids, list):
                continue
            for dimension_id in raw_dimension_ids:
                role_by_dimension.setdefault(str(dimension_id), []).append(str(role))

    for dimension_position, (dimension_id, size) in enumerate(
        zip(dimension_ids, sizes, strict=True)
    ):
        dimension = cast(Mapping[str, Any], dimensions[dimension_id])
        dimension_node = _dimension_subject(store_id, dimension_id)
        classification_scheme = _classification_subject(store_id, dimension_id)
        dimension_property = _dimension_property(dimension_field_names[dimension_id])
        classification_enrichment = classification_enrichments.get(dimension_id)
        append_quad(Quad(dataset, _jsonstat_predicate_for("dimension"), dimension_node))
        append_quad(Quad(dimension_node, rdf_type, NamedNode(JSONSTAT_ONTOLOGY_BASE + "Dimension")))
        append_quad(
            Quad(dimension_node, _jsonstat_predicate_for("codeList"), classification_scheme)
        )
        append_quad(
            Quad(dimension_node, _jsonstat_predicate_for("dimensionId"), Literal(dimension_id))
        )
        append_quad(
            Quad(
                dimension_node,
                _jsonstat_predicate_for("position"),
                Literal(str(dimension_position), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
            )
        )
        dimension_label = dimension.get("label")
        if isinstance(dimension_label, str) and dimension_label.strip():
            normalized_dimension_label = Literal(dimension_label.strip())
            append_quad(
                Quad(
                    dimension_node,
                    _jsonstat_predicate_for("label"),
                    normalized_dimension_label,
                )
            )
            append_quad(
                Quad(dimension_node, NamedNode(_RDFS_LABEL_IRI), normalized_dimension_label)
            )
            append_quad(
                Quad(
                    classification_scheme,
                    NamedNode(_SKOS_BASE + "prefLabel"),
                    normalized_dimension_label,
                )
            )
        for role in role_by_dimension.get(dimension_id, []):
            append_quad(Quad(dimension_node, _jsonstat_predicate_for("role"), Literal(role)))

        append_quad(Quad(classification_scheme, rdf_type, NamedNode(_SKOS_BASE + "ConceptScheme")))
        append_quad(Quad(dimension_property, rdf_type, NamedNode(_RDF_PROPERTY_IRI)))
        append_quad(Quad(dimension_property, rdf_type, NamedNode(_QB_BASE + "DimensionProperty")))
        append_quad(Quad(dimension_property, rdf_type, NamedNode(_QB_BASE + "CodedProperty")))
        append_quad(
            Quad(dimension_property, NamedNode(_RDFS_RANGE_IRI), NamedNode(_SKOS_BASE + "Concept"))
        )
        append_quad(
            Quad(dimension_property, NamedNode(_QB_BASE + "codeList"), classification_scheme)
        )
        append_quad(
            Quad(
                dimension_property,
                NamedNode(_RDFS_LABEL_IRI),
                Literal(
                    dimension_label.strip()
                    if isinstance(dimension_label, str) and dimension_label.strip()
                    else dimension_id
                ),
            )
        )
        _append_component_specification_quads(
            store_id,
            dsd,
            "dimension",
            dimension_id,
            dimension_property,
            quad_buffer,
            order=dimension_position + 1,
        )

        if classification_enrichment is not None:
            classification_scheme = _append_classification_scheme_quads(
                dimension_node,
                store_id,
                classification_enrichment,
                quad_buffer,
            )

        category = cast(Mapping[str, Any], dimension["category"])
        labels = category.get("label")
        units = category.get("unit")
        category_ids = source._ordered_category_ids(dimension_id, dimension, size)
        for category_position, category_id in enumerate(category_ids):
            category_node = _category_subject(store_id, dimension_id, category_id)
            append_quad(Quad(dimension_node, _jsonstat_predicate_for("category"), category_node))
            append_quad(
                Quad(category_node, rdf_type, NamedNode(JSONSTAT_ONTOLOGY_BASE + "Category"))
            )
            append_quad(Quad(category_node, rdf_type, NamedNode(_SKOS_BASE + "Concept")))
            append_quad(
                Quad(category_node, NamedNode(_SKOS_BASE + "inScheme"), classification_scheme)
            )
            append_quad(
                Quad(category_node, NamedNode(_SKOS_BASE + "notation"), Literal(category_id))
            )
            append_quad(
                Quad(category_node, _jsonstat_predicate_for("categoryCode"), Literal(category_id))
            )
            append_quad(
                Quad(
                    category_node,
                    _jsonstat_predicate_for("position"),
                    Literal(str(category_position), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
                )
            )
            if isinstance(labels, Mapping):
                label = labels.get(category_id)
                if label is not None:
                    normalized_category_label = Literal(str(label))
                    append_quad(
                        Quad(
                            category_node,
                            _jsonstat_predicate_for("label"),
                            normalized_category_label,
                        )
                    )
                    append_quad(
                        Quad(
                            category_node,
                            NamedNode(_SKOS_BASE + "prefLabel"),
                            normalized_category_label,
                        )
                    )
            if classification_enrichment is not None:
                classification_item = classification_enrichment.items.get(category_id)
                if classification_item is not None:
                    original_label = (
                        labels.get(category_id) if isinstance(labels, Mapping) else None
                    )
                    _append_enriched_category_quads(
                        store_id,
                        classification_enrichment,
                        classification_item,
                        original_label,
                        classification_scheme,
                        quad_buffer,
                    )
            if isinstance(units, Mapping):
                unit = units.get(category_id)
                if isinstance(unit, Mapping):
                    for unit_key, unit_value in unit.items():
                        if unit_value is None or isinstance(unit_value, (Mapping, list)):
                            continue
                        append_quad(
                            Quad(
                                category_node,
                                _jsonstat_predicate_for("unit_{}".format(unit_key)),
                                rdf.literal_for(unit_value),
                            )
                        )
                    _append_mapped_unit_quads(
                        category_node,
                        unit,
                        runtime_links,
                        quad_buffer,
                    )


def _append_observation_quads(
    store_id: int,
    position: int,
    record: Mapping[str, Any],
    loaded_at: str,
    dimension_field_names: Mapping[str, str],
    structure_accumulator: rdf.StructureAccumulator,
    quad_buffer: List[Any],
) -> None:
    subject = _observation_subject(store_id, position)
    append_quad = quad_buffer.append
    rdf_type = NamedNode(rdf.RDF_TYPE_IRI)
    append_quad(
        Quad(
            subject,
            rdf_type,
            NamedNode(JSONSTAT_ONTOLOGY_BASE + "Observation"),
        )
    )
    append_quad(Quad(subject, _jsonstat_predicate_for("dataset"), _dataset_subject(store_id)))
    if record.get("value") is not None:
        append_quad(Quad(subject, rdf_type, NamedNode(_QB_BASE + "Observation")))
        append_quad(Quad(subject, NamedNode(_QB_BASE + "dataSet"), _dataset_subject(store_id)))
    append_quad(
        Quad(
            subject,
            _jsonstat_predicate_for("position"),
            Literal(str(position), datatype=NamedNode(rdf.XSD_INTEGER_IRI)),
        )
    )
    append_quad(
        Quad(
            subject,
            _jsonstat_predicate_for("loadedAt"),
            Literal(loaded_at, datatype=NamedNode(rdf.XSD_DATE_TIME_IRI)),
        )
    )

    row_field_kinds: Dict[str, int] = {}
    row_field_value_counts: Dict[str, int] = {}
    for key, value, sparql_type in rdf.iter_typed_scalar_fields(record):
        rdf._track_row_field_kind(row_field_kinds, key, sparql_type)
        rdf._track_row_field_value_count(row_field_value_counts, key)
        append_quad(
            Quad(
                subject,
                _jsonstat_predicate_for(key),
                rdf.object_term_for_typed_value(value, sparql_type),
            )
        )

    for dimension_id, field_name in dimension_field_names.items():
        category_code = record.get(field_name)
        if not isinstance(category_code, str):
            continue
        concept_field_name = "{}_concept".format(field_name)
        rdf._track_row_field_kind(row_field_kinds, concept_field_name, rdf.SPARQL_IRI_TYPE)
        rdf._track_row_field_value_count(row_field_value_counts, concept_field_name)
        append_quad(
            Quad(
                subject,
                _jsonstat_predicate_for(concept_field_name),
                _category_subject(store_id, dimension_id, category_code),
            )
        )
    structure_accumulator.add_row_field_kinds(
        row_field_kinds,
        row_field_value_counts=row_field_value_counts,
    )


def _flush_quads(store_instance: Any, quad_buffer: List[Any]) -> None:
    if not quad_buffer:
        return
    store_instance.bulk_extend(quad_buffer)
    quad_buffer.clear()


def _build_store_meta(
    store_id: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]],
    loaded_at: str,
    structure: StructureSummary,
) -> StoreMeta:
    return StoreMetaModel(
        psid=store_id,
        records=len(records),
        source_url=source_url,
        source_params=source.normalize_source_params(source_params),
        loaded_at=loaded_at,
        structure=structure,
    ).to_dict()


def build_store(
    store_id: int,
    records: Sequence[Mapping[str, Any]],
    payload: Mapping[str, Any],
    source_url: str,
    source_params: Optional[Mapping[str, Any]] = None,
    classification_enrichments: Optional[
        Mapping[str, classification.ClassificationEnrichment]
    ] = None,
    runtime_links: Optional[runtime_links_service.RuntimeLinks] = None,
) -> StoreMeta:
    store_path = _reset_store_directory(store_id)
    store_instance = _require_store_class()(str(store_path))
    try:
        loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        structure_accumulator = rdf.StructureAccumulator()
        quad_buffer: List[Any] = []
        normalized_enrichments = classification_enrichments or {}
        raw_dimension_ids = payload.get("id", [])
        dimension_ids = (
            [dimension_id for dimension_id in raw_dimension_ids if isinstance(dimension_id, str)]
            if isinstance(raw_dimension_ids, list)
            else []
        )
        dimension_field_names = source._dimension_field_names(dimension_ids)
        _append_dataset_metadata_quads(
            store_id,
            payload,
            source_url,
            loaded_at,
            dimension_field_names,
            normalized_enrichments,
            runtime_links,
            quad_buffer,
        )

        for position, record in enumerate(records):
            _append_observation_quads(
                store_id,
                position,
                record,
                loaded_at,
                dimension_field_names,
                structure_accumulator,
                quad_buffer,
            )
            if len(quad_buffer) >= _QUAD_BUFFER_TARGET:
                _flush_quads(store_instance, quad_buffer)

        _flush_quads(store_instance, quad_buffer)
        store_instance.optimize()
        store_instance.flush()
        structure = _jsonstat_structure_summary(
            structure_accumulator.build_summary(row_count=len(records))
        )
        meta = _build_store_meta(
            store_id,
            records,
            source_url,
            source_params,
            loaded_at,
            structure,
        )
        store.meta_path(store_id).write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return meta
    finally:
        store_instance = None
