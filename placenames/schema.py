"""Static and shared RDF schema definitions for local place-name datasets."""

from typing import Final

from petscan.service_types import StructureField, StructureSummary

from .datasets import DatasetSpec

PREDICATE_BASE: Final = "https://sparqlbridge.toolforge.org/ontology/placenames/"
RECORD_CLASS_IRI: Final = PREDICATE_BASE + "PlaceNameRecord"

_SAAMI_FIELD_TYPES: Final[dict[str, str]] = {
    "easting": "xsd:decimal",
    "gmlId": "xsd:string",
    "gslsMapSheet": "xsd:string",
    "language": "xsd:string",
    "languageDominance": "xsd:string",
    "languageOfficiality": "xsd:string",
    "municipality": "xsd:string",
    "northing": "xsd:decimal",
    "place": "iri",
    "placeCreationTime": "xsd:dateTime",
    "placeDeletionTimeNil": "xsd:boolean",
    "placeElevation": "xsd:decimal",
    "placeId": "xsd:string",
    "placeModificationTime": "xsd:dateTime",
    "placeNameCreationTime": "xsd:dateTime",
    "placeNameDeletionTimeNil": "xsd:boolean",
    "placeNameId": "xsd:string",
    "placeNameModificationTime": "xsd:dateTime",
    "placeNameSource": "xsd:string",
    "placeNameStatus": "xsd:string",
    "placeNameVersionId": "xsd:string",
    "placeType": "xsd:string",
    "placeTypeCategory": "xsd:string",
    "placeTypeDescription": "xsd:string",
    "placeTypeGroup": "xsd:string",
    "placeTypeSubgroup": "xsd:string",
    "placeVersionId": "xsd:string",
    "region": "xsd:string",
    "rescueGridSquare": "xsd:string",
    "scaleRelevance": "xsd:integer",
    "sourceCrs": "xsd:string",
    "sourceWKT": "geo:wktLiteral",
    "spelling": "rdf:langString",
    "subregion": "xsd:string",
    "tm35MapSheet": "xsd:string",
    "wgs84Latitude": "xsd:decimal",
    "wgs84Longitude": "xsd:decimal",
    "wgs84WKT": "geo:wktLiteral",
}


def hardcoded_structure(spec: DatasetSpec) -> StructureSummary:
    if spec.slug != "saami":
        raise ValueError(f"No hard-coded place-name schema exists for {spec.slug!r}.")

    fields: list[StructureField] = []
    for source_key, field_type in _SAAMI_FIELD_TYPES.items():
        fields.append(
            {
                "source_key": source_key,
                "predicate": PREDICATE_BASE + source_key,
                "present_in_rows": spec.record_count,
                "primary_type": field_type,
                "observed_types": [field_type],
                "row_side_cardinality": "1",
            }
        )
    return {
        "row_count": spec.record_count,
        "field_count": len(fields),
        "fields": fields,
    }
