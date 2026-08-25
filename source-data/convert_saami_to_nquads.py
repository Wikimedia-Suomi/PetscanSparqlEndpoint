#!/usr/bin/env python3
"""Convert the extracted Sámi Place Names GML/XML dataset to N-Quads.

The default flat model keeps every source field on one resource per source
record. A normalized place/place-name model remains available with
``--model normalized``. Both models include EPSG:3067 and WGS84 coordinates.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import TextIO

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
XSD = "http://www.w3.org/2001/XMLSchema#"
DCT = "http://purl.org/dc/terms/"
PROV = "http://www.w3.org/ns/prov#"
VOID = "http://rdfs.org/ns/void#"
GEO = "http://www.opengis.net/ont/geosparql#"
SCHEMA = "https://schema.org/"

PN = "https://sparqlbridge.toolforge.org/ontology/placenames/"
GRAPH = "https://sparqlbridge.toolforge.org/placenames/graph/saami/2026-05"
MML_PLACE = "https://tietokortit.maanmittauslaitos.fi/nimisto/paikka/"
MML_NAME = "https://tietokortit.maanmittauslaitos.fi/nimisto/paikannimi/"

SOURCE_CRS = "http://www.opengis.net/def/crs/EPSG/0/3067"
WGS84_CRS = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
XSI_NIL = "{http://www.w3.org/2001/XMLSchema-instance}nil"

LANGUAGE_TAGS = {
    "sme": "se",  # North Sámi: source uses ISO 639-3, RDF uses BCP 47 se
    "smn": "smn",
    "sms": "sms",
}

NAME_STRING_FIELDS = (
    "placeNameId",
    "placeNameVersionId",
    "language",
    "languageOfficiality",
    "languageDominance",
    "placeNameSource",
    "placeNameStatus",
)
NAME_DATETIME_FIELDS = (
    "placeNameCreationTime",
    "placeNameModificationTime",
    "placeNameDeletionTime",
)
PLACE_STRING_FIELDS = (
    "placeId",
    "placeVersionId",
    "placeType",
    "placeTypeDescription",
    "placeTypeCategory",
    "placeTypeGroup",
    "placeTypeSubgroup",
    "tm35MapSheet",
    "gslsMapSheet",
    "rescueGridSquare",
    "municipality",
    "subregion",
    "region",
)
PLACE_DATETIME_FIELDS = (
    "placeCreationTime",
    "placeModificationTime",
    "placeDeletionTime",
)
PLACE_SIGNATURE_FIELDS = (
    *PLACE_STRING_FIELDS,
    "placeLocation",
    "wgs84Latitude",
    "wgs84Longitude",
    "placeElevation",
    "scaleRelevance",
    *PLACE_DATETIME_FIELDS,
)


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def escape_literal(value: str) -> str:
    """Escape a Unicode string using N-Triples/N-Quads literal rules."""
    result: list[str] = []
    for char in value:
        codepoint = ord(char)
        if char == "\\":
            result.append("\\\\")
        elif char == '"':
            result.append('\\"')
        elif char == "\n":
            result.append("\\n")
        elif char == "\r":
            result.append("\\r")
        elif char == "\t":
            result.append("\\t")
        elif codepoint < 0x20 or codepoint == 0x7F:
            result.append(f"\\u{codepoint:04X}")
        else:
            result.append(char)
    return "".join(result)


def iri(value: str) -> str:
    return f"<{value}>"


def literal(value: str, *, datatype: str | None = None, language: str | None = None) -> str:
    escaped = escape_literal(value)
    if language is not None:
        return f'"{escaped}"@{language}'
    if datatype is not None:
        return f'"{escaped}"^^<{datatype}>'
    return f'"{escaped}"'


class NQuadsWriter:
    def __init__(self, output: TextIO, graph: str) -> None:
        self.output = output
        self.graph_term = iri(graph)
        self.quad_count = 0

    def write(self, subject: str, predicate: str, object_term: str) -> None:
        self.output.write(f"{iri(subject)} {iri(predicate)} {object_term} {self.graph_term} .\n")
        self.quad_count += 1


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def child_map(feature: ET.Element) -> dict[str, ET.Element]:
    return {local_name(child.tag): child for child in feature}


def element_signature(element: ET.Element) -> tuple[object, ...]:
    """Return a namespace-independent signature for consistency checks."""
    return (
        local_name(element.tag),
        (element.text or "").strip(),
        tuple(sorted(element.attrib.items())),
        tuple(element_signature(child) for child in element),
    )


def text_of(fields: dict[str, ET.Element], name: str) -> str:
    element = fields[name]
    if element.attrib.get(XSI_NIL) == "true":
        raise ValueError(f"Field {name} is explicitly nil")
    if element.text is None:
        raise ValueError(f"Field {name} has no value")
    return element.text.strip()


def write_source_field(
    writer: NQuadsWriter,
    subject: str,
    fields: dict[str, ET.Element],
    field_name: str,
    *,
    datatype: str | None = None,
) -> None:
    element = fields[field_name]
    predicate = PN + field_name
    if element.attrib.get(XSI_NIL) == "true":
        writer.write(
            subject,
            PN + field_name + "Nil",
            literal("true", datatype=XSD + "boolean"),
        )
        return
    if element.text is None:
        writer.write(
            subject,
            PN + field_name + "Nil",
            literal("true", datatype=XSD + "boolean"),
        )
        return
    writer.write(subject, predicate, literal(element.text.strip(), datatype=datatype))


def write_metadata(writer: NQuadsWriter, source_path: Path, source_digest: str) -> None:
    graph = GRAPH
    source = "urn:sha256:" + source_digest
    writer.write(graph, RDF + "type", iri(VOID + "Dataset"))
    writer.write(
        graph,
        DCT + "title",
        literal("MML Place Names Simple: Sámi-language records", language="en"),
    )
    writer.write(
        graph,
        DCT + "title",
        literal("MML Place Names Simple: saamenkieliset paikannimet", language="fi"),
    )
    writer.write(graph, DCT + "identifier", literal("placenames-simple-saami-2026-05"))
    writer.write(graph, DCT + "created", literal("2026-08-24", datatype=XSD + "date"))
    writer.write(graph, PN + "sourceDatasetVersion", literal("2026-05"))
    writer.write(graph, PROV + "wasDerivedFrom", iri(source))

    writer.write(source, RDF + "type", iri(PROV + "Entity"))
    writer.write(source, DCT + "identifier", literal(source_path.name))
    writer.write(source, DCT + "format", literal("application/gml+xml"))
    writer.write(source, PN + "sha256", literal(source_digest))


def source_location_values(fields: dict[str, ET.Element], place_id: str) -> tuple[str, str, str]:
    """Read and validate the source CRS point values for one record."""
    location = fields["placeLocation"]
    point = next(iter(location), None)
    if point is None:
        raise ValueError(f"placeLocation is missing a Point for place {place_id}")
    srs_name = point.attrib.get("srsName")
    if srs_name != "EPSG:3067":
        raise ValueError(f"Unexpected CRS {srs_name!r} for place {place_id}")
    position = next(iter(point), None)
    if position is None or position.text is None:
        raise ValueError(f"Point is missing gml:pos for place {place_id}")
    coordinates = position.text.strip().split()
    if len(coordinates) != 2:
        raise ValueError(f"Unexpected position for place {place_id}: {position.text!r}")
    easting, northing = coordinates
    return srs_name, easting, northing


def location_values(
    fields: dict[str, ET.Element],
    place_id: str,
    wgs84_values: tuple[str, str] | None = None,
) -> tuple[str, str, str, str, str]:
    """Read source and WGS84 values, accepting externally transformed WGS84."""
    srs_name, easting, northing = source_location_values(fields, place_id)
    if wgs84_values is None:
        latitude = text_of(fields, "wgs84Latitude")
        longitude = text_of(fields, "wgs84Longitude")
    else:
        latitude, longitude = wgs84_values
    return srs_name, easting, northing, latitude, longitude


def write_flat_feature(
    writer: NQuadsWriter,
    feature: ET.Element,
    wgs84_values: tuple[str, str] | None = None,
) -> str:
    """Write one self-contained PlaceNameRecord resource."""
    fields = child_map(feature)
    place_id = text_of(fields, "placeId")
    place_name_id = text_of(fields, "placeNameId")
    language_code = text_of(fields, "language")
    language_tag = LANGUAGE_TAGS[language_code]
    spelling = text_of(fields, "spelling")
    record = MML_NAME + place_name_id

    writer.write(record, RDF + "type", iri(PN + "PlaceNameRecord"))
    writer.write(record, DCT + "identifier", literal(place_name_id))
    writer.write(record, PN + "place", iri(MML_PLACE + place_id))
    gml_id = feature.attrib.get("{http://www.opengis.net/gml}id")
    if gml_id is not None:
        writer.write(record, PN + "gmlId", literal(gml_id))

    for field_name in NAME_STRING_FIELDS:
        write_source_field(writer, record, fields, field_name)
    writer.write(record, PN + "spelling", literal(spelling, language=language_tag))
    writer.write(record, RDFS + "label", literal(spelling, language=language_tag))
    for field_name in NAME_DATETIME_FIELDS:
        write_source_field(writer, record, fields, field_name, datatype=XSD + "dateTime")

    for field_name in PLACE_STRING_FIELDS:
        write_source_field(writer, record, fields, field_name)
    write_source_field(writer, record, fields, "placeElevation", datatype=XSD + "decimal")
    write_source_field(writer, record, fields, "scaleRelevance", datatype=XSD + "integer")
    for field_name in PLACE_DATETIME_FIELDS:
        write_source_field(writer, record, fields, field_name, datatype=XSD + "dateTime")

    srs_name, easting, northing, latitude, longitude = location_values(
        fields, place_id, wgs84_values
    )
    writer.write(record, PN + "sourceCrs", literal(srs_name))
    writer.write(record, PN + "easting", literal(easting, datatype=XSD + "decimal"))
    writer.write(record, PN + "northing", literal(northing, datatype=XSD + "decimal"))
    source_wkt = f"<{SOURCE_CRS}> POINT({easting} {northing})"
    writer.write(
        record,
        PN + "sourceWKT",
        literal(source_wkt, datatype=GEO + "wktLiteral"),
    )
    writer.write(record, PN + "wgs84Latitude", literal(latitude, datatype=XSD + "decimal"))
    writer.write(record, PN + "wgs84Longitude", literal(longitude, datatype=XSD + "decimal"))
    wgs84_wkt = f"<{WGS84_CRS}> POINT({longitude} {latitude})"
    writer.write(
        record,
        PN + "wgs84WKT",
        literal(wgs84_wkt, datatype=GEO + "wktLiteral"),
    )
    return language_code


def write_normalized_feature(
    writer: NQuadsWriter,
    feature: ET.Element,
    place_signatures: dict[str, tuple[object, ...]],
) -> str:
    fields = child_map(feature)
    place_id = text_of(fields, "placeId")
    place_name_id = text_of(fields, "placeNameId")
    language_code = text_of(fields, "language")
    language_tag = LANGUAGE_TAGS[language_code]
    spelling = text_of(fields, "spelling")

    place = MML_PLACE + place_id
    place_name = MML_NAME + place_name_id
    source_geometry = place + "/geometry/epsg3067"
    wgs84_geometry = place + "/geometry/crs84"

    signature = tuple(
        (field_name, element_signature(fields[field_name])) for field_name in PLACE_SIGNATURE_FIELDS
    )
    previous_signature = place_signatures.get(place_id)
    is_new_place = previous_signature is None
    if is_new_place:
        place_signatures[place_id] = signature
        writer.write(place, RDF + "type", iri(PN + "Place"))
        writer.write(place, RDF + "type", iri(GEO + "Feature"))
        writer.write(place, RDF + "type", iri(SCHEMA + "Place"))
        writer.write(place, DCT + "identifier", literal(place_id))
    elif previous_signature != signature:
        raise ValueError(f"Conflicting source facts for repeated place {place_id}")

    writer.write(place, PN + "hasPlaceName", iri(place_name))
    writer.write(place, RDFS + "label", literal(spelling, language=language_tag))

    writer.write(place_name, RDF + "type", iri(PN + "PlaceName"))
    writer.write(place_name, DCT + "identifier", literal(place_name_id))
    writer.write(place_name, PN + "place", iri(place))
    gml_id = feature.attrib.get("{http://www.opengis.net/gml}id")
    if gml_id is not None:
        writer.write(place_name, PN + "gmlId", literal(gml_id))

    for field_name in NAME_STRING_FIELDS:
        write_source_field(writer, place_name, fields, field_name)
    writer.write(place_name, PN + "spelling", literal(spelling, language=language_tag))
    writer.write(place_name, RDFS + "label", literal(spelling, language=language_tag))
    for field_name in NAME_DATETIME_FIELDS:
        write_source_field(
            writer,
            place_name,
            fields,
            field_name,
            datatype=XSD + "dateTime",
        )

    if not is_new_place:
        return language_code

    for field_name in PLACE_STRING_FIELDS:
        write_source_field(writer, place, fields, field_name)
    write_source_field(writer, place, fields, "placeElevation", datatype=XSD + "decimal")
    write_source_field(writer, place, fields, "scaleRelevance", datatype=XSD + "integer")
    for field_name in PLACE_DATETIME_FIELDS:
        write_source_field(writer, place, fields, field_name, datatype=XSD + "dateTime")

    srs_name, easting, northing, latitude, longitude = location_values(fields, place_id)

    writer.write(place, PN + "placeLocation", iri(source_geometry))
    writer.write(place, PN + "wgs84Location", iri(wgs84_geometry))
    writer.write(place, GEO + "hasGeometry", iri(source_geometry))
    writer.write(place, GEO + "hasGeometry", iri(wgs84_geometry))

    writer.write(source_geometry, RDF + "type", iri(GEO + "Geometry"))
    writer.write(source_geometry, PN + "sourceCrs", literal(srs_name))
    writer.write(source_geometry, PN + "easting", literal(easting, datatype=XSD + "decimal"))
    writer.write(
        source_geometry,
        PN + "northing",
        literal(northing, datatype=XSD + "decimal"),
    )
    source_wkt = f"<{SOURCE_CRS}> POINT({easting} {northing})"
    writer.write(
        source_geometry,
        GEO + "asWKT",
        literal(source_wkt, datatype=GEO + "wktLiteral"),
    )

    writer.write(wgs84_geometry, RDF + "type", iri(GEO + "Geometry"))
    wgs84_wkt = f"<{WGS84_CRS}> POINT({longitude} {latitude})"
    writer.write(
        wgs84_geometry,
        GEO + "asWKT",
        literal(wgs84_wkt, datatype=GEO + "wktLiteral"),
    )
    writer.write(place, PN + "wgs84Latitude", literal(latitude, datatype=XSD + "decimal"))
    writer.write(place, PN + "wgs84Longitude", literal(longitude, datatype=XSD + "decimal"))
    writer.write(place, SCHEMA + "latitude", literal(latitude, datatype=XSD + "decimal"))
    writer.write(place, SCHEMA + "longitude", literal(longitude, datatype=XSD + "decimal"))

    return language_code


def convert(
    source_path: Path,
    output_path: Path,
    *,
    force: bool,
    model: str = "flat",
    report: bool = True,
) -> int:
    if model not in {"flat", "normalized"}:
        raise ValueError(f"Unknown RDF model: {model}")
    output_mode = output_path.stat().st_mode & 0o777 if output_path.exists() else 0o644
    if output_path.exists() and not force:
        raise FileExistsError(f"Output already exists: {output_path}. Use --force to replace it.")

    source_digest = sha256_file(source_path)
    language_counts = {"sme": 0, "smn": 0, "sms": 0}
    place_signatures: dict[str, tuple[object, ...]] = {}
    place_ids: set[str] = set()
    feature_count = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="\n",
            prefix=output_path.name + ".",
            suffix=".tmp",
            dir=output_path.parent,
            delete=False,
        ) as output:
            temporary_path = Path(output.name)
            writer = NQuadsWriter(output, GRAPH)
            write_metadata(writer, source_path, source_digest)

            for _event, element in ET.iterparse(source_path, events=("end",)):
                if local_name(element.tag) != "PlaceNameSimple":
                    continue
                fields = child_map(element)
                place_ids.add(text_of(fields, "placeId"))
                if model == "flat":
                    language = write_flat_feature(writer, element)
                else:
                    language = write_normalized_feature(writer, element, place_signatures)
                language_counts[language] += 1
                feature_count += 1
                element.clear()
                if report and feature_count % 1000 == 0:
                    print(
                        f"Converted {feature_count:,} records / {writer.quad_count:,} quads",
                        file=sys.stderr,
                    )

            output.flush()
            os.fsync(output.fileno())

        temporary_path.chmod(output_mode)
        os.replace(temporary_path, output_path)
        temporary_path = None
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()

    if report:
        print(f"Output: {output_path}")
        print(f"RDF model: {model}")
        print(f"Records: {feature_count}")
        print(f"Distinct places: {len(place_ids)}")
        print(f"Quads written: {writer.quad_count}")
        print(
            "Languages: "
            + ", ".join(f"{language}={count}" for language, count in language_counts.items())
        )
        print(f"Source SHA-256: {source_digest}")
    return writer.quad_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=Path("placenames_simple_saami.xml"),
        help="source GML/XML file",
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=Path,
        default=Path("placenames_simple_saami_2026-05.nq"),
        help="output N-Quads file",
    )
    parser.add_argument(
        "--force", action="store_true", help="replace the output if it already exists"
    )
    parser.add_argument(
        "--model",
        choices=("flat", "normalized"),
        default="flat",
        help="RDF structure to generate (default: flat)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        convert(
            args.source.resolve(),
            args.output.resolve(),
            force=args.force,
            model=args.model,
        )
    except (FileExistsError, KeyError, ValueError, ET.ParseError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
