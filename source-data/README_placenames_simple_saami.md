# Sámi place names RDF model

The production distribution `placenames_simple_saami_2026-05.nq.gz` uses the
flat model. Its named graph is:

```text
https://sparqlbridge.toolforge.org/placenames/graph/saami/2026-05
```

The uncompressed data contains 490,207 unique quads, 11,956 name records, and
references 10,752 distinct places. Its size is 113.5 MiB (118,972,770 bytes);
the deterministic gzip file stored in version control is 3,345,932 bytes.

Checksums are recorded in `placenames_simple_saami_2026-05.json`. The
application verifies the compressed file against that manifest before it
builds a local Oxigraph index.

## Source and licence

The source is the National Land Survey of Finland (NLS) [Geographic names
dataset](https://www.maanmittauslaitos.fi/en/maps-and-spatial-data/datasets-and-interfaces/product-descriptions/geographic-names),
specifically its **Place names, simple** file product in XML/GML format. This
RDF distribution was derived from the 2026-05 source dataset and contains the
records whose name language is North Sámi, Inari Sámi, or Skolt Sámi.

According to the product description, the geographic names products are based
on the NLS Place Name Register and data checked by the Institute for the
Languages of Finland. The source coordinate reference system is ETRS89 /
TM35FIN (EPSG:3067). The WGS84 coordinates in this distribution were added
during the RDF conversion from the source coordinates.

The source dataset is licensed under [Creative Commons Attribution 4.0
International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
The National Land Survey requires attribution to include the licensor, dataset
name, and delivery time. Use the following attribution for this distribution:

> Contains data from the National Land Survey of Finland Geographic Names
> dataset, 05/2026. Licensed under CC BY 4.0.

## Flat production model

Each source `PlaceNameSimple` feature is one `pn:PlaceNameRecord` resource:

```text
https://tietokortit.maanmittauslaitos.fi/nimisto/paikannimi/{placeNameId}
```

All 32 source fields are available directly from that resource. The nested
`placeLocation` field is represented without an extra RDF node by
`pn:sourceCrs`, `pn:easting`, `pn:northing`, and `pn:sourceWKT`. The existing
WGS84 values are `pn:wgs84Latitude`, `pn:wgs84Longitude`, and
`pn:wgs84WKT`.

Every record also links to the stable MML place URI with `pn:place`:

```text
https://tietokortit.maanmittauslaitos.fi/nimisto/paikka/{placeId}
```

Place properties are repeated for different names of the same place. This is
intentional: common SPARQL Bridge queries need no joins between local place and
place-name resources. Use `DISTINCT ?place` or `DISTINCT ?placeId` when the
result should contain each physical place only once.

Source fields use predicates below
`https://sparqlbridge.toolforge.org/ontology/placenames/`. Their local names
are identical to the XML element names, except for the decomposed
`placeLocation` properties above. Code and identifier values are plain strings,
so leading zeroes are preserved.

The `pn:spelling` and `rdfs:label` values are RDF language-tagged literals.
The original ISO 639-3 value remains in `pn:language`. North Sámi `sme` maps to
BCP 47 tag `se`; Inari Sámi and Skolt Sámi use `smn` and `sms`.
Explicit `xsi:nil="true"` fields are preserved with a boolean predicate whose
name ends in `Nil`.

The WKT literals use GeoSPARQL's `geo:wktLiteral` datatype. EPSG:3067 WKT uses
easting-northing order, and WGS84/CRS84 WKT uses longitude-latitude order.

## Query example

```sparql
PREFIX pn: <https://sparqlbridge.toolforge.org/ontology/placenames/>

SELECT ?record ?place ?name ?municipalityCode ?placeTypeCode ?wkt WHERE {
  GRAPH <https://sparqlbridge.toolforge.org/placenames/graph/saami/2026-05> {
    ?record a pn:PlaceNameRecord ;
            pn:place ?place ;
            pn:spelling ?name ;
            pn:municipality ?municipalityCode ;
            pn:placeType ?placeTypeCode ;
            pn:wgs84WKT ?wkt .
  }
}
```

The main federation keys `pn:municipality`, `pn:placeId`, and `pn:placeType`
correspond to Wikidata properties P1203, P4119, and P9230 respectively.

## Generation

The production file can be built directly from the full 1+ GiB MML
`placenames_simple.xml`. This single streaming command filters the three Sámi
languages, converts EPSG:3067 coordinates to WGS84, and writes flat N-Quads
without an intermediate XML file:

```shell
./.venv/bin/python source-data/build_saami_nquads.py \
  /path/to/placenames_simple.xml \
  source-data/placenames_simple_saami_2026-05.nq.gz --force
```

The older converter remains available when an already extracted and
WGS84-enriched Sámi XML file is the input:

```shell
./.venv/bin/python source-data/convert_saami_to_nquads.py \
  /path/to/placenames_simple_saami.xml \
  source-data/placenames_simple_saami_2026-05.nq --force
```

Validate a generated gzip file and compare it with the committed manifest:

```shell
gzip -t source-data/placenames_simple_saami_2026-05.nq.gz
shasum -a 256 source-data/placenames_simple_saami_2026-05.nq.gz
```

The repository test suite additionally loads the complete asset with
pyoxigraph and verifies its graph, quad count, record count, and place count.

The previous normalized place/place-name/geometry model can still be generated
for comparison or specialized place-centric queries:

```shell
./.venv/bin/python source-data/convert_saami_to_nquads.py \
  placenames_simple_saami.xml placenames_simple_saami_normalized_2026-05.nq \
  --model normalized --force
```
