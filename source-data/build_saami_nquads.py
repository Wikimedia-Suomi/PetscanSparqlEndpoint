#!/usr/bin/env python3
"""Extract Sámi records from full MML GML and build flat WGS84 N-Quads."""

from __future__ import annotations

import argparse
import gzip
import os
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Iterator
from io import TextIOWrapper
from pathlib import Path
from typing import TextIO

import convert_saami_to_nquads as rdf
from pyproj import Transformer

SAAMI_LANGUAGES = frozenset(rdf.LANGUAGE_TAGS)


def iter_place_name_features(source_path: Path) -> Iterator[ET.Element]:
    """Yield features while removing processed wrappers from the XML tree."""
    context = ET.iterparse(source_path, events=("start", "end"))
    first_event, root = next(context)
    if first_event != "start" or rdf.local_name(root.tag) != "FeatureCollection":
        raise ValueError("The XML root is not a GML FeatureCollection")

    for event, element in context:
        if event != "end" or rdf.local_name(element.tag) != "featureMember":
            continue
        children = list(element)
        if len(children) != 1 or rdf.local_name(children[0].tag) != "PlaceNameSimple":
            raise ValueError("A featureMember does not contain one PlaceNameSimple")
        yield children[0]
        element.clear()
        root.remove(element)


def _write_dataset(
    output: TextIO,
    source_path: Path,
    source_digest: str,
    transformer: Transformer,
) -> tuple[int, int, int, Counter[str], int]:
    language_counts: Counter[str] = Counter()
    place_ids: set[str] = set()
    scanned_count = 0
    selected_count = 0
    writer = rdf.NQuadsWriter(output, rdf.GRAPH)
    rdf.write_metadata(writer, source_path, source_digest)

    for feature in iter_place_name_features(source_path):
        scanned_count += 1
        fields = rdf.child_map(feature)
        language = rdf.text_of(fields, "language")
        if language in SAAMI_LANGUAGES:
            place_id = rdf.text_of(fields, "placeId")
            _, easting, northing = rdf.source_location_values(fields, place_id)
            longitude, latitude = transformer.transform(float(easting), float(northing))
            rdf.write_flat_feature(
                writer,
                feature,
                wgs84_values=(f"{latitude:.8f}", f"{longitude:.8f}"),
            )
            language_counts[language] += 1
            place_ids.add(place_id)
            selected_count += 1

        if scanned_count % 100_000 == 0:
            print(
                f"Scanned {scanned_count:,} records; selected "
                f"{selected_count:,}; wrote {writer.quad_count:,} quads",
                file=sys.stderr,
            )

    return (
        scanned_count,
        selected_count,
        len(place_ids),
        language_counts,
        writer.quad_count,
    )


def _write_output(
    temporary_path: Path,
    output_path: Path,
    source_path: Path,
    source_digest: str,
    transformer: Transformer,
) -> tuple[int, int, int, Counter[str], int]:
    if output_path.suffix == ".gz":
        with temporary_path.open("wb") as raw_output:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                compresslevel=9,
                fileobj=raw_output,
                mtime=0,
            ) as compressed_output:
                with TextIOWrapper(compressed_output, encoding="utf-8", newline="\n") as output:
                    result = _write_dataset(output, source_path, source_digest, transformer)
            # Gzip's OS header byte is otherwise platform-dependent. Use the
            # Unix value emitted by `gzip -n`, which matches the committed asset.
            raw_output.seek(9)
            raw_output.write(b"\x03")
            raw_output.flush()
            os.fsync(raw_output.fileno())
        return result

    with temporary_path.open("w", encoding="utf-8", newline="\n") as output:
        result = _write_dataset(output, source_path, source_digest, transformer)
        output.flush()
        os.fsync(output.fileno())
    return result


def build(source_path: Path, output_path: Path, *, force: bool) -> int:
    """Build the production flat model directly from the full source XML."""
    if source_path == output_path:
        raise ValueError("Source and output paths must be different")
    if output_path.exists() and not force:
        raise FileExistsError(f"Output already exists: {output_path}. Use --force to replace it.")
    output_mode = output_path.stat().st_mode & 0o777 if output_path.exists() else 0o644

    started = time.perf_counter()
    source_digest = rdf.sha256_file(source_path)
    transformer = Transformer.from_crs("EPSG:3067", "EPSG:4326", always_xy=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            prefix=output_path.name + ".",
            suffix=".tmp",
            dir=output_path.parent,
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)

        (
            scanned_count,
            selected_count,
            place_count,
            language_counts,
            quad_count,
        ) = _write_output(
            temporary_path,
            output_path,
            source_path,
            source_digest,
            transformer,
        )

        temporary_path.chmod(output_mode)
        os.replace(temporary_path, output_path)
        temporary_path = None
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()

    elapsed = time.perf_counter() - started
    output_digest = rdf.sha256_file(output_path)
    print(f"Output: {output_path}")
    print("RDF model: flat")
    print(f"Source records scanned: {scanned_count}")
    print(f"Sámi records selected: {selected_count}")
    print(f"Distinct places: {place_count}")
    print(f"Quads written: {quad_count}")
    print(
        "Languages: "
        + ", ".join(
            f"{language}={language_counts[language]}" for language in sorted(SAAMI_LANGUAGES)
        )
    )
    print(f"Source SHA-256: {source_digest}")
    print(f"Output SHA-256: {output_digest}")
    print(f"Elapsed: {elapsed:.1f} s")
    return quad_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="full placenames_simple XML/GML")
    parser.add_argument(
        "output",
        type=Path,
        help="output flat N-Quads file (use .nq.gz for deterministic gzip)",
    )
    parser.add_argument(
        "--force", action="store_true", help="replace the output if it already exists"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        build(args.source.resolve(), args.output.resolve(), force=args.force)
    except (FileExistsError, KeyError, OSError, ValueError, ET.ParseError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
