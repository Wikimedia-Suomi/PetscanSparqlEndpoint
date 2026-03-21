import gc
import gzip
import hashlib
import json
import unittest
from collections import Counter
from collections.abc import Iterable, Sequence
from collections.abc import Mapping as RuntimeMapping
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple
from unittest.mock import patch
from urllib.parse import urlparse

from django.conf import settings
from django.test import SimpleTestCase

from petscan import service_links as links
from petscan import service_rdf as rdf
from petscan import service_source as source
from petscan import service_store_builder as store_builder

EXAMPLES_DIR = Path(settings.BASE_DIR) / "data" / "examples"
FIXED_LOADED_AT = "2026-03-21T00:00:00Z"
_LEGACY_SCALAR_VALUE_TYPES = (str, int, float, bool)

try:
    from pyoxigraph import DefaultGraph as _DefaultGraph
except ImportError:  # pragma: no cover - dependency check at runtime
    DefaultGraph: Any = None
else:
    DefaultGraph = _DefaultGraph


@dataclass(frozen=True)
class _GraphSignature:
    quad_count: int
    xor_digest_hex: str
    sum_digest_a_hex: str
    sum_digest_b_hex: str
    predicate_counts: Tuple[Tuple[str, int], ...]


def _load_records(file_name: str) -> List[Dict[str, Any]]:
    payload_path = EXAMPLES_DIR / file_name
    if payload_path.suffix == ".gz":
        with gzip.open(payload_path, mode="rt", encoding="utf-8") as payload_file:
            payload = json.load(payload_file)
    else:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    return list(source.extract_records(payload))


def _fake_enrichment_fetch(api_url: str, titles: Sequence[str], **_kwargs: Any) -> Dict[str, Dict[str, Any]]:
    site = urlparse(api_url).netloc.lower()
    resolved: Dict[str, Dict[str, Any]] = {}
    for title in titles:
        payload = _fake_enrichment_payload(site, title)
        if payload is not None:
            resolved[title] = payload
    return resolved


def _fake_enrichment_payload(site: str, title: str) -> Optional[Dict[str, Any]]:
    seed = hashlib.blake2b(
        "{}|{}".format(site, title).encode("utf-8"),
        digest_size=16,
        person=b"gil-parity-seed",
    ).digest()
    selector = seed[0] % 4
    qid = "Q{}".format(1 + (int.from_bytes(seed[1:5], "big") % 90_000_000))
    page_len = 100 + (int.from_bytes(seed[5:9], "big") % 900_000)
    timestamp = "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}".format(
        2020 + (seed[9] % 7),
        1 + (seed[10] % 12),
        1 + (seed[11] % 28),
        seed[12] % 24,
        seed[13] % 60,
        seed[14] % 60,
    )

    if selector == 0:
        return {"wikidata_id": qid, "page_len": None, "rev_timestamp": None}
    if selector == 1:
        return {"wikidata_id": None, "page_len": page_len, "rev_timestamp": timestamp}
    if selector == 2:
        return {"wikidata_id": qid, "page_len": page_len, "rev_timestamp": timestamp}
    return None


def _legacy_iter_scalar_fields(
    record: RuntimeMapping[str, Any],
    gil_links: Optional[Sequence[str]] = None,
) -> Iterable[Tuple[str, Any]]:
    metadata = record.get("metadata")
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
        yield "thumbnail_image", rdf._thumbnail_url(normalized_image_name)
        yield "thumbnail_image_file", normalized_image_name

    coordinates_value = metadata_map.get("coordinates")
    parsed_coordinates = rdf._parse_coordinates(coordinates_value)
    if parsed_coordinates is not None:
        lat, lon = parsed_coordinates
        yield "coordinates", str(coordinates_value).strip()
        yield "coordinate_lat", lat
        yield "coordinate_lon", lon

    for key, value in record.items():
        if value is None:
            continue

        if key == "gil" and isinstance(value, str):
            yield key, value
            resolved_gil_links = list(gil_links) if gil_links is not None else links.iter_gil_link_uris(record)
            yield "gil_link_count", len(resolved_gil_links)
            continue

        if isinstance(value, _LEGACY_SCALAR_VALUE_TYPES):
            yield key, value
            continue
        if isinstance(value, list):
            scalar_values = []
            for item in value:
                if not isinstance(item, _LEGACY_SCALAR_VALUE_TYPES):
                    continue
                text = str(item).strip()
                if text:
                    scalar_values.append(text)
            if scalar_values:
                yield key, "; ".join(scalar_values)


def _legacy_append_scalar_field_quads(
    *,
    subject: Any,
    record: RuntimeMapping[str, Any],
    quad_buffer: List[Any],
    row_field_kinds: MutableMapping[str, int],
    gil_links: Optional[Sequence[str]] = None,
) -> None:
    for key, raw_value in _legacy_iter_scalar_fields(record, gil_links=gil_links):
        value, sparql_type = rdf._normalize_scalar_field_value_and_type(key, raw_value)
        rdf._track_row_field_kind(row_field_kinds, key, sparql_type)
        quad_buffer.append(
            rdf.Quad(
                subject,
                rdf.predicate_for(key),
                rdf.object_term_for_typed_value(value, sparql_type),
                DefaultGraph(),
            )
        )


def _build_current_store_signature(
    psid: int,
    records: Sequence[RuntimeMapping[str, Any]],
    gil_link_enrichment_map: Mapping[str, Mapping[str, Any]],
    resolved_gil_links_by_row: Sequence[Sequence[Tuple[str, Optional[str]]]],
) -> _GraphSignature:
    signature: Optional[_GraphSignature] = None
    with TemporaryDirectory(prefix="graph-parity-current-") as temp_dir:
        store_instance: Any = store_builder.Store(str(Path(temp_dir) / "store"))
        predicates = store_builder._build_store_predicates()
        xsd_integer_type = rdf.NamedNode(rdf.XSD_INTEGER_IRI)
        write_context = store_builder._RecordWriteContext(
            predicates=predicates,
            psid=psid,
            gil_link_enrichment_map=gil_link_enrichment_map,
            xsd_integer_type=xsd_integer_type,
            psid_literal=rdf.Literal(str(psid), datatype=xsd_integer_type),
            loaded_at_literal=rdf.Literal(
                FIXED_LOADED_AT,
                datatype=rdf.NamedNode(rdf.XSD_DATE_TIME_IRI),
            ),
        )
        quad_buffer: List[Any] = []

        for index, row in enumerate(records):
            store_builder._write_record_quads(
                index=index,
                row=row,
                context=write_context,
                resolved_gil_links=resolved_gil_links_by_row[index],
                quad_buffer=quad_buffer,
            )
            if len(quad_buffer) >= store_builder._QUAD_BUFFER_TARGET:
                store_builder._flush_quads(store_instance, quad_buffer)

        store_builder._flush_quads(store_instance, quad_buffer)
        store_instance.flush()
        signature = _graph_signature(store_instance)
        store_instance = None
        gc.collect()
    if signature is None:
        raise AssertionError("Current graph signature was not computed")
    return signature


def _build_legacy_store_signature(
    psid: int,
    records: Sequence[RuntimeMapping[str, Any]],
    gil_link_enrichment_map: Mapping[str, Mapping[str, Any]],
    resolved_gil_links_by_row: Sequence[Sequence[Tuple[str, Optional[str]]]],
) -> _GraphSignature:
    signature: Optional[_GraphSignature] = None
    with TemporaryDirectory(prefix="graph-parity-legacy-") as temp_dir:
        store_instance: Any = store_builder.Store(str(Path(temp_dir) / "store"))
        predicates = store_builder._build_store_predicates()
        quad_buffer: List[Any] = []
        xsd_integer_type = rdf.NamedNode(rdf.XSD_INTEGER_IRI)
        psid_literal = rdf.Literal(str(psid), datatype=xsd_integer_type)
        loaded_at_literal = rdf.Literal(
            FIXED_LOADED_AT,
            datatype=rdf.NamedNode(rdf.XSD_DATE_TIME_IRI),
        )

        for index, row in enumerate(records):
            subject = rdf.item_subject(psid, row, index)
            resolved_gil_links = resolved_gil_links_by_row[index]
            row_field_kinds: Dict[str, int] = {}
            gil_link_uris = [link_uri for link_uri, _qid in resolved_gil_links] if "gil" in row else None
            gil_link_predicate = rdf.predicate_for("gil_link")
            quad_buffer.append(
                rdf.Quad(subject, predicates.rdf_type, predicates.page_class, DefaultGraph())
            )
            quad_buffer.append(
                rdf.Quad(subject, predicates.psid, psid_literal, DefaultGraph())
            )
            quad_buffer.append(
                rdf.Quad(
                    subject,
                    predicates.position,
                    rdf.Literal(str(index), datatype=xsd_integer_type),
                    DefaultGraph(),
                )
            )
            quad_buffer.append(
                rdf.Quad(subject, predicates.loaded_at, loaded_at_literal, DefaultGraph())
            )
            _legacy_append_scalar_field_quads(
                subject=subject,
                record=row,
                quad_buffer=quad_buffer,
                row_field_kinds=row_field_kinds,
                gil_links=gil_link_uris,
            )

            for link_uri, qid in resolved_gil_links:
                link_node = rdf.NamedNode(link_uri)
                for key, value, sparql_type in rdf.iter_typed_gil_link_fields(
                    link_uri,
                    qid,
                    gil_link_enrichment_map=gil_link_enrichment_map,
                ):
                    rdf._track_row_field_kind(row_field_kinds, key, sparql_type)
                    quad_subject = subject if key == "gil_link" else link_node
                    quad_object = (
                        link_node
                        if key == "gil_link"
                        else rdf.object_term_for_typed_value(value, sparql_type)
                    )
                    quad_buffer.append(
                        rdf.Quad(
                            quad_subject,
                            gil_link_predicate if key == "gil_link" else rdf.predicate_for(key),
                            quad_object,
                            DefaultGraph(),
                        )
                    )

            if len(quad_buffer) >= store_builder._QUAD_BUFFER_TARGET:
                store_builder._flush_quads(store_instance, quad_buffer)

        store_builder._flush_quads(store_instance, quad_buffer)
        store_instance.flush()
        signature = _graph_signature(store_instance)
        store_instance = None
        gc.collect()
    if signature is None:
        raise AssertionError("Legacy graph signature was not computed")
    return signature


def _graph_signature(store_instance: Any) -> _GraphSignature:
    xor_digest = 0
    sum_digest_a = 0
    sum_digest_b = 0
    predicate_counts: Counter[str] = Counter()
    quad_count = 0
    digest_modulus = 1 << 128

    for quad in store_instance:
        graph_name = str(quad.graph_name)
        quad_text = "{} {} {} {}".format(quad.subject, quad.predicate, quad.object, graph_name)
        payload = quad_text.encode("utf-8")
        digest_a_bytes = hashlib.blake2b(
            payload,
            digest_size=16,
            person=b"graph-parity-a",
        ).digest()
        digest_b_bytes = hashlib.blake2b(
            payload,
            digest_size=16,
            person=b"graph-parity-b",
        ).digest()
        digest_a = int.from_bytes(digest_a_bytes, "big")
        digest_b = int.from_bytes(digest_b_bytes, "big")
        xor_digest ^= digest_a
        sum_digest_a = (sum_digest_a + digest_a) % digest_modulus
        sum_digest_b = (sum_digest_b + digest_b) % digest_modulus
        predicate_counts[str(quad.predicate)] += 1
        quad_count += 1

    return _GraphSignature(
        quad_count=quad_count,
        xor_digest_hex="{:032x}".format(xor_digest),
        sum_digest_a_hex="{:032x}".format(sum_digest_a),
        sum_digest_b_hex="{:032x}".format(sum_digest_b),
        predicate_counts=tuple(sorted(predicate_counts.items())),
    )


@unittest.skipUnless(
    bool(getattr(settings, "GRAPH_PARITY_REGRESSION_TESTS", False)),
    "Graph parity regression tests are disabled.",
)
class GraphParityRegressionTests(SimpleTestCase):
    def _assert_parity_for_example(self, *, psid: int, file_name: str) -> None:
        if store_builder.Store is None or rdf.Quad is None or DefaultGraph is None:
            self.skipTest("pyoxigraph is not installed")

        records = _load_records(file_name)
        resolved_gil_links_by_row: List[List[Tuple[str, Optional[str]]]] = []
        with patch("petscan.service_links.fetch_wikibase_items_for_site_api", side_effect=_fake_enrichment_fetch):
            gil_link_enrichment_map = links.build_gil_link_enrichment_map(
                records,
                backend=links.LOOKUP_BACKEND_API,
                resolved_links_by_row_out=resolved_gil_links_by_row,
            )

        if any("gil" in row for row in records):
            self.assertTrue(gil_link_enrichment_map)
            self.assertGreater(
                sum(len(row_links) for row_links in resolved_gil_links_by_row),
                0,
            )
            self.assertTrue(
                any(payload.get("page_len") is not None for payload in gil_link_enrichment_map.values())
            )
            self.assertTrue(
                any(payload.get("rev_timestamp") is not None for payload in gil_link_enrichment_map.values())
            )

        current_signature = _build_current_store_signature(
            psid=psid,
            records=records,
            gil_link_enrichment_map=gil_link_enrichment_map,
            resolved_gil_links_by_row=resolved_gil_links_by_row,
        )
        legacy_signature = _build_legacy_store_signature(
            psid=psid,
            records=records,
            gil_link_enrichment_map=gil_link_enrichment_map,
            resolved_gil_links_by_row=resolved_gil_links_by_row,
        )

        self.assertEqual(
            current_signature,
            legacy_signature,
            msg="Graph mismatch for {} with psid={}".format(file_name, psid),
        )

    def test_current_graph_matches_legacy_graph_for_enriched_example(self) -> None:
        self._assert_parity_for_example(
            psid=43641756,
            file_name="petscan-43641756.json.gz",
        )

    def test_current_graph_matches_legacy_graph_for_large_parse_only_example(self) -> None:
        self._assert_parity_for_example(
            psid=43706364,
            file_name="petscan-43706364.json.gz",
        )
