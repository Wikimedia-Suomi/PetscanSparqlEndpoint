from typing import Any, Dict, List, Mapping

from petscan import service_links as links
from petscan import service_rdf as rdf
from petscan import service_source as source
from tests.service_test_support import (
    PRIMARY_EXAMPLE_FILE,
    PRIMARY_EXAMPLE_PSID,
    PRIMARY_RECORD_COUNT,
    SECONDARY_EXAMPLE_FILE,
    ServiceTestCase,
)


class ServiceRdfTests(ServiceTestCase):
    @staticmethod
    def _field_map(summary: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
        return {field["source_key"]: field for field in summary["fields"]}

    def test_structure_summary_contains_title_field_and_predicate(self) -> None:
        payload = self._load_payload(PRIMARY_EXAMPLE_FILE)
        records = source.extract_records(payload)

        summary = rdf.summarize_structure(records)
        field_map = self._field_map(summary)

        self.assertEqual(summary["row_count"], PRIMARY_RECORD_COUNT)
        self.assertGreater(summary["field_count"], 0)
        self.assertIn("title", field_map)
        self.assertEqual(
            field_map["title"]["predicate"],
            "https://petscan.wmcloud.org/ontology/title",
        )
        self.assertIn("id", field_map)
        self.assertEqual(
            field_map["id"]["predicate"],
            "https://petscan.wmcloud.org/ontology/page_id",
        )
        self.assertIn("gil_link", field_map)
        self.assertIn("gil_link_count", field_map)

    def test_gil_field_emits_link_count_scalar_field(self) -> None:
        payload = self._load_payload(PRIMARY_EXAMPLE_FILE)
        records = source.extract_records(payload)

        first_row = records[0]
        rdf_fields = list(rdf.iter_scalar_fields(first_row))

        raw_gil_values = [value for key, value in rdf_fields if key == "gil"]
        parsed_counts = [value for key, value in rdf_fields if key == "gil_link_count"]

        self.assertEqual(len(raw_gil_values), 1)
        self.assertEqual(len(parsed_counts), 1)
        self.assertEqual(parsed_counts[0], len(links.iter_gil_link_uris(first_row)))

    def test_iter_scalar_fields_skips_irrelevant_metadata_mapping(self) -> None:
        fields = list(
            rdf.iter_scalar_fields(
                {
                    "id": 1,
                    "title": "Example",
                    "metadata": {
                        "img_height": 2000,
                        "img_width": 1000,
                    },
                }
            )
        )

        self.assertEqual(fields, [("id", 1), ("title", "Example")])

    def test_item_subject_for_commons_file_uses_commons_entity_iri(self) -> None:
        record = {
            "id": 574781,
            "namespace": 6,
            "nstext": "File",
            "img_media_type": "AUDIO",
        }
        subject = rdf.item_subject(PRIMARY_EXAMPLE_PSID, record, 0)

        self.assertEqual(subject.value, "https://commons.wikimedia.org/entity/M574781")

    def test_item_subject_for_explicit_commons_host_file_page_uses_commons_entity_iri(self) -> None:
        record = {
            "pageid": "98765",
            "wiki": "commons.wikimedia.org",
            "namespace": "6",
            "nstext": "file",
            "title": "File:Example image.jpg",
        }
        subject = rdf.item_subject(PRIMARY_EXAMPLE_PSID, record, 0)

        self.assertEqual(subject.value, "https://commons.wikimedia.org/entity/M98765")

    def test_item_subject_for_non_commons_record_uses_local_psid_item_iri(self) -> None:
        record = {
            "id": 42,
            "namespace": 0,
            "nstext": "",
            "title": "Example",
        }
        subject = rdf.item_subject(PRIMARY_EXAMPLE_PSID, record, 0)

        self.assertEqual(subject.value, "https://petscan.wmcloud.org/psid/43641756/item/42")

    def test_item_subject_for_wikidata_id_uses_wikidata_entity_iri(self) -> None:
        record = {
            "id": 574781,
            "namespace": 6,
            "nstext": "File",
            "img_media_type": "AUDIO",
            "wikidata_id": "Q378619",
        }
        subject = rdf.item_subject(PRIMARY_EXAMPLE_PSID, record, 0)

        self.assertEqual(subject.value, "https://commons.wikimedia.org/entity/M574781")

    def test_item_subject_for_non_commons_wikidata_id_uses_wikidata_entity_iri(self) -> None:
        record = {
            "id": 123,
            "namespace": 0,
            "nstext": "",
            "title": "Example city",
            "wikidata_id": "Q378619",
        }
        subject = rdf.item_subject(PRIMARY_EXAMPLE_PSID, record, 0)

        self.assertEqual(subject.value, "http://www.wikidata.org/entity/Q378619")

    def test_item_subject_does_not_use_commons_entity_for_non_file_commons_page(self) -> None:
        record = {
            "id": 321,
            "wiki": "commonswiki",
            "namespace": 0,
            "nstext": "",
            "title": "Commons_main_page",
        }
        subject = rdf.item_subject(PRIMARY_EXAMPLE_PSID, record, 0)

        self.assertEqual(subject.value, "https://petscan.wmcloud.org/psid/43641756/item/321")

    def test_item_subject_does_not_use_commons_entity_for_non_commons_file_page(self) -> None:
        record = {
            "id": 777,
            "wiki": "enwiki",
            "namespace": 6,
            "nstext": "File",
            "img_media_type": "BITMAP",
            "wikidata_id": "Q42",
        }
        subject = rdf.item_subject(PRIMARY_EXAMPLE_PSID, record, 0)

        self.assertEqual(subject.value, "http://www.wikidata.org/entity/Q42")

    def test_thumbnail_url_normalizes_spaces_and_preserves_safe_characters(self) -> None:
        thumbnail_url = rdf._thumbnail_url("Example file(name),v1.jpg")

        self.assertEqual(
            thumbnail_url,
            "https://commons.wikimedia.org/wiki/Special:FilePath/Example_file(name),v1.jpg?width=320",
        )

    def test_parse_coordinates_accepts_semicolon_separator(self) -> None:
        self.assertEqual(
            rdf._parse_coordinates("60.45138889;22.26666667"),
            (60.45138889, 22.26666667),
        )

    def test_parse_coordinates_rejects_out_of_bounds_values(self) -> None:
        self.assertIsNone(rdf._parse_coordinates("95,22.2"))
        self.assertIsNone(rdf._parse_coordinates("60.4,190"))

    def test_normalize_datetime_xsd_supports_compact_timestamp(self) -> None:
        self.assertEqual(
            rdf.normalize_datetime_xsd("20260315100000"),
            "2026-03-15T10:00:00Z",
        )

    def test_normalize_datetime_xsd_converts_offset_to_utc(self) -> None:
        self.assertEqual(
            rdf.normalize_datetime_xsd("2026-03-15T12:00:00+02:00"),
            "2026-03-15T10:00:00Z",
        )

    def test_normalize_datetime_xsd_rejects_invalid_value(self) -> None:
        self.assertIsNone(rdf.normalize_datetime_xsd("not-a-datetime"))

    def test_iter_typed_scalar_fields_normalizes_datetime_semantics_once(self) -> None:
        fields = list(
            rdf.iter_typed_scalar_fields(
                {
                    "title": "Example",
                    "img_timestamp": "20260315123456",
                    "touched": "2026-03-15T12:35:30+02:00",
                }
            )
        )

        self.assertIn(("title", "Example", "xsd:string"), fields)
        self.assertIn(("img_timestamp", "2026-03-15T12:34:56Z", "xsd:dateTime"), fields)
        self.assertIn(("touched", "2026-03-15T10:35:30Z", "xsd:dateTime"), fields)

    def test_append_scalar_field_quads_uses_default_graph(self) -> None:
        if rdf.NamedNode is None:
            self.skipTest("pyoxigraph is not installed")

        quad_buffer: List[Any] = []
        row_field_kinds: Dict[str, int] = {}

        rdf.append_scalar_field_quads(
            subject=rdf.NamedNode("https://example.com/item"),
            record={"title": "Example", "img_timestamp": "20260315123456"},
            quad_buffer=quad_buffer,
            row_field_kinds=row_field_kinds,
        )

        self.assertEqual(len(quad_buffer), 2)
        self.assertTrue(all(str(quad.graph_name) == "DEFAULT" for quad in quad_buffer))

    def test_append_scalar_field_quads_does_not_coerce_stringified_integer_field(self) -> None:
        if rdf.NamedNode is None:
            self.skipTest("pyoxigraph is not installed")

        quad_buffer: List[Any] = []
        row_field_kinds: Dict[str, int] = {}

        rdf.append_scalar_field_quads(
            subject=rdf.NamedNode("https://example.com/item"),
            record={"id": "123"},
            quad_buffer=quad_buffer,
            row_field_kinds=row_field_kinds,
        )

        self.assertEqual(len(quad_buffer), 1)
        self.assertEqual(
            quad_buffer[0].object.datatype.value,
            "http://www.w3.org/2001/XMLSchema#string",
        )

    def test_iter_typed_gil_link_fields_normalizes_enrichment_semantics(self) -> None:
        fields = list(
            rdf.iter_typed_gil_link_fields(
                "https://en.wikipedia.org/wiki/Federalist_No._42",
                "Q5440615",
                gil_link_enrichment_map={
                    "https://en.wikipedia.org/wiki/Federalist_No._42": {
                        "page_len": "12345",
                        "rev_timestamp": "20260315100000",
                    }
                },
            )
        )

        self.assertEqual(fields[0], ("gil_link", "https://en.wikipedia.org/wiki/Federalist_No._42", "iri"))
        self.assertIn(("gil_link_page_len", 12345, "xsd:integer"), fields)
        self.assertIn(("gil_link_rev_timestamp", "2026-03-15T10:00:00Z", "xsd:dateTime"), fields)
        self.assertIn(("gil_link_wikidata_id", "Q5440615", "xsd:string"), fields)
        self.assertIn(
            (
                "gil_link_wikidata_entity",
                "http://www.wikidata.org/entity/Q5440615",
                "iri",
            ),
            fields,
        )

    def test_structure_accumulator_accepts_compact_row_field_kind_values(self) -> None:
        accumulator = rdf.StructureAccumulator()
        accumulator.add_row_field_kinds(
            {
                "title": "xsd:string",
                "mixed": {"xsd:string", "xsd:integer"},
            }
        )

        summary = accumulator.build_summary(row_count=1)
        field_map = self._field_map(summary)

        self.assertEqual(field_map["title"]["observed_types"], ["xsd:string"])
        self.assertEqual(field_map["mixed"]["observed_types"], ["xsd:integer", "xsd:string"])

    def test_second_example_parses_qid_thumbnail_and_coordinates(self) -> None:
        payload = self._load_payload(SECONDARY_EXAMPLE_FILE)
        records = source.extract_records(payload)

        first_row = records[0]
        fields_by_key: Dict[str, List[Any]] = {}
        for key, value in rdf.iter_scalar_fields(first_row):
            fields_by_key.setdefault(key, []).append(value)

        self.assertIn("qid", fields_by_key)
        self.assertIn("Q38511", fields_by_key["qid"])
        self.assertIn("wikidata_entity", fields_by_key)
        self.assertIn(
            "https://www.wikidata.org/entity/Q38511",
            fields_by_key["wikidata_entity"],
        )

        self.assertIn("thumbnail_image", fields_by_key)
        self.assertTrue(
            fields_by_key["thumbnail_image"][0].startswith(
                "https://commons.wikimedia.org/wiki/Special:FilePath/Turku_postcard_2013.png?width="
            )
        )

        self.assertIn("coordinate_lat", fields_by_key)
        self.assertIn("coordinate_lon", fields_by_key)
        self.assertAlmostEqual(fields_by_key["coordinate_lat"][0], 60.45138889)
        self.assertAlmostEqual(fields_by_key["coordinate_lon"][0], 22.26666667)

    def test_second_example_structure_summary_includes_derived_fields(self) -> None:
        payload = self._load_payload(SECONDARY_EXAMPLE_FILE)
        records = source.extract_records(payload)
        summary = rdf.summarize_structure(records)
        field_map = self._field_map(summary)

        self.assertIn("qid", field_map)
        self.assertIn("wikidata_entity", field_map)
        self.assertIn("thumbnail_image", field_map)
        self.assertIn("coordinate_lat", field_map)
        self.assertIn("coordinate_lon", field_map)

    def test_summary_includes_gil_link_relation_fields(self) -> None:
        record = {"gil": "enwiki:0:Federalist_No._42"}
        gil_map = {
            "https://en.wikipedia.org/wiki/Federalist_No._42": {
                "wikidata_id": "Q5440615",
                "page_len": "12345",
                "rev_timestamp": "20260315100000",
            }
        }
        summary = rdf.summarize_structure([record], gil_link_enrichment_map=gil_map)
        field_map = self._field_map(summary)

        self.assertIn("gil_link", field_map)
        self.assertIn("gil_link_wikidata_id", field_map)
        self.assertIn("gil_link_wikidata_entity", field_map)
        self.assertIn("gil_link_page_len", field_map)
        self.assertIn("gil_link_rev_timestamp", field_map)
        self.assertEqual(field_map["gil_link_page_len"]["primary_type"], "xsd:integer")
        self.assertEqual(field_map["gil_link_rev_timestamp"]["primary_type"], "xsd:dateTime")

    def test_gil_wikidata_fields_are_emitted_when_mapping_exists(self) -> None:
        record = {"gil": "enwiki:0:Albert_Einstein|dewiki:0:Berlin"}
        gil_map = {
            "https://en.wikipedia.org/wiki/Albert_Einstein": {
                "wikidata_id": "Q937",
                "page_len": None,
                "rev_timestamp": None,
            }
        }

        enriched_links = links.iter_gil_link_enrichment(record, gil_link_enrichment_map=gil_map)
        self.assertEqual(
            enriched_links,
            [
                ("https://en.wikipedia.org/wiki/Albert_Einstein", "Q937"),
                ("https://de.wikipedia.org/wiki/Berlin", None),
            ],
        )

    def test_iter_scalar_fields_handles_metadata_and_list_edge_cases(self) -> None:
        record = {
            "title": "Example",
            "tags": ["  first  ", "", None, "second", {"skip": True}, 3],
            "empty_values": ["", "   ", None],
            "metadata": "not-a-mapping",
            "other": {"nested": True},
        }

        fields = list(rdf.iter_scalar_fields(record))

        self.assertIn(("title", "Example"), fields)
        self.assertIn(("tags", "first; second; 3"), fields)
        self.assertNotIn(("empty_values", ""), fields)
        self.assertFalse(any(key == "thumbnail_image" for key, _value in fields))
        self.assertFalse(any(key == "coordinates" for key, _value in fields))
        self.assertFalse(any(key == "other" for key, _value in fields))

    def test_iter_scalar_fields_uses_explicit_gil_links_for_link_count(self) -> None:
        record = {"gil": "enwiki:0:Albert_Einstein|dewiki:0:Berlin"}

        fields = list(
            rdf.iter_scalar_fields(
                record,
                gil_links=["https://example.org/one", "https://example.org/two", "https://example.org/three"],
            )
        )

        self.assertIn(("gil", "enwiki:0:Albert_Einstein|dewiki:0:Berlin"), fields)
        self.assertIn(("gil_link_count", 3), fields)

    def test_iter_scalar_fields_emits_thumbnail_and_coordinates_from_metadata(self) -> None:
        record = {
            "metadata": {
                "image": "Turku postcard 2013.png",
                "coordinates": "60.45138889 / 22.26666667",
            }
        }

        fields = list(rdf.iter_scalar_fields(record))
        fields_by_key: Dict[str, List[Any]] = {}
        for key, value in fields:
            fields_by_key.setdefault(key, []).append(value)

        self.assertEqual(
            fields_by_key["thumbnail_image"],
            ["https://commons.wikimedia.org/wiki/Special:FilePath/Turku_postcard_2013.png?width=320"],
        )
        self.assertEqual(fields_by_key["thumbnail_image_file"], ["Turku postcard 2013.png"])
        self.assertEqual(fields_by_key["coordinates"], ["60.45138889 / 22.26666667"])
        self.assertAlmostEqual(fields_by_key["coordinate_lat"][0], 60.45138889)
        self.assertAlmostEqual(fields_by_key["coordinate_lon"][0], 22.26666667)
