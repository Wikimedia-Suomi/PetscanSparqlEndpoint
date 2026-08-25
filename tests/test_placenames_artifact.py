import gzip
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

from django.conf import settings
from django.test import SimpleTestCase, override_settings
from pyoxigraph import NamedNode, Store

from petscan.service_errors import PetscanServiceError
from placenames import service_store
from placenames.datasets import get_dataset


class PlacenamesArtifactTests(SimpleTestCase):
    def test_committed_saami_asset_matches_manifest_and_rdf_counts(self) -> None:
        spec = get_dataset("saami")

        self.assertTrue(spec.asset_path.is_file())
        self.assertEqual(spec.asset_path.stat().st_size, spec.compressed_bytes)
        self.assertEqual(hashlib.sha256(spec.asset_path.read_bytes()).hexdigest(), spec.sha256)

        uncompressed_digest = hashlib.sha256()
        uncompressed_bytes = 0
        with gzip.open(spec.asset_path, "rb") as input_file:
            while block := input_file.read(1024 * 1024):
                uncompressed_digest.update(block)
                uncompressed_bytes += len(block)
        self.assertEqual(uncompressed_digest.hexdigest(), spec.uncompressed_sha256)
        self.assertEqual(uncompressed_bytes, spec.uncompressed_bytes)

        temporary_directory = self.enterContext(tempfile.TemporaryDirectory())
        with override_settings(
            OXIGRAPH_BASE_DIR=str(Path(temporary_directory).resolve()),
            PLACENAMES_SCHEMA_MODE="hardcoded",
        ):
            meta = service_store.ensure_loaded(spec)
            store = service_store.open_query_store(spec)
        graph_term = str(NamedNode(spec.graph_iri))
        self.assertEqual(len(store), spec.quad_count)
        self.assertEqual(
            self._count(
                store,
                f"""
                PREFIX pn: <https://sparqlbridge.toolforge.org/ontology/placenames/>
                SELECT (COUNT(?record) AS ?count) WHERE {{
                  GRAPH {graph_term} {{
                    ?record a pn:PlaceNameRecord .
                  }}
                }}
                """,
            ),
            spec.record_count,
        )
        self.assertEqual(
            self._count(
                store,
                f"""
                PREFIX pn: <https://sparqlbridge.toolforge.org/ontology/placenames/>
                SELECT (COUNT(DISTINCT ?place) AS ?count) WHERE {{
                  GRAPH {graph_term} {{
                    ?record pn:place ?place .
                  }}
                }}
                """,
            ),
            spec.place_count,
        )

        structure = meta["structure"]
        dynamically_derived_structure = service_store._derive_structure(store, spec)
        expected_presence = {
            field["predicate"]: field["present_in_rows"]
            for field in structure["fields"]
        }
        presence_result: Any = store.query(
            f"""
            PREFIX pn: <https://sparqlbridge.toolforge.org/ontology/placenames/>
            SELECT ?predicate (COUNT(DISTINCT ?record) AS ?present) WHERE {{
              GRAPH {graph_term} {{
                ?record a pn:PlaceNameRecord ; ?predicate ?value .
                FILTER(STRSTARTS(STR(?predicate), STR(pn:)))
              }}
            }}
            GROUP BY ?predicate
            """
        )
        actual_presence = {
            row["predicate"].value: int(row["present"].value)
            for row in presence_result
        }
        self.assertEqual(structure["row_count"], spec.record_count)
        self.assertEqual(structure["field_count"], 38)
        self.assertEqual(structure, dynamically_derived_structure)
        self.assertEqual(actual_presence, expected_presence)
        field_map = {field["source_key"]: field for field in structure["fields"]}
        self.assertEqual(field_map["place"]["primary_type"], "iri")
        self.assertEqual(field_map["spelling"]["primary_type"], "rdf:langString")
        self.assertEqual(field_map["wgs84WKT"]["primary_type"], "geo:wktLiteral")

    @staticmethod
    def _count(store: Store, query: str) -> int:
        result: Any = store.query(query)
        row = next(iter(result))
        return int(row["count"].value)


class PlacenamesManifestTests(SimpleTestCase):
    def test_saami_dataset_is_allowlisted(self) -> None:
        spec = get_dataset(" SAAMI ")

        self.assertEqual(spec.slug, "saami")
        self.assertEqual(spec.version, "2026-05")
        self.assertEqual(spec.asset_path.name, "placenames_simple_saami_2026-05.nq.gz")
        self.assertNotIn("structure", spec.public_metadata())

    def test_unknown_dataset_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown place-name dataset"):
            get_dataset("../../etc/passwd")

    def test_manifest_failures_are_internal_service_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            base_directory = Path(temporary_directory).resolve()
            source_data = base_directory / "source-data"
            source_data.mkdir()
            (source_data / "placenames_simple_saami_2026-05.json").write_text(
                "not valid JSON",
                encoding="utf-8",
            )
            with (
                override_settings(BASE_DIR=base_directory),
                self.assertRaises(PetscanServiceError) as raised,
            ):
                get_dataset("saami")

        self.assertEqual(
            raised.exception.public_message,
            "Local place-name data is unavailable.",
        )

    def test_source_data_directory_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            base_directory = Path(temporary_directory).resolve()
            outside_source_data = base_directory / "outside-source-data"
            outside_source_data.mkdir()
            marker = outside_source_data / "marker"
            marker.write_text("must not be read", encoding="utf-8")
            (base_directory / "source-data").symlink_to(
                outside_source_data,
                target_is_directory=True,
            )

            with (
                override_settings(BASE_DIR=base_directory),
                self.assertRaises(PetscanServiceError) as raised,
            ):
                get_dataset("saami")

        self.assertEqual(
            raised.exception.public_message,
            "Local place-name data is unavailable.",
        )

    def test_manifest_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            base_directory = Path(temporary_directory).resolve()
            source_data = base_directory / "source-data"
            source_data.mkdir()
            outside_manifest = base_directory / "outside-manifest.json"
            outside_manifest.write_text("{}", encoding="utf-8")
            (source_data / "placenames_simple_saami_2026-05.json").symlink_to(
                outside_manifest
            )

            with (
                override_settings(BASE_DIR=base_directory),
                self.assertRaises(PetscanServiceError) as raised,
            ):
                get_dataset("saami")

        self.assertEqual(
            raised.exception.public_message,
            "Local place-name data is unavailable.",
        )

    def test_asset_symlink_is_rejected(self) -> None:
        manifest = json.loads(
            (Path(settings.BASE_DIR) / "source-data" / "placenames_simple_saami_2026-05.json")
            .read_text(encoding="utf-8")
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            base_directory = Path(temporary_directory).resolve()
            source_data = base_directory / "source-data"
            source_data.mkdir()
            (source_data / "placenames_simple_saami_2026-05.json").write_text(
                json.dumps(manifest),
                encoding="utf-8",
            )
            outside_asset = base_directory / "outside-asset.nq.gz"
            outside_asset.write_bytes(b"must not be read")
            (source_data / manifest["asset"]).symlink_to(outside_asset)

            with (
                override_settings(BASE_DIR=base_directory),
                self.assertRaises(PetscanServiceError) as raised,
            ):
                get_dataset("saami")

        self.assertEqual(
            raised.exception.public_message,
            "Local place-name data is unavailable.",
        )
