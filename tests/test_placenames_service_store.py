import gzip
import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings
from django.test.utils import TestContextDecorator as _TestContextDecorator

from petscan.service_errors import PetscanServiceError
from placenames import service, service_store
from placenames.datasets import DatasetSpec

_GRAPH = "https://example.invalid/graph/saami/test"
_NQUADS = (
    "<https://example.invalid/name/1> "
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
    "<https://sparqlbridge.toolforge.org/ontology/placenames/PlaceNameRecord> "
    "<https://example.invalid/graph/saami/test> .\n"
    "<https://example.invalid/name/1> "
    "<https://sparqlbridge.toolforge.org/ontology/placenames/spelling> "
    '"Anár"@smn <https://example.invalid/graph/saami/test> .\n'
    "<https://example.invalid/name/1> "
    "<https://sparqlbridge.toolforge.org/ontology/placenames/place> "
    "<https://example.invalid/place/1> <https://example.invalid/graph/saami/test> .\n"
    "<https://example.invalid/name/1> "
    "<https://sparqlbridge.toolforge.org/ontology/placenames/alias> "
    '"Anar" <https://example.invalid/graph/saami/test> .\n'
    "<https://example.invalid/name/1> "
    "<https://sparqlbridge.toolforge.org/ontology/placenames/alias> "
    '"1"^^<http://www.w3.org/2001/XMLSchema#integer> '
    "<https://example.invalid/graph/saami/test> .\n"
    "<https://example.invalid/name/2> "
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
    "<https://sparqlbridge.toolforge.org/ontology/placenames/PlaceNameRecord> "
    "<https://example.invalid/graph/saami/test> .\n"
    "<https://example.invalid/name/2> "
    "<https://sparqlbridge.toolforge.org/ontology/placenames/alias> "
    '"Aanaar" <https://example.invalid/graph/saami/test> .\n'
)


class PlacenamesStoreTests(SimpleTestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name).resolve()
        self.asset_path = self.root / "saami.nq.gz"
        with self.asset_path.open("wb") as raw_output:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_output, mtime=0) as output:
                output.write(_NQUADS.encode("utf-8"))
        compressed = self.asset_path.read_bytes()
        self.spec = DatasetSpec(
            slug="saami",
            version="test",
            asset_path=self.asset_path,
            graph_iri=_GRAPH,
            sha256=hashlib.sha256(compressed).hexdigest(),
            uncompressed_sha256=hashlib.sha256(_NQUADS.encode()).hexdigest(),
            compressed_bytes=len(compressed),
            uncompressed_bytes=len(_NQUADS.encode()),
            quad_count=7,
            record_count=2,
            place_count=1,
            source_url="https://example.invalid/source",
            license="CC BY 4.0",
            license_url="https://creativecommons.org/licenses/by/4.0/",
            attribution="Example attribution.",
        )

    def _settings(self, schema_mode: str = "hardcoded") -> _TestContextDecorator:
        return override_settings(
            OXIGRAPH_BASE_DIR=str(self.root / "oxigraph"),
            PLACENAMES_SCHEMA_MODE=schema_mode,
        )

    def test_builds_once_and_reuses_fixed_store(self) -> None:
        with self._settings(), patch("placenames.service_store._derive_structure") as derive:
            first_meta = service_store.ensure_loaded(self.spec)
            store_directory = service_store.store_path(self.spec)
            self.assertTrue(store_directory.is_dir())
            self.assertEqual(first_meta["asset_sha256"], self.spec.sha256)
            self.assertEqual(first_meta["structure_schema_version"], 2)
            self.assertEqual(first_meta["structure_mode"], "hardcoded")
            self.assertEqual(first_meta["structure"]["field_count"], 38)
            self.assertEqual(json.loads((store_directory / "meta.json").read_text()), first_meta)
            derive.assert_not_called()

            with patch("placenames.service_store._build_store") as build_mock:
                second_meta = service_store.ensure_loaded(self.spec)

        self.assertEqual(second_meta, first_meta)
        build_mock.assert_not_called()

    def test_import_derives_structure_from_loaded_rdf(self) -> None:
        with self._settings("dynamic"):
            meta = service_store.ensure_loaded(self.spec)

        self.assertEqual(meta["structure_mode"], "dynamic")
        structure = meta["structure"]
        self.assertEqual(structure["row_count"], 2)
        self.assertEqual(structure["field_count"], 3)
        fields = {field["source_key"]: field for field in structure["fields"]}
        self.assertEqual(fields["place"]["primary_type"], "iri")
        self.assertEqual(fields["place"]["present_in_rows"], 1)
        self.assertEqual(fields["spelling"]["primary_type"], "rdf:langString")
        self.assertEqual(fields["spelling"]["present_in_rows"], 1)
        self.assertEqual(fields["alias"]["primary_type"], "xsd:string")
        self.assertEqual(fields["alias"]["observed_types"], ["xsd:integer", "xsd:string"])
        self.assertEqual(fields["alias"]["present_in_rows"], 2)
        self.assertEqual(fields["alias"]["row_side_cardinality"], "M")

    def test_reused_store_without_schema_version_refreshes_metadata(self) -> None:
        with self._settings():
            first_meta = service_store.ensure_loaded(self.spec)
            legacy_meta = dict(first_meta)
            legacy_meta.pop("structure_schema_version")
            service_store.meta_path(self.spec).write_text(
                json.dumps(legacy_meta),
                encoding="utf-8",
            )

            with patch(
                "placenames.service_store._build_store",
                wraps=service_store._build_store,
            ) as build_mock:
                current_meta = service_store.ensure_loaded(self.spec)

        self.assertEqual(current_meta["structure_schema_version"], 2)
        self.assertEqual(current_meta["structure"], first_meta["structure"])
        build_mock.assert_not_called()

    def test_schema_mode_switch_reuses_store_and_refreshes_only_metadata(self) -> None:
        with self._settings("hardcoded"):
            hardcoded_meta = service_store.ensure_loaded(self.spec)

        with (
            self._settings("dynamic"),
            patch("placenames.service_store._build_store") as build_store,
            patch(
                "placenames.service_store._derive_structure",
                wraps=service_store._derive_structure,
            ) as derive_structure,
        ):
            dynamic_meta = service_store.ensure_loaded(self.spec)

        self.assertEqual(hardcoded_meta["structure_mode"], "hardcoded")
        self.assertEqual(dynamic_meta["structure_mode"], "dynamic")
        self.assertEqual(dynamic_meta["structure"]["field_count"], 3)
        build_store.assert_not_called()
        derive_structure.assert_called_once()

        with (
            self._settings("hardcoded"),
            patch("placenames.service_store._build_store") as build_store,
            patch("placenames.service_store._derive_structure") as derive_structure,
        ):
            hardcoded_again_meta = service_store.ensure_loaded(self.spec)

        self.assertEqual(hardcoded_again_meta["structure_mode"], "hardcoded")
        self.assertEqual(hardcoded_again_meta["structure"]["field_count"], 38)
        build_store.assert_not_called()
        derive_structure.assert_not_called()

    def test_store_path_uses_fixed_allowlisted_directory_name(self) -> None:
        traversal_spec = DatasetSpec(
            **{
                **self.spec.__dict__,
                "version": "x/../../../outside",
                "sha256": "1" * 64,
            }
        )

        with self._settings():
            current_path = service_store.store_path(self.spec)
            traversal_path = service_store.store_path(traversal_spec)

        self.assertEqual(current_path.name, "saami")
        self.assertEqual(traversal_path, current_path)
        self.assertEqual(current_path.parent.name, "placenames")

    def test_configured_store_root_symlink_is_rejected(self) -> None:
        configured_target = self.root / "configured-target"
        configured_target.mkdir()
        configured_link = self.root / "configured-link"
        configured_link.symlink_to(configured_target, target_is_directory=True)

        with (
            override_settings(OXIGRAPH_BASE_DIR=str(configured_link)),
            self.assertRaises(PetscanServiceError),
        ):
            service_store.store_path(self.spec)

    def test_store_root_ancestor_symlink_is_rejected(self) -> None:
        ancestor_target = self.root / "ancestor-target"
        ancestor_target.mkdir()
        ancestor_link = self.root / "ancestor-link"
        ancestor_link.symlink_to(ancestor_target, target_is_directory=True)
        configured_root = ancestor_link / "oxigraph"

        with (
            override_settings(OXIGRAPH_BASE_DIR=str(configured_root)),
            self.assertRaises(PetscanServiceError),
        ):
            service_store.store_path(self.spec)

        self.assertFalse((ancestor_target / "oxigraph").exists())

    def test_static_cache_directory_symlink_is_rejected(self) -> None:
        configured_root = self.root / "static-link-oxigraph"
        configured_root.mkdir()
        outside_target = self.root / "static-link-target"
        outside_target.mkdir()
        (configured_root / "_static").symlink_to(
            outside_target,
            target_is_directory=True,
        )

        with (
            override_settings(OXIGRAPH_BASE_DIR=str(configured_root)),
            self.assertRaises(PetscanServiceError),
        ):
            service_store.store_path(self.spec)

        self.assertEqual(list(outside_target.iterdir()), [])

    def test_placenames_cache_directory_symlink_is_rejected(self) -> None:
        configured_root = self.root / "placenames-link-oxigraph"
        static_root = configured_root / "_static"
        static_root.mkdir(parents=True)
        outside_target = self.root / "placenames-link-target"
        outside_target.mkdir()
        (static_root / "placenames").symlink_to(
            outside_target,
            target_is_directory=True,
        )

        with (
            override_settings(OXIGRAPH_BASE_DIR=str(configured_root)),
            self.assertRaises(PetscanServiceError),
        ):
            service_store.store_path(self.spec)

        self.assertEqual(list(outside_target.iterdir()), [])

    def test_relative_store_root_is_rejected(self) -> None:
        with (
            override_settings(OXIGRAPH_BASE_DIR="relative/oxigraph"),
            self.assertRaises(PetscanServiceError),
        ):
            service_store.store_path(self.spec)

    def test_successful_import_replaces_only_fixed_store(self) -> None:
        with self._settings():
            fixed_store = service_store.store_path(self.spec)
            fixed_store.mkdir(parents=True)
            old_data = fixed_store / "old-data"
            old_data.write_text("old", encoding="utf-8")
            root = fixed_store.parent
            unrelated_store = root / "saami-manual-backup"
            unrelated_store.mkdir()

            service_store.ensure_loaded(self.spec)
            current_store = service_store.store_path(self.spec)

        self.assertFalse(old_data.exists())
        self.assertTrue(unrelated_store.exists())
        self.assertTrue(current_store.exists())

    def test_failed_import_keeps_previous_store(self) -> None:
        with self._settings():
            fixed_store = service_store.store_path(self.spec)
            fixed_store.mkdir(parents=True)
            old_data = fixed_store / "old-data"
            old_data.write_text("old", encoding="utf-8")
            invalid_new_spec = DatasetSpec(
                **{
                    **self.spec.__dict__,
                    "version": "newer",
                    "sha256": "0" * 64,
                }
            )

            with self.assertRaisesRegex(PetscanServiceError, "checksum"):
                service_store.ensure_loaded(invalid_new_spec)

        self.assertTrue(old_data.exists())

    def test_fixed_store_symlink_is_rejected_without_touching_its_target(self) -> None:
        with self._settings():
            fixed_store = service_store.store_path(self.spec)
            unrelated_target = self.root / "must-survive"
            unrelated_target.mkdir()
            marker = unrelated_target / "marker"
            marker.write_text("keep", encoding="utf-8")
            (unrelated_target / "meta.json").write_text(
                '{"outside": "must-not-be-read"}',
                encoding="utf-8",
            )
            fixed_store.symlink_to(unrelated_target, target_is_directory=True)

            with self.assertRaises(PetscanServiceError):
                service_store.read_meta(self.spec)
            with self.assertRaises(PetscanServiceError):
                service_store.open_query_store(self.spec)
            with self.assertRaises(PetscanServiceError):
                service_store.ensure_loaded(self.spec)

        self.assertTrue(fixed_store.is_symlink())
        self.assertEqual(marker.read_text(encoding="utf-8"), "keep")

    def test_temporary_store_symlink_is_rejected_without_touching_its_target(self) -> None:
        with self._settings():
            fixed_store = service_store.store_path(self.spec)
            temporary_store = fixed_store.parent / ".saami.import"
            unrelated_target = self.root / "temporary-target-must-survive"
            unrelated_target.mkdir()
            marker = unrelated_target / "marker"
            marker.write_text("keep", encoding="utf-8")
            temporary_store.symlink_to(unrelated_target, target_is_directory=True)

            with self.assertRaises(PetscanServiceError):
                service_store.ensure_loaded(self.spec)

        self.assertTrue(temporary_store.is_symlink())
        self.assertEqual(marker.read_text(encoding="utf-8"), "keep")

    def test_metadata_symlink_is_rejected_without_reading_its_target(self) -> None:
        with self._settings():
            fixed_store = service_store.store_path(self.spec)
            fixed_store.mkdir()
            outside_metadata = self.root / "metadata-target-must-not-be-read"
            outside_metadata.write_text('{"secret": true}', encoding="utf-8")
            service_store.meta_path(self.spec).symlink_to(outside_metadata)

            with self.assertRaises(PetscanServiceError):
                service_store.read_meta(self.spec)
            with self.assertRaises(PetscanServiceError):
                service_store.ensure_loaded(self.spec)

        self.assertEqual(outside_metadata.read_text(encoding="utf-8"), '{"secret": true}')

    def test_lock_symlink_is_rejected_without_creating_its_target(self) -> None:
        with self._settings():
            store_directory = service_store.store_path(self.spec)
            lock_path = store_directory.parent / ".saami.lock"
            outside_target = self.root / "lock-target-must-not-be-created"
            lock_path.symlink_to(outside_target)

            with self.assertRaises(PetscanServiceError):
                service_store.ensure_loaded(self.spec)

        self.assertTrue(lock_path.is_symlink())
        self.assertFalse(outside_target.exists())

    def test_unknown_dataset_has_no_store_directory(self) -> None:
        unknown_spec = DatasetSpec(**{**self.spec.__dict__, "slug": "unknown"})

        with self._settings(), self.assertRaises(PetscanServiceError):
            service_store.store_path(unknown_spec)

    def test_invalid_schema_mode_is_rejected(self) -> None:
        with self._settings("invalid"), self.assertRaises(PetscanServiceError) as raised:
            service_store.ensure_loaded(self.spec)

        self.assertEqual(raised.exception.public_message, "Local place-name data is unavailable.")

    def test_checksum_mismatch_is_rejected_before_loading(self) -> None:
        bad_spec = DatasetSpec(**{**self.spec.__dict__, "sha256": "0" * 64})

        with self._settings():
            with self.assertRaisesRegex(PetscanServiceError, "checksum"):
                service_store.ensure_loaded(bad_spec)

    def test_service_queries_named_dataset_as_the_default_graph(self) -> None:
        query = """
        PREFIX pn: <https://sparqlbridge.toolforge.org/ontology/placenames/>
        SELECT ?name WHERE { ?record pn:spelling ?name . }
        """
        with self._settings(), patch("placenames.service.get_dataset", return_value=self.spec):
            execution = service.execute_query("saami", query)

        bindings = execution["sparql_json"]["results"]["bindings"]
        self.assertEqual(bindings[0]["name"]["value"], "Anár")
        self.assertEqual(bindings[0]["name"]["xml:lang"], "smn")

    def test_service_sanitizes_internal_query_failures(self) -> None:
        with (
            patch("placenames.service.get_dataset", return_value=self.spec),
            patch("placenames.service_store.ensure_loaded", return_value={}),
            patch("placenames.service_store.open_query_store") as open_store,
            self.assertRaises(PetscanServiceError) as raised,
        ):
            open_store.return_value.query.side_effect = OSError("/secret/store/path")
            service.execute_query("saami", "ASK { ?s ?p ?o }")

        self.assertEqual(raised.exception.public_message, "Local place-name query failed.")
        self.assertIn("/secret/store/path", str(raised.exception))

    def test_service_does_not_expose_syntax_error_details(self) -> None:
        with (
            patch("placenames.service.get_dataset", return_value=self.spec),
            patch("placenames.service_store.ensure_loaded", return_value={}),
            patch("placenames.service_store.open_query_store") as open_store,
            self.assertRaisesRegex(ValueError, r"^SPARQL query is invalid\.$") as raised,
        ):
            open_store.return_value.query.side_effect = SyntaxError(
                "syntax error while reading /secret/store/path"
            )
            service.execute_query("saami", "ASK { ?s ?p ?o }")

        self.assertNotIn("/secret/store/path", str(raised.exception))

    def test_service_does_not_treat_backend_error_text_as_a_client_error(self) -> None:
        with (
            patch("placenames.service.get_dataset", return_value=self.spec),
            patch("placenames.service_store.ensure_loaded", return_value={}),
            patch("placenames.service_store.open_query_store") as open_store,
            self.assertRaises(PetscanServiceError) as raised,
        ):
            open_store.return_value.query.side_effect = OSError(
                "syntax error while reading /secret/store/path"
            )
            service.execute_query("saami", "ASK { ?s ?p ?o }")

        self.assertEqual(raised.exception.public_message, "Local place-name query failed.")
