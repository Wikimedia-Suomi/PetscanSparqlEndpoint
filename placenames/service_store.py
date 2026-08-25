"""Build and cache Oxigraph stores from versioned local N-Quads assets."""

import fcntl
import gzip
import hashlib
import json
import logging
import os
import shutil
import stat
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from django.conf import settings

from petscan.service_errors import PetscanServiceError
from petscan.service_types import StructureField, StructureSummary

from . import schema
from .datasets import DatasetSpec

try:
    from pyoxigraph import Literal, NamedNode, RdfFormat, Store
except ImportError:  # pragma: no cover - dependency check at runtime
    Literal = None  # type: ignore[misc,assignment]
    NamedNode = None  # type: ignore[misc,assignment]
    RdfFormat = None  # type: ignore[misc,assignment]
    Store = None  # type: ignore[misc,assignment]

_BUILD_LOCK = threading.Lock()
_LOGGER = logging.getLogger(__name__)
_STORE_UNAVAILABLE_PUBLIC_MESSAGE = "Local place-name data is unavailable."
_STRUCTURE_SCHEMA_VERSION = 2
_SCHEMA_MODES = frozenset({"hardcoded", "dynamic"})
_STORE_DIRECTORY_NAMES = {
    "saami": "saami",
}
_COMPACT_TYPE_PREFIXES = (
    ("http://www.w3.org/2001/XMLSchema#", "xsd:"),
    ("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:"),
    ("http://www.opengis.net/ont/geosparql#", "geo:"),
)


def _ensure_oxigraph() -> None:
    if Store is None or RdfFormat is None or NamedNode is None or Literal is None:
        raise PetscanServiceError(
            "pyoxigraph is not installed. Install dependencies from requirements.txt first.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )


def _schema_mode() -> str:
    value = getattr(settings, "PLACENAMES_SCHEMA_MODE", "hardcoded")
    if not isinstance(value, str) or value not in _SCHEMA_MODES:
        raise PetscanServiceError(
            "PLACENAMES_SCHEMA_MODE must be either 'hardcoded' or 'dynamic'.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    return value


def _ensure_directory_tree_without_symlinks(path: Path) -> Path:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    current_fd: int | None = None
    current_path = Path(path.anchor)
    try:
        current_fd = os.open(path.anchor, flags)
        for component in path.parts[1:]:
            try:
                next_fd = os.open(component, flags, dir_fd=current_fd)
            except FileNotFoundError:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=current_fd)
                except FileExistsError:
                    pass
                next_fd = os.open(component, flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
            current_path /= component
    except OSError as exc:
        raise PetscanServiceError(
            f"Oxigraph cache path must contain only real directories: {path}.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if current_fd is not None:
            os.close(current_fd)
    return current_path


def _store_root() -> Path:
    configured_root = Path(settings.OXIGRAPH_BASE_DIR)
    if not configured_root.is_absolute() or ".." in configured_root.parts:
        raise PetscanServiceError(
            "OXIGRAPH_BASE_DIR must be an absolute path without parent traversal.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    return _ensure_directory_tree_without_symlinks(
        configured_root / "_static" / "placenames"
    )


def _store_directory_name(spec: DatasetSpec) -> str:
    directory_name = _STORE_DIRECTORY_NAMES.get(spec.slug)
    if directory_name is None:
        raise PetscanServiceError(
            f"No place-name store directory is configured for {spec.slug!r}.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    return directory_name


def store_path(spec: DatasetSpec) -> Path:
    return _store_root() / _store_directory_name(spec)


def _temporary_store_path(spec: DatasetSpec) -> Path:
    return _store_root() / f".{_store_directory_name(spec)}.import"


def meta_path(spec: DatasetSpec) -> Path:
    return store_path(spec) / "meta.json"


def read_meta(spec: DatasetSpec) -> dict[str, Any]:
    directory = store_path(spec)
    if directory.is_symlink():
        raise PetscanServiceError(
            "Place-name cache directory must not be a symlink.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    if not directory.is_dir():
        return {}
    path = directory / "meta.json"
    if path.is_symlink():
        raise PetscanServiceError(
            "Place-name cache metadata must not be a symlink.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _structure_is_usable(value: Any, spec: DatasetSpec) -> bool:
    if not isinstance(value, Mapping):
        return False
    fields = value.get("fields")
    if (
        value.get("row_count") != spec.record_count
        or not isinstance(fields, list)
        or value.get("field_count") != len(fields)
    ):
        return False
    for field in fields:
        if not isinstance(field, Mapping):
            return False
        observed_types = field.get("observed_types")
        if (
            not isinstance(field.get("source_key"), str)
            or not isinstance(field.get("predicate"), str)
            or not isinstance(field.get("present_in_rows"), int)
            or not isinstance(field.get("primary_type"), str)
            or not isinstance(observed_types, list)
            or not observed_types
            or field.get("row_side_cardinality") not in {"1", "M"}
        ):
            return False
    return True


def _store_meta_is_usable(meta: Mapping[str, Any], spec: DatasetSpec) -> bool:
    directory = store_path(spec)
    return (
        meta.get("dataset") == spec.slug
        and meta.get("version") == spec.version
        and meta.get("asset_sha256") == spec.sha256
        and meta.get("graph_iri") == spec.graph_iri
        and meta.get("quads") == spec.quad_count
        and isinstance(meta.get("loaded_at"), str)
        and not directory.is_symlink()
        and directory.is_dir()
    )


def _meta_is_usable(meta: Mapping[str, Any], spec: DatasetSpec) -> bool:
    return (
        _store_meta_is_usable(meta, spec)
        and meta.get("structure_schema_version") == _STRUCTURE_SCHEMA_VERSION
        and meta.get("structure_mode") == _schema_mode()
        and _structure_is_usable(meta.get("structure"), spec)
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        for block in iter(lambda: input_file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_asset(spec: DatasetSpec) -> None:
    try:
        stat = spec.asset_path.stat()
    except OSError as exc:
        raise PetscanServiceError(
            f"Place-name dataset asset is missing: {spec.asset_path.name}.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        ) from exc
    if not spec.asset_path.is_file() or stat.st_size != spec.compressed_bytes:
        raise PetscanServiceError(
            f"Place-name dataset asset size does not match its manifest: {spec.asset_path.name}.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    if _sha256_file(spec.asset_path) != spec.sha256:
        raise PetscanServiceError(
            f"Place-name dataset checksum does not match its manifest: {spec.asset_path.name}.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )


@contextmanager
def _dataset_lock(spec: DatasetSpec) -> Iterator[None]:
    lock_path = _store_root() / f".{_store_directory_name(spec)}.lock"
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
    with _BUILD_LOCK:
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise PetscanServiceError(
                "Place-name dataset lock must be a regular non-symlink file.",
                public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
            ) from exc
        with os.fdopen(descriptor, "r+b") as lock_file:
            lock_stat = os.fstat(lock_file.fileno())
            if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_nlink != 1:
                raise PetscanServiceError(
                    "Place-name dataset lock must be an unlinked regular file.",
                    public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
                )
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _compact_rdf_type(raw_type: str) -> str:
    for base, prefix in _COMPACT_TYPE_PREFIXES:
        if raw_type.startswith(base):
            return prefix + raw_type[len(base) :]
    return raw_type


def _result_int(row: Any, key: str) -> int:
    value = row[key]
    if value is None:
        raise ValueError(f"Oxigraph structure query did not bind {key!r}.")
    return int(value.value)


def _derive_structure(store_instance: Any, spec: DatasetSpec) -> StructureSummary:
    graph_term = str(NamedNode(spec.graph_iri))
    record_class_term = str(NamedNode(schema.RECORD_CLASS_IRI))
    predicate_base_term = str(Literal(schema.PREDICATE_BASE))

    record_count_result: Any = store_instance.query(
        f"""
        SELECT (COUNT(DISTINCT ?record) AS ?count) WHERE {{
          GRAPH {graph_term} {{
            ?record a {record_class_term} .
          }}
        }}
        """
    )
    record_count_row = next(iter(record_count_result))
    row_count = _result_int(record_count_row, "count")
    if row_count != spec.record_count:
        raise PetscanServiceError(
            f"Place-name dataset contains {row_count} records in {spec.graph_iri}; "
            f"expected {spec.record_count}.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )

    presence_by_predicate: dict[str, tuple[int, int]] = {}
    presence_result: Any = store_instance.query(
        f"""
        SELECT ?predicate
               (COUNT(DISTINCT ?record) AS ?present)
               (COUNT(?value) AS ?value_count)
        WHERE {{
          GRAPH {graph_term} {{
            ?record a {record_class_term} ; ?predicate ?value .
            FILTER(STRSTARTS(STR(?predicate), {predicate_base_term}))
          }}
        }}
        GROUP BY ?predicate
        """
    )
    for row in presence_result:
        predicate = str(row["predicate"].value)
        presence_by_predicate[predicate] = (
            _result_int(row, "present"),
            _result_int(row, "value_count"),
        )

    type_counts_by_predicate: dict[str, dict[str, int]] = {}
    type_result: Any = store_instance.query(
        f"""
        SELECT ?predicate ?term_type (COUNT(DISTINCT ?record) AS ?type_count)
        WHERE {{
          GRAPH {graph_term} {{
            ?record a {record_class_term} ; ?predicate ?value .
            FILTER(STRSTARTS(STR(?predicate), {predicate_base_term}))
            BIND(
              IF(
                isIRI(?value),
                "iri",
                IF(
                  isBlank(?value),
                  "bnode",
                  IF(isLiteral(?value), STR(DATATYPE(?value)), "rdf:term")
                )
              ) AS ?term_type
            )
          }}
        }}
        GROUP BY ?predicate ?term_type
        """
    )
    for row in type_result:
        predicate = str(row["predicate"].value)
        compact_type = _compact_rdf_type(str(row["term_type"].value))
        type_counts_by_predicate.setdefault(predicate, {})[compact_type] = _result_int(
            row, "type_count"
        )

    fields: list[StructureField] = []
    for predicate in sorted(presence_by_predicate):
        type_counts = type_counts_by_predicate.get(predicate, {})
        if not type_counts:
            raise ValueError(f"Oxigraph did not report RDF types for predicate {predicate!r}.")
        present_in_rows, value_count = presence_by_predicate[predicate]
        observed_types = sorted(type_counts)
        primary_type = max(
            observed_types,
            key=lambda field_type: (type_counts[field_type], field_type),
        )
        fields.append(
            {
                "source_key": predicate.removeprefix(schema.PREDICATE_BASE),
                "predicate": predicate,
                "present_in_rows": present_in_rows,
                "primary_type": primary_type,
                "observed_types": observed_types,
                "row_side_cardinality": "M" if value_count > present_in_rows else "1",
            }
        )

    return {
        "row_count": row_count,
        "field_count": len(fields),
        "fields": fields,
    }


def _selected_structure(
    store_instance: Any, spec: DatasetSpec, schema_mode: str
) -> StructureSummary:
    if schema_mode == "hardcoded":
        return schema.hardcoded_structure(spec)
    return _derive_structure(store_instance, spec)


def _build_meta(
    spec: DatasetSpec, structure: StructureSummary, schema_mode: str
) -> dict[str, Any]:
    meta = spec.public_metadata()
    meta.update(
        {
            "asset_sha256": spec.sha256,
            "loaded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "structure_schema_version": _STRUCTURE_SCHEMA_VERSION,
            "structure_mode": schema_mode,
            "structure": structure,
        }
    )
    return meta


def _with_current_public_metadata(
    meta: Mapping[str, Any], spec: DatasetSpec
) -> dict[str, Any]:
    current = dict(meta)
    current.update(spec.public_metadata())
    return current


def _validate_owned_cache_path(path: Path, expected_name: str) -> None:
    root = _store_root()
    if path.parent != root or path.name != expected_name:
        raise PetscanServiceError(
            "Refusing to remove a path outside the place-name cache.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )


def _remove_fixed_store(spec: DatasetSpec) -> None:
    path = store_path(spec)
    _validate_owned_cache_path(path, _store_directory_name(spec))
    if path.is_symlink():
        raise PetscanServiceError(
            "Refusing to remove a symlinked place-name cache path.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    if not path.is_dir():
        raise PetscanServiceError(
            "Refusing to remove a non-directory place-name cache path.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    shutil.rmtree(path, ignore_errors=False)


def _remove_temporary_store(path: Path, spec: DatasetSpec) -> None:
    expected_name = f".{_store_directory_name(spec)}.import"
    _validate_owned_cache_path(path, expected_name)
    if path.is_symlink():
        raise PetscanServiceError(
            "Refusing to remove a symlinked place-name temporary path.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    if not path.is_dir():
        raise PetscanServiceError(
            "Refusing to remove a non-directory place-name temporary path.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    shutil.rmtree(path, ignore_errors=False)


def _persist_meta_atomically(spec: DatasetSpec, meta: Mapping[str, Any]) -> None:
    directory = store_path(spec)
    _validate_owned_cache_path(directory, _store_directory_name(spec))
    if directory.is_symlink() or not directory.is_dir():
        raise PetscanServiceError(
            "Refusing to write metadata through an invalid place-name cache path.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    destination = directory / "meta.json"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".meta.", dir=destination.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(meta, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, destination)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _refresh_structure_meta(
    spec: DatasetSpec, current_meta: Mapping[str, Any]
) -> dict[str, Any]:
    schema_mode = _schema_mode()
    store_instance: Any = None
    try:
        if schema_mode == "dynamic":
            store_instance = open_query_store(spec)
        structure = _selected_structure(store_instance, spec, schema_mode)
        refreshed_meta = _with_current_public_metadata(current_meta, spec)
        refreshed_meta.update(
            {
                "structure_schema_version": _STRUCTURE_SCHEMA_VERSION,
                "structure_mode": schema_mode,
                "structure": structure,
            }
        )
        _persist_meta_atomically(spec, refreshed_meta)
        return refreshed_meta
    except PetscanServiceError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise PetscanServiceError(
            f"Failed to update place-name schema metadata: {exc}",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        ) from exc
    finally:
        store_instance = None


def _build_store(spec: DatasetSpec) -> dict[str, Any]:
    _ensure_oxigraph()
    _validate_asset(spec)
    final_path = store_path(spec)
    temporary_path = _temporary_store_path(spec)
    if temporary_path.exists() or temporary_path.is_symlink():
        _remove_temporary_store(temporary_path, spec)
    temporary_path.mkdir(mode=0o700)
    store_instance: Any = None
    try:
        schema_mode = _schema_mode()
        store_instance = Store(str(temporary_path))
        with gzip.open(spec.asset_path, "rb") as input_file:
            store_instance.bulk_load(input=input_file, format=RdfFormat.N_QUADS)
        actual_quads = len(store_instance)
        if actual_quads != spec.quad_count:
            raise PetscanServiceError(
                f"Place-name dataset contains {actual_quads} quads; expected {spec.quad_count}.",
                public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
            )
        store_instance.optimize()
        structure = _selected_structure(store_instance, spec, schema_mode)
        store_instance.flush()
        store_instance = None

        meta = _build_meta(spec, structure, schema_mode)
        (temporary_path / "meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if final_path.exists() or final_path.is_symlink():
            _remove_fixed_store(spec)
        os.replace(temporary_path, final_path)
        return meta
    except PetscanServiceError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise PetscanServiceError(
            f"Failed to build place-name data store: {exc}",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        ) from exc
    finally:
        store_instance = None
        if temporary_path.exists() or temporary_path.is_symlink():
            try:
                _remove_temporary_store(temporary_path, spec)
            except (OSError, PetscanServiceError) as cleanup_error:
                _LOGGER.warning(
                    "Failed to clean up place-name import directory: %s",
                    cleanup_error,
                )


def ensure_loaded(spec: DatasetSpec) -> dict[str, Any]:
    _ensure_oxigraph()
    current_meta = read_meta(spec)
    if _meta_is_usable(current_meta, spec):
        return _with_current_public_metadata(current_meta, spec)

    with _dataset_lock(spec):
        current_meta = read_meta(spec)
        if _meta_is_usable(current_meta, spec):
            return _with_current_public_metadata(current_meta, spec)
        if _store_meta_is_usable(current_meta, spec):
            return _refresh_structure_meta(spec, current_meta)
        return _build_store(spec)


def open_query_store(spec: DatasetSpec) -> Any:
    _ensure_oxigraph()
    directory = store_path(spec)
    if directory.is_symlink() or not directory.is_dir():
        raise PetscanServiceError(
            "Refusing to open an invalid place-name cache path.",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        )
    path = str(directory)
    try:
        return Store.read_only(path)
    except AttributeError:
        return Store(path)
    except OSError as exc:
        raise PetscanServiceError(
            f"Failed to open place-name data store: {exc}",
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        ) from exc


def usable_meta_for_tests(meta: Mapping[str, Any], spec: DatasetSpec) -> bool:
    """Expose strict cache validation to focused tests without duplicating it."""
    return _meta_is_usable(meta, spec)
