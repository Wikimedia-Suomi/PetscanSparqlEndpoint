"""Allowlisted metadata for versioned static place-name datasets."""

import json
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from django.conf import settings

from petscan.service_errors import PetscanServiceError

_MANIFESTS = {
    "saami": "placenames_simple_saami_2026-05.json",
}
_DATASET_UNAVAILABLE_PUBLIC_MESSAGE = "Local place-name data is unavailable."


def _configuration_error(message: str) -> PetscanServiceError:
    return PetscanServiceError(
        message,
        public_message=_DATASET_UNAVAILABLE_PUBLIC_MESSAGE,
    )


@contextmanager
def _open_directory_without_symlinks(path: Path) -> Iterator[int]:
    if not path.is_absolute() or ".." in path.parts:
        raise _configuration_error(
            "Dataset source directory must be an absolute path without parent traversal."
        )

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    current_fd: int | None = None
    try:
        current_fd = os.open(path.anchor, flags)
        for component in path.parts[1:]:
            next_fd = os.open(component, flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
    except OSError as exc:
        if current_fd is not None:
            try:
                os.close(current_fd)
            except OSError:
                pass
        raise _configuration_error(
            "Dataset source path must contain only real directories."
        ) from exc

    if current_fd is None:  # Defensive invariant; os.open either returns an fd or raises.
        raise _configuration_error("Could not open dataset source directory.")
    try:
        yield current_fd
    finally:
        os.close(current_fd)


def _read_manifest(source_data_dir: Path, manifest_name: str) -> Any:
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        with _open_directory_without_symlinks(source_data_dir) as source_data_fd:
            descriptor = os.open(
                manifest_name,
                flags,
                dir_fd=source_data_fd,
            )
            with os.fdopen(descriptor, "r", encoding="utf-8") as manifest_file:
                return json.load(manifest_file)
    except (OSError, json.JSONDecodeError) as exc:
        raise _configuration_error(
            f"Could not read dataset manifest {manifest_name!r}."
        ) from exc


def _validate_asset_entry(source_data_dir: Path, asset_name: str) -> None:
    try:
        with _open_directory_without_symlinks(source_data_dir) as source_data_fd:
            asset_stat = os.stat(
                asset_name,
                dir_fd=source_data_fd,
                follow_symlinks=False,
            )
    except OSError as exc:
        raise _configuration_error(
            f"Could not inspect dataset asset {asset_name!r}."
        ) from exc
    if not stat.S_ISREG(asset_stat.st_mode):
        raise _configuration_error("Dataset asset must be a regular non-symlink file.")


@dataclass(frozen=True)
class DatasetSpec:
    slug: str
    version: str
    asset_path: Path
    graph_iri: str
    sha256: str
    uncompressed_sha256: str
    compressed_bytes: int
    uncompressed_bytes: int
    quad_count: int
    record_count: int
    place_count: int
    source_url: str
    license: str
    license_url: str
    attribution: str

    def public_metadata(self) -> dict[str, Any]:
        return {
            "dataset": self.slug,
            "version": self.version,
            "graph_iri": self.graph_iri,
            "records": self.record_count,
            "places": self.place_count,
            "quads": self.quad_count,
            "source_url": self.source_url,
            "license": self.license,
            "license_url": self.license_url,
            "attribution": self.attribution,
        }


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _configuration_error(
            f"Dataset manifest field {key!r} must be a non-empty string."
        )
    return value.strip()


def _required_nonnegative_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _configuration_error(
            f"Dataset manifest field {key!r} must be a non-negative integer."
        )
    return value


def get_dataset(dataset: str) -> DatasetSpec:
    slug = str(dataset or "").strip().lower()
    manifest_name = _MANIFESTS.get(slug)
    if manifest_name is None:
        raise ValueError("Unknown place-name dataset. Supported datasets: saami.")

    source_data_dir = Path(settings.BASE_DIR) / "source-data"
    raw_payload = _read_manifest(source_data_dir, manifest_name)
    if not isinstance(raw_payload, dict):
        raise _configuration_error("Dataset manifest root must be a JSON object.")
    payload: dict[str, Any] = raw_payload

    manifest_slug = _required_string(payload, "dataset").lower()
    if manifest_slug != slug:
        raise _configuration_error(
            "Dataset manifest identifier does not match its allowlist entry."
        )

    asset_name = _required_string(payload, "asset")
    if Path(asset_name).name != asset_name:
        raise _configuration_error(
            "Dataset asset must be a filename without directory components."
        )
    _validate_asset_entry(source_data_dir, asset_name)
    asset_path = source_data_dir / asset_name

    sha256 = _required_string(payload, "sha256").lower()
    uncompressed_sha256 = _required_string(payload, "uncompressed_sha256").lower()
    if len(sha256) != 64 or len(uncompressed_sha256) != 64:
        raise _configuration_error(
            "Dataset SHA-256 values must contain 64 hexadecimal characters."
        )
    try:
        int(sha256, 16)
        int(uncompressed_sha256, 16)
    except ValueError as exc:
        raise _configuration_error("Dataset SHA-256 values must be hexadecimal.") from exc

    return DatasetSpec(
        slug=slug,
        version=_required_string(payload, "version"),
        asset_path=asset_path,
        graph_iri=_required_string(payload, "graph_iri"),
        sha256=sha256,
        uncompressed_sha256=uncompressed_sha256,
        compressed_bytes=_required_nonnegative_int(payload, "compressed_bytes"),
        uncompressed_bytes=_required_nonnegative_int(payload, "uncompressed_bytes"),
        quad_count=_required_nonnegative_int(payload, "quad_count"),
        record_count=_required_nonnegative_int(payload, "record_count"),
        place_count=_required_nonnegative_int(payload, "place_count"),
        source_url=_required_string(payload, "source_url"),
        license=_required_string(payload, "license"),
        license_url=_required_string(payload, "license_url"),
        attribution=_required_string(payload, "attribution"),
    )
