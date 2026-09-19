"""Memory-bounded access to the local Statistics Finland classification export."""

import json
import mmap
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Tuple

__all__ = [
    "ClassificationSnapshotError",
    "SnapshotMetadata",
    "SnapshotSelection",
    "clear_snapshot_cache",
    "load_classifications",
]

_WHITESPACE = frozenset(b" \t\r\n")
_INDEX_LOCK = threading.Lock()


class ClassificationSnapshotError(Exception):
    """The configured local classification snapshot cannot be used safely."""


@dataclass(frozen=True)
class SnapshotMetadata:
    language: str
    retrieved_at: str
    classification_count: int
    classification_item_count: int
    classification_item_failure_count: int


@dataclass(frozen=True)
class SnapshotSelection:
    metadata: SnapshotMetadata
    classifications: Dict[str, Mapping[str, Any]]


@dataclass(frozen=True)
class _SnapshotIndex:
    path: Path
    identity: Tuple[int, int, int, int]
    metadata: SnapshotMetadata
    offsets: Dict[str, Tuple[int, int]]


_INDEX_CACHE: Dict[Tuple[str, Tuple[int, int, int, int]], _SnapshotIndex] = {}


def clear_snapshot_cache() -> None:
    with _INDEX_LOCK:
        _INDEX_CACHE.clear()


def _skip_whitespace(data: Any, position: int, limit: int) -> int:
    while position < limit and data[position] in _WHITESPACE:
        position += 1
    return position


def _string_end(data: Any, position: int, limit: int) -> int:
    if position >= limit or data[position] != ord('"'):
        raise ClassificationSnapshotError("Classification snapshot contains invalid JSON.")
    position += 1
    while position < limit:
        byte = data[position]
        if byte == ord('"'):
            return position + 1
        if byte == ord("\\"):
            position += 2
        else:
            position += 1
    raise ClassificationSnapshotError("Classification snapshot contains an unterminated string.")


def _value_end(data: Any, position: int, limit: int) -> int:
    position = _skip_whitespace(data, position, limit)
    if position >= limit:
        raise ClassificationSnapshotError("Classification snapshot contains incomplete JSON.")

    first = data[position]
    if first == ord('"'):
        return _string_end(data, position, limit)
    if first in (ord("{"), ord("[")):
        closing = ord("}") if first == ord("{") else ord("]")
        stack = [closing]
        position += 1
        while position < limit and stack:
            byte = data[position]
            if byte == ord('"'):
                position = _string_end(data, position, limit)
                continue
            if byte == ord("{"):
                stack.append(ord("}"))
            elif byte == ord("["):
                stack.append(ord("]"))
            elif byte in (ord("}"), ord("]")):
                if byte != stack[-1]:
                    raise ClassificationSnapshotError(
                        "Classification snapshot contains mismatched JSON delimiters."
                    )
                stack.pop()
            position += 1
        if stack:
            raise ClassificationSnapshotError("Classification snapshot contains incomplete JSON.")
        return position

    start = position
    while position < limit and data[position] not in b",]} \t\r\n":
        position += 1
    if position == start:
        raise ClassificationSnapshotError("Classification snapshot contains an invalid value.")
    return position


def _decode_json(data: Any, start: int, end: int) -> Any:
    try:
        return json.loads(data[start:end])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClassificationSnapshotError(
            "Classification snapshot contains invalid JSON."
        ) from exc


def _object_fields(
    data: Any,
    object_start: int,
    object_end: int,
) -> Iterator[Tuple[str, int, int]]:
    if data[object_start] != ord("{"):
        raise ClassificationSnapshotError("Classification snapshot expected a JSON object.")
    position = _skip_whitespace(data, object_start + 1, object_end)
    if position < object_end and data[position] == ord("}"):
        return

    while position < object_end:
        key_start = position
        key_end = _string_end(data, key_start, object_end)
        key = _decode_json(data, key_start, key_end)
        if not isinstance(key, str):
            raise ClassificationSnapshotError("Classification snapshot has an invalid object key.")
        position = _skip_whitespace(data, key_end, object_end)
        if position >= object_end or data[position] != ord(":"):
            raise ClassificationSnapshotError("Classification snapshot has an invalid object.")
        value_start = _skip_whitespace(data, position + 1, object_end)
        value_end = _value_end(data, value_start, object_end)
        yield key, value_start, value_end

        position = _skip_whitespace(data, value_end, object_end)
        if position >= object_end:
            raise ClassificationSnapshotError("Classification snapshot has an incomplete object.")
        if data[position] == ord("}"):
            return
        if data[position] != ord(","):
            raise ClassificationSnapshotError("Classification snapshot has an invalid object.")
        position = _skip_whitespace(data, position + 1, object_end)

    raise ClassificationSnapshotError("Classification snapshot has an incomplete object.")


def _array_values(data: Any, array_start: int, limit: int) -> Iterator[Tuple[int, int]]:
    if data[array_start] != ord("["):
        raise ClassificationSnapshotError("Classification snapshot expected a JSON array.")
    position = _skip_whitespace(data, array_start + 1, limit)
    if position < limit and data[position] == ord("]"):
        return

    while position < limit:
        value_start = position
        value_end = _value_end(data, value_start, limit)
        yield value_start, value_end
        position = _skip_whitespace(data, value_end, limit)
        if position >= limit:
            raise ClassificationSnapshotError("Classification snapshot has an incomplete array.")
        if data[position] == ord("]"):
            return
        if data[position] != ord(","):
            raise ClassificationSnapshotError("Classification snapshot has an invalid array.")
        position = _skip_whitespace(data, position + 1, limit)

    raise ClassificationSnapshotError("Classification snapshot has an incomplete array.")


def _top_level_value_start(data: Any, key_to_find: str) -> int:
    limit = len(data)
    root_start = _skip_whitespace(data, 0, limit)
    if root_start >= limit or data[root_start] != ord("{"):
        raise ClassificationSnapshotError("Classification snapshot root must be an object.")
    position = _skip_whitespace(data, root_start + 1, limit)
    while position < limit and data[position] != ord("}"):
        key_start = position
        key_end = _string_end(data, key_start, limit)
        key = _decode_json(data, key_start, key_end)
        position = _skip_whitespace(data, key_end, limit)
        if position >= limit or data[position] != ord(":"):
            raise ClassificationSnapshotError("Classification snapshot has an invalid root object.")
        value_start = _skip_whitespace(data, position + 1, limit)
        if key == key_to_find:
            return value_start
        value_end = _value_end(data, value_start, limit)
        position = _skip_whitespace(data, value_end, limit)
        if position >= limit or data[position] not in (ord(","), ord("}")):
            raise ClassificationSnapshotError("Classification snapshot has an invalid root object.")
        if data[position] == ord("}"):
            break
        position = _skip_whitespace(data, position + 1, limit)
    raise ClassificationSnapshotError(
        "Classification snapshot is missing the '{}' field.".format(key_to_find)
    )


def _classification_local_id(data: Any, start: int, end: int) -> str:
    for key, value_start, value_end in _object_fields(data, start, end):
        if key != "localId":
            continue
        value = _decode_json(data, value_start, value_end)
        if isinstance(value, str) and value:
            return value
        break
    raise ClassificationSnapshotError("Classification snapshot has a classification without localId.")


def _fast_classification_offsets(
    data: Any,
    array_start: int,
    expected_count: int,
) -> Dict[str, Tuple[int, int]] | None:
    """Use the export's stable object prefix while validating every resulting boundary."""

    limit = len(data)
    first_start = _skip_whitespace(data, array_start + 1, limit)
    if expected_count == 0:
        return {} if first_start < limit and data[first_start] == ord("]") else None

    marker = b'{"classificationSerie":'
    if data.find(marker, first_start, limit) != first_start:
        return None
    starts = [first_start]
    search_position = first_start + len(marker)
    for _index in range(expected_count - 1):
        next_start = data.find(marker, search_position, limit)
        if next_start < 0:
            return None
        starts.append(next_start)
        search_position = next_start + len(marker)

    last_end = _value_end(data, starts[-1], limit)
    after_last = _skip_whitespace(data, last_end, limit)
    if after_last >= limit or data[after_last] != ord("]"):
        return None

    offsets: Dict[str, Tuple[int, int]] = {}
    for index, start in enumerate(starts):
        if index + 1 == len(starts):
            end = last_end
        else:
            separator = starts[index + 1] - 1
            while separator > start and data[separator] in _WHITESPACE:
                separator -= 1
            if data[separator] != ord(","):
                return None
            end = separator
            while end > start and data[end - 1] in _WHITESPACE:
                end -= 1
        if end <= start or data[end - 1] != ord("}"):
            return None
        local_id = _classification_local_id(data, start, end)
        if local_id in offsets:
            raise ClassificationSnapshotError(
                "Classification snapshot contains duplicate localId '{}'.".format(local_id)
            )
        offsets[local_id] = (start, end)
    return offsets


def _classification_offsets(
    data: Any,
    array_start: int,
    expected_count: int,
) -> Dict[str, Tuple[int, int]]:
    offsets = _fast_classification_offsets(data, array_start, expected_count)
    if offsets is not None:
        return offsets

    offsets = {}
    for start, end in _array_values(data, array_start, len(data)):
        local_id = _classification_local_id(data, start, end)
        if local_id in offsets:
            raise ClassificationSnapshotError(
                "Classification snapshot contains duplicate localId '{}'.".format(local_id)
            )
        offsets[local_id] = (start, end)
    return offsets


def _positive_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ClassificationSnapshotError(
            "Classification snapshot metadata field '{}' is invalid.".format(field_name)
        )
    return value


def _metadata_from_payload(payload: Any) -> SnapshotMetadata:
    if not isinstance(payload, Mapping):
        raise ClassificationSnapshotError("Classification snapshot metadata must be an object.")
    if payload.get("schemaVersion") != "1.0":
        raise ClassificationSnapshotError("Classification snapshot schema version is unsupported.")
    language = payload.get("language")
    if not isinstance(language, str) or not language.strip():
        raise ClassificationSnapshotError("Classification snapshot language is invalid.")
    retrieved_at = payload.get("retrievedAt")
    if not isinstance(retrieved_at, str):
        retrieved_at = ""
    return SnapshotMetadata(
        language=language.strip().lower(),
        retrieved_at=retrieved_at.strip(),
        classification_count=_positive_int(payload.get("classificationCount"), "classificationCount"),
        classification_item_count=_positive_int(
            payload.get("classificationItemCount"),
            "classificationItemCount",
        ),
        classification_item_failure_count=_positive_int(
            payload.get("classificationItemFailureCount"),
            "classificationItemFailureCount",
        ),
    )


def _identity(file_descriptor: int) -> Tuple[int, int, int, int]:
    file_stat = os.fstat(file_descriptor)
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
    )


def _build_index(path: Path, identity: Tuple[int, int, int, int]) -> _SnapshotIndex:
    try:
        with path.open("rb") as snapshot_file:
            if _identity(snapshot_file.fileno()) != identity:
                raise ClassificationSnapshotError("Classification snapshot changed while indexing.")
            with mmap.mmap(snapshot_file.fileno(), 0, access=mmap.ACCESS_READ) as data:
                metadata_start = _top_level_value_start(data, "metadata")
                metadata_end = _value_end(data, metadata_start, len(data))
                metadata = _metadata_from_payload(
                    _decode_json(data, metadata_start, metadata_end)
                )
                classifications_start = _top_level_value_start(data, "classifications")
                offsets = _classification_offsets(
                    data,
                    classifications_start,
                    metadata.classification_count,
                )
    except (OSError, ValueError) as exc:
        raise ClassificationSnapshotError(
            "Could not open the local classification snapshot: {}".format(exc)
        ) from exc

    if len(offsets) != metadata.classification_count:
        raise ClassificationSnapshotError(
            "Classification snapshot count does not match its metadata."
        )
    return _SnapshotIndex(path=path, identity=identity, metadata=metadata, offsets=offsets)


def _snapshot_index(path_value: Any) -> _SnapshotIndex:
    try:
        path = Path(path_value).expanduser().resolve(strict=True)
        with path.open("rb") as snapshot_file:
            identity = _identity(snapshot_file.fileno())
    except (OSError, TypeError, ValueError) as exc:
        raise ClassificationSnapshotError(
            "Could not open the local classification snapshot: {}".format(exc)
        ) from exc
    if not path.is_file():
        raise ClassificationSnapshotError("The local classification snapshot is not a file.")

    cache_key = (str(path), identity)
    with _INDEX_LOCK:
        cached = _INDEX_CACHE.get(cache_key)
    if cached is not None:
        return cached

    index = _build_index(path, identity)
    with _INDEX_LOCK:
        stale_keys = [key for key in _INDEX_CACHE if key[0] == str(path)]
        for stale_key in stale_keys:
            _INDEX_CACHE.pop(stale_key, None)
        _INDEX_CACHE[cache_key] = index
    return index


def load_classifications(path_value: Any, classification_ids: Any) -> SnapshotSelection:
    """Load only exact requested classifications from the indexed local snapshot."""

    requested_ids = frozenset(
        value for value in classification_ids if isinstance(value, str) and value
    )
    for _attempt in range(2):
        index = _snapshot_index(path_value)
        if not requested_ids:
            return SnapshotSelection(metadata=index.metadata, classifications={})

        try:
            with index.path.open("rb") as snapshot_file:
                if _identity(snapshot_file.fileno()) != index.identity:
                    clear_snapshot_cache()
                    continue
                with mmap.mmap(snapshot_file.fileno(), 0, access=mmap.ACCESS_READ) as data:
                    classifications: Dict[str, Mapping[str, Any]] = {}
                    for classification_id in requested_ids:
                        offset = index.offsets.get(classification_id)
                        if offset is None:
                            continue
                        payload = _decode_json(data, offset[0], offset[1])
                        if (
                            not isinstance(payload, Mapping)
                            or payload.get("localId") != classification_id
                        ):
                            raise ClassificationSnapshotError(
                                "Classification snapshot index does not match its content."
                            )
                        classifications[classification_id] = payload
        except OSError as exc:
            raise ClassificationSnapshotError(
                "Could not read the local classification snapshot: {}".format(exc)
            ) from exc
        return SnapshotSelection(metadata=index.metadata, classifications=classifications)

    raise ClassificationSnapshotError("Classification snapshot changed while it was being read.")
