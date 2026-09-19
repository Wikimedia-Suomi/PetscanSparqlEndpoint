"""Read the small, curated Statistics Finland runtime linking snapshot."""

import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from django.conf import settings

__all__ = [
    "ExternalLink",
    "RuntimeLinks",
    "RuntimeLinksError",
    "StatisticsIdentifier",
    "UnitMapping",
    "clear_runtime_links_cache",
    "load_runtime_links",
]

_CACHE_LOCK = threading.Lock()
_CACHE: Dict[Tuple[str, int, int], "RuntimeLinks"] = {}


class RuntimeLinksError(Exception):
    """The curated runtime snapshot is missing or malformed."""


@dataclass(frozen=True)
class ExternalLink:
    relation: str
    targets: Tuple[str, ...]
    match_method: str
    confidence: Optional[float]


@dataclass(frozen=True)
class StatisticsIdentifier:
    identifier: str
    url: str
    aliases: Tuple[str, ...]
    labels_fi: Tuple[str, ...]
    table_count: int
    external_links: Tuple[ExternalLink, ...]


@dataclass(frozen=True)
class UnitMapping:
    base: str
    mapping_status: str
    model: Mapping[str, Any]


@dataclass(frozen=True)
class RuntimeLinks:
    path: Path
    retrieved_at: str
    statistics: Mapping[str, StatisticsIdentifier]
    units: Mapping[str, UnitMapping]


def _require_http_iri(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    parsed = urlsplit(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeLinksError("Runtime linking snapshot {} is invalid.".format(field_name))
    return text


def _external_links(value: Any) -> Tuple[ExternalLink, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise RuntimeLinksError("Runtime linking snapshot externalLinks must be an array.")
    links = []
    seen = set()
    for raw_link in value:
        if not isinstance(raw_link, Mapping):
            raise RuntimeLinksError("Runtime linking snapshot external link is invalid.")
        relation = _require_http_iri(raw_link.get("relation"), "external link relation")
        raw_targets = raw_link.get("targets")
        if not isinstance(raw_targets, list) or not raw_targets:
            raise RuntimeLinksError("Runtime linking snapshot external link targets are invalid.")
        targets = tuple(
            dict.fromkeys(
                _require_http_iri(target, "external link target") for target in raw_targets
            )
        )
        match_method = str(raw_link.get("matchMethod") or "").strip()
        raw_confidence = raw_link.get("confidence")
        confidence = (
            float(raw_confidence)
            if isinstance(raw_confidence, (int, float)) and not isinstance(raw_confidence, bool)
            else None
        )
        key = (relation, targets, match_method)
        if key in seen:
            continue
        seen.add(key)
        links.append(
            ExternalLink(
                relation=relation,
                targets=targets,
                match_method=match_method,
                confidence=confidence,
            )
        )
    return tuple(links)


def _string_tuple(value: Any, field_name: str) -> Tuple[str, ...]:
    if not isinstance(value, list):
        raise RuntimeLinksError("Runtime linking snapshot {} must be an array.".format(field_name))
    values = tuple(str(item).strip() for item in value if str(item).strip())
    if len(values) != len(value):
        raise RuntimeLinksError("Runtime linking snapshot {} is invalid.".format(field_name))
    return tuple(dict.fromkeys(values))


def _statistics(value: Any) -> Mapping[str, StatisticsIdentifier]:
    if not isinstance(value, list):
        raise RuntimeLinksError("Runtime linking snapshot statistics must be an array.")
    result: Dict[str, StatisticsIdentifier] = {}
    for raw_entry in value:
        if not isinstance(raw_entry, Mapping):
            raise RuntimeLinksError("Runtime linking snapshot statistics entry is invalid.")
        identifier = str(raw_entry.get("id") or "").strip().lower()
        if not identifier or identifier in result:
            raise RuntimeLinksError("Runtime linking snapshot statistics ID is invalid.")
        table_count = raw_entry.get("tableCount")
        if not isinstance(table_count, int) or isinstance(table_count, bool) or table_count < 0:
            raise RuntimeLinksError("Runtime linking snapshot tableCount is invalid.")
        result[identifier] = StatisticsIdentifier(
            identifier=identifier,
            url=_require_http_iri(raw_entry.get("url"), "statistics URL"),
            aliases=_string_tuple(raw_entry.get("aliases"), "statistics aliases"),
            labels_fi=_string_tuple(raw_entry.get("labelsFi"), "statistics labelsFi"),
            table_count=table_count,
            external_links=_external_links(raw_entry.get("externalLinks")),
        )
    return result


def _units(value: Any) -> Mapping[str, UnitMapping]:
    if not isinstance(value, list):
        raise RuntimeLinksError("Runtime linking snapshot units must be an array.")
    result: Dict[str, UnitMapping] = {}
    for raw_entry in value:
        if not isinstance(raw_entry, Mapping):
            raise RuntimeLinksError("Runtime linking snapshot unit entry is invalid.")
        base = raw_entry.get("base")
        model = raw_entry.get("model")
        if not isinstance(base, str) or not base or not isinstance(model, Mapping):
            raise RuntimeLinksError("Runtime linking snapshot unit mapping is invalid.")
        mapping_status = str(model.get("mappingStatus") or "").strip()
        if mapping_status not in {"accepted", "external", "partial"} or base in result:
            raise RuntimeLinksError("Runtime linking snapshot unit mapping status is invalid.")
        result[base] = UnitMapping(
            base=base,
            mapping_status=mapping_status,
            model=dict(model),
        )
    return result


def _load(path: Path) -> RuntimeLinks:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeLinksError("Could not read the runtime linking snapshot.") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeLinksError("Runtime linking snapshot must be an object.")
    metadata = payload.get("metadata")
    if (
        not isinstance(metadata, Mapping)
        or metadata.get("schemaVersion") != "1.0"
        or metadata.get("snapshotType") != "statfi-linking-runtime"
    ):
        raise RuntimeLinksError("Runtime linking snapshot metadata is invalid.")
    concept_policy = metadata.get("conceptIdentifiers")
    if (
        not isinstance(concept_policy, Mapping)
        or concept_policy.get("included") is not False
        or concept_policy.get("wikidataProperty") != "P14864"
    ):
        raise RuntimeLinksError("Runtime linking snapshot concept ID policy is invalid.")
    return RuntimeLinks(
        path=path,
        retrieved_at=str(metadata.get("retrievedAt") or "").strip(),
        statistics=_statistics(payload.get("statistics")),
        units=_units(payload.get("units")),
    )


def load_runtime_links(path_value: Any = None) -> RuntimeLinks:
    configured = (
        path_value
        if path_value is not None
        else getattr(settings, "JSONSTAT_LINKING_SNAPSHOT_PATH", "")
    )
    try:
        path = Path(configured).expanduser().resolve(strict=True)
        file_stat = path.stat()
    except (OSError, TypeError, ValueError) as exc:
        raise RuntimeLinksError("Could not open the runtime linking snapshot.") from exc
    if not path.is_file():
        raise RuntimeLinksError("The runtime linking snapshot is not a file.")
    cache_key = (str(path), file_stat.st_size, file_stat.st_mtime_ns)
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
    if cached is not None:
        return cached
    result = _load(path)
    with _CACHE_LOCK:
        stale_keys = [key for key in _CACHE if key[0] == str(path)]
        for stale_key in stale_keys:
            _CACHE.pop(stale_key, None)
        _CACHE[cache_key] = result
    return result


def clear_runtime_links_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
