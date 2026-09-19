"""Best-effort enrichment from local and remote Statistics Finland classifications."""

import json
import logging
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, cast
from urllib.parse import quote, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from django.conf import settings

from petscan.service_source import HTTP_USER_AGENT

from . import service_classification_snapshot as snapshot
from . import service_source as source

__all__ = [
    "ClassificationEnrichment",
    "ClassificationItemEnrichment",
    "classification_api_url",
    "classification_item_api_url",
    "classification_languages",
    "clear_classification_cache",
    "enrich_classifications",
]

logger = logging.getLogger(__name__)

_CLASSIFICATION_API_ROOT = "https://api.stat.fi/classificationservice/open/api/classifications/v2"
_CLASSIFICATION_API_HOST = "api.stat.fi"
_CLASSIFICATION_API_PATH_PREFIX = "/classificationservice/open/api/classifications/v2/"
_DEFAULT_MAX_RESPONSE_BYTES = 20 * 1024 * 1024
_SUPPORTED_LANGUAGES = frozenset({"fi", "sv", "en"})
_NOTE_FIELDS = ("generalNote", "includes", "includesAlso", "excludes")
_CACHE_LOCK = threading.Lock()
_RESPONSE_CACHE: Dict[str, Tuple[float, Any]] = {}


class ClassificationServiceError(Exception):
    """Internal classification API error which must not fail JSON-stat loading."""


@dataclass
class ClassificationItemEnrichment:
    code: str
    api_url: str
    labels: Dict[str, List[str]] = field(default_factory=dict)
    level: Optional[float] = None
    order: Optional[int] = None
    parent_code: Optional[str] = None
    notes: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)
    external_links: List[Tuple[str, str]] = field(default_factory=list)


@dataclass
class ClassificationEnrichment:
    local_id: str
    api_url: str
    labels: Dict[str, List[str]] = field(default_factory=dict)
    descriptions: Dict[str, List[str]] = field(default_factory=dict)
    purposes: Dict[str, List[str]] = field(default_factory=dict)
    detailed_descriptions: Dict[str, List[str]] = field(default_factory=dict)
    international_relationships: Dict[str, List[str]] = field(default_factory=dict)
    series_id: Optional[str] = None
    series_labels: Dict[str, List[str]] = field(default_factory=dict)
    release_date: Optional[str] = None
    termination_date: Optional[str] = None
    modified_at: Optional[str] = None
    international_recommendation: Optional[bool] = None
    national_recommendation: Optional[bool] = None
    items: Dict[str, ClassificationItemEnrichment] = field(default_factory=dict)


def classification_languages() -> Tuple[str, ...]:
    configured = getattr(settings, "JSONSTAT_CLASSIFICATION_LANGUAGES", ("fi",))
    if isinstance(configured, str):
        raw_languages: Sequence[Any] = configured.split(",")
    elif isinstance(configured, Sequence):
        raw_languages = configured
    else:
        raise ValueError("JSONSTAT_CLASSIFICATION_LANGUAGES must be a list of languages.")

    languages: List[str] = []
    for raw_language in raw_languages:
        language = str(raw_language or "").strip().lower()
        if language not in _SUPPORTED_LANGUAGES:
            raise ValueError("JSONSTAT_CLASSIFICATION_LANGUAGES supports only fi, sv, and en.")
        if language not in languages:
            languages.append(language)
    if not languages:
        raise ValueError("JSONSTAT_CLASSIFICATION_LANGUAGES must contain at least one language.")
    return tuple(languages)


def classification_api_url(classification_id: str) -> str:
    return "{}/classifications/{}".format(
        _CLASSIFICATION_API_ROOT,
        quote(classification_id, safe=""),
    )


def classification_item_api_url(classification_id: str, code: str) -> str:
    return "{}/classificationItems/{}".format(
        classification_api_url(classification_id),
        quote(code, safe=""),
    )


def _data_url(path: str, language: str, meta: str = "max") -> str:
    query = urlencode({"content": "data", "meta": meta, "lang": language})
    return "{}?{}".format(path, query)


def _normalize_api_url(value: Any) -> str:
    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
        port = parsed.port
    except ValueError as exc:
        raise ClassificationServiceError("Classification API returned an invalid URL.") from exc

    hostname = str(parsed.hostname or "").strip().rstrip(".").lower()
    if (
        parsed.scheme.lower() != "https"
        or hostname != _CLASSIFICATION_API_HOST
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not parsed.path.startswith(_CLASSIFICATION_API_PATH_PREFIX)
    ):
        raise ClassificationServiceError("Classification API redirected outside its trusted API.")
    return urlunsplit(("https", _CLASSIFICATION_API_HOST, parsed.path, parsed.query, ""))


class _ClassificationRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Optional[Request]:
        normalized_url = _normalize_api_url(urljoin(req.full_url, newurl))
        source._ensure_public_host(normalized_url)
        return super().redirect_request(req, fp, code, msg, headers, normalized_url)


def _read_limited_response(response: Any, max_bytes: int) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except (TypeError, ValueError):
            declared_length = 0
        if declared_length > max_bytes:
            raise ClassificationServiceError("Classification API response is too large.")

    raw = response.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ClassificationServiceError("Classification API response is too large.")
    return cast(bytes, raw)


def _fetch_api_json(url: str) -> Any:
    normalized_url = _normalize_api_url(url)
    source._ensure_public_host(normalized_url)
    request = Request(
        normalized_url,
        headers={"Accept": "application/json", "User-Agent": HTTP_USER_AGENT},
    )
    timeout = int(getattr(settings, "JSONSTAT_CLASSIFICATION_TIMEOUT_SECONDS", 10))
    max_bytes = int(
        getattr(
            settings,
            "JSONSTAT_CLASSIFICATION_MAX_RESPONSE_BYTES",
            _DEFAULT_MAX_RESPONSE_BYTES,
        )
    )
    opener = build_opener(_ClassificationRedirectHandler())
    try:
        with opener.open(request, timeout=timeout) as response:  # nosec B310
            raw = _read_limited_response(response, max_bytes)
            _normalize_api_url(response.geturl())
    except ClassificationServiceError:
        raise
    except Exception as exc:
        raise ClassificationServiceError(
            "Failed to fetch Statistics Finland classification data: {}".format(exc)
        ) from exc

    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ClassificationServiceError(
            "Statistics Finland classification API returned invalid JSON."
        ) from exc


def _cached_api_json(url: str) -> Any:
    cache_seconds = int(getattr(settings, "JSONSTAT_CLASSIFICATION_CACHE_SECONDS", 86_400))
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _RESPONSE_CACHE.get(url)
        if cached is not None and cached[0] > now:
            return cached[1]

    payload = _fetch_api_json(url)
    if cache_seconds > 0:
        with _CACHE_LOCK:
            _RESPONSE_CACHE[url] = (now + cache_seconds, payload)
    return payload


def clear_classification_cache() -> None:
    with _CACHE_LOCK:
        _RESPONSE_CACHE.clear()
    snapshot.clear_snapshot_cache()


def _classification_id_index() -> frozenset[str]:
    url = _data_url("{}/classifications".format(_CLASSIFICATION_API_ROOT), "fi", meta="min")
    payload = _cached_api_json(url)
    if not isinstance(payload, list):
        raise ClassificationServiceError("Classification API returned an invalid index.")
    return frozenset(
        local_id
        for entry in payload
        if isinstance(entry, Mapping)
        for local_id in [entry.get("localId")]
        if isinstance(local_id, str) and local_id
    )


def _append_localized_text(
    target: Dict[str, List[str]],
    language: str,
    value: Any,
) -> None:
    if not isinstance(value, str) or not value.strip():
        return
    text = value.strip()
    values = target.setdefault(language, [])
    if text not in values:
        values.append(text)


def _merge_localized_entries(
    target: Dict[str, List[str]],
    entries: Any,
    value_key: str,
    requested_language: str,
) -> None:
    if not isinstance(entries, list):
        return
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        language = entry.get("lang")
        if language != requested_language:
            continue
        _append_localized_text(target, requested_language, entry.get(value_key))


def _exact_classification_entry(
    payload: Any, classification_id: str
) -> Optional[Mapping[str, Any]]:
    if not isinstance(payload, list):
        raise ClassificationServiceError("Classification API returned invalid classification data.")
    for entry in payload:
        if isinstance(entry, Mapping) and entry.get("localId") == classification_id:
            return entry
    return None


def _merge_scalar_metadata(
    enrichment: ClassificationEnrichment,
    entry: Mapping[str, Any],
) -> None:
    for source_key, target_key in (
        ("releaseDate", "release_date"),
        ("terminationDate", "termination_date"),
        ("modifiedDate", "modified_at"),
    ):
        value = entry.get(source_key)
        if isinstance(value, str) and value.strip() and getattr(enrichment, target_key) is None:
            setattr(enrichment, target_key, value.strip())

    for source_key, target_key in (
        ("internationalRecommendation", "international_recommendation"),
        ("nationalRecommendation", "national_recommendation"),
    ):
        value = entry.get(source_key)
        if isinstance(value, bool) and getattr(enrichment, target_key) is None:
            setattr(enrichment, target_key, value)


def _merge_classification_metadata(
    enrichment: ClassificationEnrichment,
    entry: Mapping[str, Any],
    language: str,
) -> None:
    _merge_scalar_metadata(enrichment, entry)
    _merge_localized_entries(enrichment.labels, entry.get("classificationName"), "name", language)
    _merge_localized_entries(
        enrichment.descriptions,
        entry.get("classificationDescription"),
        "description",
        language,
    )
    _merge_localized_entries(
        enrichment.purposes,
        entry.get("classificationPurpose"),
        "purpose",
        language,
    )
    _merge_localized_entries(
        enrichment.detailed_descriptions,
        entry.get("classificationDetailedDescription"),
        "detailedDescription",
        language,
    )
    _merge_localized_entries(
        enrichment.international_relationships,
        entry.get("classificationRelationshipToInternationalStandards"),
        "relationshipToInternationalStandards",
        language,
    )

    series = entry.get("classificationSerie")
    if not isinstance(series, Mapping):
        return
    series_id = series.get("localId")
    if isinstance(series_id, str) and series_id.strip() and enrichment.series_id is None:
        enrichment.series_id = series_id.strip()
    _merge_localized_entries(
        enrichment.series_labels,
        series.get("classificationSerieName"),
        "name",
        language,
    )


def _values_with_languages(
    raw_value: Any, raw_languages: Any, fallback: str
) -> List[Tuple[str, str]]:
    values = raw_value if isinstance(raw_value, list) else [raw_value]
    languages = raw_languages if isinstance(raw_languages, list) else [raw_languages]
    localized: List[Tuple[str, str]] = []
    for index, value in enumerate(values):
        if not isinstance(value, str) or not value.strip():
            continue
        raw_language = languages[index] if index < len(languages) else fallback
        language = raw_language if isinstance(raw_language, str) else fallback
        if language not in _SUPPORTED_LANGUAGES:
            language = fallback
        localized.append((language, value.strip()))
    return localized


def _merge_item_notes(
    item: ClassificationItemEnrichment,
    raw_notes: Any,
    requested_language: str,
) -> None:
    if not isinstance(raw_notes, list):
        return
    for raw_note in raw_notes:
        if not isinstance(raw_note, Mapping):
            continue
        raw_languages = raw_note.get("lang")
        for note_field in _NOTE_FIELDS:
            note_by_language = item.notes.setdefault(note_field, {})
            for language, text in _values_with_languages(
                raw_note.get(note_field),
                raw_languages,
                requested_language,
            ):
                if language != requested_language:
                    continue
                values = note_by_language.setdefault(language, [])
                if text not in values:
                    values.append(text)


def _merge_item_external_links(
    item: ClassificationItemEnrichment,
    raw_links: Any,
) -> None:
    if not isinstance(raw_links, list):
        return
    for raw_link in raw_links:
        if not isinstance(raw_link, Mapping):
            continue
        relation = raw_link.get("relation")
        targets = raw_link.get("targets")
        if not isinstance(relation, str) or not relation.startswith(("http://", "https://")):
            continue
        if not isinstance(targets, list):
            continue
        for target in targets:
            if not isinstance(target, str) or not target.startswith(("http://", "https://")):
                continue
            pair = (relation, target)
            if pair not in item.external_links:
                item.external_links.append(pair)


def _merge_classification_items(
    enrichment: ClassificationEnrichment,
    payload: Any,
    category_ids: frozenset[str],
    language: str,
) -> None:
    if not isinstance(payload, list):
        raise ClassificationServiceError("Classification API returned invalid item data.")

    for entry in payload:
        if not isinstance(entry, Mapping):
            continue
        code = entry.get("code")
        if not isinstance(code, str) or code not in category_ids:
            continue
        classification = entry.get("classification")
        if isinstance(classification, Mapping):
            item_classification_id = classification.get("localId")
        else:
            item_classification_id = entry.get("classificationId")
        if item_classification_id != enrichment.local_id:
            continue
        if entry.get("localId") != "{}/{}".format(enrichment.local_id, code):
            continue

        item = enrichment.items.get(code)
        if item is None:
            item = ClassificationItemEnrichment(
                code=code,
                api_url=classification_item_api_url(enrichment.local_id, code),
            )
            enrichment.items[code] = item

        level = entry.get("level")
        if item.level is None and isinstance(level, (int, float)) and not isinstance(level, bool):
            item.level = float(level)
        order = entry.get("order")
        if item.order is None and isinstance(order, int) and not isinstance(order, bool):
            item.order = order
        parent_code = entry.get("parentCode")
        if item.parent_code is None and isinstance(parent_code, str) and parent_code.strip():
            item.parent_code = parent_code.strip()

        _merge_localized_entries(
            item.labels,
            entry.get("classificationItemNames"),
            "name",
            language,
        )
        _merge_item_notes(item, entry.get("explanatoryNotes"), language)
        _merge_item_external_links(item, entry.get("externalLinks"))


def _merge_items_for_language(
    enrichment: ClassificationEnrichment,
    category_ids: frozenset[str],
    language: str,
) -> None:
    items_url = _data_url(
        "{}/classificationItems".format(enrichment.api_url),
        language,
    )
    _merge_classification_items(
        enrichment,
        _cached_api_json(items_url),
        category_ids,
        language,
    )


def _enrich_dimension(
    classification_id: str,
    category_ids: frozenset[str],
    languages: Sequence[str],
    enrichment: Optional[ClassificationEnrichment] = None,
) -> Optional[ClassificationEnrichment]:
    if enrichment is None:
        enrichment = ClassificationEnrichment(
            local_id=classification_id,
            api_url=classification_api_url(classification_id),
        )
        metadata_matched = False
    else:
        metadata_matched = True

    for language in languages:
        metadata_url = _data_url(enrichment.api_url, language)
        try:
            entry = _exact_classification_entry(
                _cached_api_json(metadata_url),
                classification_id,
            )
        except ClassificationServiceError as exc:
            logger.warning(
                "Classification metadata enrichment failed for %s (%s): %s",
                classification_id,
                language,
                exc,
            )
            continue
        if entry is None:
            logger.warning(
                "Classification API did not return the exact localId %s for language %s.",
                classification_id,
                language,
            )
            continue

        metadata_matched = True
        _merge_classification_metadata(enrichment, entry, language)
        try:
            _merge_items_for_language(
                enrichment,
                category_ids,
                language,
            )
        except ClassificationServiceError as exc:
            logger.warning(
                "Classification item enrichment failed for %s (%s): %s",
                classification_id,
                language,
                exc,
            )

    return enrichment if metadata_matched else None


def _is_statistics_finland_source(source_url: str) -> bool:
    hostname = str(urlsplit(source_url).hostname or "").strip().rstrip(".").lower()
    return hostname == "stat.fi" or hostname.endswith(".stat.fi")


def _dimension_category_ids(
    payload: Mapping[str, Any],
) -> Dict[str, frozenset[str]]:
    raw_dimension_ids = payload.get("id")
    raw_sizes = payload.get("size")
    raw_dimensions = payload.get("dimension")
    if (
        not isinstance(raw_dimension_ids, list)
        or not isinstance(raw_sizes, list)
        or not isinstance(raw_dimensions, Mapping)
    ):
        return {}

    dimensions: Dict[str, frozenset[str]] = {}
    for dimension_id, raw_size in zip(raw_dimension_ids, raw_sizes, strict=False):
        if (
            not isinstance(dimension_id, str)
            or not isinstance(raw_size, int)
            or isinstance(raw_size, bool)
        ):
            continue
        dimension = raw_dimensions.get(dimension_id)
        if not isinstance(dimension, Mapping):
            continue
        try:
            dimensions[dimension_id] = frozenset(
                source._ordered_category_ids(dimension_id, dimension, raw_size)
            )
        except Exception as exc:
            logger.warning(
                "Classification enrichment could not read dimension %s: %s",
                dimension_id,
                exc,
            )
    return dimensions


def _local_classification_enrichments(
    dimensions: Mapping[str, frozenset[str]],
    languages: Sequence[str],
) -> Tuple[Dict[str, ClassificationEnrichment], Optional[str]]:
    configured_path = getattr(settings, "JSONSTAT_CLASSIFICATION_SNAPSHOT_PATH", "")
    if not str(configured_path or "").strip():
        return {}, None

    selection = snapshot.load_classifications(configured_path, dimensions)
    snapshot_language = selection.metadata.language
    if snapshot_language not in languages:
        return {}, snapshot_language
    if selection.metadata.classification_item_failure_count:
        logger.warning(
            "Local classification snapshot contains %s failed item downloads.",
            selection.metadata.classification_item_failure_count,
        )

    enrichments: Dict[str, ClassificationEnrichment] = {}
    for classification_id, entry in selection.classifications.items():
        enrichment = ClassificationEnrichment(
            local_id=classification_id,
            api_url=classification_api_url(classification_id),
        )
        try:
            _merge_classification_metadata(enrichment, entry, snapshot_language)
            _merge_classification_items(
                enrichment,
                entry.get("items"),
                dimensions[classification_id],
                snapshot_language,
            )
        except ClassificationServiceError as exc:
            logger.warning(
                "Local classification enrichment failed for %s: %s",
                classification_id,
                exc,
            )
            continue
        enrichments[classification_id] = enrichment
    return enrichments, snapshot_language


def enrich_classifications(
    payload: Mapping[str, Any],
    source_url: str,
) -> Dict[str, ClassificationEnrichment]:
    """Return exact local matches, with an explicitly enabled network fallback."""

    if not bool(getattr(settings, "JSONSTAT_CLASSIFICATION_ENRICHMENT_ENABLED", True)):
        return {}
    if not _is_statistics_finland_source(source_url):
        return {}

    try:
        languages = classification_languages()
    except ValueError as exc:
        logger.warning("JSON-stat classification enrichment was skipped: %s", exc)
        return {}

    dimensions = _dimension_category_ids(payload)
    if not dimensions:
        return {}

    try:
        enrichments, snapshot_language = _local_classification_enrichments(
            dimensions,
            languages,
        )
    except snapshot.ClassificationSnapshotError as exc:
        logger.warning("Local JSON-stat classification enrichment was skipped: %s", exc)
        enrichments = {}
        snapshot_language = None

    if not bool(
        getattr(settings, "JSONSTAT_CLASSIFICATION_NETWORK_FALLBACK_ENABLED", False)
    ):
        return enrichments

    try:
        classification_ids = _classification_id_index()
    except ClassificationServiceError as exc:
        logger.warning("Network classification enrichment was skipped: %s", exc)
        return enrichments

    for dimension_id, category_ids in dimensions.items():
        if dimension_id not in classification_ids:
            continue
        network_languages = tuple(
            language
            for language in languages
            if dimension_id not in enrichments or language != snapshot_language
        )
        if not network_languages:
            continue
        try:
            enrichment = _enrich_dimension(
                dimension_id,
                category_ids,
                network_languages,
                enrichment=enrichments.get(dimension_id),
            )
        except Exception as exc:
            logger.warning(
                "Network classification enrichment failed for exact dimension %s: %s",
                dimension_id,
                exc,
            )
            continue
        if enrichment is not None:
            enrichments[dimension_id] = enrichment
    return enrichments
