"""Service workflow for JSON-stat ingestion and SPARQL execution."""

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, cast
from urllib.parse import urlsplit

from django.conf import settings

from petscan import service_sparql as sparql
from petscan import service_store as store
from petscan.service_errors import PetscanServiceError
from petscan.service_types import QueryExecution, QueryExecutionModel, StoreMeta

from . import service_classification as classification
from . import service_runtime_links as runtime_links_service
from . import service_source as source
from . import service_store_builder as store_builder

__all__ = [
    "PetscanServiceError",
    "ensure_loaded",
    "execute_query",
    "internal_store_id",
    "meta_has_matching_source_params",
]

try:
    from pyoxigraph import Store
except ImportError:  # pragma: no cover - dependency check at runtime
    Store = None  # type: ignore[misc,assignment]

_MAX_STORE_META_AGE = timedelta(minutes=30)
_JSONSTAT_STORE_ID_OFFSET = 5_000_000_000_000
_STORE_UNAVAILABLE_PUBLIC_MESSAGE = "Local data store is unavailable."
_CLIENT_QUERY_ERROR_HINTS = (
    "prefix not found",
    "unbound prefix",
    "undefined prefix",
    "parse error",
    "syntax error",
)
logger = logging.getLogger(__name__)


def _runtime_links_for_source(
    source_url: str,
) -> Optional[runtime_links_service.RuntimeLinks]:
    hostname = str(urlsplit(source_url).hostname or "").strip().rstrip(".").lower()
    if hostname != "stat.fi" and not hostname.endswith(".stat.fi"):
        return None
    configured_path = getattr(settings, "JSONSTAT_LINKING_SNAPSHOT_PATH", "")
    if not str(configured_path or "").strip():
        return None
    try:
        return runtime_links_service.load_runtime_links()
    except runtime_links_service.RuntimeLinksError as exc:
        logger.warning("JSON-stat runtime linking snapshot was skipped: %s", exc)
        return None


def internal_store_id(source_url: Any) -> int:
    normalized_url = source.normalize_source_url(source_url)
    digest = hashlib.sha256("jsonstat|{}".format(normalized_url).encode("utf-8")).hexdigest()[:12]
    return _JSONSTAT_STORE_ID_OFFSET + int(digest, 16)


def _build_source_params(source_url: str) -> Dict[str, Any]:
    return {"url": [source.normalize_source_url(source_url)]}


def _ensure_oxigraph() -> None:
    if Store is None:
        raise PetscanServiceError(
            "pyoxigraph is not installed. Install dependencies from requirements.txt first."
        )


def _open_query_store(store_id: int) -> Any:
    _ensure_oxigraph()
    path = str(store.store_path(store_id))
    try:
        return Store.read_only(path)
    except AttributeError:
        return Store(path)
    except OSError as exc:
        raise PetscanServiceError(
            "Failed to open Oxigraph store: {}".format(exc),
            public_message=_STORE_UNAVAILABLE_PUBLIC_MESSAGE,
        ) from exc


def meta_has_matching_source_params(
    meta: Mapping[str, Any], source_params: Mapping[str, Any]
) -> bool:
    expected = source.normalize_source_params(source_params)
    actual_source_params = meta.get("source_params")
    actual = (
        source.normalize_source_params(actual_source_params)
        if isinstance(actual_source_params, Mapping)
        else {}
    )
    return expected == actual


def _meta_is_usable(meta: Mapping[str, Any], store_id: int) -> bool:
    if not isinstance(meta, Mapping) or not meta:
        return False
    if meta.get("psid") != store_id or isinstance(meta.get("psid"), bool):
        return False
    records = meta.get("records")
    if not isinstance(records, int) or isinstance(records, bool) or records < 0:
        return False
    if not isinstance(meta.get("source_url"), str) or not str(meta.get("source_url")).strip():
        return False
    if not isinstance(meta.get("loaded_at"), str) or not str(meta.get("loaded_at")).strip():
        return False
    return isinstance(meta.get("source_params", {}), Mapping)


def _parse_loaded_at(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = "{}+00:00".format(text[:-1])
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _meta_is_fresh(meta: Mapping[str, Any]) -> bool:
    loaded_at = _parse_loaded_at(meta.get("loaded_at"))
    return loaded_at is not None and datetime.now(timezone.utc) - loaded_at <= _MAX_STORE_META_AGE


def _as_client_query_error(exc: Exception) -> Optional[str]:
    raw_message = str(exc).strip()
    lower_message = raw_message.lower()
    if "prefix not found" in lower_message or "unbound prefix" in lower_message:
        return (
            "SPARQL query is invalid: missing PREFIX declaration for a prefixed name "
            "(for example, add PREFIX jsonstat: "
            "<https://sparqlbridge.toolforge.org/ontology/jsonstat/>)."
        )
    if isinstance(exc, SyntaxError) or any(
        hint in lower_message for hint in _CLIENT_QUERY_ERROR_HINTS
    ):
        detail = raw_message or exc.__class__.__name__
        return "SPARQL query is invalid: {}".format(detail)
    return None


def ensure_loaded(source_url: Any, refresh: bool = False) -> StoreMeta:
    _ensure_oxigraph()
    normalized_url = source.normalize_source_url(source_url)
    store_id = internal_store_id(normalized_url)
    expected_source_params = _build_source_params(normalized_url)
    store.prune_expired_stores(exclude_psids=[store_id])
    lock = store.get_psid_lock(store_id)

    with lock:
        if not refresh and store.has_existing_store(store_id):
            meta = store.read_meta(store_id)
            if (
                _meta_is_usable(meta, store_id)
                and _meta_is_fresh(meta)
                and meta_has_matching_source_params(meta, expected_source_params)
            ):
                return cast(StoreMeta, meta)

        payload, final_url = source.fetch_jsonstat_json(normalized_url)
        records = source.extract_records(payload)
        classification_enrichments = classification.enrich_classifications(payload, final_url)
        runtime_links = _runtime_links_for_source(final_url)
        return store_builder.build_store(
            store_id=store_id,
            records=records,
            payload=payload,
            source_url=final_url,
            source_params=expected_source_params,
            classification_enrichments=classification_enrichments,
            runtime_links=runtime_links,
        )


def execute_query(
    source_url: Any,
    query: str,
    refresh: bool = False,
    stream_select_results: bool = False,
) -> QueryExecution:
    query_type = sparql.validate_query(query)
    normalized_url = source.normalize_source_url(source_url)
    store_id = internal_store_id(normalized_url)
    meta = ensure_loaded(normalized_url, refresh=refresh)
    store_instance = _open_query_store(store_id)
    raw_result = None
    execution: Optional[QueryExecution] = None
    try:
        try:
            raw_result = store_instance.query(query)
        except Exception as exc:
            client_error = _as_client_query_error(exc)
            if client_error is not None:
                raise ValueError(client_error) from exc
            raise PetscanServiceError("SPARQL query failed: {}".format(exc)) from exc

        if query_type == "SELECT":
            if stream_select_results:
                execution = QueryExecutionModel(
                    query_type=query_type,
                    result_format="sparql-json-stream",
                    sparql_json_stream=sparql.serialize_select_stream(
                        raw_result,
                        keepalive=store_instance,
                    ),
                    meta=meta,
                ).to_dict()
            else:
                execution = QueryExecutionModel(
                    query_type=query_type,
                    result_format="sparql-json",
                    sparql_json=sparql.serialize_select(raw_result),
                    meta=meta,
                ).to_dict()
        elif query_type == "ASK":
            execution = QueryExecutionModel(
                query_type=query_type,
                result_format="sparql-json",
                sparql_json=sparql.serialize_ask(raw_result),
                meta=meta,
            ).to_dict()
        else:
            execution = QueryExecutionModel(
                query_type=query_type,
                result_format="n-triples",
                ntriples=sparql.serialize_graph(raw_result),
                meta=meta,
            ).to_dict()
    finally:
        raw_result = None
        store_instance = None
    if execution is None:
        raise PetscanServiceError("SPARQL query did not return a result.")
    return execution
