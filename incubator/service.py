"""Service-layer workflow for Incubator ingestion and SPARQL execution."""

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, cast

from petscan import service_sparql as sparql
from petscan import service_store as store
from petscan.service_errors import PetscanServiceError
from petscan.service_types import QueryExecution, QueryExecutionModel, StoreMeta

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
_CLIENT_QUERY_ERROR_HINTS = (
    "prefix not found",
    "unbound prefix",
    "undefined prefix",
    "parse error",
    "syntax error",
)
_INCUBATOR_STORE_ID_OFFSET = 3_000_000_000_000
_STORE_UNAVAILABLE_PUBLIC_MESSAGE = "Local data store is unavailable."


def internal_store_id(limit: Optional[int], recentchanges_only: bool = False) -> int:
    token = "incubator|limit={}|recentchanges_only={}".format(
        limit if limit is not None else "",
        "1" if recentchanges_only else "0",
    )
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:12]
    return _INCUBATOR_STORE_ID_OFFSET + int(digest, 16)


def _build_source_params(limit: Optional[int], recentchanges_only: bool = False) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if limit is not None:
        params["limit"] = [str(limit)]
    if recentchanges_only:
        params["recentchanges_only"] = ["1"]
    return params


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


def meta_has_matching_source_params(meta: Mapping[str, Any], source_params: Mapping[str, Any]) -> bool:
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

    meta_psid = meta.get("psid")
    if not isinstance(meta_psid, int) or isinstance(meta_psid, bool) or meta_psid != store_id:
        return False

    records = meta.get("records")
    if not isinstance(records, int) or isinstance(records, bool) or records < 0:
        return False

    source_url = meta.get("source_url")
    if not isinstance(source_url, str) or not source_url.strip():
        return False

    loaded_at = meta.get("loaded_at")
    if not isinstance(loaded_at, str) or not loaded_at.strip():
        return False

    source_params = meta.get("source_params", {})
    if not isinstance(source_params, Mapping):
        return False

    return True


def _parse_loaded_at(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
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
    if loaded_at is None:
        return False
    return (datetime.now(timezone.utc) - loaded_at) <= _MAX_STORE_META_AGE


def _as_client_query_error(exc: Exception) -> Optional[str]:
    raw_message = str(exc).strip()
    lower_message = raw_message.lower()

    if (
        isinstance(exc, SyntaxError)
        and ("prefix not found" in lower_message or "unbound prefix" in lower_message)
    ) or ("prefix not found" in lower_message):
        return (
            "SPARQL query is invalid: missing PREFIX declaration for a prefixed name "
            "(for example, add PREFIX schema: <http://schema.org/> or "
            "PREFIX wikibase: <http://wikiba.se/ontology#>)."
        )

    if isinstance(exc, SyntaxError) or any(hint in lower_message for hint in _CLIENT_QUERY_ERROR_HINTS):
        detail = raw_message if raw_message else exc.__class__.__name__
        return "SPARQL query is invalid: {}".format(detail)

    return None


def ensure_loaded(
    refresh: bool = False,
    limit: Optional[int] = None,
    recentchanges_only: bool = False,
) -> StoreMeta:
    _ensure_oxigraph()
    store_id = internal_store_id(limit, recentchanges_only=recentchanges_only)
    store.prune_expired_stores(exclude_psids=[store_id])
    lock = store.get_psid_lock(store_id)
    expected_source_params = _build_source_params(limit, recentchanges_only=recentchanges_only)

    with lock:
        if not refresh and store.has_existing_store(store_id):
            meta = store.read_meta(store_id)
            if (
                _meta_is_usable(meta, store_id)
                and _meta_is_fresh(meta)
                and meta_has_matching_source_params(meta, expected_source_params)
            ):
                return cast(StoreMeta, meta)

        records, source_url = source.fetch_incubator_records(
            limit=limit,
            recentchanges_only=recentchanges_only,
        )
        if not records:
            raise PetscanServiceError("Incubator returned zero rows for the requested filters.")

        return store_builder.build_store(
            store_id=store_id,
            records=records,
            source_url=source_url,
            source_params=expected_source_params,
        )


def execute_query(
    query: str,
    refresh: bool = False,
    limit: Optional[int] = None,
    recentchanges_only: bool = False,
) -> QueryExecution:
    qtype = sparql.validate_query(query)
    store_id = internal_store_id(limit, recentchanges_only=recentchanges_only)
    meta = ensure_loaded(refresh=refresh, limit=limit, recentchanges_only=recentchanges_only)

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

        if qtype == "SELECT":
            result = QueryExecutionModel(
                query_type=qtype,
                result_format="sparql-json",
                sparql_json=sparql.serialize_select(raw_result),
                meta=meta,
            )
            execution = result.to_dict()
        elif qtype == "ASK":
            result = QueryExecutionModel(
                query_type=qtype,
                result_format="sparql-json",
                sparql_json=sparql.serialize_ask(raw_result),
                meta=meta,
            )
            execution = result.to_dict()
        else:
            result = QueryExecutionModel(
                query_type=qtype,
                result_format="n-triples",
                ntriples=sparql.serialize_graph(raw_result),
                meta=meta,
            )
            execution = result.to_dict()
    finally:
        raw_result = None
        store_instance = None

    if execution is None:
        raise PetscanServiceError("SPARQL query did not return a result.")

    return execution
