"""Service-layer workflow for PagePile ingestion and SPARQL execution."""

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
_PAGEPILE_STORE_ID_OFFSET = 4_000_000_000
_STORE_UNAVAILABLE_PUBLIC_MESSAGE = "Local data store is unavailable."


def internal_store_id(pagepile_id: int) -> int:
    return _PAGEPILE_STORE_ID_OFFSET + pagepile_id


def _build_source_params(pagepile_id: int, limit: Optional[int]) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "pagepile_id": [str(pagepile_id)],
    }
    if limit is not None:
        params["limit"] = [str(limit)]
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
            "PREFIX pagepile: <https://pagepile.toolforge.org/ontology/>)."
        )

    if isinstance(exc, SyntaxError) or any(hint in lower_message for hint in _CLIENT_QUERY_ERROR_HINTS):
        detail = raw_message if raw_message else exc.__class__.__name__
        return "SPARQL query is invalid: {}".format(detail)

    return None


def ensure_loaded(
    pagepile_id: int,
    refresh: bool = False,
    limit: Optional[int] = None,
) -> StoreMeta:
    _ensure_oxigraph()
    normalized_pagepile_id = source.normalize_pagepile_id(pagepile_id)
    normalized_limit = source.normalize_load_limit(limit)
    effective_limit = source.effective_load_limit(normalized_limit)
    store_id = internal_store_id(normalized_pagepile_id)
    expected_source_params = _build_source_params(normalized_pagepile_id, effective_limit)
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

        records, source_url = source.fetch_pagepile_records(
            pagepile_id=normalized_pagepile_id,
            limit=effective_limit,
        )
        if not records:
            raise PetscanServiceError(
                "PagePile returned zero pages with Wikidata sitelinks for pile {}.".format(
                    normalized_pagepile_id
                )
            )

        return store_builder.build_store(
            store_id=store_id,
            records=records,
            source_url=source_url,
            source_params=expected_source_params,
        )


def execute_query(
    pagepile_id: int,
    query: str,
    refresh: bool = False,
    limit: Optional[int] = None,
) -> QueryExecution:
    qtype = sparql.validate_query(query)
    normalized_pagepile_id = source.normalize_pagepile_id(pagepile_id)
    normalized_limit = source.normalize_load_limit(limit)
    store_id = internal_store_id(normalized_pagepile_id)
    meta = ensure_loaded(
        normalized_pagepile_id,
        refresh=refresh,
        limit=normalized_limit,
    )

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

    assert execution is not None
    return execution
