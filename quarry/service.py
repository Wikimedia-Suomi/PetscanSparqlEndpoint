"""Service-layer workflow for Quarry ingestion and SPARQL execution."""

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
_QUARRY_STORE_ID_OFFSET = 2_000_000_000


def internal_store_id(quarry_id: int) -> int:
    return _QUARRY_STORE_ID_OFFSET + quarry_id


def _build_source_params(
    quarry_id: int,
    qrun_id: int,
    limit: Optional[int],
    query_db: Optional[str] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "quarry_id": [str(quarry_id)],
        "qrun_id": [str(qrun_id)],
    }
    if limit is not None:
        params["limit"] = [str(limit)]
    if query_db is not None and str(query_db).strip():
        params["query_db"] = [str(query_db).strip()]
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
        raise PetscanServiceError("Failed to open Oxigraph store: {}".format(exc)) from exc


def _normalize_source_params(source_params: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = {}  # type: Dict[str, Any]
    for key, raw_value in source_params.items():
        key_text = str(key).strip()
        if not key_text:
            continue
        if isinstance(raw_value, (list, tuple, set)):
            values = [str(value).strip() for value in raw_value if str(value).strip()]
        else:
            values = [str(raw_value).strip()] if str(raw_value).strip() else []
        if values:
            normalized[key_text] = values
    return normalized


def meta_has_matching_source_params(meta: Mapping[str, Any], source_params: Mapping[str, Any]) -> bool:
    expected = _normalize_source_params(source_params)
    actual_source_params = meta.get("source_params")
    actual = (
        _normalize_source_params(actual_source_params)
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
            "(for example, add PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>)."
        )

    if isinstance(exc, SyntaxError) or any(hint in lower_message for hint in _CLIENT_QUERY_ERROR_HINTS):
        detail = raw_message if raw_message else exc.__class__.__name__
        return "SPARQL query is invalid: {}".format(detail)

    return None


def ensure_loaded(
    quarry_id: int,
    refresh: bool = False,
    limit: Optional[int] = None,
) -> StoreMeta:
    _ensure_oxigraph()
    store_id = internal_store_id(quarry_id)
    store.prune_expired_stores(exclude_psids=[store_id])
    lock = store.get_psid_lock(store_id)

    with lock:
        if not refresh and store.has_existing_store(store_id):
            meta = store.read_meta(store_id)
            if _meta_is_usable(meta, store_id) and _meta_is_fresh(meta):
                current_source_params = meta.get("source_params", {})
                if isinstance(current_source_params, Mapping):
                    qrun_values = current_source_params.get("qrun_id", [])
                    if isinstance(qrun_values, list) and qrun_values:
                        try:
                            qrun_id = int(str(qrun_values[-1]).strip())
                        except (TypeError, ValueError):
                            qrun_id = None
                        if qrun_id is not None and qrun_id > 0:
                            query_db_values = current_source_params.get("query_db", [])
                            query_db = None
                            if isinstance(query_db_values, list) and query_db_values:
                                query_db = str(query_db_values[-1]).strip() or None
                            expected_source_params = _build_source_params(quarry_id, qrun_id, limit, query_db)
                            if meta_has_matching_source_params(meta, expected_source_params):
                                return cast(StoreMeta, meta)

        resolution = source.resolve_quarry_run(quarry_id)
        qrun_id = int(resolution["qrun_id"])
        query_db = str(resolution.get("query_db") or "").strip() or None
        payload, json_url = source.fetch_quarry_json(qrun_id)
        records = source.extract_records(payload, limit=limit)
        if not records:
            raise PetscanServiceError("Quarry returned zero rows for query {}.".format(quarry_id))

        source_params = _build_source_params(quarry_id, qrun_id, limit, query_db)
        return store_builder.build_store(
            store_id=store_id,
            quarry_id=quarry_id,
            records=records,
            source_url=json_url,
            source_params=source_params,
            query_db=query_db,
        )


def execute_query(
    quarry_id: int,
    query: str,
    refresh: bool = False,
    limit: Optional[int] = None,
) -> QueryExecution:
    qtype = sparql.validate_query(query)
    store_id = internal_store_id(quarry_id)
    meta = ensure_loaded(quarry_id, refresh=refresh, limit=limit)

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
