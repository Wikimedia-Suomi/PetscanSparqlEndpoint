"""Service-layer workflow for PetScan ingestion and SPARQL execution."""

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, cast

from . import service_source as source
from . import service_sparql as sparql
from . import service_store as store
from . import service_store_builder as store_builder
from .service_errors import PetscanServiceError
from .service_types import QueryExecution, QueryExecutionModel, StoreMeta

__all__ = [
    "PetscanServiceError",
    "ensure_loaded",
    "execute_query",
    "meta_has_matching_source_params",
]

try:
    from pyoxigraph import Store
except ImportError:  # pragma: no cover - dependency check at runtime
    Store = None  # type: ignore[misc,assignment]

_MAX_STORE_META_AGE = timedelta(minutes=30)


def _ensure_oxigraph() -> None:
    if Store is None:
        raise PetscanServiceError(
            "pyoxigraph is not installed. Install dependencies from requirements.txt first."
        )


def _open_query_store(psid: int) -> Any:
    _ensure_oxigraph()
    path = str(store.store_path(psid))
    try:
        return Store.read_only(path)
    except AttributeError:
        return Store(path)
    except OSError as exc:
        raise PetscanServiceError("Failed to open Oxigraph store: {}".format(exc)) from exc


def meta_has_matching_source_params(meta: Mapping[str, Any], petscan_params: Mapping[str, Any]) -> bool:
    expected = source.normalize_petscan_params(petscan_params)
    actual = source.normalize_petscan_params(meta.get("source_params") if isinstance(meta, Mapping) else {})
    return expected == actual


def _meta_is_usable(meta: Mapping[str, Any], psid: int) -> bool:
    if not isinstance(meta, Mapping) or not meta:
        return False

    meta_psid = meta.get("psid")
    if not isinstance(meta_psid, int) or isinstance(meta_psid, bool) or meta_psid != psid:
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


def ensure_loaded(
    psid: int,
    refresh: bool = False,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> StoreMeta:
    _ensure_oxigraph()
    store.prune_expired_stores(exclude_psids=[psid])
    lock = store.get_psid_lock(psid)
    normalized_params = source.normalize_petscan_params(petscan_params)

    with lock:
        if not refresh and store.has_existing_store(psid):
            meta = store.read_meta(psid)
            if (
                _meta_is_usable(meta, psid)
                and _meta_is_fresh(meta)
                and meta_has_matching_source_params(meta, normalized_params)
            ):
                return cast(StoreMeta, meta)

        payload, source_url = source.fetch_petscan_json(psid, petscan_params=normalized_params)
        records = source.extract_records(payload)
        if not records:
            raise PetscanServiceError("PetScan returned zero rows for psid {}.".format(psid))

        return store_builder.build_store(psid, records, source_url, source_params=normalized_params)


def execute_query(
    psid: int,
    query: str,
    refresh: bool = False,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> QueryExecution:
    qtype = sparql.validate_query(query)

    meta = ensure_loaded(psid, refresh=refresh, petscan_params=petscan_params)

    store_instance = _open_query_store(psid)

    try:
        raw_result = store_instance.query(query)
    except Exception as exc:
        raise PetscanServiceError("SPARQL query failed: {}".format(exc)) from exc
    finally:
        store_instance = None

    if qtype == "SELECT":
        result = QueryExecutionModel(
            query_type=qtype,
            result_format="sparql-json",
            sparql_json=sparql.serialize_select(raw_result),
            meta=meta,
        )
        return result.to_dict()

    if qtype == "ASK":
        result = QueryExecutionModel(
            query_type=qtype,
            result_format="sparql-json",
            sparql_json=sparql.serialize_ask(raw_result),
            meta=meta,
        )
        return result.to_dict()

    result = QueryExecutionModel(
        query_type=qtype,
        result_format="n-triples",
        ntriples=sparql.serialize_graph(raw_result),
        meta=meta,
    )
    return result.to_dict()
