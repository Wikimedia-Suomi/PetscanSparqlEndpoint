"""Query service for immutable local place-name datasets."""

from typing import Any, Optional

from petscan import service_sparql as sparql
from petscan.service_errors import PetscanServiceError

from . import service_store
from .datasets import get_dataset

try:
    from pyoxigraph import NamedNode
except ImportError:  # pragma: no cover - dependency check at runtime
    NamedNode = None  # type: ignore[misc,assignment]

__all__ = ["PetscanServiceError", "ensure_loaded", "execute_query"]

_INVALID_QUERY_PUBLIC_MESSAGE = "SPARQL query is invalid."
_QUERY_FAILURE_PUBLIC_MESSAGE = "Local place-name query failed."


def _as_client_query_error(exc: Exception) -> Optional[str]:
    if not isinstance(exc, SyntaxError):
        return None

    raw_message = str(exc).strip()
    lower_message = raw_message.lower()
    if "prefix not found" in lower_message or "unbound prefix" in lower_message:
        return (
            "SPARQL query is invalid: missing PREFIX declaration for a prefixed name "
            "(for example, add PREFIX pn: "
            "<https://sparqlbridge.toolforge.org/ontology/placenames/>)."
        )
    return _INVALID_QUERY_PUBLIC_MESSAGE


def ensure_loaded(dataset: str) -> dict[str, Any]:
    spec = get_dataset(dataset)
    return service_store.ensure_loaded(spec)


def execute_query(dataset: str, query: str) -> dict[str, Any]:
    query_form = sparql.validate_query(query)
    spec = get_dataset(dataset)
    meta = service_store.ensure_loaded(spec)
    if NamedNode is None:
        raise PetscanServiceError("pyoxigraph is not installed.")
    store_instance = service_store.open_query_store(spec)
    raw_result: Any = None
    try:
        try:
            raw_result = store_instance.query(query, default_graph=NamedNode(spec.graph_iri))
        except Exception as exc:
            client_error = _as_client_query_error(exc)
            if client_error is not None:
                raise ValueError(client_error) from exc
            raise PetscanServiceError(
                f"SPARQL query failed: {exc}",
                public_message=_QUERY_FAILURE_PUBLIC_MESSAGE,
            ) from exc
        if query_form == "SELECT":
            return {
                "query_type": query_form,
                "result_format": "sparql-json",
                "sparql_json": sparql.serialize_select(raw_result),
                "meta": meta,
            }
        if query_form == "ASK":
            return {
                "query_type": query_form,
                "result_format": "sparql-json",
                "sparql_json": sparql.serialize_ask(raw_result),
                "meta": meta,
            }
        return {
            "query_type": query_form,
            "result_format": "n-triples",
            "ntriples": sparql.serialize_graph(raw_result),
            "meta": meta,
        }
    finally:
        raw_result = None
        store_instance = None
