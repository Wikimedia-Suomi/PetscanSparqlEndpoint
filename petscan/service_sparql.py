"""SPARQL query validation and result serialization."""

from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Set

from .service_errors import PetscanServiceError

try:
    from pyoxigraph import BlankNode as OxigraphBlankNode
    from pyoxigraph import Literal as OxigraphLiteral
    from pyoxigraph import NamedNode as OxigraphNamedNode
    from pyoxigraph import QueryBoolean as OxigraphQueryBoolean
except ImportError:  # pragma: no cover - dependency check at runtime
    OxigraphBlankNode = None  # type: ignore[misc,assignment]
    OxigraphLiteral = None  # type: ignore[misc,assignment]
    OxigraphNamedNode = None  # type: ignore[misc,assignment]
    OxigraphQueryBoolean = None  # type: ignore[misc,assignment]

try:
    from pyparsing.results import ParseResults
    from rdflib.plugins.sparql.parser import parseQuery
    from rdflib.plugins.sparql.parserutils import CompValue
except ImportError:  # pragma: no cover - dependency check at runtime
    ParseResults = None  # type: ignore[misc,assignment]
    parseQuery = None  # type: ignore[assignment]
    CompValue = None  # type: ignore[misc,assignment]

__all__ = [
    "contains_service_clause",
    "contains_dataset_clause",
    "query_type",
    "serialize_ask",
    "serialize_graph",
    "serialize_select",
    "validate_query",
]

_QUERY_TYPES = {"SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}
_QUERY_NAME_TO_TYPE = {
    "SelectQuery": "SELECT",
    "AskQuery": "ASK",
    "ConstructQuery": "CONSTRUCT",
    "DescribeQuery": "DESCRIBE",
}
_FORBIDDEN_PATTERN_NAMES = {
    "ServiceGraphPattern": "SERVICE clauses are not allowed in this endpoint.",
    "DatasetClause": "Dataset clauses are not allowed in this endpoint.",
}
_URI_TERM_MARKERS = frozenset({"iri", "uri", "namednode", "named_node"})
_BNODE_TERM_MARKERS = frozenset({"bnode", "blanknode", "blank_node"})
_LITERAL_TERM_MARKERS = frozenset({"literal"})


@dataclass(frozen=True)
class _ClassifiedTerm:
    kind: str
    value: str
    language: Optional[str] = None
    datatype: Optional[Any] = None


def _ensure_parser() -> None:
    if parseQuery is None or ParseResults is None or CompValue is None:
        raise PetscanServiceError("rdflib is not installed. Install dependencies from requirements.txt first.")


def _parse_query(query: str) -> Any:
    _ensure_parser()

    try:
        parsed = parseQuery(query)
    except Exception as exc:
        raise PetscanServiceError("SPARQL query is invalid: {}".format(exc)) from exc

    if not isinstance(parsed, ParseResults) or len(parsed) < 2:
        raise PetscanServiceError("SPARQL query must contain SELECT, ASK, CONSTRUCT, or DESCRIBE.")
    return parsed


def _query_root(parsed_query: Any) -> Any:
    query = parsed_query[1]
    query_name = getattr(query, "name", "")
    if query_name not in _QUERY_NAME_TO_TYPE:
        raise PetscanServiceError("SPARQL query must contain SELECT, ASK, CONSTRUCT, or DESCRIBE.")
    return query


def _iter_comp_values(node: Any, seen: Optional[Set[int]] = None) -> Iterator[Any]:
    visited = seen if seen is not None else set()

    if isinstance(node, CompValue):
        node_id = id(node)
        if node_id in visited:
            return
        visited.add(node_id)
        yield node
        for value in node.values():
            yield from _iter_comp_values(value, visited)
        return

    if isinstance(node, ParseResults):
        node_id = id(node)
        if node_id in visited:
            return
        visited.add(node_id)
        for value in node:
            yield from _iter_comp_values(value, visited)
        for _key, value in node.items():  # type: ignore[no-untyped-call]
            yield from _iter_comp_values(value, visited)
        return

    if isinstance(node, dict):
        for value in node.values():
            yield from _iter_comp_values(value, visited)
        return

    if isinstance(node, (list, tuple, set)):
        for value in node:
            yield from _iter_comp_values(value, visited)


def query_type(query: str) -> str:
    parsed_query = _parse_query(query)
    query = _query_root(parsed_query)
    query_name = getattr(query, "name", "")
    query_form = _QUERY_NAME_TO_TYPE.get(query_name)
    if query_form in _QUERY_TYPES:
        return query_form
    raise PetscanServiceError("SPARQL query must contain SELECT, ASK, CONSTRUCT, or DESCRIBE.")


def contains_service_clause(query: str) -> bool:
    try:
        parsed_query = _parse_query(query)
    except PetscanServiceError:
        return False

    return any(getattr(node, "name", "") == "ServiceGraphPattern" for node in _iter_comp_values(parsed_query))


def contains_dataset_clause(query: str) -> bool:
    try:
        parsed_query = _parse_query(query)
    except PetscanServiceError:
        return False

    return any(getattr(node, "name", "") == "DatasetClause" for node in _iter_comp_values(parsed_query))


def validate_query(query: str) -> str:
    try:
        parsed_query = _parse_query(query)
        query = _query_root(parsed_query)
        query_name = getattr(query, "name", "")
        query_form = _QUERY_NAME_TO_TYPE.get(query_name)
    except PetscanServiceError as exc:
        raise ValueError(str(exc)) from exc

    if query_form not in _QUERY_TYPES:
        raise ValueError("SPARQL query must contain SELECT, ASK, CONSTRUCT, or DESCRIBE.")

    for node in _iter_comp_values(parsed_query):
        message = _FORBIDDEN_PATTERN_NAMES.get(getattr(node, "name", ""))
        if message is not None:
            raise ValueError(message)

    return query_form


def _variable_name(value: Any) -> str:
    text = str(value)
    return text[1:] if text.startswith("?") else text


def _normalize_term_marker(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _classify_term(term: Any) -> Optional[_ClassifiedTerm]:
    if term is None:
        return None
    if OxigraphNamedNode is not None and isinstance(term, OxigraphNamedNode):
        return _ClassifiedTerm("uri", _term_value(term))
    if OxigraphBlankNode is not None and isinstance(term, OxigraphBlankNode):
        return _ClassifiedTerm("bnode", _term_value(term))
    if OxigraphLiteral is not None and isinstance(term, OxigraphLiteral):
        language = getattr(term, "language", None)
        return _ClassifiedTerm(
            "literal",
            _term_value(term),
            str(language) if language else None,
            getattr(term, "datatype", None),
        )

    if not hasattr(term, "value"):
        return None

    marker = ""
    for attr_name in ("term_type", "kind"):
        marker = _normalize_term_marker(getattr(term, attr_name, ""))
        if marker:
            break

    if marker in _URI_TERM_MARKERS or bool(getattr(term, "is_named_node", False)):
        return _ClassifiedTerm("uri", _term_value(term))

    if marker in _BNODE_TERM_MARKERS or bool(getattr(term, "is_blank_node", False)):
        return _ClassifiedTerm("bnode", _term_value(term))

    language = getattr(term, "language", None)
    datatype = getattr(term, "datatype", None)
    if (
        marker in _LITERAL_TERM_MARKERS
        or bool(getattr(term, "is_literal", False))
        or language is not None
        or datatype is not None
    ):
        return _ClassifiedTerm(
            "literal",
            _term_value(term),
            str(language) if language else None,
            datatype,
        )

    return _ClassifiedTerm("literal", _term_value(term))


def _is_query_boolean(result: Any) -> bool:
    if result is None or isinstance(result, bool):
        return False
    if OxigraphQueryBoolean is not None and isinstance(result, OxigraphQueryBoolean):
        return True
    return hasattr(result, "value") and hasattr(result, "__bool__")


def _term_value(term: Any) -> str:
    value = getattr(term, "value", None)
    return str(value if value is not None else term)


def _term_to_sparql_binding(term: Any) -> Dict[str, Any]:
    classified = _classify_term(term)
    if classified is None:
        return {"type": "literal", "value": str(term)}

    if classified.kind == "uri":
        return {"type": "uri", "value": classified.value}

    if classified.kind == "bnode":
        raw = classified.value
        return {
            "type": "bnode",
            "value": raw[2:] if raw.startswith("_:") else raw,
        }

    if classified.kind == "literal":
        data = {"type": "literal", "value": classified.value}
        if classified.language:
            data["xml:lang"] = classified.language
        elif classified.datatype:
            datatype_iri = _term_value(classified.datatype)
            if datatype_iri != "http://www.w3.org/2001/XMLSchema#string":
                data["datatype"] = datatype_iri
        return data

    return {"type": "literal", "value": str(term)}


def _term_to_ntriples(term: Any) -> str:
    classified = _classify_term(term)
    if classified is None:
        return '"{}"'.format(str(term).replace('"', '\\"'))

    if classified.kind == "uri":
        return "<{}>".format(classified.value)

    if classified.kind == "bnode":
        text = classified.value
        return text if text.startswith("_:") else "_:{}".format(text)

    if classified.kind == "literal":
        escaped = (
            classified.value
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace('"', '\\"')
        )
        if classified.language:
            return '"{}"@{}'.format(escaped, classified.language)
        if classified.datatype:
            return '"{}"^^<{}>'.format(escaped, _term_value(classified.datatype))
        return '"{}"'.format(escaped)

    return '"{}"'.format(str(term).replace('"', '\\"'))


def serialize_select(result: Any) -> Dict[str, Any]:
    variables = [_variable_name(v) for v in getattr(result, "variables", [])]
    rows = []  # type: list[Dict[str, Any]]

    for solution in result:
        bindings = {}  # type: Dict[str, Any]
        items = []
        if hasattr(solution, "items"):
            items = list(solution.items())

        if items:
            for variable, term in items:
                bindings[_variable_name(variable)] = _term_to_sparql_binding(term)
        else:
            for variable in variables:
                try:
                    term = solution[variable]
                except (KeyError, TypeError, IndexError):
                    continue
                bindings[variable] = _term_to_sparql_binding(term)

        rows.append(bindings)

    return {
        "head": {"vars": variables},
        "results": {"bindings": rows},
    }


def serialize_ask(result: Any) -> Dict[str, Any]:
    if isinstance(result, bool):
        return {"head": {}, "boolean": result}

    if _is_query_boolean(result):
        return {"head": {}, "boolean": bool(result)}

    # Some implementations expose ASK as iterable with one row; fallback handles that.
    try:
        first = next(iter(result))
    except Exception:
        first = None

    if isinstance(first, bool):
        return {"head": {}, "boolean": first}

    raise PetscanServiceError("ASK result could not be serialized.")


def serialize_graph(result: Any) -> str:
    lines = []  # type: list[str]

    for triple in result:
        subject = getattr(triple, "subject", None)
        predicate = getattr(triple, "predicate", None)
        object_term = getattr(triple, "object", None)

        if subject is None or predicate is None or object_term is None:
            if isinstance(triple, tuple) and len(triple) == 3:
                subject, predicate, object_term = triple
            else:
                continue

        lines.append(
            "{} {} {} .".format(
                _term_to_ntriples(subject),
                _term_to_ntriples(predicate),
                _term_to_ntriples(object_term),
            )
        )

    return "\n".join(lines) + ("\n" if lines else "")
