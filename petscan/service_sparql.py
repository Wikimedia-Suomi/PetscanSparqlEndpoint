"""SPARQL query validation and result serialization."""

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


def _is_named_node(term: Any) -> bool:
    if term is None:
        return False
    if OxigraphNamedNode is not None and isinstance(term, OxigraphNamedNode):
        return True
    text = str(term)
    return hasattr(term, "value") and text.startswith("<") and text.endswith(">")


def _is_blank_node(term: Any) -> bool:
    if term is None:
        return False
    if OxigraphBlankNode is not None and isinstance(term, OxigraphBlankNode):
        return True
    return hasattr(term, "value") and str(term).startswith("_:")


def _is_literal(term: Any) -> bool:
    if term is None:
        return False
    if OxigraphLiteral is not None and isinstance(term, OxigraphLiteral):
        return True
    return hasattr(term, "value") and (hasattr(term, "language") or hasattr(term, "datatype"))


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
    if _is_named_node(term):
        return {"type": "uri", "value": _term_value(term)}

    if _is_blank_node(term):
        raw = _term_value(term)
        return {
            "type": "bnode",
            "value": raw[2:] if raw.startswith("_:") else raw,
        }

    if _is_literal(term):
        data = {"type": "literal", "value": _term_value(term)}
        language = getattr(term, "language", None)
        datatype = getattr(term, "datatype", None)
        if language:
            data["xml:lang"] = str(language)
        elif datatype:
            datatype_iri = _term_value(datatype)
            if datatype_iri != "http://www.w3.org/2001/XMLSchema#string":
                data["datatype"] = datatype_iri
        return data

    return {"type": "literal", "value": str(term)}


def _term_to_ntriples(term: Any) -> str:
    if _is_named_node(term):
        return "<{}>".format(_term_value(term))

    if _is_blank_node(term):
        text = _term_value(term)
        return text if text.startswith("_:") else "_:{}".format(text)

    if _is_literal(term):
        escaped = (
            _term_value(term)
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace('"', '\\"')
        )
        language = getattr(term, "language", None)
        datatype = getattr(term, "datatype", None)
        if language:
            return '"{}"@{}'.format(escaped, language)
        if datatype:
            return '"{}"^^<{}>'.format(escaped, _term_value(datatype))
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
