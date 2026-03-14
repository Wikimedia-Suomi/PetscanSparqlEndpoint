"""SPARQL query-form detection, SERVICE blocking, and result serialization."""

import re
from typing import Any, Dict

from .service_errors import PetscanServiceError

__all__ = [
    "contains_service_clause",
    "query_type",
    "serialize_ask",
    "serialize_graph",
    "serialize_select",
]

_QUERY_TYPES = {"SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}
_SPARQL_COMMENT_LINE_RE = re.compile(r"(?m)^\s*#.*$")
_SPARQL_PREFIX_PROLOGUE_RE = re.compile(r"(?is)\A\s*PREFIX\s+[A-Za-z][A-Za-z0-9._-]*:\s*<[^>]*>")
_SPARQL_BASE_PROLOGUE_RE = re.compile(r"(?is)\A\s*BASE\s*<[^>]*>")
_SPARQL_QUERY_FORM_RE = re.compile(r"(?is)\A\s*(SELECT|ASK|CONSTRUCT|DESCRIBE)\b")
_SERVICE_CLAUSE_RE = re.compile(
    r"(?is)\bSERVICE\b(?:\s+SILENT\b)?\s*(?:<[^>]+>|[?$][A-Za-z_][A-Za-z0-9_]*|[A-Za-z][A-Za-z0-9_-]*:[^\s{>]*)\s*\{"
)


def _strip_comment_lines(query: str) -> str:
    return _SPARQL_COMMENT_LINE_RE.sub("", query)


def query_type(query: str) -> str:
    remaining = _strip_comment_lines(query)

    # Strip SPARQL prologue declarations to avoid matching query-form keywords
    # inside prefixed names (for example `PREFIX select: <...>`).
    while True:
        prefix_match = _SPARQL_PREFIX_PROLOGUE_RE.match(remaining)
        if prefix_match is not None:
            remaining = remaining[prefix_match.end() :]
            continue

        base_match = _SPARQL_BASE_PROLOGUE_RE.match(remaining)
        if base_match is not None:
            remaining = remaining[base_match.end() :]
            continue

        break

    form_match = _SPARQL_QUERY_FORM_RE.match(remaining)
    if form_match is not None:
        query_type = str(form_match.group(1)).upper()
        if query_type in _QUERY_TYPES:
            return query_type

    raise PetscanServiceError("SPARQL query must contain SELECT, ASK, CONSTRUCT, or DESCRIBE.")


def contains_service_clause(query: str) -> bool:
    clean_query = _strip_comment_lines(query)
    return bool(_SERVICE_CLAUSE_RE.search(clean_query))


def _variable_name(value: Any) -> str:
    text = str(value)
    return text[1:] if text.startswith("?") else text


def _is_named_node(term: Any) -> bool:
    return term is not None and term.__class__.__name__ == "NamedNode"


def _is_blank_node(term: Any) -> bool:
    return term is not None and term.__class__.__name__ == "BlankNode"


def _is_literal(term: Any) -> bool:
    return term is not None and term.__class__.__name__ == "Literal"


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

    if result is not None and result.__class__.__name__ == "QueryBoolean":
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
