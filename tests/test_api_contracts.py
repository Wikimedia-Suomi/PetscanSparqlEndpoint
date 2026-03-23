import json
from typing import Any, Dict
from unittest.mock import patch
from urllib.parse import urlencode

from jsonschema import Draft202012Validator

API_STRUCTURE_PATH = "/petscan/api/structure"
SPARQL_PATH = "/petscan/sparql/psid=123"
SPARQL_FEDERATED_PATH = "/petscan/sparql/psid=43641756&categories=Turku"

ASK_QUERY = "ASK { ?s ?p ?o }"
FEDERATED_SUBQUERY = """
SELECT ?item ?title WHERE {
  ?item a <https://petscan.wmcloud.org/ontology/Page> .
  OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title }
}
LIMIT 20
""".strip()

ERROR_RESPONSE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "error": {"type": "string", "minLength": 1},
    },
    "required": ["error"],
    "additionalProperties": False,
}

STRUCTURE_FIELD_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "source_key": {"type": "string", "minLength": 1},
        "predicate": {"type": "string", "format": "uri"},
        "present_in_rows": {"type": "integer", "minimum": 0},
        "primary_type": {"type": "string", "minLength": 1},
        "row_side_cardinality": {"type": "string", "enum": ["1", "M"]},
        "observed_types": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        },
    },
    "required": ["source_key", "predicate", "present_in_rows", "primary_type", "observed_types"],
    "additionalProperties": False,
}

STRUCTURE_RESPONSE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "psid": {"type": "integer", "minimum": 1},
        "meta": {
            "type": "object",
            "properties": {
                "psid": {"type": "integer", "minimum": 1},
                "records": {"type": "integer", "minimum": 0},
                "source_url": {"type": "string", "format": "uri"},
                "loaded_at": {"type": "string", "format": "date-time"},
                "source_params": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                    },
                },
                "structure": {
                    "type": "object",
                    "properties": {
                        "row_count": {"type": "integer", "minimum": 0},
                        "field_count": {"type": "integer", "minimum": 0},
                        "fields": {
                            "type": "array",
                            "items": STRUCTURE_FIELD_SCHEMA,
                        },
                    },
                    "required": ["row_count", "field_count", "fields"],
                    "additionalProperties": False,
                },
            },
            "required": ["psid", "records", "source_url", "loaded_at", "source_params", "structure"],
            "additionalProperties": False,
        },
    },
    "required": ["psid", "meta"],
    "additionalProperties": False,
}

TERM_BINDING_SCHEMA: Dict[str, Any] = {
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "type": {"const": "uri"},
                "value": {"type": "string", "format": "uri"},
            },
            "required": ["type", "value"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "type": {"const": "bnode"},
                "value": {"type": "string", "minLength": 1},
            },
            "required": ["type", "value"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "type": {"const": "literal"},
                "value": {"type": "string"},
                "datatype": {"type": "string", "format": "uri"},
                "xml:lang": {"type": "string", "minLength": 1},
            },
            "required": ["type", "value"],
            "additionalProperties": False,
            "not": {
                "required": ["datatype", "xml:lang"],
            },
        },
    ]
}

SELECT_RESULTS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "head": {
            "type": "object",
            "properties": {
                "vars": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
            },
            "required": ["vars"],
            "additionalProperties": False,
        },
        "results": {
            "type": "object",
            "properties": {
                "bindings": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "patternProperties": {
                            "^[A-Za-z_][A-Za-z0-9_]*$": TERM_BINDING_SCHEMA,
                        },
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["bindings"],
            "additionalProperties": False,
        },
    },
    "required": ["head", "results"],
    "additionalProperties": False,
}

ASK_RESULTS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "head": {"type": "object"},
        "boolean": {"type": "boolean"},
    },
    "required": ["head", "boolean"],
    "additionalProperties": False,
}


def _assert_matches_schema(payload: Any, schema: Dict[str, Any]) -> None:
    validator = Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
    errors = sorted(validator.iter_errors(payload), key=lambda error: list(error.absolute_path))
    assert not errors, "\n".join(
        "{}: {}".format(
            "$" if not error.absolute_path else "$." + ".".join(str(part) for part in error.absolute_path),
            error.message,
        )
        for error in errors
    )


def _ask_execution_result() -> Dict[str, Any]:
    return {
        "query_type": "ASK",
        "result_format": "sparql-json",
        "sparql_json": {"head": {}, "boolean": True},
        "meta": {},
    }


@patch("petscan.views.petscan_service.ensure_loaded")
def test_structure_endpoint_success_matches_json_schema(ensure_loaded: Any, client: Any) -> None:
    ensure_loaded.return_value = {
        "psid": 123,
        "records": 2,
        "source_url": "https://petscan.wmcloud.org/?psid=123&format=json",
        "loaded_at": "2026-03-13T10:00:00+00:00",
        "source_params": {"category": ["Turku"]},
        "structure": {
            "row_count": 2,
            "field_count": 2,
            "fields": [
                {
                    "source_key": "title",
                    "predicate": "https://petscan.wmcloud.org/ontology/title",
                    "present_in_rows": 2,
                    "primary_type": "string",
                    "row_side_cardinality": "1",
                    "observed_types": ["string"],
                },
                {
                    "source_key": "namespace",
                    "predicate": "https://petscan.wmcloud.org/ontology/namespace",
                    "present_in_rows": 2,
                    "primary_type": "integer",
                    "row_side_cardinality": "1",
                    "observed_types": ["integer"],
                },
            ],
        },
    }

    response = client.get(API_STRUCTURE_PATH, data={"psid": 123, "category": "Turku"})

    assert response.status_code == 200
    _assert_matches_schema(response.json(), STRUCTURE_RESPONSE_SCHEMA)


def test_structure_endpoint_validation_error_matches_json_schema(client: Any) -> None:
    response = client.get(API_STRUCTURE_PATH)

    assert response.status_code == 400
    _assert_matches_schema(response.json(), ERROR_RESPONSE_SCHEMA)


def test_structure_endpoint_method_not_allowed_error_matches_json_schema(client: Any) -> None:
    response = client.post(API_STRUCTURE_PATH, data=json.dumps({"psid": 123}), content_type="application/json")

    assert response.status_code == 405
    _assert_matches_schema(response.json(), ERROR_RESPONSE_SCHEMA)


@patch("petscan.views.petscan_service.execute_query")
def test_sparql_endpoint_ask_response_matches_json_schema(execute_query: Any, client: Any) -> None:
    execute_query.return_value = _ask_execution_result()

    response = client.get(SPARQL_PATH, data={"query": ASK_QUERY})

    assert response.status_code == 200
    _assert_matches_schema(json.loads(response.content.decode("utf-8")), ASK_RESULTS_SCHEMA)


@patch("petscan.views.petscan_service.execute_query")
def test_sparql_endpoint_select_response_matches_json_schema(execute_query: Any, client: Any) -> None:
    execute_query.return_value = {
        "query_type": "SELECT",
        "result_format": "sparql-json",
        "sparql_json": {
            "head": {"vars": ["item", "title", "ns"]},
            "results": {
                "bindings": [
                    {
                        "item": {"type": "uri", "value": "https://fi.wikipedia.org/wiki/Turku"},
                        "title": {"type": "literal", "value": "Turku", "xml:lang": "fi"},
                        "ns": {
                            "type": "literal",
                            "value": "0",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                        },
                    }
                ]
            },
        },
        "meta": {},
    }

    response = client.post(
        SPARQL_FEDERATED_PATH + "&refresh=1",
        data=urlencode({"query": FEDERATED_SUBQUERY}),
        content_type="application/x-www-form-urlencoded",
    )

    assert response.status_code == 200
    _assert_matches_schema(json.loads(response.content.decode("utf-8")), SELECT_RESULTS_SCHEMA)
