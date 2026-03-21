import json
from typing import Any, Dict
from unittest.mock import patch
from urllib.parse import urlencode

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


def _response_contract(response) -> Dict[str, Any]:
    contract: Dict[str, Any] = {
        "status_code": response.status_code,
        "content_type": response["Content-Type"],
    }

    if "Access-Control-Allow-Origin" in response:
        contract["cors"] = {
            "allow_origin": response["Access-Control-Allow-Origin"],
            "allow_methods": response["Access-Control-Allow-Methods"],
            "allow_headers": response["Access-Control-Allow-Headers"],
        }

    decoded_body = response.content.decode("utf-8")
    if response["Content-Type"].startswith(("application/json", "application/sparql-results+json")):
        contract["body"] = json.loads(decoded_body)
    else:
        contract["body"] = decoded_body

    return contract


def _ask_execution_result() -> Dict[str, Any]:
    return {
        "query_type": "ASK",
        "result_format": "sparql-json",
        "sparql_json": {"head": {}, "boolean": True},
        "meta": {},
    }


@patch("petscan.views.petscan_service.ensure_loaded")
def test_structure_endpoint_success_snapshot(ensure_loaded, client, snapshot):
    ensure_loaded.return_value = {
        "psid": 123,
        "records": 2,
        "source_url": "https://petscan.wmcloud.org/?psid=123&format=json",
        "loaded_at": "2026-03-13T10:00:00+00:00",
        "source_params": {"category": ["Turku"]},
        "structure": {"row_count": 2, "field_count": 1, "fields": []},
    }

    response = client.get(
        API_STRUCTURE_PATH,
        data={"psid": 123, "refresh": "1", "category": "Turku"},
    )

    ensure_loaded.assert_called_once_with(
        123,
        refresh=True,
        petscan_params={"category": ["Turku"]},
    )
    assert _response_contract(response) == snapshot


def test_structure_endpoint_validation_error_snapshot(client, snapshot):
    response = client.get(API_STRUCTURE_PATH)

    assert _response_contract(response) == snapshot


@patch("petscan.views.petscan_service.execute_query")
def test_sparql_endpoint_get_success_snapshot(execute_query, client, snapshot):
    execute_query.return_value = _ask_execution_result()

    response = client.get(
        SPARQL_PATH,
        data={"query": ASK_QUERY},
    )

    execute_query.assert_called_once_with(123, ASK_QUERY, refresh=False, petscan_params={})
    assert _response_contract(response) == snapshot


@patch("petscan.views.petscan_service.execute_query")
def test_sparql_endpoint_form_post_success_snapshot(execute_query, client, snapshot):
    execute_query.return_value = {
        "query_type": "SELECT",
        "result_format": "sparql-json",
        "sparql_json": {"head": {"vars": ["item", "title"]}, "results": {"bindings": []}},
        "meta": {},
    }

    response = client.post(
        SPARQL_FEDERATED_PATH + "&refresh=1",
        data=urlencode({"query": FEDERATED_SUBQUERY}),
        content_type="application/x-www-form-urlencoded",
    )

    execute_query.assert_called_once_with(
        43641756,
        FEDERATED_SUBQUERY,
        refresh=True,
        petscan_params={"categories": ["Turku"]},
    )
    assert _response_contract(response) == snapshot


@patch("petscan.views.petscan_service.execute_query")
def test_sparql_endpoint_service_clause_error_snapshot(execute_query, client, snapshot):
    execute_query.side_effect = ValueError("SERVICE clauses are not allowed in this endpoint.")

    response = client.get(
        SPARQL_PATH,
        data={"query": "SELECT * WHERE { SERVICE <https://x> { ?s ?p ?o } }"},
    )

    assert _response_contract(response) == snapshot
