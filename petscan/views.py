import json
from typing import Any, Dict, Tuple

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from . import service as petscan_service


def index(request: HttpRequest) -> HttpResponse:
    return render(request, "index.html")


def _parse_json_body(request: HttpRequest) -> Dict[str, Any]:
    if not request.body:
        return {}
    try:
        decoded = request.body.decode("utf-8")
        return json.loads(decoded)
    except Exception as exc:
        raise ValueError("Request body must be valid JSON.") from exc


def _parse_psid(value: Any) -> int:
    if value is None or str(value).strip() == "":
        raise ValueError("A numeric psid is required.")
    try:
        psid = int(str(value).strip())
    except Exception as exc:
        raise ValueError("psid must be an integer.") from exc
    if psid <= 0:
        raise ValueError("psid must be greater than zero.")
    return psid


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _json_error(message: str, status: int = 400) -> JsonResponse:
    return JsonResponse({"error": message}, status=status)


@csrf_exempt
def load_psid(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return _json_error("Method not allowed. Use POST.", status=405)

    try:
        payload = _parse_json_body(request)
        psid = _parse_psid(payload.get("psid"))
        refresh = _parse_bool(payload.get("refresh"), default=False)
        meta = petscan_service.ensure_loaded(psid, refresh=refresh)
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except petscan_service.PetscanServiceError as exc:
        return _json_error(str(exc), status=502)

    return JsonResponse({"psid": psid, "meta": meta})


@csrf_exempt
def run_query(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return _json_error("Method not allowed. Use POST.", status=405)

    try:
        payload = _parse_json_body(request)
        psid = _parse_psid(payload.get("psid"))
        query = str(payload.get("query", "")).strip()
        if not query:
            raise ValueError("query must not be empty.")
        refresh = _parse_bool(payload.get("refresh"), default=False)

        execution = petscan_service.execute_query(psid, query, refresh=refresh)
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except petscan_service.PetscanServiceError as exc:
        return _json_error(str(exc), status=502)

    if execution["result_format"] == "sparql-json":
        result_payload = execution["sparql_json"]
    else:
        result_payload = execution["ntriples"]

    return JsonResponse(
        {
            "psid": psid,
            "query_type": execution["query_type"],
            "result_format": execution["result_format"],
            "result": result_payload,
            "meta": execution["meta"],
        }
    )


def _parse_sparql_request(request: HttpRequest) -> Tuple[Any, str, bool]:
    psid_value = request.GET.get("psid")
    query = request.GET.get("query", "").strip()
    refresh = _parse_bool(request.GET.get("refresh"), default=False)

    if request.method == "POST":
        content_type = (request.content_type or "").split(";", 1)[0].strip().lower()

        if content_type == "application/sparql-query":
            body_query = request.body.decode("utf-8").strip()
            if body_query:
                query = body_query
            if psid_value is None:
                psid_value = request.headers.get("X-Petscan-Psid")

        elif content_type == "application/json":
            payload = _parse_json_body(request)
            if payload.get("query") is not None:
                query = str(payload.get("query")).strip()
            if payload.get("psid") is not None:
                psid_value = payload.get("psid")
            refresh = _parse_bool(payload.get("refresh"), default=refresh)

        else:
            if request.POST.get("query"):
                query = request.POST.get("query", "").strip()
            if request.POST.get("psid"):
                psid_value = request.POST.get("psid")
            refresh = _parse_bool(request.POST.get("refresh"), default=refresh)

    return psid_value, query, refresh


def _add_cors_headers(response: HttpResponse) -> HttpResponse:
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response["Access-Control-Allow-Headers"] = "Content-Type, Accept, X-Petscan-Psid"
    return response


@csrf_exempt
def sparql_endpoint(request: HttpRequest) -> HttpResponse:
    if request.method == "OPTIONS":
        response = HttpResponse(status=204)
        return _add_cors_headers(response)

    if request.method not in {"GET", "POST"}:
        response = HttpResponse("Method not allowed. Use GET or POST.", status=405)
        return _add_cors_headers(response)

    try:
        psid_value, query, refresh = _parse_sparql_request(request)
        psid = _parse_psid(psid_value)
        if not query:
            raise ValueError("query must not be empty.")

        execution = petscan_service.execute_query(psid, query, refresh=refresh)
    except ValueError as exc:
        response = HttpResponse(str(exc), status=400, content_type="text/plain; charset=utf-8")
        return _add_cors_headers(response)
    except petscan_service.PetscanServiceError as exc:
        response = HttpResponse(str(exc), status=502, content_type="text/plain; charset=utf-8")
        return _add_cors_headers(response)

    if execution["result_format"] == "sparql-json":
        body = json.dumps(execution["sparql_json"])  # SPARQL Results JSON format
        response = HttpResponse(body, content_type="application/sparql-results+json; charset=utf-8")
        return _add_cors_headers(response)

    response = HttpResponse(execution["ntriples"], content_type="application/n-triples; charset=utf-8")
    return _add_cors_headers(response)
