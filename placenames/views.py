import json
import logging
from dataclasses import dataclass
from typing import Callable, TypeVar, cast
from urllib.parse import parse_qs

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from . import service

logger = logging.getLogger(__name__)
_ViewFunc = TypeVar("_ViewFunc", bound=Callable[..., HttpResponse])
_MAX_SPARQL_QUERY_BYTES = 500 * 1024
_SPARQL_QUERY_SIZE_ERROR = "SPARQL query must be at most 500 KB."
_SERVICE_FAILURE_PUBLIC_MESSAGE = "Local place-name service is unavailable."


def _csrf_exempt(view_func: _ViewFunc) -> _ViewFunc:
    return cast(_ViewFunc, csrf_exempt(view_func))


@dataclass(frozen=True)
class SparqlRequest:
    dataset: str
    query: str


def index(request: HttpRequest) -> HttpResponse:
    return render(request, "placenames.html")


def _json_error(message: str, status: int = 400) -> JsonResponse:
    return JsonResponse({"error": message}, status=status)


def _text_error(message: str, status: int = 400) -> HttpResponse:
    return HttpResponse(message, status=status, content_type="text/plain; charset=utf-8")


def _public_service_error_message(exc: Exception, path: str) -> str:
    logger.exception("Place-name backend error for %s", path)
    public_message = getattr(exc, "public_message", None)
    if isinstance(public_message, str) and public_message.strip():
        return public_message
    if settings.DEBUG:
        return str(exc)
    return _SERVICE_FAILURE_PUBLIC_MESSAGE


def _dataset_from_path(service_params: str) -> str:
    raw = str(service_params or "").strip().lstrip("/")
    if not raw:
        raise ValueError("Path parameters are required. Use /placenames/sparql/dataset=saami.")
    parsed = parse_qs(raw, keep_blank_values=False)
    values = [value.strip() for value in parsed.get("dataset", []) if value.strip()]
    if len(values) != 1:
        raise ValueError("Exactly one dataset path parameter is required.")
    unexpected = sorted(key for key in parsed if key.lower() not in {"dataset", "format"})
    if unexpected:
        raise ValueError(f"Unsupported path parameter: {unexpected[0]}.")
    return values[0]


def _validate_query_size(query: str) -> str:
    if len(query.encode("utf-8")) > _MAX_SPARQL_QUERY_BYTES:
        raise ValueError(_SPARQL_QUERY_SIZE_ERROR)
    return query


def _parse_query(request: HttpRequest) -> str:
    if request.method == "GET":
        value = request.GET.get("query")
        return _validate_query_size(str(value) if value is not None else "").strip()

    content_type = (request.content_type or "").split(";", 1)[0].strip().lower()
    if content_type == "application/sparql-query":
        body = bytes(request.body)
        if len(body) > _MAX_SPARQL_QUERY_BYTES:
            raise ValueError(_SPARQL_QUERY_SIZE_ERROR)
        try:
            return body.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise ValueError("SPARQL query body must be valid UTF-8.") from exc
    if content_type == "application/x-www-form-urlencoded":
        value = request.POST.get("query")
        return _validate_query_size(str(value) if value is not None else "").strip()
    raise ValueError(
        "POST /placenames/sparql requires Content-Type: application/sparql-query "
        "or application/x-www-form-urlencoded."
    )


def _parse_sparql_request(request: HttpRequest, service_params: str) -> SparqlRequest:
    query = _parse_query(request)
    if not query:
        raise ValueError("query must not be empty.")
    return SparqlRequest(dataset=_dataset_from_path(service_params), query=query)


def _add_cors_headers(response: HttpResponse) -> HttpResponse:
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response["Access-Control-Allow-Headers"] = "Content-Type, Accept"
    return response


@_csrf_exempt
def structure_endpoint(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return _json_error("Method not allowed. Use GET.", status=405)
    dataset = str(request.GET.get("dataset", "")).strip()
    try:
        meta = service.ensure_loaded(dataset)
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except service.PetscanServiceError as exc:
        return _json_error(_public_service_error_message(exc, request.path), status=503)
    return JsonResponse({"source": "placenames", "dataset": dataset.lower(), "meta": meta})


@_csrf_exempt
def sparql_endpoint(request: HttpRequest, service_params: str) -> HttpResponse:
    if request.method == "OPTIONS":
        return _add_cors_headers(HttpResponse(status=204))
    if request.method not in {"GET", "POST"}:
        return _add_cors_headers(HttpResponse("Method not allowed. Use GET or POST.", status=405))
    try:
        parsed = _parse_sparql_request(request, service_params)
        execution = service.execute_query(parsed.dataset, parsed.query)
    except ValueError as exc:
        return _add_cors_headers(_text_error(str(exc), status=400))
    except service.PetscanServiceError as exc:
        message = _public_service_error_message(exc, request.path)
        return _add_cors_headers(_text_error(message, status=503))

    if execution["result_format"] == "sparql-json":
        response = HttpResponse(
            json.dumps(execution["sparql_json"]),
            content_type="application/sparql-results+json; charset=utf-8",
        )
        return _add_cors_headers(response)
    response = HttpResponse(
        execution["ntriples"], content_type="application/n-triples; charset=utf-8"
    )
    return _add_cors_headers(response)
