import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, cast
from urllib.parse import parse_qs

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from petscan_endpoint.example_queries import build_pagepile_example_query_url

from . import service as pagepile_service
from . import service_source

logger = logging.getLogger(__name__)
_ViewFunc = TypeVar("_ViewFunc", bound=Callable[..., HttpResponse])
_MAX_SPARQL_QUERY_BYTES = 500 * 1024
_SPARQL_QUERY_SIZE_ERROR = "SPARQL query must be at most 500 KB."


def _csrf_exempt(view_func: _ViewFunc) -> _ViewFunc:
    return cast(_ViewFunc, csrf_exempt(view_func))


@dataclass(frozen=True)
class RequestContext:
    pagepile_id: int
    refresh: bool
    limit: int | None


@dataclass(frozen=True)
class SparqlRequest(RequestContext):
    query: str


def index(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "pagepile.html",
        {
            "pagepile_example_query_url": build_pagepile_example_query_url(),
        },
    )


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


def _text_error(message: str, status: int = 400) -> HttpResponse:
    return HttpResponse(message, status=status, content_type="text/plain; charset=utf-8")


def _public_service_error_message(exc: Exception, path: str) -> str:
    public_message = getattr(exc, "public_message", None)
    if isinstance(public_message, str) and public_message.strip():
        logger.exception("Returning sanitized backend error response for %s", path)
        return public_message
    return str(exc)


def _validate_sparql_query_size(query: str) -> str:
    if len(query.encode("utf-8")) > _MAX_SPARQL_QUERY_BYTES:
        raise ValueError(_SPARQL_QUERY_SIZE_ERROR)
    return query


def _parse_request_context(request: HttpRequest) -> RequestContext:
    return RequestContext(
        pagepile_id=service_source.normalize_pagepile_id(request.GET.get("pagepile_id")),
        refresh=_parse_bool(request.GET.get("refresh"), default=False),
        limit=service_source.normalize_load_limit(request.GET.get("limit")),
    )


def _parse_path_request_context(service_params: str) -> RequestContext:
    raw = str(service_params or "").strip().lstrip("/")
    if not raw:
        raise ValueError(
            "Path parameters are required. Use /pagepile/sparql/pagepile_id=<id>[&limit=<n>]"
        )

    parsed = parse_qs(raw, keep_blank_values=False)
    pagepile_id_values = [
        str(value).strip() for value in parsed.get("pagepile_id", []) if str(value).strip()
    ]
    if not pagepile_id_values:
        raise ValueError("A numeric pagepile_id is required in path parameters.")

    refresh_values = [str(value).strip() for value in parsed.get("refresh", []) if str(value).strip()]
    limit_values = [str(value).strip() for value in parsed.get("limit", []) if str(value).strip()]
    return RequestContext(
        pagepile_id=service_source.normalize_pagepile_id(pagepile_id_values[-1]),
        refresh=_parse_bool(refresh_values[-1] if refresh_values else None, default=False),
        limit=service_source.normalize_load_limit(limit_values[-1] if limit_values else None),
    )


def _parse_sparql_query(request: HttpRequest) -> str:
    if request.method == "GET":
        query = request.GET.get("query")
        text = str(query) if query is not None else ""
        return _validate_sparql_query_size(text).strip()

    raw_content_type = str(request.headers.get("Content-Type", "") or request.META.get("CONTENT_TYPE", "")).strip()
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
        query = request.POST.get("query")
        text = str(query) if query is not None else ""
        return _validate_sparql_query_size(text).strip()

    logger.warning(
        (
            "[sparql-content-type-debug] Rejected POST /pagepile/sparql due to unsupported Content-Type. "
            "parsed_content_type=%r raw_content_type=%r method=%s path=%s query_string=%r "
            "accept=%r user_agent=%r content_length=%r"
        ),
        content_type,
        raw_content_type,
        request.method,
        request.path,
        request.META.get("QUERY_STRING", ""),
        request.headers.get("Accept", ""),
        request.headers.get("User-Agent", ""),
        request.META.get("CONTENT_LENGTH", ""),
    )
    raise ValueError(
        "POST /pagepile/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded."
    )


def _parse_sparql_request(request: HttpRequest, service_params: str) -> SparqlRequest:
    context = _parse_path_request_context(service_params)
    query = _parse_sparql_query(request)
    if not query:
        raise ValueError("query must not be empty.")
    return SparqlRequest(
        pagepile_id=context.pagepile_id,
        refresh=context.refresh,
        limit=context.limit,
        query=query,
    )


def _add_cors_headers(response: HttpResponse) -> HttpResponse:
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response["Access-Control-Allow-Headers"] = "Content-Type, Accept"
    return response


@_csrf_exempt
def structure_endpoint(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return _json_error("Method not allowed. Use GET.", status=405)

    try:
        request_context = _parse_request_context(request)
        meta = pagepile_service.ensure_loaded(
            request_context.pagepile_id,
            refresh=request_context.refresh,
            limit=request_context.limit,
        )
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except pagepile_service.PetscanServiceError as exc:
        return _json_error(_public_service_error_message(exc, request.path), status=502)

    return JsonResponse(
        {
            "source": "pagepile",
            "pagepile_id": request_context.pagepile_id,
            "limit": request_context.limit,
            "meta": meta,
        }
    )


@_csrf_exempt
def sparql_endpoint(request: HttpRequest, service_params: str) -> HttpResponse:
    if request.method == "OPTIONS":
        response = HttpResponse(status=204)
        return _add_cors_headers(response)

    if request.method not in {"GET", "POST"}:
        response = HttpResponse("Method not allowed. Use GET or POST.", status=405)
        return _add_cors_headers(response)

    try:
        parsed_request = _parse_sparql_request(request, service_params)
        execution = pagepile_service.execute_query(
            parsed_request.pagepile_id,
            parsed_request.query,
            refresh=parsed_request.refresh,
            limit=parsed_request.limit,
        )
    except ValueError as exc:
        return _add_cors_headers(_text_error(str(exc), status=400))
    except pagepile_service.PetscanServiceError as exc:
        return _add_cors_headers(_text_error(_public_service_error_message(exc, request.path), status=502))

    if execution["result_format"] == "sparql-json":
        body = json.dumps(execution["sparql_json"])
        response = HttpResponse(body, content_type="application/sparql-results+json; charset=utf-8")
        return _add_cors_headers(response)

    response = HttpResponse(execution["ntriples"], content_type="application/n-triples; charset=utf-8")
    return _add_cors_headers(response)
