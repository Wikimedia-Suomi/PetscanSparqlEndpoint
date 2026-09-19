import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, cast
from urllib.parse import parse_qs

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from . import service as jsonstat_service
from . import service_source as source

logger = logging.getLogger(__name__)
_ViewFunc = TypeVar("_ViewFunc", bound=Callable[..., HttpResponse])
_MAX_SPARQL_QUERY_BYTES = 500 * 1024
_SPARQL_QUERY_SIZE_ERROR = "SPARQL query must be at most 500 KB."


def _csrf_exempt(view_func: _ViewFunc) -> _ViewFunc:
    return cast(_ViewFunc, csrf_exempt(view_func))


@dataclass(frozen=True)
class RequestContext:
    source_url: str
    refresh: bool


@dataclass(frozen=True)
class SparqlRequest(RequestContext):
    query: str


def index(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "jsonstat.html",
        {"jsonstat_allowed_source_domains": source.allowed_source_domains()},
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
        source_url=source.normalize_source_url(request.GET.get("url")),
        refresh=_parse_bool(request.GET.get("refresh"), default=False),
    )


def _parse_path_request_context(service_params: str) -> RequestContext:
    raw = str(service_params or "").strip().lstrip("/")
    if not raw:
        raise ValueError("Path parameters are required. Use /jsonstat/sparql/source=<token>.")
    parsed = parse_qs(raw, keep_blank_values=False)
    token_values = [str(value).strip() for value in parsed.get("source", []) if str(value).strip()]
    if not token_values:
        raise ValueError("A JSON-stat source token is required in path parameters.")
    refresh_values = [
        str(value).strip() for value in parsed.get("refresh", []) if str(value).strip()
    ]
    return RequestContext(
        source_url=source.decode_source_token(token_values[-1]),
        refresh=_parse_bool(refresh_values[-1] if refresh_values else None, default=False),
    )


def _parse_sparql_query(request: HttpRequest) -> str:
    if request.method == "GET":
        query = request.GET.get("query")
        return _validate_sparql_query_size(str(query) if query is not None else "").strip()

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
        return _validate_sparql_query_size(str(query) if query is not None else "").strip()
    raise ValueError(
        "POST /jsonstat/sparql requires Content-Type: application/sparql-query "
        "or application/x-www-form-urlencoded."
    )


def _parse_sparql_request(request: HttpRequest, service_params: str) -> SparqlRequest:
    context = _parse_path_request_context(service_params)
    query = _parse_sparql_query(request)
    if not query:
        raise ValueError("A SPARQL query is required.")
    return SparqlRequest(
        source_url=context.source_url,
        refresh=context.refresh,
        query=query,
    )


def _add_cors_headers(response: HttpResponse) -> HttpResponse:
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response["Access-Control-Allow-Headers"] = "Content-Type, Accept"
    return response


def structure_endpoint(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return _json_error("Method not allowed. Use GET.", status=405)
    try:
        context = _parse_request_context(request)
        source_token = source.encode_source_token(context.source_url)
        meta = jsonstat_service.ensure_loaded(context.source_url, refresh=context.refresh)
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except jsonstat_service.PetscanServiceError as exc:
        return _json_error(_public_service_error_message(exc, request.path), status=502)
    return JsonResponse(
        {
            "url": context.source_url,
            "source_token": source_token,
            "meta": meta,
        }
    )


@_csrf_exempt
def sparql_endpoint(request: HttpRequest, service_params: str) -> HttpResponse:
    if request.method == "OPTIONS":
        return _add_cors_headers(HttpResponse(status=204))
    if request.method not in {"GET", "POST"}:
        return _add_cors_headers(_text_error("Method not allowed. Use GET or POST.", status=405))
    try:
        parsed = _parse_sparql_request(request, service_params)
        execution = jsonstat_service.execute_query(
            parsed.source_url,
            parsed.query,
            refresh=parsed.refresh,
        )
    except ValueError as exc:
        return _add_cors_headers(_text_error(str(exc), status=400))
    except jsonstat_service.PetscanServiceError as exc:
        return _add_cors_headers(
            _text_error(_public_service_error_message(exc, request.path), status=502)
        )

    if execution["result_format"] == "sparql-json":
        response = HttpResponse(
            json.dumps(execution["sparql_json"]),
            content_type="application/sparql-results+json; charset=utf-8",
        )
        return _add_cors_headers(response)
    response = HttpResponse(
        execution["ntriples"],
        content_type="application/n-triples; charset=utf-8",
    )
    return _add_cors_headers(response)
