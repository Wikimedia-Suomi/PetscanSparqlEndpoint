import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, cast
from urllib.parse import parse_qs

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from petscan_endpoint.example_queries import build_incubator_example_query_url

from . import service as incubator_service
from . import service_source

logger = logging.getLogger(__name__)
_ViewFunc = TypeVar("_ViewFunc", bound=Callable[..., HttpResponse])


def _csrf_exempt(view_func: _ViewFunc) -> _ViewFunc:
    return cast(_ViewFunc, csrf_exempt(view_func))


@dataclass(frozen=True)
class RequestContext:
    refresh: bool
    limit: int | None
    namespaces: list[int]
    page_latest: int | None
    page_prefixes: list[str]
    recentchanges_only: bool


@dataclass(frozen=True)
class SparqlRequest(RequestContext):
    query: str


def index(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "incubator.html",
        {
            "incubator_example_query_url": build_incubator_example_query_url(),
            "namespace_options": service_source.available_incubator_namespace_options(),
            "namespace_help_text": _namespace_help_text(),
            "replica_only_filters_enabled": _replica_only_filters_enabled(),
            "recentchanges_help_text": _recentchanges_help_text(),
        },
    )


def _replica_only_filters_enabled() -> bool:
    return service_source.incubator_lookup_backend() == service_source.LOOKUP_BACKEND_TOOLFORGE_SQL


def _recentchanges_help_text() -> str:
    if service_source.incubator_lookup_backend() == service_source.LOOKUP_BACKEND_TOOLFORGE_SQL:
        return "This uses recent changes from the last 30 days, including normal edits and page moves."
    return (
        "In API mode, it uses category member timestamps sorted from newest to oldest "
        "and stops when the 30-day window is exceeded."
    )


def _namespace_help_text() -> str:
    if service_source.incubator_lookup_backend() == service_source.LOOKUP_BACKEND_TOOLFORGE_SQL:
        return "Choose one or more namespaces to include in the results."
    return (
        "In API mode, namespace filtering is affected by MediaWiki miser mode, "
        "so the filter may miss valid matches or appear not to work."
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


def _parse_request_context(request: HttpRequest) -> RequestContext:
    return RequestContext(
        refresh=_parse_bool(request.GET.get("refresh"), default=False),
        limit=service_source.normalize_load_limit(request.GET.get("limit")),
        namespaces=service_source.normalize_namespaces(request.GET.getlist("namespace")),
        page_latest=service_source.normalize_page_latest(request.GET.get("page_latest")),
        page_prefixes=service_source.normalize_page_prefixes(request.GET.getlist("page_prefix")),
        recentchanges_only=_parse_bool(request.GET.get("recentchanges_only"), default=False),
    )


def _parse_path_request_context(service_params: str) -> RequestContext:
    raw = str(service_params or "").strip().lstrip("/")
    if not raw:
        return RequestContext(
            refresh=False,
            limit=None,
            namespaces=[],
            page_latest=None,
            page_prefixes=[],
            recentchanges_only=False,
        )

    parsed = parse_qs(raw, keep_blank_values=False)
    refresh_values = [str(value).strip() for value in parsed.get("refresh", []) if str(value).strip()]
    limit_values = [str(value).strip() for value in parsed.get("limit", []) if str(value).strip()]
    namespace_values = [str(value).strip() for value in parsed.get("namespace", []) if str(value).strip()]
    page_latest_values = [str(value).strip() for value in parsed.get("page_latest", []) if str(value).strip()]
    page_prefix_values = [str(value).strip() for value in parsed.get("page_prefix", []) if str(value).strip()]
    recentchanges_values = [
        str(value).strip()
        for value in parsed.get("recentchanges_only", [])
        if str(value).strip()
    ]
    return RequestContext(
        refresh=_parse_bool(refresh_values[-1] if refresh_values else None, default=False),
        limit=service_source.normalize_load_limit(limit_values[-1] if limit_values else None),
        namespaces=service_source.normalize_namespaces(namespace_values),
        page_latest=service_source.normalize_page_latest(
            page_latest_values[-1] if page_latest_values else None
        ),
        page_prefixes=service_source.normalize_page_prefixes(page_prefix_values),
        recentchanges_only=_parse_bool(
            recentchanges_values[-1] if recentchanges_values else None,
            default=False,
        ),
    )


def _parse_sparql_query(request: HttpRequest) -> str:
    if request.method == "GET":
        query = request.GET.get("query")
        return str(query).strip() if query is not None else ""

    raw_content_type = str(request.headers.get("Content-Type", "") or request.META.get("CONTENT_TYPE", "")).strip()
    content_type = (request.content_type or "").split(";", 1)[0].strip().lower()
    if content_type == "application/sparql-query":
        body = bytes(request.body)
        return body.decode("utf-8").strip()

    if content_type == "application/x-www-form-urlencoded":
        query = request.POST.get("query")
        return str(query).strip() if query is not None else ""

    logger.warning(
        (
            "[sparql-content-type-debug] Rejected POST /incubator/sparql due to unsupported Content-Type. "
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
        "POST /incubator/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded."
    )


def _parse_sparql_request(request: HttpRequest, service_params: str = "") -> SparqlRequest:
    context = _parse_path_request_context(service_params)
    query = _parse_sparql_query(request)
    if not query:
        raise ValueError("query must not be empty.")
    return SparqlRequest(
        refresh=context.refresh,
        limit=context.limit,
        namespaces=context.namespaces,
        page_latest=context.page_latest,
        page_prefixes=context.page_prefixes,
        recentchanges_only=context.recentchanges_only,
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
        meta = incubator_service.ensure_loaded(
            refresh=request_context.refresh,
            limit=request_context.limit,
            namespaces=request_context.namespaces,
            page_latest=request_context.page_latest,
            page_prefixes=request_context.page_prefixes,
            recentchanges_only=request_context.recentchanges_only,
        )
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except incubator_service.PetscanServiceError as exc:
        return _json_error(_public_service_error_message(exc, request.path), status=502)

    return JsonResponse(
        {
            "source": "incubator",
            "limit": request_context.limit,
            "namespaces": request_context.namespaces,
            "page_latest": request_context.page_latest,
            "page_prefixes": request_context.page_prefixes,
            "recentchanges_only": request_context.recentchanges_only,
            "meta": meta,
        }
    )


@_csrf_exempt
def sparql_endpoint(request: HttpRequest, service_params: str = "") -> HttpResponse:
    if request.method == "OPTIONS":
        response = HttpResponse(status=204)
        return _add_cors_headers(response)

    if request.method not in {"GET", "POST"}:
        response = HttpResponse("Method not allowed. Use GET or POST.", status=405)
        return _add_cors_headers(response)

    try:
        parsed_request = _parse_sparql_request(request, service_params=service_params)
        execution = incubator_service.execute_query(
            parsed_request.query,
            refresh=parsed_request.refresh,
            limit=parsed_request.limit,
            namespaces=parsed_request.namespaces,
            page_latest=parsed_request.page_latest,
            page_prefixes=parsed_request.page_prefixes,
            recentchanges_only=parsed_request.recentchanges_only,
        )
    except ValueError as exc:
        return _add_cors_headers(_text_error(str(exc), status=400))
    except incubator_service.PetscanServiceError as exc:
        return _add_cors_headers(_text_error(_public_service_error_message(exc, request.path), status=502))

    if execution["result_format"] == "sparql-json":
        body = json.dumps(execution["sparql_json"])
        response = HttpResponse(body, content_type="application/sparql-results+json; charset=utf-8")
        return _add_cors_headers(response)

    response = HttpResponse(execution["ntriples"], content_type="application/n-triples; charset=utf-8")
    return _add_cors_headers(response)
