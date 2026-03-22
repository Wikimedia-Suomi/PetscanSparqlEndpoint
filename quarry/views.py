import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping, TypeVar, cast
from urllib.parse import parse_qs

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from . import service as quarry_service
from . import service_source as quarry_source

logger = logging.getLogger(__name__)
_ViewFunc = TypeVar("_ViewFunc", bound=Callable[..., HttpResponse])


def _csrf_exempt(view_func: _ViewFunc) -> _ViewFunc:
    return cast(_ViewFunc, csrf_exempt(view_func))


@dataclass(frozen=True)
class RequestContext:
    quarry_id: int
    refresh: bool
    limit: int | None


@dataclass(frozen=True)
class SparqlRequest(RequestContext):
    query: str


def index(request: HttpRequest) -> HttpResponse:
    return render(request, "quarry.html")


def _parse_quarry_id(value: Any) -> int:
    if value is None or str(value).strip() == "":
        raise ValueError("A numeric quarry_id is required.")
    try:
        quarry_id = int(str(value).strip())
    except Exception as exc:
        raise ValueError("quarry_id must be an integer.") from exc
    if quarry_id <= 0:
        raise ValueError("quarry_id must be greater than zero.")
    return quarry_id


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


def _parse_limit(value: Any) -> int | None:
    return quarry_source.normalize_load_limit(value)


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
        quarry_id=_parse_quarry_id(request.GET.get("quarry_id")),
        refresh=_parse_bool(request.GET.get("refresh"), default=False),
        limit=_parse_limit(request.GET.get("limit")),
    )


def _parse_path_request_context(service_params: str) -> RequestContext:
    raw = str(service_params or "").strip().lstrip("/")
    if not raw:
        raise ValueError("Path parameters are required. Use /quarry/sparql/quarry_id=<id>[&limit=<n>]")

    parsed = parse_qs(raw, keep_blank_values=False)
    quarry_id_values = [str(value).strip() for value in parsed.get("quarry_id", []) if str(value).strip()]
    if not quarry_id_values:
        raise ValueError("A numeric quarry_id is required in path parameters.")
    quarry_id = _parse_quarry_id(quarry_id_values[-1])

    refresh_values = [str(value).strip() for value in parsed.get("refresh", []) if str(value).strip()]
    refresh = _parse_bool(refresh_values[-1] if refresh_values else None, default=False)

    limit_values = [str(value).strip() for value in parsed.get("limit", []) if str(value).strip()]
    limit = _parse_limit(limit_values[-1] if limit_values else None)

    return RequestContext(quarry_id=quarry_id, refresh=refresh, limit=limit)


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
            "[sparql-content-type-debug] Rejected POST /quarry/sparql due to unsupported Content-Type. "
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
        "POST /quarry/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded."
    )


def _parse_sparql_request(request: HttpRequest, service_params: str) -> SparqlRequest:
    context = _parse_path_request_context(service_params)
    query = _parse_sparql_query(request)
    if not query:
        raise ValueError("query must not be empty.")
    return SparqlRequest(
        quarry_id=context.quarry_id,
        refresh=context.refresh,
        limit=context.limit,
        query=query,
    )


def _source_param_value_from_meta(meta: Mapping[str, Any], key: str) -> str | None:
    source_params = meta.get("source_params")
    if not isinstance(source_params, Mapping):
        return None
    values = source_params.get(key)
    if not isinstance(values, list) or not values:
        return None
    value = str(values[-1]).strip()
    return value or None


def _qrun_id_from_meta(meta: Mapping[str, Any]) -> int | None:
    raw_value = _source_param_value_from_meta(meta, "qrun_id")
    if raw_value is None:
        return None
    try:
        qrun_id = int(raw_value)
    except (TypeError, ValueError):
        return None
    return qrun_id if qrun_id > 0 else None


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
        meta = quarry_service.ensure_loaded(
            request_context.quarry_id,
            refresh=request_context.refresh,
            limit=request_context.limit,
        )
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except quarry_service.PetscanServiceError as exc:
        return _json_error(_public_service_error_message(exc, request.path), status=502)

    qrun_id = _qrun_id_from_meta(meta)
    return JsonResponse(
        {
            "quarry_id": request_context.quarry_id,
            "qrun_id": qrun_id,
            "query_db": _source_param_value_from_meta(meta, "query_db"),
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
        execution = quarry_service.execute_query(
            parsed_request.quarry_id,
            parsed_request.query,
            refresh=parsed_request.refresh,
            limit=parsed_request.limit,
        )
    except ValueError as exc:
        return _add_cors_headers(_text_error(str(exc), status=400))
    except quarry_service.PetscanServiceError as exc:
        return _add_cors_headers(_text_error(_public_service_error_message(exc, request.path), status=502))

    if execution["result_format"] == "sparql-json":
        body = json.dumps(execution["sparql_json"])
        response = HttpResponse(body, content_type="application/sparql-results+json; charset=utf-8")
        return _add_cors_headers(response)

    response = HttpResponse(execution["ntriples"], content_type="application/n-triples; charset=utf-8")
    return _add_cors_headers(response)
