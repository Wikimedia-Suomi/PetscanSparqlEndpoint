import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, TypeVar, cast
from urllib.parse import parse_qs

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from . import service as petscan_service

logger = logging.getLogger(__name__)
_ViewFunc = TypeVar("_ViewFunc", bound=Callable[..., HttpResponse])


def _csrf_exempt(view_func: _ViewFunc) -> _ViewFunc:
    return cast(_ViewFunc, csrf_exempt(view_func))


@dataclass(frozen=True)
class RequestContext:
    psid: int
    refresh: bool
    petscan_params: Dict[str, List[str]]


@dataclass(frozen=True)
class SparqlRequest(RequestContext):
    query: str


def index(request: HttpRequest) -> HttpResponse:
    return render(request, "index.html")


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


def _text_error(message: str, status: int = 400) -> HttpResponse:
    return HttpResponse(message, status=status, content_type="text/plain; charset=utf-8")


def _public_service_error_message(exc: Exception, path: str) -> str:
    public_message = getattr(exc, "public_message", None)
    if isinstance(public_message, str) and public_message.strip():
        logger.exception("Returning sanitized backend error response for %s", path)
        return public_message
    return str(exc)


def _extract_forwarded_petscan_params(request: HttpRequest) -> Dict[str, List[str]]:
    forwarded = {}  # type: Dict[str, List[str]]
    for key in request.GET.keys():
        if key.lower() in {"psid", "query", "refresh"}:
            continue
        values = [str(value).strip() for value in request.GET.getlist(key) if str(value).strip()]
        if values:
            forwarded[key] = values
    return forwarded


def _parse_request_context(request: HttpRequest) -> RequestContext:
    return RequestContext(
        psid=_parse_psid(request.GET.get("psid")),
        refresh=_parse_bool(request.GET.get("refresh"), default=False),
        petscan_params=_extract_forwarded_petscan_params(request),
    )


def _parse_path_request_context(service_params: str) -> RequestContext:
    raw = str(service_params or "").strip().lstrip("/")
    if not raw:
        raise ValueError("Path parameters are required. Use /petscan/sparql/psid=<id>[&key=value...]")

    parsed = parse_qs(raw, keep_blank_values=False)
    psid_values = [str(value).strip() for value in parsed.get("psid", []) if str(value).strip()]
    if not psid_values:
        raise ValueError("A numeric psid is required in path parameters.")
    psid = _parse_psid(psid_values[-1])

    refresh_values = [str(value).strip() for value in parsed.get("refresh", []) if str(value).strip()]
    refresh = _parse_bool(refresh_values[-1] if refresh_values else None, default=False)

    forwarded = {}  # type: Dict[str, List[str]]
    for key, values in parsed.items():
        if key.lower() in {"psid", "query", "refresh", "format"}:
            continue
        normalized_values = [str(value).strip() for value in values if str(value).strip()]
        if normalized_values:
            forwarded[key] = normalized_values

    return RequestContext(psid=psid, refresh=refresh, petscan_params=forwarded)


def _parse_sparql_query(request: HttpRequest) -> str:
    if request.method == "GET":
        query = request.GET.get("query")
        return str(query).strip() if query is not None else ""

    raw_content_type = str(request.headers.get("Content-Type", "") or request.META.get("CONTENT_TYPE", "")).strip()
    content_type = (request.content_type or "").split(";", 1)[0].strip().lower()
    if content_type == "application/sparql-query":
        body = bytes(request.body)
        try:
            return body.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise ValueError("SPARQL query body must be valid UTF-8.") from exc

    if content_type == "application/x-www-form-urlencoded":
        query = request.POST.get("query")
        return str(query).strip() if query is not None else ""

    logger.warning(
        (
            "[sparql-content-type-debug] Rejected POST /petscan/sparql due to unsupported Content-Type. "
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
        "POST /petscan/sparql requires Content-Type: application/sparql-query or application/x-www-form-urlencoded."
    )


def _parse_sparql_request(request: HttpRequest, service_params: str) -> SparqlRequest:
    context = _parse_path_request_context(service_params)
    query = _parse_sparql_query(request)
    if not query:
        raise ValueError("query must not be empty.")
    return SparqlRequest(
        psid=context.psid,
        refresh=context.refresh,
        petscan_params=context.petscan_params,
        query=query,
    )


@_csrf_exempt
def structure_endpoint(request: HttpRequest) -> JsonResponse:
    if request.method != "GET":
        return _json_error("Method not allowed. Use GET.", status=405)

    try:
        request_context = _parse_request_context(request)
        meta = petscan_service.ensure_loaded(
            request_context.psid,
            refresh=request_context.refresh,
            petscan_params=request_context.petscan_params,
        )
    except ValueError as exc:
        return _json_error(str(exc), status=400)
    except petscan_service.PetscanServiceError as exc:
        return _json_error(_public_service_error_message(exc, request.path), status=502)

    return JsonResponse({"psid": request_context.psid, "meta": meta})


def _add_cors_headers(response: HttpResponse) -> HttpResponse:
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response["Access-Control-Allow-Headers"] = "Content-Type, Accept"
    return response


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
        execution = petscan_service.execute_query(
            parsed_request.psid,
            parsed_request.query,
            refresh=parsed_request.refresh,
            petscan_params=parsed_request.petscan_params,
        )
    except ValueError as exc:
        return _add_cors_headers(_text_error(str(exc), status=400))
    except petscan_service.PetscanServiceError as exc:
        return _add_cors_headers(_text_error(_public_service_error_message(exc, request.path), status=502))

    if execution["result_format"] == "sparql-json":
        body = json.dumps(execution["sparql_json"])  # SPARQL Results JSON format
        response = HttpResponse(body, content_type="application/sparql-results+json; charset=utf-8")
        return _add_cors_headers(response)

    response = HttpResponse(execution["ntriples"], content_type="application/n-triples; charset=utf-8")
    return _add_cors_headers(response)
