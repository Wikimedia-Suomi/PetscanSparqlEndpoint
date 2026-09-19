"""Safe URL fetching and observation extraction for JSON-stat 2.0 datasets."""

import ipaddress
import json
import math
import re
import socket
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple, cast
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from django.conf import settings

from petscan.service_errors import PetscanServiceError
from petscan.service_source import HTTP_USER_AGENT

__all__ = [
    "allowed_source_domains",
    "decode_source_token",
    "encode_source_token",
    "extract_records",
    "fetch_jsonstat_json",
    "normalize_source_params",
    "normalize_source_url",
]

_DEFAULT_MAX_CELLS = 300_000
_DEFAULT_MAX_RESPONSE_BYTES = 50 * 1024 * 1024
_MAX_SOURCE_URL_LENGTH = 4096
_MAX_SOURCE_TOKEN_LENGTH = 300
_JSONSTAT_FETCH_PUBLIC_MESSAGE = "Failed to load JSON-stat data from the upstream service."
_PXWEB_QUERY_ID_PATTERN = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_READABLE_SOURCE_HOSTNAME_PATTERN = (
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
)
_PXWEB_SAVED_QUERY_PATH_RE = re.compile(
    r"^/PxWeb/sq/(?P<query_id>{})$".format(_PXWEB_QUERY_ID_PATTERN)
)
_READABLE_SOURCE_TOKEN_RE = re.compile(
    r"^(?P<hostname>{})_(?P<query_id>{})$".format(
        _READABLE_SOURCE_HOSTNAME_PATTERN,
        _PXWEB_QUERY_ID_PATTERN,
    )
)
_FIELD_NAME_RE = re.compile(r"[^0-9A-Za-z_]+")
_FIELD_UNDERSCORE_RUN_RE = re.compile(r"_+")
_RESERVED_RECORD_FIELDS = frozenset({"dataset", "position", "status", "value"})
_DISALLOWED_HOSTNAMES = frozenset({"localhost", "localhost.localdomain"})
_DISALLOWED_HOST_SUFFIXES = (".internal", ".invalid", ".local", ".localhost", ".test")
_UNIT_KEYS = ("base", "decimals", "label", "position", "symbol")


def _normalize_allowed_domain(value: Any) -> str:
    domain = str(value or "").strip().lower().strip(".")
    if not domain or any(character in domain for character in "/:@?#[]"):
        raise ValueError("JSONSTAT_ALLOWED_SOURCE_DOMAINS contains an invalid domain.")
    try:
        ascii_domain = domain.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("JSONSTAT_ALLOWED_SOURCE_DOMAINS contains an invalid domain.") from exc
    if not ascii_domain or "." not in ascii_domain:
        raise ValueError("JSONSTAT_ALLOWED_SOURCE_DOMAINS contains an invalid domain.")
    return ascii_domain


def allowed_source_domains() -> Tuple[str, ...]:
    configured = getattr(settings, "JSONSTAT_ALLOWED_SOURCE_DOMAINS", ("stat.fi",))
    if isinstance(configured, str):
        raw_domains: Sequence[Any] = configured.split(",")
    elif isinstance(configured, Sequence):
        raw_domains = configured
    else:
        raise ValueError("JSONSTAT_ALLOWED_SOURCE_DOMAINS must be a list of domains.")

    normalized = []  # type: List[str]
    for raw_domain in raw_domains:
        domain = _normalize_allowed_domain(raw_domain)
        if domain not in normalized:
            normalized.append(domain)
    if not normalized:
        raise ValueError("JSONSTAT_ALLOWED_SOURCE_DOMAINS must contain at least one domain.")
    return tuple(normalized)


def _host_is_allowed(hostname: str, allowed_domains: Sequence[str]) -> bool:
    return any(
        hostname == allowed_domain or hostname.endswith(".{}".format(allowed_domain))
        for allowed_domain in allowed_domains
    )


def normalize_source_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("A JSON-stat 2.0 source URL is required.")
    if len(text) > _MAX_SOURCE_URL_LENGTH:
        raise ValueError("The JSON-stat source URL is too long.")

    try:
        parsed = urlsplit(text)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("The JSON-stat source URL is invalid.") from exc

    if parsed.scheme.lower() != "https":
        raise ValueError("The JSON-stat source URL must use HTTPS.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("The JSON-stat source URL must not contain credentials.")
    if parsed.fragment:
        raise ValueError("The JSON-stat source URL must not contain a fragment.")

    hostname = str(parsed.hostname or "").strip().rstrip(".").lower()
    if not hostname:
        raise ValueError("The JSON-stat source URL must include a host name.")
    if hostname in _DISALLOWED_HOSTNAMES or hostname.endswith(_DISALLOWED_HOST_SUFFIXES):
        raise ValueError("The JSON-stat source URL must use a public host.")

    try:
        ascii_hostname = hostname.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("The JSON-stat source URL contains an invalid host name.") from exc

    allowed_domains = allowed_source_domains()
    if not _host_is_allowed(ascii_hostname, allowed_domains):
        raise ValueError(
            "The JSON-stat source host is not allowed. Allowed domains: {}.".format(
                ", ".join(allowed_domains)
            )
        )

    host = ascii_hostname
    if ":" in ascii_hostname:
        host = "[{}]".format(ascii_hostname)
    if port is not None:
        host = "{}:{}".format(host, port)

    path = parsed.path or "/"
    return urlunsplit(("https", host, path, parsed.query, ""))


def _ensure_public_host(source_url: str) -> None:
    parsed = urlsplit(source_url)
    hostname = str(parsed.hostname or "").strip()
    port = parsed.port or 443
    try:
        address_info = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise PetscanServiceError(
            "Failed to resolve JSON-stat source host: {}".format(exc),
            public_message=_JSONSTAT_FETCH_PUBLIC_MESSAGE,
        ) from exc

    addresses = {str(entry[4][0]).split("%", 1)[0] for entry in address_info if entry[4]}
    if not addresses:
        raise PetscanServiceError(
            "JSON-stat source host did not resolve to an IP address.",
            public_message=_JSONSTAT_FETCH_PUBLIC_MESSAGE,
        )
    for address in addresses:
        try:
            parsed_address = ipaddress.ip_address(address)
        except ValueError as exc:
            raise PetscanServiceError(
                "JSON-stat source host resolved to an invalid IP address.",
                public_message=_JSONSTAT_FETCH_PUBLIC_MESSAGE,
            ) from exc
        if not parsed_address.is_global:
            raise PetscanServiceError(
                "JSON-stat source host resolved to a non-public IP address.",
                public_message="The JSON-stat source URL must use a public host.",
            )


class _SafeRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Optional[Request]:
        normalized_url = normalize_source_url(urljoin(req.full_url, newurl))
        _ensure_public_host(normalized_url)
        return super().redirect_request(req, fp, code, msg, headers, normalized_url)


def _read_limited_response(response: Any, max_bytes: int) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except (TypeError, ValueError):
            declared_length = 0
        if declared_length > max_bytes:
            raise PetscanServiceError(
                "JSON-stat response exceeds the configured size limit.",
                public_message="The JSON-stat source file is too large.",
            )

    raw = response.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise PetscanServiceError(
            "JSON-stat response exceeds the configured size limit.",
            public_message="The JSON-stat source file is too large.",
        )
    return cast(bytes, raw)


def fetch_jsonstat_json(source_url: Any) -> Tuple[Dict[str, Any], str]:
    normalized_url = normalize_source_url(source_url)
    _ensure_public_host(normalized_url)
    request = Request(
        normalized_url,
        headers={
            "Accept": "application/json, application/json-stat+json",
            "User-Agent": HTTP_USER_AGENT,
        },
    )
    timeout = int(getattr(settings, "JSONSTAT_TIMEOUT_SECONDS", 30))
    max_bytes = int(getattr(settings, "JSONSTAT_MAX_RESPONSE_BYTES", _DEFAULT_MAX_RESPONSE_BYTES))
    opener = build_opener(_SafeRedirectHandler())

    try:
        with opener.open(request, timeout=timeout) as response:  # nosec B310
            raw = _read_limited_response(response, max_bytes)
            final_url = normalize_source_url(response.geturl())
    except PetscanServiceError:
        raise
    except (ValueError, HTTPError) as exc:
        raise PetscanServiceError(
            "Failed to fetch JSON-stat data: {}".format(exc),
            public_message=_JSONSTAT_FETCH_PUBLIC_MESSAGE,
        ) from exc
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch JSON-stat data: {}".format(exc),
            public_message=_JSONSTAT_FETCH_PUBLIC_MESSAGE,
        ) from exc

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise PetscanServiceError("JSON-stat source returned a non-JSON payload.") from exc
    if not isinstance(payload, dict):
        raise PetscanServiceError("Unexpected JSON-stat format (expected an object).")
    return payload, final_url


def encode_source_token(source_url: Any) -> str:
    normalized_url = normalize_source_url(source_url)
    parsed = urlsplit(normalized_url)
    path_match = _PXWEB_SAVED_QUERY_PATH_RE.fullmatch(parsed.path)
    if path_match is not None and parsed.port is None and not parsed.query:
        return "{}_{}".format(
            parsed.hostname,
            path_match.group("query_id"),
        )
    raise ValueError(
        "The JSON-stat source URL must be a PxWeb saved-query URL in the form "
        "https://<allowed-host>/PxWeb/sq/<UUID>."
    )


def decode_source_token(value: Any) -> str:
    token = str(value or "").strip()
    if not token:
        raise ValueError("A JSON-stat source token is required in path parameters.")
    if len(token) > _MAX_SOURCE_TOKEN_LENGTH:
        raise ValueError("The JSON-stat source token is invalid.")

    readable_match = _READABLE_SOURCE_TOKEN_RE.fullmatch(token)
    if readable_match is None:
        raise ValueError("The JSON-stat source token is invalid.")
    readable_url = "https://{}/PxWeb/sq/{}".format(
        readable_match.group("hostname"),
        readable_match.group("query_id"),
    )
    return normalize_source_url(readable_url)


def normalize_source_params(params: Optional[Mapping[str, Any]]) -> Dict[str, List[str]]:
    if not params or not isinstance(params, Mapping):
        return {}
    raw_url = params.get("url")
    if isinstance(raw_url, (list, tuple)):
        values = [str(value).strip() for value in raw_url if str(value).strip()]
        raw_url = values[-1] if values else None
    if raw_url is None or not str(raw_url).strip():
        return {}
    return {"url": [normalize_source_url(raw_url)]}


def _require_string_list(value: Any, field_name: str) -> List[str]:
    if not isinstance(value, list) or not value:
        raise PetscanServiceError("JSON-stat {} must be a non-empty array.".format(field_name))
    result = []  # type: List[str]
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise PetscanServiceError(
                "JSON-stat {} must contain non-empty strings.".format(field_name)
            )
        result.append(item)
    if len(set(result)) != len(result):
        raise PetscanServiceError(
            "JSON-stat {} must not contain duplicate values.".format(field_name)
        )
    return result


def _require_sizes(value: Any, dimension_count: int) -> List[int]:
    if not isinstance(value, list) or len(value) != dimension_count:
        raise PetscanServiceError("JSON-stat size must match the id array.")
    sizes = []  # type: List[int]
    for item in value:
        if not isinstance(item, int) or isinstance(item, bool) or item <= 0:
            raise PetscanServiceError("JSON-stat size values must be positive integers.")
        sizes.append(item)
    return sizes


def _ordered_category_ids(dimension_id: str, dimension: Mapping[str, Any], size: int) -> List[str]:
    category = dimension.get("category")
    if not isinstance(category, Mapping):
        raise PetscanServiceError(
            "JSON-stat dimension '{}' is missing a category object.".format(dimension_id)
        )
    raw_index = category.get("index")
    ordered_ids: List[str]
    if isinstance(raw_index, list):
        ordered_ids = [str(value) for value in raw_index]
    elif isinstance(raw_index, Mapping):
        positioned = []  # type: List[Tuple[int, str]]
        for category_id, raw_position in raw_index.items():
            if not isinstance(raw_position, int) or isinstance(raw_position, bool):
                raise PetscanServiceError(
                    "JSON-stat category positions must be integers for dimension '{}'.".format(
                        dimension_id
                    )
                )
            positioned.append((raw_position, str(category_id)))
        positioned.sort()
        if [position for position, _category_id in positioned] != list(range(size)):
            raise PetscanServiceError(
                "JSON-stat category positions are invalid for dimension '{}'.".format(dimension_id)
            )
        ordered_ids = [category_id for _position, category_id in positioned]
    elif raw_index is None and size == 1:
        labels = category.get("label")
        if not isinstance(labels, Mapping) or len(labels) != 1:
            raise PetscanServiceError(
                "JSON-stat constant dimension '{}' needs one category label.".format(dimension_id)
            )
        ordered_ids = [str(next(iter(labels.keys())))]
    else:
        raise PetscanServiceError(
            "JSON-stat dimension '{}' is missing a category index.".format(dimension_id)
        )

    if len(ordered_ids) != size or len(set(ordered_ids)) != size:
        raise PetscanServiceError(
            "JSON-stat category count does not match size for dimension '{}'.".format(dimension_id)
        )
    return ordered_ids


def _normalize_field_base(raw_name: str, fallback: str) -> str:
    normalized = _FIELD_NAME_RE.sub("_", raw_name.strip())
    normalized = _FIELD_UNDERSCORE_RUN_RE.sub("_", normalized).strip("_")
    if not normalized:
        normalized = fallback
    if normalized[0].isdigit():
        normalized = "dimension_{}".format(normalized)
    return normalized


def _dimension_field_names(dimension_ids: Sequence[str]) -> Dict[str, str]:
    used = set(_RESERVED_RECORD_FIELDS)
    names: Dict[str, str] = {}
    for index, dimension_id in enumerate(dimension_ids):
        base = _normalize_field_base(dimension_id, "dimension_{}".format(index + 1))
        candidate = base
        suffix = 1
        while (
            candidate in used
            or "{}_label".format(candidate) in used
            or "{}_concept".format(candidate) in used
        ):
            suffix += 1
            candidate = "{}_{}".format(base, suffix)
        used.add(candidate)
        used.add("{}_label".format(candidate))
        used.add("{}_concept".format(candidate))
        names[dimension_id] = candidate
    return names


def _status_at(status: Any, position: int, cell_count: int) -> Any:
    if status is None:
        return None
    if isinstance(status, str):
        return status
    if isinstance(status, list):
        if len(status) == 1:
            return status[0]
        if len(status) != cell_count:
            raise PetscanServiceError("JSON-stat status array length does not match dataset size.")
        return status[position]
    if isinstance(status, Mapping):
        return status.get(str(position))
    raise PetscanServiceError("JSON-stat status must be a string, array, or object.")


def _value_at(values: Any, position: int, cell_count: int) -> Any:
    if isinstance(values, list):
        if len(values) != cell_count:
            raise PetscanServiceError("JSON-stat value array length does not match dataset size.")
        value = values[position]
    elif isinstance(values, Mapping):
        value = values.get(str(position))
    else:
        raise PetscanServiceError("JSON-stat value must be an array or object.")
    if isinstance(value, (Mapping, list, tuple, set)):
        raise PetscanServiceError("JSON-stat observation values must be scalar or null.")
    return value


def _validate_sparse_positions(values: Any, status: Any, cell_count: int) -> None:
    for field_name, candidate in (("value", values), ("status", status)):
        if not isinstance(candidate, Mapping):
            continue
        for raw_position in candidate.keys():
            text = str(raw_position)
            if not text.isdigit() or str(int(text)) != text:
                raise PetscanServiceError(
                    "JSON-stat {} positions must be canonical integers.".format(field_name)
                )
            position = int(text)
            if position < 0 or position >= cell_count:
                raise PetscanServiceError(
                    "JSON-stat {} position is outside the dataset.".format(field_name)
                )


def _category_metadata(
    dimension: Mapping[str, Any],
    category_id: str,
    field_name: str,
) -> Dict[str, Any]:
    category = cast(Mapping[str, Any], dimension["category"])
    metadata: Dict[str, Any] = {}
    labels = category.get("label")
    label = labels.get(category_id) if isinstance(labels, Mapping) else None
    metadata["{}_label".format(field_name)] = str(label) if label is not None else category_id

    notes = category.get("note")
    if isinstance(notes, Mapping):
        note = notes.get(category_id)
        if isinstance(note, list):
            cleaned_notes = [str(item).strip() for item in note if str(item).strip()]
            if cleaned_notes:
                metadata["{}_note".format(field_name)] = cleaned_notes

    coordinates = category.get("coordinates")
    if isinstance(coordinates, Mapping):
        raw_coordinates = coordinates.get(category_id)
        if isinstance(raw_coordinates, list) and len(raw_coordinates) == 2:
            metadata["{}_longitude".format(field_name)] = raw_coordinates[0]
            metadata["{}_latitude".format(field_name)] = raw_coordinates[1]

    units = category.get("unit")
    if isinstance(units, Mapping):
        unit = units.get(category_id)
        if isinstance(unit, Mapping):
            for unit_key in _UNIT_KEYS:
                unit_value = unit.get(unit_key)
                if unit_value is not None and not isinstance(unit_value, (Mapping, list)):
                    metadata["{}_unit_{}".format(field_name, unit_key)] = unit_value
    return metadata


def extract_records(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(payload, Mapping):
        raise PetscanServiceError("Unexpected JSON-stat format (expected an object).")
    if payload.get("version") != "2.0":
        raise PetscanServiceError("JSON-stat source must use version 2.0.")
    if payload.get("class") != "dataset":
        raise PetscanServiceError("JSON-stat source must contain a dataset response.")

    dimension_ids = _require_string_list(payload.get("id"), "id")
    sizes = _require_sizes(payload.get("size"), len(dimension_ids))
    cell_count = math.prod(sizes)
    max_cells = int(getattr(settings, "JSONSTAT_MAX_CELLS", _DEFAULT_MAX_CELLS))
    if cell_count > max_cells:
        raise PetscanServiceError(
            "JSON-stat dataset contains {} cells; the configured maximum is {}.".format(
                cell_count, max_cells
            ),
            public_message="The JSON-stat dataset contains too many cells.",
        )

    dimensions = payload.get("dimension")
    if not isinstance(dimensions, Mapping):
        raise PetscanServiceError("JSON-stat source is missing a dimension object.")
    ordered_categories: Dict[str, List[str]] = {}
    for dimension_id, size in zip(dimension_ids, sizes, strict=True):
        dimension = dimensions.get(dimension_id)
        if not isinstance(dimension, Mapping):
            raise PetscanServiceError(
                "JSON-stat source is missing dimension '{}'.".format(dimension_id)
            )
        ordered_categories[dimension_id] = _ordered_category_ids(dimension_id, dimension, size)

    values = payload.get("value")
    status = payload.get("status")
    _validate_sparse_positions(values, status, cell_count)
    field_names = _dimension_field_names(dimension_ids)
    records = []  # type: List[Dict[str, Any]]

    for position in range(cell_count):
        coordinates = [0] * len(sizes)
        remainder = position
        for dimension_index in range(len(sizes) - 1, -1, -1):
            coordinates[dimension_index] = remainder % sizes[dimension_index]
            remainder //= sizes[dimension_index]

        record: Dict[str, Any] = {}
        for dimension_index, dimension_id in enumerate(dimension_ids):
            category_id = ordered_categories[dimension_id][coordinates[dimension_index]]
            field_name = field_names[dimension_id]
            record[field_name] = category_id
            dimension = cast(Mapping[str, Any], dimensions[dimension_id])
            record.update(_category_metadata(dimension, category_id, field_name))

        record["value"] = _value_at(values, position, cell_count)
        observation_status = _status_at(status, position, cell_count)
        if observation_status is not None:
            record["status"] = observation_status
        records.append(record)

    return records
