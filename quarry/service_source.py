"""Quarry source URL handling, qrun_id resolution, and JSON row extraction."""

import gzip
import html as html_lib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, TypedDict
from urllib.request import Request, urlopen

from django.conf import settings

from petscan.service_errors import PetscanServiceError
from petscan.service_source import HTTP_USER_AGENT

__all__ = [
    "build_quarry_json_url",
    "build_quarry_query_url",
    "extract_query_db_name",
    "extract_qrun_id",
    "extract_records",
    "fetch_quarry_json",
    "fetch_quarry_query_html",
    "normalize_load_limit",
    "resolve_quarry_run",
]

_VARS_SCRIPT_RE = re.compile(r"var\s+vars\s*=\s*(\{.*?\})\s*;", re.IGNORECASE | re.DOTALL)
_QRUN_ID_RE = re.compile(r'"qrun_id"\s*:\s*(\d+)', re.IGNORECASE)
_INPUT_TAG_RE = re.compile(r"<input\b[^>]*>", re.IGNORECASE | re.DOTALL)
_INPUT_ID_RE = re.compile(r"""\bid\s*=\s*(["'])query-db\1""", re.IGNORECASE | re.DOTALL)
_INPUT_VALUE_RE = re.compile(r"""\bvalue\s*=\s*(["'])(.*?)\1""", re.IGNORECASE | re.DOTALL)
_NON_ASCII_HEADER_CHAR_RE = re.compile(r"[^A-Za-z0-9_]")
_HEADER_UNDERSCORE_RUN_RE = re.compile(r"_+")
_DEFAULT_QUARRY_BASE_URL = "https://quarry.wmcloud.org"
_EXAMPLES_DIR = Path(settings.BASE_DIR) / "data" / "examples"
_QUARRY_FETCH_PUBLIC_MESSAGE = "Failed to load Quarry data from the upstream service."


class _BundledQuarryExample(TypedDict):
    quarry_id: int
    qrun_id: int
    query_db: str
    file_name: str


_BUNDLED_QUARRY_EXAMPLES: Tuple[_BundledQuarryExample, ...] = (
    {
        "quarry_id": 103479,
        "qrun_id": 1084300,
        "query_db": "fiwiki_p",
        "file_name": "quarry-103479-run-1084300.json.gz",
    },
    {
        "quarry_id": 103514,
        "qrun_id": 1084648,
        "query_db": "fiwiki_p",
        "file_name": "quarry-103514-run-1084648.json.gz",
    },
)
_BUNDLED_QUARRY_EXAMPLES_BY_QUERY_ID: Dict[int, _BundledQuarryExample] = {
    entry["quarry_id"]: entry for entry in _BUNDLED_QUARRY_EXAMPLES
}
_BUNDLED_QUARRY_EXAMPLES_BY_QRUN_ID: Dict[int, _BundledQuarryExample] = {
    entry["qrun_id"]: entry for entry in _BUNDLED_QUARRY_EXAMPLES
}


def _quarry_base_url() -> str:
    endpoint = str(getattr(settings, "QUARRY_ENDPOINT", _DEFAULT_QUARRY_BASE_URL)).strip()
    return endpoint.rstrip("/") if endpoint else _DEFAULT_QUARRY_BASE_URL


def _bundled_quarry_example_path(file_name: str) -> Path:
    return _EXAMPLES_DIR / str(file_name).strip()


def _load_bundled_quarry_example_payload(file_name: str) -> Optional[Dict[str, Any]]:
    payload_path = _bundled_quarry_example_path(file_name)
    if not payload_path.exists():
        return None

    try:
        if payload_path.suffix == ".gz":
            with gzip.open(payload_path, mode="rt", encoding="utf-8") as payload_file:
                payload = json.load(payload_file)
        else:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    return payload if isinstance(payload, dict) else None


def _bundled_quarry_example_for_query_id(quarry_id: int) -> Optional[_BundledQuarryExample]:
    bundled = _BUNDLED_QUARRY_EXAMPLES_BY_QUERY_ID.get(int(quarry_id))
    if bundled is None:
        return None
    payload = _load_bundled_quarry_example_payload(str(bundled.get("file_name", "")))
    if payload is None:
        return None
    return bundled.copy()


def _bundled_quarry_example_for_qrun_id(qrun_id: int) -> Optional[_BundledQuarryExample]:
    bundled = _BUNDLED_QUARRY_EXAMPLES_BY_QRUN_ID.get(int(qrun_id))
    if bundled is None:
        return None
    payload = _load_bundled_quarry_example_payload(str(bundled.get("file_name", "")))
    if payload is None:
        return None
    return bundled.copy()


def build_quarry_query_url(quarry_id: int) -> str:
    return "{}/query/{}".format(_quarry_base_url(), quarry_id)


def build_quarry_json_url(qrun_id: int) -> str:
    return "{}/run/{}/output/0/json".format(_quarry_base_url(), qrun_id)


def fetch_quarry_query_html(quarry_id: int) -> Tuple[str, str]:
    source_url = build_quarry_query_url(quarry_id)
    request = Request(
        source_url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": HTTP_USER_AGENT,
        },
    )
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))

    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310
            raw = response.read()
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch Quarry query page: {}".format(exc),
            public_message=_QUARRY_FETCH_PUBLIC_MESSAGE,
        ) from exc

    try:
        return raw.decode("utf-8"), source_url
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace"), source_url


def extract_qrun_id(html: str) -> int:
    text = str(html or "")
    script_match = _VARS_SCRIPT_RE.search(text)
    candidate = script_match.group(1) if script_match else text
    qrun_match = _QRUN_ID_RE.search(candidate)
    if qrun_match is None:
        raise PetscanServiceError("Could not locate qrun_id in Quarry query page.")

    try:
        qrun_id = int(qrun_match.group(1))
    except (TypeError, ValueError) as exc:
        raise PetscanServiceError("Quarry query page contained an invalid qrun_id.") from exc
    if qrun_id <= 0:
        raise PetscanServiceError("Quarry query page contained an invalid qrun_id.")
    return qrun_id


def extract_query_db_name(html: str) -> Optional[str]:
    text = str(html or "")
    for input_match in _INPUT_TAG_RE.finditer(text):
        tag = input_match.group(0)
        if _INPUT_ID_RE.search(tag) is None:
            continue

        value_match = _INPUT_VALUE_RE.search(tag)
        if value_match is None:
            return None

        query_db = html_lib.unescape(value_match.group(2)).strip()
        return query_db or None

    return None


def resolve_quarry_run(quarry_id: int) -> Dict[str, Any]:
    bundled = _bundled_quarry_example_for_query_id(quarry_id)
    if bundled is not None:
        qrun_id = int(bundled["qrun_id"])
        query_db = str(bundled.get("query_db", "")).strip() or None
        return {
            "quarry_id": quarry_id,
            "qrun_id": qrun_id,
            "query_db": query_db,
            "query_url": build_quarry_query_url(quarry_id),
            "json_url": build_quarry_json_url(qrun_id),
        }

    html, query_url = fetch_quarry_query_html(quarry_id)
    qrun_id = extract_qrun_id(html)
    query_db = extract_query_db_name(html)
    return {
        "quarry_id": quarry_id,
        "qrun_id": qrun_id,
        "query_db": query_db,
        "query_url": query_url,
        "json_url": build_quarry_json_url(qrun_id),
    }


def fetch_quarry_json(qrun_id: int) -> Tuple[Dict[str, Any], str]:
    source_url = build_quarry_json_url(qrun_id)
    bundled = _bundled_quarry_example_for_qrun_id(qrun_id)
    if bundled is not None:
        payload = _load_bundled_quarry_example_payload(str(bundled.get("file_name", "")))
        if payload is not None:
            return payload, source_url

    request = Request(
        source_url,
        headers={
            "Accept": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        },
    )
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))

    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310
            raw = response.read()
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch Quarry JSON data: {}".format(exc),
            public_message=_QUARRY_FETCH_PUBLIC_MESSAGE,
        ) from exc

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise PetscanServiceError("Quarry returned non-JSON payload.") from exc

    if not isinstance(payload, dict):
        raise PetscanServiceError("Unexpected Quarry JSON format (expected object).")

    return payload, source_url


def normalize_load_limit(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        limit = int(text)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer.") from exc
    if limit <= 0:
        raise ValueError("limit must be greater than zero.")
    return limit


def _normalize_header_name(raw_header: Any) -> str:
    normalized = _NON_ASCII_HEADER_CHAR_RE.sub("_", str(raw_header).strip())
    return _HEADER_UNDERSCORE_RUN_RE.sub("_", normalized)


def _normalized_unique_names(raw_names: Sequence[Any]) -> List[str]:
    normalized_names = []  # type: List[str]
    counts = {}  # type: Dict[str, int]
    used = set()  # type: set[str]

    for index, raw_name in enumerate(raw_names):
        base_name = str(raw_name).strip()
        if not base_name:
            base_name = "column_{}".format(index + 1)

        normalized_base = _normalize_header_name(base_name)
        if not normalized_base:
            normalized_base = "column_{}".format(index + 1)

        duplicate_count = int(counts.get(normalized_base, 0)) + 1
        counts[normalized_base] = duplicate_count

        if duplicate_count == 1:
            candidate = normalized_base
        elif normalized_base.endswith("_"):
            candidate = "{}{}".format(normalized_base, duplicate_count)
        else:
            candidate = "{}_{}".format(normalized_base, duplicate_count)

        suffix = duplicate_count
        while candidate in used:
            suffix += 1
            if normalized_base.endswith("_"):
                candidate = "{}{}".format(normalized_base, suffix)
            else:
                candidate = "{}_{}".format(normalized_base, suffix)

        used.add(candidate)
        normalized_names.append(candidate)

    return normalized_names


def _normalize_headers(headers: Sequence[Any]) -> List[str]:
    return _normalized_unique_names(headers)


def _row_to_record(headers: Sequence[str], row: Any) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        items = list(row.items())
        normalized_keys = _normalized_unique_names([key for key, _value in items])
        return {
            normalized_keys[index]: value
            for index, (_key, value) in enumerate(items)
        }

    if not isinstance(row, Sequence) or isinstance(row, (str, bytes, bytearray)):
        raise PetscanServiceError("Unexpected Quarry row format (expected array rows).")

    record = {}  # type: Dict[str, Any]
    for index, header in enumerate(headers):
        record[header] = row[index] if index < len(row) else None

    for index in range(len(headers), len(row)):
        record["column_{}".format(index + 1)] = row[index]

    return record


def extract_records(payload: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
    headers = payload.get("headers")
    rows = payload.get("rows")

    if not isinstance(headers, list) or not headers:
        raise PetscanServiceError("Unexpected Quarry JSON format (missing headers).")
    if not isinstance(rows, list):
        raise PetscanServiceError("Unexpected Quarry JSON format (missing rows).")

    normalized_headers = _normalize_headers(headers)
    records = [_row_to_record(normalized_headers, row) for row in rows]

    if limit is not None:
        return records[:limit]
    return records
