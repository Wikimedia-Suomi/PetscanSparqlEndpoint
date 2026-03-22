"""PetScan source URL handling and JSON record extraction."""

import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from django.conf import settings

from .service_errors import PetscanServiceError

HTTP_USER_AGENT = "PetscanSparqlEndpoint (https://meta.wikimedia.org/wiki/user:Zache)"
_PETSCAN_RESERVED_QUERY_PARAMS = {"psid", "format", "query", "refresh"}
_ROW_HINT_KEYS = {
    "id",
    "pageid",
    "title",
    "len",
    "namespace",
    "nstext",
    "qid",
    "wikidata",
    "wiki",
}
__all__ = [
    "HTTP_USER_AGENT",
    "build_petscan_url",
    "extract_records",
    "fetch_petscan_json",
    "normalize_petscan_params",
]
_PETSCAN_FETCH_PUBLIC_MESSAGE = "Failed to load PetScan data from the upstream service."


def normalize_petscan_params(params: Optional[Mapping[str, Any]]) -> Dict[str, List[str]]:
    normalized = {}  # type: Dict[str, List[str]]
    if not params or not isinstance(params, Mapping):
        return normalized

    for key, raw_value in params.items():
        text_key = str(key).strip()
        if not text_key:
            continue
        if text_key.lower() in _PETSCAN_RESERVED_QUERY_PARAMS:
            continue

        values = []  # type: List[str]
        if isinstance(raw_value, (list, tuple, set)):
            for item in raw_value:
                if item is None:
                    continue
                text_value = str(item).strip()
                if text_value:
                    values.append(text_value)
        else:
            if raw_value is None:
                continue
            text_value = str(raw_value).strip()
            if text_value:
                values.append(text_value)

        if values:
            normalized[text_key] = values

    return normalized


def build_petscan_url(psid: int, petscan_params: Optional[Mapping[str, Any]] = None) -> str:
    endpoint = str(settings.PETSCAN_ENDPOINT).rstrip("/")
    normalized_params = normalize_petscan_params(petscan_params)
    query_pairs = [("psid", str(psid)), ("format", "json")]
    for key in sorted(normalized_params.keys()):
        for value in normalized_params[key]:
            query_pairs.append((key, value))
    query = urlencode(query_pairs)
    return "{}/?{}".format(endpoint, query)


def fetch_petscan_json(
    psid: int,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], str]:
    source_url = build_petscan_url(psid, petscan_params=petscan_params)
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
            "Failed to fetch PetScan data: {}".format(exc),
            public_message=_PETSCAN_FETCH_PUBLIC_MESSAGE,
        ) from exc

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise PetscanServiceError("PetScan returned non-JSON payload.") from exc

    if not isinstance(payload, dict):
        raise PetscanServiceError("Unexpected PetScan JSON format (expected object).")

    return payload, source_url


def _collect_record_lists(node: Any, collector: List[List[Dict[str, Any]]], depth: int = 0) -> None:
    if depth > 8:
        return
    if isinstance(node, list):
        dict_rows = [row for row in node if isinstance(row, dict)]
        if dict_rows:
            collector.append(dict_rows)
        for value in node:
            _collect_record_lists(value, collector, depth + 1)
        return
    if isinstance(node, dict):
        for value in node.values():
            _collect_record_lists(value, collector, depth + 1)


def _score_records(records: Sequence[Mapping[str, Any]]) -> int:
    found_keys = set()
    for row in records[:20]:
        found_keys.update(set(row.keys()) & _ROW_HINT_KEYS)
    return len(records) * 10 + len(found_keys)


def _as_dict_rows(node: Any) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(node, list):
        return None
    rows = [row for row in node if isinstance(row, dict)]
    if not rows:
        return None
    return rows


def _looks_like_record_rows(rows: Sequence[Mapping[str, Any]]) -> bool:
    sample_size = min(len(rows), 5)
    for row in rows[:sample_size]:
        if set(row.keys()) & _ROW_HINT_KEYS:
            return True
    return False


def _collect_direct_record_candidates(payload: Mapping[str, Any]) -> List[List[Dict[str, Any]]]:
    candidates = []  # type: List[List[Dict[str, Any]]]

    def _add_candidate(node: Any) -> None:
        rows = _as_dict_rows(node)
        if rows and _looks_like_record_rows(rows):
            candidates.append(rows)

    _add_candidate(payload.get("pages"))
    _add_candidate(payload.get("*"))

    top_level_a = payload.get("a")
    if isinstance(top_level_a, Mapping):
        _add_candidate(top_level_a.get("*"))

    root_star = payload.get("*")
    if isinstance(root_star, list):
        for entry in root_star:
            if not isinstance(entry, Mapping):
                continue
            nested_a = entry.get("a")
            if isinstance(nested_a, Mapping):
                _add_candidate(nested_a.get("*"))

    return candidates


def _extract_records_exhaustive(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    candidates = []  # type: List[List[Dict[str, Any]]]

    if isinstance(payload.get("*"), list):
        direct = [row for row in payload["*"] if isinstance(row, dict)]
        if direct:
            candidates.append(direct)

    if isinstance(payload.get("pages"), list):
        direct_pages = [row for row in payload["pages"] if isinstance(row, dict)]
        if direct_pages:
            candidates.append(direct_pages)

    _collect_record_lists(payload, candidates)
    if not candidates:
        raise PetscanServiceError("Could not locate row data in PetScan JSON payload.")

    best = max(candidates, key=_score_records)
    return best


def extract_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    direct_candidates = _collect_direct_record_candidates(payload)
    if direct_candidates:
        return max(direct_candidates, key=_score_records)
    return _extract_records_exhaustive(payload)
