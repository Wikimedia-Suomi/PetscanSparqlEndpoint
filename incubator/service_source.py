"""Incubator source fetching via MediaWiki API or Toolforge replica."""

import json
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Tuple, cast
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from django.conf import settings

from petscan import enrichment_sql, service_links
from petscan.normalization import normalize_page_title, normalize_qid
from petscan.service_errors import PetscanServiceError
from petscan.service_source import HTTP_USER_AGENT

__all__ = [
    "LOOKUP_BACKEND_API",
    "LOOKUP_BACKEND_TOOLFORGE_SQL",
    "build_incubator_category_url",
    "fetch_incubator_records",
    "incubator_lookup_backend",
    "normalize_load_limit",
    "normalize_source_params",
]

LOOKUP_BACKEND_API = service_links.LOOKUP_BACKEND_API
LOOKUP_BACKEND_TOOLFORGE_SQL = service_links.LOOKUP_BACKEND_TOOLFORGE_SQL
_DEFAULT_INCUBATOR_API_URL = "https://incubator.wikimedia.org/w/api.php"
_DEFAULT_INCUBATOR_PAGE_BASE_URL = "https://incubator.wikimedia.org/wiki/"
_INCUBATOR_WIKIDATA_CATEGORY_PAGE = "Category:Maintenance:Wikidata_interwiki_links"
_INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE = "Maintenance:Wikidata_interwiki_links"
_INCUBATOR_FETCH_PUBLIC_MESSAGE = "Failed to load Incubator data from the upstream service."
_INCUBATOR_REPLICA_HOST = "incubatorwiki.web.db.svc.wikimedia.cloud"
_INCUBATOR_REPLICA_DB = "incubatorwiki_p"
_WIKI_GROUP_BY_CODE = {
    "Wp": "wikipedia",
    "Wt": "wiktionary",
    "Wq": "wikiquote",
    "Wb": "wikibooks",
    "Wn": "wikinews",
    "Wy": "wikivoyage",
    "Ws": "wikisource",
    "Wv": "wikiversity",
}
_SOURCE_PARAM_KEYS = frozenset({"limit", "recentchanges_only"})
_MAX_INCUBATOR_API_BATCH_SIZE = 50
_INCUBATOR_URL_SAFE_CHARS = "/:()-,._"
pymysql = cast(Any, enrichment_sql.pymysql)


def _console_log(message: str) -> None:
    print("[incubator-api] {}".format(message), flush=True)


def _incubator_api_url() -> str:
    endpoint = str(getattr(settings, "INCUBATOR_API_ENDPOINT", _DEFAULT_INCUBATOR_API_URL)).strip()
    return endpoint or _DEFAULT_INCUBATOR_API_URL


def _incubator_page_base_url() -> str:
    endpoint = str(
        getattr(settings, "INCUBATOR_PAGE_BASE_URL", _DEFAULT_INCUBATOR_PAGE_BASE_URL)
    ).strip()
    return endpoint or _DEFAULT_INCUBATOR_PAGE_BASE_URL


@lru_cache(maxsize=256)
def _quote_incubator_path(path: str) -> str:
    return quote(path, safe=_INCUBATOR_URL_SAFE_CHARS)


def build_incubator_category_url() -> str:
    return "{}{}".format(_incubator_page_base_url(), _quote_incubator_path(_INCUBATOR_WIKIDATA_CATEGORY_PAGE))


def incubator_lookup_backend() -> str:
    return service_links.wikidata_lookup_backend()


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


def normalize_source_params(params: Optional[Mapping[str, Any]]) -> Dict[str, List[str]]:
    normalized = {}  # type: Dict[str, List[str]]
    if not params or not isinstance(params, Mapping):
        return normalized

    for key, raw_value in params.items():
        text_key = str(key).strip()
        if not text_key or text_key not in _SOURCE_PARAM_KEYS:
            continue

        if isinstance(raw_value, (list, tuple, set)):
            values = [str(value).strip() for value in raw_value if str(value).strip()]
        else:
            text_value = str(raw_value).strip()
            values = [text_value] if text_value else []

        if values:
            normalized[text_key] = values

    return normalized


def _normalize_db_page_title(value: object) -> str:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    return normalize_page_title(value)


def _split_incubator_title(page_title: str) -> Tuple[str, str, str]:
    parts = page_title.split("/", 2)
    wiki_project = parts[0] if len(parts) >= 1 else ""
    lang_code = parts[1] if len(parts) >= 2 else ""
    page_name = parts[2] if len(parts) >= 3 else parts[-1] if parts else ""
    return wiki_project, lang_code, page_name


def _wiki_group_for_code(wiki_project: str) -> str:
    fallback = str(wiki_project or "").strip().lower()
    return _WIKI_GROUP_BY_CODE.get(wiki_project, fallback)


def _incubator_url_for_title(page_title: str, page_base_url: Optional[str] = None) -> str:
    return "{}{}".format(
        page_base_url or _incubator_page_base_url(),
        _quote_incubator_path(page_title),
    )


def _site_url_for_parts(
    wiki_project: str,
    lang_code: str,
    page_base_url: Optional[str] = None,
) -> Optional[str]:
    normalized_project = str(wiki_project or "").strip()
    normalized_lang = str(lang_code or "").strip()
    if not normalized_project or not normalized_lang:
        return None
    return "{}{}".format(
        page_base_url or _incubator_page_base_url(),
        _quote_incubator_path("{}/{}/".format(normalized_project, normalized_lang)),
    )


def _build_incubator_record(
    page_title: str,
    wikidata_id: Optional[str],
    page_base_url: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_title = normalize_page_title(page_title)
    wiki_project, lang_code, page_name = _split_incubator_title(normalized_title)
    page_label = page_name.replace("_", " ")
    normalized_page_base_url = page_base_url or _incubator_page_base_url()
    site_url = _site_url_for_parts(wiki_project, lang_code, page_base_url=normalized_page_base_url)
    record: Dict[str, Any] = {
        "page_title": normalized_title,
        "wiki_project": wiki_project,
        "wiki_group": _wiki_group_for_code(wiki_project),
        "lang_code": lang_code,
        "page_name": page_name,
        "page_label": page_label,
        "incubator_url": _incubator_url_for_title(
            normalized_title,
            page_base_url=normalized_page_base_url,
        ),
    }
    if site_url is not None:
        record["site_url"] = site_url
    if wikidata_id is not None:
        record["wikidata_id"] = wikidata_id
        record["wikidata_entity"] = "http://www.wikidata.org/entity/{}".format(wikidata_id)
    return record


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _recentchanges_cutoff() -> datetime:
    return _utc_now() - timedelta(days=30)


def _normalize_api_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = "{}+00:00".format(text[:-1])
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _api_request(params: Mapping[str, Any]) -> Dict[str, Any]:
    request_url = "{}?{}".format(_incubator_api_url(), urlencode(params))
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        },
    )
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    started_at = perf_counter()
    _console_log("GET {}".format(request_url))

    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310
            raw = response.read()
    except Exception as exc:
        elapsed_ms = (perf_counter() - started_at) * 1000.0
        _console_log("ERROR {:.1f} ms {} -> {}".format(elapsed_ms, request_url, exc))
        raise PetscanServiceError(
            "Failed to fetch Incubator API data: {}".format(exc),
            public_message=_INCUBATOR_FETCH_PUBLIC_MESSAGE,
        ) from exc

    elapsed_ms = (perf_counter() - started_at) * 1000.0
    _console_log("OK {:.1f} ms {} bytes".format(elapsed_ms, len(raw)))

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise PetscanServiceError("Incubator API returned non-JSON payload.") from exc

    if not isinstance(payload, dict):
        raise PetscanServiceError("Unexpected Incubator API format (expected object).")

    query_payload = payload.get("query")
    if isinstance(query_payload, Mapping):
        categorymembers = query_payload.get("categorymembers")
        if isinstance(categorymembers, list):
            continue_payload = payload.get("continue")
            has_continue = isinstance(continue_payload, Mapping) and bool(
                str(continue_payload.get("cmcontinue", "")).strip()
            )
            _console_log(
                "categorymembers={} continue={}".format(len(categorymembers), "yes" if has_continue else "no")
            )
    return payload


def _api_batch_limit(limit: Optional[int], collected_count: int) -> int:
    if limit is None:
        return _MAX_INCUBATOR_API_BATCH_SIZE
    remaining = max(limit - collected_count, 1)
    return min(remaining, _MAX_INCUBATOR_API_BATCH_SIZE)


def _fetch_page_batch_api(
    *,
    batch_limit: int,
    recentchanges_only: bool = False,
    continuation: Optional[str] = None,
) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
    params: Dict[str, Any]
    if recentchanges_only:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": _INCUBATOR_WIKIDATA_CATEGORY_PAGE,
            "cmlimit": str(max(1, batch_limit)),
            "cmprop": "ids|title|sortkeyprefix|timestamp",
            "cmsort": "timestamp",
            "cmdir": "desc",
            "format": "json",
            "formatversion": "2",
        }
    else:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": _INCUBATOR_WIKIDATA_CATEGORY_PAGE,
            "cmlimit": str(max(1, batch_limit)),
            "cmprop": "ids|title|sortkeyprefix",
            "format": "json",
            "formatversion": "2",
        }
    if continuation:
        params["cmcontinue"] = continuation

    payload = _api_request(params)
    query_payload = payload.get("query")
    if not isinstance(query_payload, Mapping):
        raise PetscanServiceError("Incubator API returned no query payload.")
    categorymembers = query_payload.get("categorymembers")
    if not isinstance(categorymembers, list):
        raise PetscanServiceError("Incubator API returned no categorymembers payload.")
    continue_payload = payload.get("continue")
    if not isinstance(continue_payload, Mapping):
        return [entry for entry in categorymembers if isinstance(entry, Mapping)], None
    continuation_text = str(continue_payload.get("cmcontinue", "")).strip()
    return [entry for entry in categorymembers if isinstance(entry, Mapping)], continuation_text or None


def _fetch_incubator_records_api(
    limit: Optional[int],
    recentchanges_only: bool = False,
) -> Tuple[List[Dict[str, Any]], str]:
    records: List[Dict[str, Any]] = []
    continuation: Optional[str] = None
    cutoff = _recentchanges_cutoff() if recentchanges_only else None
    page_base_url = _incubator_page_base_url()

    while True:
        categorymembers, continuation = _fetch_page_batch_api(
            batch_limit=_api_batch_limit(limit, len(records)),
            recentchanges_only=recentchanges_only,
            continuation=continuation,
        )
        accepted_in_batch = 0
        reached_cutoff = False

        for entry in categorymembers:
            if cutoff is not None:
                entry_timestamp = _normalize_api_timestamp(entry.get("timestamp"))
                if entry_timestamp is None:
                    continue
                if entry_timestamp < cutoff:
                    reached_cutoff = True
                    break

            page_title = normalize_page_title(entry.get("title"))
            if not page_title:
                continue
            wikidata_id = normalize_qid(entry.get("sortkeyprefix"))

            records.append(
                _build_incubator_record(
                    page_title=page_title,
                    wikidata_id=wikidata_id,
                    page_base_url=page_base_url,
                )
            )
            accepted_in_batch += 1

            if limit is not None and len(records) >= limit:
                break

        _console_log("accepted={} accumulated={}".format(accepted_in_batch, len(records)))

        if limit is not None and len(records) >= limit:
            break
        if reached_cutoff:
            break
        if continuation is None:
            break

    return records, build_incubator_category_url()


def _fetch_incubator_records_sql(
    limit: Optional[int],
    recentchanges_only: bool = False,
) -> Tuple[List[Dict[str, Any]], str]:
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_INCUBATOR_FETCH_PUBLIC_MESSAGE,
        )

    replica_cnf = str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip()
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    connect_kwargs: Dict[str, Any] = {
        "host": _INCUBATOR_REPLICA_HOST,
        "database": _INCUBATOR_REPLICA_DB,
        "charset": "utf8mb4",
        "connect_timeout": timeout,
        "read_timeout": timeout,
        "write_timeout": timeout,
        "autocommit": True,
    }
    if replica_cnf:
        connect_kwargs["read_default_file"] = os.path.expanduser(os.path.expandvars(replica_cnf))

    params: List[object]
    if recentchanges_only:
        sql = (
            "SELECT latest_rc.rc_title, cl.cl_sortkey_prefix "
            "FROM ("
            "SELECT rc.rc_cur_id, MAX(rc.rc_id) AS latest_rc_id "
            "FROM recentchanges AS rc "
            "WHERE (rc.rc_source = %s "
            "OR (rc.rc_source = %s AND rc.rc_log_type = %s)) "
            "AND rc.rc_cur_id > 0 "
            "GROUP BY rc.rc_cur_id"
            ") AS latest_per_page "
            "JOIN recentchanges AS latest_rc "
            "ON latest_rc.rc_cur_id = latest_per_page.rc_cur_id "
            "AND latest_rc.rc_id = latest_per_page.latest_rc_id "
            "JOIN categorylinks AS cl ON cl.cl_from = latest_per_page.rc_cur_id "
            "JOIN linktarget AS lt ON lt.lt_id = cl.cl_target_id "
            "WHERE lt.lt_namespace = 14 "
            "AND lt.lt_title = %s "
            "ORDER BY latest_per_page.latest_rc_id DESC"
        )
        params = ["mw.edit", "mw.log", "move", _INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE]
    else:
        sql = (
            "SELECT p.page_title, cl.cl_sortkey_prefix "
            "FROM page AS p "
            "JOIN categorylinks AS cl ON cl.cl_from = p.page_id "
            "JOIN linktarget AS lt ON lt.lt_id = cl.cl_target_id "
            "WHERE lt.lt_namespace = 14 "
            "AND lt.lt_title = %s"
        )
        params = [_INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    connection = None
    try:
        connection = pymysql.connect(**connect_kwargs)
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch Incubator replica data: {}".format(exc),
            public_message=_INCUBATOR_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    records: List[Dict[str, Any]] = []
    page_base_url = _incubator_page_base_url()
    for row in rows:
        if not isinstance(row, (tuple, list)) or len(row) < 2:
            continue
        page_title = _normalize_db_page_title(row[0])
        if not page_title:
            continue
        records.append(
            _build_incubator_record(
                page_title=page_title,
                wikidata_id=normalize_qid(row[1]),
                page_base_url=page_base_url,
            )
        )

    return records, build_incubator_category_url()


def fetch_incubator_records(
    limit: Optional[int] = None,
    recentchanges_only: bool = False,
) -> Tuple[List[Dict[str, Any]], str]:
    backend = incubator_lookup_backend()
    _console_log(
        "backend={} limit={} recentchanges_only={}".format(
            backend,
            limit if limit is not None else "all",
            "yes" if recentchanges_only else "no",
        )
    )
    if backend == LOOKUP_BACKEND_TOOLFORGE_SQL:
        return _fetch_incubator_records_sql(limit=limit, recentchanges_only=recentchanges_only)
    return _fetch_incubator_records_api(limit=limit, recentchanges_only=recentchanges_only)
