"""PagePile source fetching via PagePile JSON and wiki APIs or Toolforge replica."""

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, cast
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from django.conf import settings

from newpages import service_source as newpages_source
from petscan import enrichment_sql, service_links
from petscan.normalization import normalize_page_title, normalize_qid
from petscan.service_errors import PetscanServiceError
from petscan.service_source import HTTP_USER_AGENT

__all__ = [
    "effective_load_limit",
    "LOOKUP_BACKEND_API",
    "LOOKUP_BACKEND_TOOLFORGE_SQL",
    "build_pagepile_json_url",
    "fetch_pagepile_json",
    "fetch_pagepile_records",
    "normalize_load_limit",
    "normalize_pagepile_id",
    "normalize_source_params",
    "pagepile_lookup_backend",
]

LOOKUP_BACKEND_API = service_links.LOOKUP_BACKEND_API
LOOKUP_BACKEND_TOOLFORGE_SQL = service_links.LOOKUP_BACKEND_TOOLFORGE_SQL
_DEFAULT_PAGEPILE_API_URL = "https://pagepile.toolforge.org/api.php"
_PAGEPILE_FETCH_PUBLIC_MESSAGE = "Failed to load PagePile data from the upstream service."
_INCUBATOR_DOMAIN = "incubator.wikimedia.org"
_INCUBATOR_SITE_URL = "https://incubator.wikimedia.org/"
_INCUBATOR_WIKIDATA_CATEGORY_PAGE = "Category:Maintenance:Wikidata_interwiki_links"
_INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE = "Maintenance:Wikidata_interwiki_links"
_COMMONS_SITE = "commonswiki"
_COMMONS_FILE_NAMESPACE = 6
_COMMONS_MEDIAITEM_ENTITY_BASE = "https://commons.wikimedia.org/entity/M"
_MAX_API_TITLES_PER_BATCH = 50
_MAX_PAGEPILE_API_SAMPLE_LIMIT = 500
_MAX_PAGEPILE_LOAD_LIMIT = 300_000
_SOURCE_PARAM_KEYS = frozenset({"pagepile_id", "limit"})
_SITE_TOKEN_TO_WIKI_GROUP = {
    "commonswiki": "commons",
    "wikidatawiki": "wikidata",
    "metawiki": "wikimedia",
    "mediawikiwiki": "wikimedia",
    "specieswiki": "wikimedia",
    "outreachwiki": "wikimedia",
    "incubatorwiki": "wikimedia",
}
_SITE_TOKEN_SUFFIX_TO_WIKI_GROUP = (
    ("wiktionary", "wiktionary"),
    ("wikiquote", "wikiquote"),
    ("wikibooks", "wikibooks"),
    ("wikinews", "wikinews"),
    ("wikiversity", "wikiversity"),
    ("wikivoyage", "wikivoyage"),
    ("wikisource", "wikisource"),
    ("wiki", "wikipedia"),
)
pymysql = cast(Any, enrichment_sql.pymysql)


@dataclass(frozen=True)
class _SiteContext:
    site: str
    domain: str
    dbname: str
    lang_code: str
    site_url: str
    wiki_group: str
    siteinfo: newpages_source._SiteInfo


def _console_log(message: str) -> None:
    print("[pagepile-api] {}".format(message), flush=True)


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return None


def normalize_pagepile_id(value: Any) -> int:
    if value is None or str(value).strip() == "":
        raise ValueError("A numeric pagepile_id is required.")
    try:
        pagepile_id = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("pagepile_id must be an integer.") from exc
    if pagepile_id <= 0:
        raise ValueError("pagepile_id must be greater than zero.")
    return pagepile_id


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
    if limit > _MAX_PAGEPILE_LOAD_LIMIT:
        raise ValueError("limit must be at most {}.".format(_MAX_PAGEPILE_LOAD_LIMIT))
    return limit


def normalize_source_params(params: Optional[Mapping[str, Any]]) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {}
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


def pagepile_lookup_backend() -> str:
    return service_links.wikidata_lookup_backend()


def _pagepile_api_url() -> str:
    endpoint = str(getattr(settings, "PAGEPILE_API_ENDPOINT", _DEFAULT_PAGEPILE_API_URL)).strip()
    return endpoint or _DEFAULT_PAGEPILE_API_URL


def effective_load_limit(limit: Optional[int], lookup_backend: Optional[str] = None) -> int:
    normalized_limit = normalize_load_limit(limit)
    effective_limit = normalized_limit if normalized_limit is not None else _MAX_PAGEPILE_LOAD_LIMIT
    backend = (
        str(lookup_backend).strip().lower()
        if lookup_backend is not None
        else pagepile_lookup_backend()
    )
    if backend == LOOKUP_BACKEND_API:
        return min(effective_limit, _MAX_PAGEPILE_API_SAMPLE_LIMIT)
    return effective_limit


def build_pagepile_json_url(pagepile_id: int, limit: Optional[int] = None) -> str:
    normalized_pagepile_id = normalize_pagepile_id(pagepile_id)
    params = {
        "id": str(normalized_pagepile_id),
        "action": "get_data",
        "doit": "",
        "format": "json",
    }
    normalized_limit = normalize_load_limit(limit)
    if normalized_limit is not None:
        params["limit"] = str(normalized_limit)
    return "{}?{}".format(_pagepile_api_url(), urlencode(params))


def _request_json(request_url: str, error_prefix: str) -> Dict[str, Any]:
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
            "{}: {}".format(error_prefix, exc),
            public_message=_PAGEPILE_FETCH_PUBLIC_MESSAGE,
        ) from exc

    elapsed_ms = (perf_counter() - started_at) * 1000.0
    _console_log("OK {:.1f} ms {} bytes".format(elapsed_ms, len(raw)))

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise PetscanServiceError("Upstream service returned non-JSON payload.") from exc

    if not isinstance(payload, dict):
        raise PetscanServiceError("Unexpected upstream API format (expected object).")

    return payload


def fetch_pagepile_json(pagepile_id: int, limit: Optional[int] = None) -> Tuple[Dict[str, Any], str]:
    normalized_pagepile_id = normalize_pagepile_id(pagepile_id)
    request_url = build_pagepile_json_url(normalized_pagepile_id, limit=limit)
    return _request_json(request_url, "Failed to fetch PagePile JSON data"), request_url


def _wiki_group_for_site(site: str) -> str:
    normalized_site = str(site or "").strip().lower()
    special_group = _SITE_TOKEN_TO_WIKI_GROUP.get(normalized_site)
    if special_group is not None:
        return special_group
    for suffix, wiki_group in _SITE_TOKEN_SUFFIX_TO_WIKI_GROUP:
        if normalized_site.endswith(suffix):
            return wiki_group
    return normalized_site or "wikimedia"


def _chunked_titles(titles: Sequence[str], size: int) -> List[List[str]]:
    chunk_size = max(1, size)
    return [list(titles[index : index + chunk_size]) for index in range(0, len(titles), chunk_size)]


def _resolve_title_alias(title: str, alias_map: Mapping[str, str]) -> str:
    current = title
    seen = set()
    while current in alias_map and current not in seen:
        seen.add(current)
        next_title = alias_map[current]
        if next_title == current:
            break
        current = next_title
    return current


def _qid_from_page_categories(categories: Any) -> Optional[str]:
    if not isinstance(categories, list):
        return None
    for category in categories:
        if not isinstance(category, Mapping):
            continue
        qid = normalize_qid(category.get("sortkeyprefix"))
        if qid is not None:
            return qid
    return None


@lru_cache(maxsize=128)
def _site_context_for_site(site: str) -> _SiteContext:
    normalized_site = str(site or "").strip().lower()
    if not normalized_site:
        raise PetscanServiceError("PagePile payload is missing wiki site token.")

    api_url = service_links.site_to_mediawiki_api_url(normalized_site)
    if not api_url:
        raise PetscanServiceError("Unsupported PagePile wiki site token: {}.".format(normalized_site))

    parsed_api_url = urlsplit(api_url)
    domain = str(parsed_api_url.hostname or "").strip().lower()
    if not domain:
        raise PetscanServiceError("Could not resolve PagePile wiki domain for {}.".format(normalized_site))

    siteinfo = newpages_source._siteinfo_for_domain(domain)
    descriptor = newpages_source._known_wikis_by_domain().get(domain)
    site_url = descriptor.site_url if descriptor is not None else "https://{}/".format(domain)
    lang_code = (
        descriptor.lang_code
        if descriptor is not None and str(descriptor.lang_code or "").strip()
        else siteinfo.lang_code
    )
    wiki_group = descriptor.wiki_group if descriptor is not None else _wiki_group_for_site(normalized_site)
    return _SiteContext(
        site=normalized_site,
        domain=domain,
        dbname=normalized_site,
        lang_code=lang_code,
        site_url=site_url,
        wiki_group=wiki_group,
        siteinfo=siteinfo,
    )


def _api_page_query_params(full_titles: Sequence[str], site_context: _SiteContext) -> Dict[str, str]:
    params = {
        "action": "query",
        "titles": "|".join(full_titles),
        "prop": "pageprops|info",
        "ppprop": "wikibase_item",
        "redirects": "1",
        "format": "json",
        "formatversion": "2",
    }
    if site_context.domain == _INCUBATOR_DOMAIN:
        params["prop"] = "pageprops|info|categories"
        params["clcategories"] = _INCUBATOR_WIKIDATA_CATEGORY_PAGE
        params["clprop"] = "sortkey"
        params["cllimit"] = "max"
    return params


def _fetch_page_rows_api(
    site_context: _SiteContext,
    full_titles: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    rows_by_full_title: Dict[str, Dict[str, Any]] = {}
    api_url = service_links.site_to_mediawiki_api_url(site_context.site)
    if not api_url:
        raise PetscanServiceError("Unsupported PagePile wiki site token: {}.".format(site_context.site))

    for batch in _chunked_titles(full_titles, _MAX_API_TITLES_PER_BATCH):
        params = _api_page_query_params(batch, site_context)
        payload = _request_json(
            "{}?{}".format(api_url, urlencode(params)),
            "Failed to fetch page metadata from MediaWiki API",
        )
        query_payload = payload.get("query")
        if not isinstance(query_payload, Mapping):
            raise PetscanServiceError("Page metadata API returned no query payload.")

        alias_map: Dict[str, str] = {}
        for mapping_key in ("normalized", "redirects"):
            entries = query_payload.get(mapping_key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                source_title = normalize_page_title(entry.get("from"))
                target_title = normalize_page_title(entry.get("to"))
                if source_title and target_title:
                    alias_map[source_title] = target_title

        page_rows_by_title: Dict[str, Dict[str, Any]] = {}
        pages = query_payload.get("pages")
        if not isinstance(pages, list):
            raise PetscanServiceError("Page metadata API returned no pages payload.")

        for page in pages:
            if not isinstance(page, Mapping):
                continue
            if page.get("missing") is not None:
                continue

            title = normalize_page_title(page.get("title"))
            if not title:
                continue

            page_id = _coerce_int(page.get("pageid"))
            namespace_id = _coerce_int(page.get("ns", 0))
            if page_id is None or namespace_id is None:
                continue

            pageprops = page.get("pageprops")
            qid = None
            if isinstance(pageprops, Mapping):
                qid = normalize_qid(pageprops.get("wikibase_item"))
            if qid is None and site_context.domain == _INCUBATOR_DOMAIN:
                qid = _qid_from_page_categories(page.get("categories"))

            db_title = newpages_source._normalize_api_page_title(title, namespace_id, site_context.siteinfo)
            page_rows_by_title[title] = {
                "page_id": page_id,
                "namespace": namespace_id,
                "db_title": db_title,
                "wikidata_id": qid,
            }

        for full_title in batch:
            normalized_full_title = normalize_page_title(full_title)
            if not normalized_full_title:
                continue
            resolved_title = _resolve_title_alias(normalized_full_title, alias_map)
            row = page_rows_by_title.get(resolved_title) or page_rows_by_title.get(normalized_full_title)
            if row is not None:
                rows_by_full_title[normalized_full_title] = dict(row)

    return rows_by_full_title


def _replica_connect_kwargs(site_context: _SiteContext) -> Dict[str, Any]:
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    connect_kwargs: Dict[str, Any] = {
        "host": "{}.web.db.svc.wikimedia.cloud".format(site_context.site),
        "database": "{}_p".format(site_context.site),
        "charset": "utf8mb4",
        "connect_timeout": timeout,
        "read_timeout": timeout,
        "write_timeout": timeout,
        "autocommit": True,
    }
    replica_cnf = str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip()
    if replica_cnf:
        connect_kwargs["read_default_file"] = os.path.expanduser(os.path.expandvars(replica_cnf))
    return connect_kwargs


def _fetch_page_rows_sql(
    site_context: _SiteContext,
    full_titles: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_PAGEPILE_FETCH_PUBLIC_MESSAGE,
        )

    unique_targets: List[Tuple[int, str]] = []
    seen_targets = set()
    for full_title in full_titles:
        namespace_id, db_title = newpages_source._namespace_and_db_title_for_full_title(
            full_title,
            site_context.siteinfo,
        )
        if not db_title:
            continue
        key = (namespace_id, db_title)
        if key in seen_targets:
            continue
        seen_targets.add(key)
        unique_targets.append(key)

    if not unique_targets:
        return {}

    placeholders = ", ".join(["(%s, %s)"] * len(unique_targets))
    params: List[Any] = []
    if site_context.domain == _INCUBATOR_DOMAIN:
        sql = (  # nosec B608
            "SELECT p.page_namespace, p.page_title, p.page_id, "
            "COALESCE(pp.pp_value, cl.cl_sortkey_prefix) AS qid "
            "FROM page AS p "
            "LEFT JOIN page_props AS pp "
            "ON pp.pp_page = p.page_id AND pp.pp_propname = %s "
            "LEFT JOIN linktarget AS lt "
            "ON lt.lt_namespace = 14 AND lt.lt_title = %s "
            "LEFT JOIN categorylinks AS cl "
            "ON cl.cl_from = p.page_id AND cl.cl_target_id = lt.lt_id "
            "WHERE (p.page_namespace, p.page_title) IN ({}) "
        ).format(placeholders)
        params.extend(["wikibase_item", _INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE])
    else:
        sql = (  # nosec B608
            "SELECT p.page_namespace, p.page_title, p.page_id, pp.pp_value "
            "FROM page AS p "
            "LEFT JOIN page_props AS pp "
            "ON pp.pp_page = p.page_id AND pp.pp_propname = %s "
            "WHERE (p.page_namespace, p.page_title) IN ({})"
        ).format(placeholders)
        params.append("wikibase_item")

    for namespace_id, db_title in unique_targets:
        params.extend([namespace_id, db_title])

    connection = None
    try:
        connection = pymysql.connect(**cast(Any, _replica_connect_kwargs(site_context)))
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch PagePile replica data: {}".format(exc),
            public_message=_PAGEPILE_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    rows_by_key: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, (tuple, list)) or len(row) < 4:
            continue
        try:
            namespace_id = int(row[0])
            page_id = int(row[2])
        except (TypeError, ValueError):
            continue
        db_title = normalize_page_title(
            newpages_source._normalize_db_page_title(row[1])
        )
        if not db_title:
            continue
        qid = normalize_qid(row[3])
        rows_by_key[(namespace_id, db_title)] = {
            "page_id": page_id,
            "namespace": namespace_id,
            "db_title": db_title,
            "wikidata_id": qid,
        }

    rows_by_full_title: Dict[str, Dict[str, Any]] = {}
    for full_title in full_titles:
        normalized_full_title = normalize_page_title(full_title)
        if not normalized_full_title:
            continue
        key = newpages_source._namespace_and_db_title_for_full_title(
            normalized_full_title,
            site_context.siteinfo,
        )
        row = rows_by_key.get(key)
        if row is not None:
            rows_by_full_title[normalized_full_title] = dict(row)

    return rows_by_full_title


def _build_record(site_context: _SiteContext, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    page_id = _coerce_int(row.get("page_id"))
    namespace_id = _coerce_int(row.get("namespace", 0))
    if page_id is None or namespace_id is None:
        return None

    db_title = normalize_page_title(row.get("db_title"))
    if not db_title:
        return None
    full_title = newpages_source._title_with_namespace(db_title, namespace_id, site_context.siteinfo)
    page_url = newpages_source._page_url(site_context.domain, site_context.siteinfo, namespace_id, db_title)
    qid = normalize_qid(row.get("wikidata_id"))
    if not full_title or not page_url:
        return None

    lang_code = site_context.lang_code
    wiki_group = site_context.wiki_group
    site_url = site_context.site_url
    if site_context.domain == _INCUBATOR_DOMAIN:
        incubator_project, incubator_lang_code, _page_name = newpages_source._split_incubator_title(db_title)
        if incubator_lang_code:
            lang_code = incubator_lang_code
        if incubator_project:
            wiki_group = newpages_source._incubator_wiki_group_for_title(db_title)
        site_url = _INCUBATOR_SITE_URL

    record: Dict[str, Any] = {
        "page_id": page_id,
        "page_title": full_title,
        "page_label": full_title.replace("_", " "),
        "namespace": namespace_id,
        "page_url": page_url,
        "site_url": site_url,
        "wiki_domain": site_context.domain,
        "wiki_dbname": site_context.dbname,
        "wiki_group": wiki_group,
    }
    if lang_code:
        record["lang_code"] = lang_code
    if site_context.site == _COMMONS_SITE and namespace_id == _COMMONS_FILE_NAMESPACE:
        record["wikidata_entity"] = "{}{}".format(_COMMONS_MEDIAITEM_ENTITY_BASE, page_id)
    if qid is not None:
        record["wikidata_id"] = qid
        if "wikidata_entity" not in record:
            record["wikidata_entity"] = "http://www.wikidata.org/entity/{}".format(qid)
    return record


def fetch_pagepile_records(
    pagepile_id: int,
    limit: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    normalized_pagepile_id = normalize_pagepile_id(pagepile_id)
    effective_limit = effective_load_limit(limit)
    payload, source_url = fetch_pagepile_json(normalized_pagepile_id, limit=effective_limit)

    raw_site = str(payload.get("wiki", "") or "").strip().lower()
    if not raw_site:
        raise PetscanServiceError("PagePile payload is missing wiki.")

    raw_pages = payload.get("pages")
    if not isinstance(raw_pages, list):
        raise PetscanServiceError("PagePile payload is missing pages array.")

    full_titles: List[str] = []
    seen_titles = set()
    for raw_page in raw_pages:
        normalized_title = normalize_page_title(raw_page)
        if not normalized_title or normalized_title in seen_titles:
            continue
        seen_titles.add(normalized_title)
        full_titles.append(normalized_title)
        if len(full_titles) >= effective_limit:
            break
    if not full_titles:
        return [], source_url

    site_context = _site_context_for_site(raw_site)
    if pagepile_lookup_backend() == LOOKUP_BACKEND_TOOLFORGE_SQL:
        rows_by_full_title = _fetch_page_rows_sql(site_context, full_titles)
    else:
        rows_by_full_title = _fetch_page_rows_api(site_context, full_titles)

    records: List[Dict[str, Any]] = []
    for full_title in full_titles:
        row = rows_by_full_title.get(full_title)
        if row is None:
            continue
        record = _build_record(site_context, row)
        if record is not None:
            records.append(record)

    return records, source_url
