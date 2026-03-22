"""Derived MediaWiki page URI columns for Quarry rows."""

import json
from functools import lru_cache
from typing import Any, Dict, Mapping, Optional
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from django.conf import settings

from petscan import normalization
from petscan import service_links as links
from petscan.service_source import HTTP_USER_AGENT

_NAMESPACE_TITLE_PREFIXES = (
    "page",
    "ar",
    "rd",
    "pt",
    "wl",
    "rc",
    "log",
    "tl",
    "pl",
    "lt",
)
_FILE_TITLE_COLUMNS = {
    "img_name": "img_uri",
    "oi_name": "oi_uri",
    "fa_name": "fa_uri",
}
_CATEGORY_TITLE_COLUMNS = {
    "cl_to": "cl_uri",
}


def _query_db_site_token(query_db: str) -> str:
    text = str(query_db or "").strip().lower()
    if text.endswith("_p"):
        text = text[:-2]
    return text


@lru_cache(maxsize=64)
def _siteinfo_for_query_db(query_db: str) -> Optional[Dict[str, Any]]:
    site_token = _query_db_site_token(query_db)
    if not site_token:
        return None

    api_url = links.site_to_mediawiki_api_url(site_token)
    if api_url is None:
        return None

    parsed_api = urlsplit(api_url)
    domain = str(parsed_api.hostname or "").strip().lower()
    if not domain:
        return None

    request_url = (
        api_url
        + "?action=query&meta=siteinfo&siprop=general|namespaces|interwikimap"
        + "&format=json"
    )
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        },
    )
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))

    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310
            raw = response.read()
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        return None

    query_payload = payload.get("query") if isinstance(payload, Mapping) else None
    if not isinstance(query_payload, Mapping):
        return None

    general = query_payload.get("general")
    namespaces = query_payload.get("namespaces")
    interwikimap = query_payload.get("interwikimap")

    article_path = "/wiki/$1"
    if isinstance(general, Mapping):
        configured_article_path = str(general.get("articlepath", "")).strip()
        if configured_article_path:
            article_path = configured_article_path

    namespace_names = {}  # type: Dict[int, str]
    if isinstance(namespaces, Mapping):
        for raw_id, payload_value in namespaces.items():
            try:
                namespace_id = int(str(raw_id).strip())
            except (TypeError, ValueError):
                continue
            if not isinstance(payload_value, Mapping):
                continue
            namespace_name = str(payload_value.get("*", "") or payload_value.get("canonical", "")).strip()
            namespace_names[namespace_id] = namespace_name

    interwiki_urls = {}  # type: Dict[str, str]
    if isinstance(interwikimap, list):
        for entry in interwikimap:
            if not isinstance(entry, Mapping):
                continue
            prefix = str(entry.get("prefix", "")).strip()
            url = str(entry.get("url", "")).strip()
            if prefix and url:
                interwiki_urls[prefix] = url

    return {
        "domain": domain,
        "article_path": article_path,
        "namespace_names": namespace_names,
        "interwiki_urls": interwiki_urls,
    }


def _page_uri(siteinfo: Mapping[str, Any], namespace: Any, title: Any) -> Optional[str]:
    normalized_title = normalization.normalize_page_title(title)
    if not normalized_title:
        return None

    try:
        namespace_id = int(str(namespace).strip()) if namespace is not None else 0
    except (TypeError, ValueError):
        namespace_id = 0

    namespace_names = siteinfo.get("namespace_names")
    namespace_name = ""
    if isinstance(namespace_names, Mapping):
        namespace_name = str(namespace_names.get(namespace_id, "") or "").strip()

    page_title = normalized_title if not namespace_name else "{}:{}".format(namespace_name, normalized_title)
    encoded_title = quote(page_title, safe=":_/()-.,")
    article_path = str(siteinfo.get("article_path", "/wiki/$1") or "/wiki/$1")
    domain = str(siteinfo.get("domain", "")).strip().lower()
    if not domain:
        return None

    if "$1" in article_path:
        path = article_path.replace("$1", encoded_title)
    else:
        path = "{}/{}".format(article_path.rstrip("/"), encoded_title)
    if not path.startswith("/"):
        path = "/{}".format(path)
    return "https://{}{}".format(domain, path)


def _interwiki_uri(siteinfo: Mapping[str, Any], prefix: Any, title: Any) -> Optional[str]:
    normalized_prefix = str(prefix or "").strip()
    normalized_title = normalization.normalize_page_title(title)
    if not normalized_prefix or not normalized_title:
        return None

    interwiki_urls = siteinfo.get("interwiki_urls")
    if not isinstance(interwiki_urls, Mapping):
        return None

    raw_url = str(interwiki_urls.get(normalized_prefix, "") or "").strip()
    if not raw_url:
        return None

    encoded_title = quote(normalized_title, safe=":_/()-.,")
    return raw_url.replace("$1", encoded_title) if "$1" in raw_url else raw_url


def derive_uri_fields(record: Mapping[str, Any], query_db: Optional[str]) -> Dict[str, str]:
    if query_db is None or not str(query_db).strip():
        return {}

    siteinfo = _siteinfo_for_query_db(str(query_db).strip())
    if siteinfo is None:
        return {}

    derived = {}  # type: Dict[str, str]

    for prefix in _NAMESPACE_TITLE_PREFIXES:
        uri = _page_uri(siteinfo, record.get("{}_namespace".format(prefix)), record.get("{}_title".format(prefix)))
        if uri is not None:
            derived["{}_uri".format(prefix)] = uri

    for column_name, uri_column in _FILE_TITLE_COLUMNS.items():
        uri = _page_uri(siteinfo, 6, record.get(column_name))
        if uri is not None:
            derived[uri_column] = uri

    for column_name, uri_column in _CATEGORY_TITLE_COLUMNS.items():
        uri = _page_uri(siteinfo, 14, record.get(column_name))
        if uri is not None:
            derived[uri_column] = uri

    interwiki_uri = _interwiki_uri(siteinfo, record.get("iwl_prefix"), record.get("iwl_title"))
    if interwiki_uri is not None:
        derived["iwl_uri"] = interwiki_uri

    return derived
