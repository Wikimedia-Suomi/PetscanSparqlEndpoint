"""Derived MediaWiki page URI columns for Quarry rows."""

import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, Mapping, Optional
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from django.conf import settings

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


@dataclass(frozen=True)
class _CompiledSiteInfo:
    origin: str
    article_path_prefix: str
    article_path_suffix: str
    namespace_prefixes: Dict[int, str]
    interwiki_urls: Dict[str, str]


@dataclass(frozen=True)
class _PageUriSpec:
    title_key: str
    uri_key: str
    namespace_key: Optional[str] = None
    fixed_namespace: Optional[int] = None


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


def _normalize_title(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value or "").strip()
    if not text:
        return ""
    return text.lstrip(":").replace(" ", "_")


def _namespace_id(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return int(text)
        except (TypeError, ValueError):
            return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _compile_siteinfo(siteinfo: Mapping[str, Any]) -> Optional[_CompiledSiteInfo]:
    domain = str(siteinfo.get("domain", "") or "").strip().lower()
    if not domain:
        return None

    article_path = str(siteinfo.get("article_path", "/wiki/$1") or "/wiki/$1")
    if "$1" in article_path:
        article_path_prefix, article_path_suffix = article_path.split("$1", 1)
    else:
        article_path_prefix, article_path_suffix = "{}/".format(article_path.rstrip("/")), ""

    if not article_path_prefix.startswith("/"):
        article_path_prefix = "/{}".format(article_path_prefix)

    namespace_prefixes = {}  # type: Dict[int, str]
    namespace_names = siteinfo.get("namespace_names")
    if isinstance(namespace_names, Mapping):
        for raw_namespace, raw_name in namespace_names.items():
            namespace_name = str(raw_name or "").strip()
            namespace_prefixes[_namespace_id(raw_namespace)] = (
                "{}:".format(namespace_name) if namespace_name else ""
            )

    interwiki_urls = {}  # type: Dict[str, str]
    raw_interwiki_urls = siteinfo.get("interwiki_urls")
    if isinstance(raw_interwiki_urls, Mapping):
        for raw_prefix, raw_url in raw_interwiki_urls.items():
            prefix = str(raw_prefix or "").strip()
            url = str(raw_url or "").strip()
            if prefix and url:
                interwiki_urls[prefix] = url

    return _CompiledSiteInfo(
        origin="https://{}".format(domain),
        article_path_prefix=article_path_prefix,
        article_path_suffix=article_path_suffix,
        namespace_prefixes=namespace_prefixes,
        interwiki_urls=interwiki_urls,
    )


def _page_uri(siteinfo: _CompiledSiteInfo, namespace: Any, title: Any) -> Optional[str]:
    normalized_title = _normalize_title(title)
    if not normalized_title:
        return None

    namespace_id = _namespace_id(namespace)
    namespace_prefix = siteinfo.namespace_prefixes.get(namespace_id, "")
    page_title = "{}{}".format(namespace_prefix, normalized_title)
    encoded_title = quote(page_title, safe=":_/()-.,")
    return "{}{}{}".format(
        siteinfo.origin,
        siteinfo.article_path_prefix,
        encoded_title,
    ) + siteinfo.article_path_suffix


def _interwiki_uri(siteinfo: _CompiledSiteInfo, prefix: Any, title: Any) -> Optional[str]:
    normalized_prefix = str(prefix or "").strip()
    normalized_title = _normalize_title(title)
    if not normalized_prefix or not normalized_title:
        return None

    raw_url = siteinfo.interwiki_urls.get(normalized_prefix, "")
    if not raw_url:
        return None

    encoded_title = quote(normalized_title, safe=":_/()-.,")
    return raw_url.replace("$1", encoded_title) if "$1" in raw_url else raw_url


def build_uri_field_deriver(
    query_db: Optional[str],
    row_keys: Iterable[str],
) -> Optional[Callable[[Mapping[str, Any]], Dict[str, str]]]:
    query_db_text = str(query_db or "").strip()
    if not query_db_text:
        return None

    raw_siteinfo = _siteinfo_for_query_db(query_db_text)
    if raw_siteinfo is None:
        return None

    siteinfo = _compile_siteinfo(raw_siteinfo)
    if siteinfo is None:
        return None

    key_set = {str(key).strip() for key in row_keys if str(key).strip()}
    page_specs: list[_PageUriSpec] = []

    for prefix in _NAMESPACE_TITLE_PREFIXES:
        title_key = "{}_title".format(prefix)
        if title_key not in key_set:
            continue
        namespace_key = "{}_namespace".format(prefix)
        page_specs.append(
            _PageUriSpec(
                title_key=title_key,
                uri_key="{}_uri".format(prefix),
                namespace_key=namespace_key if namespace_key in key_set else None,
            )
        )

    for column_name, uri_column in _FILE_TITLE_COLUMNS.items():
        if column_name in key_set:
            page_specs.append(
                _PageUriSpec(
                    title_key=column_name,
                    uri_key=uri_column,
                    fixed_namespace=6,
                )
            )

    for column_name, uri_column in _CATEGORY_TITLE_COLUMNS.items():
        if column_name in key_set:
            page_specs.append(
                _PageUriSpec(
                    title_key=column_name,
                    uri_key=uri_column,
                    fixed_namespace=14,
                )
            )

    include_interwiki = "iwl_prefix" in key_set and "iwl_title" in key_set

    if not page_specs and not include_interwiki:
        return None

    page_specs_tuple = tuple(page_specs)

    def _derive(record: Mapping[str, Any]) -> Dict[str, str]:
        derived = {}  # type: Dict[str, str]
        record_get = record.get

        for spec in page_specs_tuple:
            namespace = (
                spec.fixed_namespace
                if spec.fixed_namespace is not None
                else record_get(spec.namespace_key) if spec.namespace_key is not None else 0
            )
            uri = _page_uri(siteinfo, namespace, record_get(spec.title_key))
            if uri is not None:
                derived[spec.uri_key] = uri

        if include_interwiki:
            interwiki_uri = _interwiki_uri(siteinfo, record_get("iwl_prefix"), record_get("iwl_title"))
            if interwiki_uri is not None:
                derived["iwl_uri"] = interwiki_uri

        return derived

    return _derive


def derive_uri_fields(record: Mapping[str, Any], query_db: Optional[str]) -> Dict[str, str]:
    deriver = build_uri_field_deriver(query_db, record.keys())
    if deriver is None:
        return {}
    return deriver(record)
