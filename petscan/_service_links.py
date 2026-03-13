"""GIL-link parsing, site normalization, and Wikidata lookup helpers."""

import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote, urlsplit

from django.conf import settings

from . import enrichment_sql as _enrichment_sql
from ._service_source import HTTP_USER_AGENT
from .enrichment_api import fetch_wikibase_items_for_site_api as _api_fetch_wikibase_items_for_site

_sql_fetch_wikibase_items_for_site = _enrichment_sql.fetch_wikibase_items_for_site_sql
pymysql = _enrichment_sql.pymysql

_QID_RE = re.compile(r"Q([1-9][0-9]*)", re.IGNORECASE)
_SITE_TOKEN_RE = re.compile(r"^[a-z0-9_-]+$")
_HOST_LABEL_RE = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
_MAX_TITLES_PER_MEDIAWIKI_BATCH = 50
_LOOKUP_BACKEND_API = "api"
_LOOKUP_BACKEND_TOOLFORGE_SQL = "toolforge_sql"


def _split_pipe_values(text: str) -> List[str]:
    values = []
    for part in text.split("|"):
        normalized = part.strip()
        if normalized:
            values.append(normalized)
    return values


def _normalize_qid(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _QID_RE.search(text)
    if not match:
        return None
    return "Q{}".format(match.group(1))


def _extract_qid(record: Mapping[str, Any]) -> Optional[str]:
    candidates = [
        record.get("wikidata_id"),
        record.get("qid"),
        record.get("q"),
        record.get("wikidata"),
    ]
    metadata = record.get("metadata")
    if isinstance(metadata, Mapping):
        candidates.append(metadata.get("wikidata"))

    for candidate in candidates:
        qid = _normalize_qid(candidate)
        if qid is not None:
            return qid
    return None


def _normalize_page_title(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # Some inputs use a leading ":" for main-namespace titles.
    return text.lstrip(":").replace(" ", "_")


def _parse_gil_link_target(link: str) -> Optional[Tuple[str, int, str]]:
    parts = str(link).split(":", 2)
    if len(parts) != 3:
        return None

    site = parts[0].strip().lower()
    try:
        namespace = int(str(parts[1]).strip())
    except Exception:
        return None
    title = _normalize_page_title(parts[2])
    if not site or not title:
        return None
    return site, namespace, title


def _is_valid_hostname(hostname: str) -> bool:
    text = str(hostname or "").strip().lower().rstrip(".")
    if not text:
        return False
    if len(text) > 253:
        return False
    if any(char in text for char in ("/", "\\", ":", "@", " ")):
        return False

    labels = text.split(".")
    if len(labels) < 2:
        return False
    for label in labels:
        if not _HOST_LABEL_RE.fullmatch(label):
            return False

    return True


def _site_to_mediawiki_domain(site: str) -> Optional[str]:
    normalized_site = str(site or "").strip().lower()
    if not normalized_site:
        return None
    if not _SITE_TOKEN_RE.fullmatch(normalized_site):
        return None

    def _build_valid_domain(prefix: str, domain: str) -> Optional[str]:
        candidate = "{}.{}".format(prefix, domain)
        return candidate if _is_valid_hostname(candidate) else None

    special_sites = {
        "commonswiki": "commons.wikimedia.org",
        "wikidatawiki": "www.wikidata.org",
        "metawiki": "meta.wikimedia.org",
        "specieswiki": "species.wikimedia.org",
        "incubatorwiki": "incubator.wikimedia.org",
        "mediawikiwiki": "www.mediawiki.org",
    }
    if normalized_site in special_sites:
        candidate = special_sites[normalized_site]
        return candidate if _is_valid_hostname(candidate) else None

    suffix_domains = [
        ("wikivoyage", "wikivoyage.org"),
        ("wikiversity", "wikiversity.org"),
        ("wikisource", "wikisource.org"),
        ("wiktionary", "wiktionary.org"),
        ("wikiquote", "wikiquote.org"),
        ("wikibooks", "wikibooks.org"),
        ("wikinews", "wikinews.org"),
    ]
    for suffix, domain in suffix_domains:
        if normalized_site.endswith(suffix):
            language_code = normalized_site[: -len(suffix)]
            if not language_code:
                return None
            language_code = language_code.replace("_", "-")
            return _build_valid_domain(language_code, domain)

    if normalized_site.endswith("wiki"):
        language_code = normalized_site[:-4]
        if not language_code:
            return None
        language_code = language_code.replace("_", "-")
        return _build_valid_domain(language_code, "wikipedia.org")

    return None


def _site_to_mediawiki_api_url(site: str) -> Optional[str]:
    domain = _site_to_mediawiki_domain(site)
    if domain is None:
        return None

    api_url = "https://{}/w/api.php".format(domain)
    parsed = urlsplit(api_url)
    if parsed.scheme != "https":
        return None
    if parsed.hostname != domain:
        return None
    if parsed.path != "/w/api.php":
        return None
    return api_url


def _gil_link_uri(site: str, title: str) -> Optional[str]:
    domain = _site_to_mediawiki_domain(site)
    normalized_title = _normalize_page_title(title)
    if domain is None or not normalized_title:
        return None
    encoded_title = quote(normalized_title, safe=":_/()-.,")
    return "https://{}/wiki/{}".format(domain, encoded_title)


def _namespace_db_title(namespace: int, title: str) -> str:
    normalized_title = _normalize_page_title(title)
    if namespace != 0 and ":" in normalized_title:
        return normalized_title.split(":", 1)[1]
    return normalized_title


def _iter_gil_link_targets(record: Mapping[str, Any]) -> List[Tuple[str, str, int, str, str]]:
    raw_gil = record.get("gil")
    if not isinstance(raw_gil, str):
        return []

    targets = []  # type: List[Tuple[str, str, int, str, str]]
    seen = set()
    for raw_link in _split_pipe_values(raw_gil):
        parsed = _parse_gil_link_target(raw_link)
        if parsed is None:
            continue
        site, namespace, title = parsed
        link_uri = _gil_link_uri(site, title)
        if not link_uri or link_uri in seen:
            continue
        seen.add(link_uri)
        targets.append((link_uri, site, namespace, title, _namespace_db_title(namespace, title)))
    return targets


def _iter_gil_link_uris(record: Mapping[str, Any]) -> List[str]:
    return [
        link_uri
        for link_uri, _site, _namespace, _api_title, _db_title in _iter_gil_link_targets(record)
    ]


def _iter_gil_link_enrichment(
    record: Mapping[str, Any],
    gil_link_wikidata_map: Optional[Mapping[str, str]] = None,
) -> List[Tuple[str, Optional[str]]]:
    enriched = []  # type: List[Tuple[str, Optional[str]]]
    for link_uri in _iter_gil_link_uris(record):
        qid = None
        if gil_link_wikidata_map is not None:
            qid = _normalize_qid(gil_link_wikidata_map.get(link_uri))
        enriched.append((link_uri, qid))
    return enriched


def _chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 1
    for index in range(0, len(values), size):
        yield list(values[index : index + size])


def _wikidata_lookup_backend() -> str:
    configured = str(getattr(settings, "WIKIDATA_LOOKUP_BACKEND", "") or "").strip().lower()
    if configured in {_LOOKUP_BACKEND_API, _LOOKUP_BACKEND_TOOLFORGE_SQL}:
        return configured
    if bool(getattr(settings, "TOOLFORGE_USE_REPLICA", False)):
        return _LOOKUP_BACKEND_TOOLFORGE_SQL
    return _LOOKUP_BACKEND_API


def _fetch_wikibase_items_for_site_api(api_url: str, titles: Sequence[str]) -> Dict[str, str]:
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    return _api_fetch_wikibase_items_for_site(
        api_url,
        titles,
        user_agent=HTTP_USER_AGENT,
        timeout_seconds=timeout,
    )


def _fetch_wikibase_items_for_site_sql(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
) -> Dict[str, str]:
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    return _sql_fetch_wikibase_items_for_site(
        site,
        targets,
        timeout_seconds=timeout,
        replica_host=str(getattr(settings, "TOOLFORGE_REPLICA_HOST", "tools.db.svc.wikimedia.cloud")),
        replica_cnf=str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip(),
        replica_user=str(getattr(settings, "TOOLFORGE_REPLICA_USER", "") or "").strip(),
        replica_password=str(getattr(settings, "TOOLFORGE_REPLICA_PASSWORD", "") or "").strip(),
    )


def _fetch_wikibase_items_for_site(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
    backend: str,
) -> Dict[str, str]:
    if not targets:
        return {}

    if backend == _LOOKUP_BACKEND_TOOLFORGE_SQL:
        return _fetch_wikibase_items_for_site_sql(site, targets)

    api_url = _site_to_mediawiki_api_url(site)
    if api_url is None:
        return {}
    titles = sorted({_normalize_page_title(api_title) for _ns, api_title, _db in targets if api_title})
    resolved = {}  # type: Dict[str, str]
    for batch in _chunked(titles, _MAX_TITLES_PER_MEDIAWIKI_BATCH):
        batch_result = _fetch_wikibase_items_for_site_api(api_url, batch)
        resolved.update(batch_result)
    return resolved


def _direct_wikidata_qid_for_target(
    site: str,
    namespace: int,
    api_title: str,
    db_title: str,
) -> Optional[str]:
    normalized_site = str(site or "").strip().lower()
    if normalized_site not in {"wikidatawiki", "www.wikidata.org"}:
        return None

    try:
        if int(namespace) != 0:
            return None
    except Exception:
        return None

    for candidate in (api_title, db_title):
        normalized_title = _normalize_page_title(candidate)
        if re.fullmatch(r"Q[1-9][0-9]*", normalized_title, flags=re.IGNORECASE):
            return "Q{}".format(normalized_title[1:])
    return None
