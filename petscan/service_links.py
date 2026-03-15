"""GIL-link parsing, site normalization, and Wikidata lookup helpers."""

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple
from urllib.parse import quote, urlsplit

from django.conf import settings

from . import enrichment_sql
from .enrichment_api import fetch_wikibase_items_for_site_api
from .normalization import normalize_page_title as _normalize_page_title
from .normalization import normalize_qid as _normalize_qid
from .service_source import HTTP_USER_AGENT

_SITE_TOKEN_RE = re.compile(r"^[a-z0-9_-]+$")
_HOST_LABEL_RE = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
_MAX_TITLES_PER_MEDIAWIKI_BATCH = 50
LOOKUP_BACKEND_API = "api"
LOOKUP_BACKEND_TOOLFORGE_SQL = "toolforge_sql"
__all__ = [
    "LOOKUP_BACKEND_API",
    "LOOKUP_BACKEND_TOOLFORGE_SQL",
    "build_gil_link_enrichment_map",
    "extract_qid",
    "iter_gil_link_enrichment",
    "iter_gil_link_uris",
    "resolve_gil_links",
    "site_to_mediawiki_api_url",
    "wikidata_lookup_backend",
]


@dataclass(frozen=True)
class GilLinkTarget:
    link_uri: str
    site: str
    namespace: int
    api_title: str
    db_title: str


@dataclass(frozen=True)
class SiteLookupTarget:
    namespace: int
    api_title: str
    db_title: str


def _split_pipe_values(text: str) -> List[str]:
    values = []
    for part in text.split("|"):
        normalized = part.strip()
        if normalized:
            values.append(normalized)
    return values


def extract_qid(record: Mapping[str, Any]) -> Optional[str]:
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
        "outreachwiki": "outreach.wikimedia.org",
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


def site_to_mediawiki_api_url(site: str) -> Optional[str]:
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


def _iter_gil_link_targets(record: Mapping[str, Any]) -> List[GilLinkTarget]:
    raw_gil = record.get("gil")
    if not isinstance(raw_gil, str):
        return []

    targets = []  # type: List[GilLinkTarget]
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
        targets.append(
            GilLinkTarget(
                link_uri=link_uri,
                site=site,
                namespace=namespace,
                api_title=title,
                db_title=_namespace_db_title(namespace, title),
            )
        )
    return targets


def iter_gil_link_uris(record: Mapping[str, Any]) -> List[str]:
    return [link_uri for link_uri, _qid in resolve_gil_links(record)]


def iter_gil_link_enrichment(
    record: Mapping[str, Any],
    gil_link_enrichment_map: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> List[Tuple[str, Optional[str]]]:
    return resolve_gil_links(record, gil_link_enrichment_map=gil_link_enrichment_map)


def resolve_gil_links(
    record: Mapping[str, Any],
    gil_link_enrichment_map: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> List[Tuple[str, Optional[str]]]:
    enriched = []  # type: List[Tuple[str, Optional[str]]]
    for target in _iter_gil_link_targets(record):
        link_uri = target.link_uri
        qid = None
        if gil_link_enrichment_map is not None:
            payload = gil_link_enrichment_map.get(link_uri)
            if isinstance(payload, Mapping):
                qid = _normalize_qid(payload.get("wikidata_id"))
        enriched.append((link_uri, qid))
    return enriched


def _chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 1
    for index in range(0, len(values), size):
        yield list(values[index : index + size])


def wikidata_lookup_backend() -> str:
    configured = str(getattr(settings, "WIKIDATA_LOOKUP_BACKEND", "") or "").strip().lower()
    if configured in {LOOKUP_BACKEND_API, LOOKUP_BACKEND_TOOLFORGE_SQL}:
        return configured
    if bool(getattr(settings, "TOOLFORGE_USE_REPLICA", False)):
        return LOOKUP_BACKEND_TOOLFORGE_SQL
    return LOOKUP_BACKEND_API


def _normalize_page_len(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        page_len = int(value)
    except Exception:
        return None
    if page_len < 0:
        return None
    return page_len


def _normalize_revision_timestamp_xsd(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    if re.fullmatch(r"\d{14}", text):
        formatted = "{}-{}-{}T{}:{}:{}+00:00".format(
            text[0:4],
            text[4:6],
            text[6:8],
            text[8:10],
            text[10:12],
            text[12:14],
        )
    else:
        formatted = text[:-1] + "+00:00" if text.endswith("Z") else text

    try:
        parsed = datetime.fromisoformat(formatted)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    normalized = parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return normalized.replace("+00:00", "Z")


def _normalize_link_enrichment_payload(payload: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    qid = _normalize_qid(payload.get("wikidata_id"))
    page_len = _normalize_page_len(payload.get("page_len"))
    rev_timestamp = _normalize_revision_timestamp_xsd(payload.get("rev_timestamp"))

    if qid is None and page_len is None and rev_timestamp is None:
        return None

    return {
        "wikidata_id": qid,
        "page_len": page_len,
        "rev_timestamp": rev_timestamp,
    }


def _fetch_wikibase_enrichment_for_site_api(
    api_url: str,
    titles: Sequence[str],
    lookup_stats: Optional[MutableMapping[str, float]] = None,
) -> Dict[str, Dict[str, Any]]:
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    fetched = fetch_wikibase_items_for_site_api(
        api_url,
        titles,
        user_agent=HTTP_USER_AGENT,
        timeout_seconds=timeout,
        lookup_stats=lookup_stats,
    )
    resolved = {}  # type: Dict[str, Dict[str, Any]]
    for title, payload in fetched.items():
        if not isinstance(payload, Mapping):
            continue
        normalized_title = _normalize_page_title(title)
        normalized_payload = _normalize_link_enrichment_payload(payload)
        if normalized_title and normalized_payload is not None:
            resolved[normalized_title] = normalized_payload
    return resolved


def _fetch_wikibase_enrichment_for_site_sql(
    site: str,
    targets: Sequence[SiteLookupTarget],
    lookup_stats: Optional[MutableMapping[str, float]] = None,
) -> Dict[str, Dict[str, Any]]:
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    sql_targets = [(target.namespace, target.api_title, target.db_title) for target in targets]
    fetched = enrichment_sql.fetch_wikibase_items_for_site_sql(
        site,
        sql_targets,
        timeout_seconds=timeout,
        replica_cnf=str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip(),
        lookup_stats=lookup_stats,
    )
    resolved = {}  # type: Dict[str, Dict[str, Any]]
    for title, payload in fetched.items():
        if not isinstance(payload, Mapping):
            continue
        normalized_title = _normalize_page_title(title)
        normalized_payload = _normalize_link_enrichment_payload(payload)
        if normalized_title and normalized_payload is not None:
            resolved[normalized_title] = normalized_payload
    return resolved


def fetch_wikibase_enrichment_for_site(
    site: str,
    targets: Sequence[SiteLookupTarget],
    backend: str,
    lookup_stats: Optional[MutableMapping[str, float]] = None,
) -> Dict[str, Dict[str, Any]]:
    if not targets:
        return {}

    if backend == LOOKUP_BACKEND_TOOLFORGE_SQL:
        return _fetch_wikibase_enrichment_for_site_sql(site, targets, lookup_stats=lookup_stats)

    api_url = site_to_mediawiki_api_url(site)
    if api_url is None:
        return {}
    titles = sorted({_normalize_page_title(target.api_title) for target in targets if target.api_title})
    resolved = {}  # type: Dict[str, Dict[str, Any]]
    for batch in _chunked(titles, _MAX_TITLES_PER_MEDIAWIKI_BATCH):
        batch_result = _fetch_wikibase_enrichment_for_site_api(api_url, batch, lookup_stats=lookup_stats)
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


def _collect_lookup_inputs(
    records: Sequence[Mapping[str, Any]],
    *,
    include_direct_lookup_targets: bool = False,
) -> Tuple[Dict[str, GilLinkTarget], Dict[str, Set[SiteLookupTarget]], Dict[str, str]]:
    link_targets_by_uri = {}  # type: Dict[str, GilLinkTarget]
    site_lookup_targets = {}  # type: Dict[str, Set[SiteLookupTarget]]
    direct_qids_by_link = {}  # type: Dict[str, str]

    for row in records:
        for target in _iter_gil_link_targets(row):
            link_targets_by_uri[target.link_uri] = target
            direct_qid = _direct_wikidata_qid_for_target(
                target.site,
                target.namespace,
                target.api_title,
                target.db_title,
            )
            if direct_qid is not None:
                direct_qids_by_link[target.link_uri] = direct_qid
                if not include_direct_lookup_targets:
                    continue

            site_lookup_targets.setdefault(target.site, set()).add(
                SiteLookupTarget(
                    namespace=target.namespace,
                    api_title=target.api_title,
                    db_title=target.db_title,
                )
            )

    return link_targets_by_uri, site_lookup_targets, direct_qids_by_link


def _resolve_site_title_enrichment(
    site_lookup_targets: Mapping[str, Set[SiteLookupTarget]],
    backend: Optional[str] = None,
    lookup_stats: Optional[MutableMapping[str, float]] = None,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    resolved_by_site_title = {}  # type: Dict[Tuple[str, str], Dict[str, Any]]
    resolved_backend = backend if backend in {LOOKUP_BACKEND_API, LOOKUP_BACKEND_TOOLFORGE_SQL} else None
    if resolved_backend is None:
        resolved_backend = wikidata_lookup_backend()

    for site, targets in site_lookup_targets.items():
        ordered_targets = sorted(
            targets,
            key=lambda item: (item.namespace, item.api_title, item.db_title),
        )
        result = fetch_wikibase_enrichment_for_site(
            site,
            ordered_targets,
            backend=resolved_backend,
            lookup_stats=lookup_stats,
        )
        for title, enrichment in result.items():
            normalized_title = _normalize_page_title(title)
            if normalized_title:
                resolved_by_site_title[(site, normalized_title)] = dict(enrichment)

    return resolved_by_site_title


def _attach_resolved_enrichment(
    link_targets_by_uri: Mapping[str, GilLinkTarget],
    direct_qids_by_link: Mapping[str, str],
    resolved_by_site_title: Mapping[Tuple[str, str], Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    link_to_enrichment = {}  # type: Dict[str, Dict[str, Any]]

    for link_uri, target in link_targets_by_uri.items():
        direct_qid = _normalize_qid(direct_qids_by_link.get(link_uri))
        resolved = resolved_by_site_title.get((target.site, _normalize_page_title(target.api_title)))
        if resolved is None:
            if direct_qid is None:
                continue
            link_to_enrichment[link_uri] = {
                "wikidata_id": direct_qid,
                "page_len": None,
                "rev_timestamp": None,
            }
            continue

        resolved_qid = _normalize_qid(resolved.get("wikidata_id")) if isinstance(resolved, Mapping) else None
        payload = {
            "wikidata_id": resolved_qid or direct_qid,
            "page_len": _normalize_page_len(resolved.get("page_len")) if isinstance(resolved, Mapping) else None,
            "rev_timestamp": (
                _normalize_revision_timestamp_xsd(resolved.get("rev_timestamp"))
                if isinstance(resolved, Mapping)
                else None
            ),
        }
        if payload["wikidata_id"] is None and payload["page_len"] is None and payload["rev_timestamp"] is None:
            continue
        link_to_enrichment[link_uri] = payload

    return link_to_enrichment


def build_gil_link_enrichment_map(
    records: Sequence[Mapping[str, Any]],
    backend: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    lookup_stats: Dict[str, float] = {
        "api_calls": 0.0,
        "api_ms_total": 0.0,
        "sql_calls": 0.0,
        "sql_ms_total": 0.0,
    }
    link_targets_by_uri, site_lookup_targets, direct_qids_by_link = _collect_lookup_inputs(
        records,
        include_direct_lookup_targets=True,
    )
    resolved_by_site_title = _resolve_site_title_enrichment(
        site_lookup_targets,
        backend=backend,
        lookup_stats=lookup_stats,
    )
    result = _attach_resolved_enrichment(
        link_targets_by_uri,
        direct_qids_by_link,
        resolved_by_site_title,
    )
    total_calls = int(lookup_stats.get("api_calls", 0.0)) + int(lookup_stats.get("sql_calls", 0.0))
    if total_calls > 0:
        print(
            "[wikimedia-lookup] SUMMARY api_calls={} api_total_ms={:.1f} sql_calls={} sql_total_ms={:.1f}".format(
                int(lookup_stats.get("api_calls", 0.0)),
                float(lookup_stats.get("api_ms_total", 0.0)),
                int(lookup_stats.get("sql_calls", 0.0)),
                float(lookup_stats.get("sql_ms_total", 0.0)),
            ),
            flush=True,
        )
    return result
