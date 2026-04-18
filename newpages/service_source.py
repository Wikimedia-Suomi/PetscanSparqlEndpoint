"""New-page source fetching via MediaWiki APIs and Toolforge replicas."""

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Tuple, cast
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit
from urllib.request import Request, urlopen

from django.conf import settings

from petscan import enrichment_sql, service_links
from petscan.normalization import normalize_page_title, normalize_qid
from petscan.service_errors import PetscanServiceError
from petscan.service_rdf import normalize_datetime_xsd
from petscan.service_source import HTTP_USER_AGENT

__all__ = [
    "LOOKUP_BACKEND_API",
    "LOOKUP_BACKEND_TOOLFORGE_SQL",
    "SITEMATRIX_SOURCE_URL",
    "fetch_newpage_records",
    "newpages_lookup_backend",
    "normalize_load_limit",
    "normalize_include_edited_pages",
    "normalize_source_params",
    "normalize_timestamp",
    "normalize_user_list_page",
    "normalize_wikis",
]

LOOKUP_BACKEND_API = service_links.LOOKUP_BACKEND_API
LOOKUP_BACKEND_TOOLFORGE_SQL = service_links.LOOKUP_BACKEND_TOOLFORGE_SQL
SITEMATRIX_SOURCE_URL = "https://meta.wikimedia.org/wiki/Special:SiteMatrix"
_DEFAULT_SITEMATRIX_API_URL = "https://meta.wikimedia.org/w/api.php"
_NEWPAGES_FETCH_PUBLIC_MESSAGE = "Failed to load new pages data from the upstream service."
_SOURCE_PARAM_KEYS = frozenset({"include_edited_pages", "limit", "timestamp", "user_list_page", "wiki"})
_SITEINFO_SNAPSHOT_PATH = Path(__file__).resolve().with_name("siteinfo_snapshot.json")
_TIMESTAMP_LENGTHS = frozenset({4, 6, 8, 10, 12, 14})
_HOST_LABEL_RE = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
_URL_SAFE_CHARS = "/:()-,._"
_DEFAULT_SQL_LIMIT = 50_000
_COMMONS_FILE_NAMESPACE = 6
_INCUBATOR_DOMAIN = "incubator.wikimedia.org"
_META_DOMAIN = "meta.wikimedia.org"
_COMMONS_DOMAIN = "commons.wikimedia.org"
_WIKIDATA_DOMAIN = "www.wikidata.org"
_INCUBATOR_WIKIDATA_CATEGORY_PAGE = "Category:Maintenance:Wikidata_interwiki_links"
_INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE = "Maintenance:Wikidata_interwiki_links"
_INCUBATOR_WIKI_GROUP_BY_CODE = {
    "Wp": "wikipedia",
    "Wt": "wiktionary",
    "Wq": "wikiquote",
    "Wb": "wikibooks",
    "Wn": "wikinews",
    "Wy": "wikivoyage",
    "Ws": "wikisource",
    "Wv": "wikiversity",
}
_ALLOWED_WIKI_GROUPS = frozenset(
    {
        "wikipedia",
        "wiktionary",
        "wikibooks",
        "wikinews",
        "wikiquote",
        "wikisource",
        "wikiversity",
        "wikivoyage",
        "wikidata",
        "commons",
    }
)
_ALLOWED_SPECIAL_DOMAINS = frozenset({_INCUBATOR_DOMAIN, _META_DOMAIN})
_SUPPORTED_WIKI_PROJECT_NAMES = (
    "Wikipedia",
    "Wiktionary",
    "Wikibooks",
    "Wikinews",
    "Wikiquote",
    "Wikisource",
    "Wikiversity",
    "Wikivoyage",
    "Wikidata",
    "Commons",
    "Incubator",
    "Meta-Wiki",
)
_WIKI_GROUP_TO_INTERWIKI_PREFIX = {
    "wikipedia": "w",
    "wiktionary": "wikt",
    "wikibooks": "b",
    "wikinews": "n",
    "wikiquote": "q",
    "wikisource": "s",
    "wikiversity": "v",
    "wikivoyage": "voy",
    "wikidata": "d",
    "commons": "commons",
}
_WIKI_GROUP_TO_DOMAIN_SUFFIX = {
    "wikipedia": ".wikipedia.org",
    "wiktionary": ".wiktionary.org",
    "wikibooks": ".wikibooks.org",
    "wikinews": ".wikinews.org",
    "wikiquote": ".wikiquote.org",
    "wikisource": ".wikisource.org",
    "wikiversity": ".wikiversity.org",
    "wikivoyage": ".wikivoyage.org",
}
_INTERWIKI_PREFIX_TO_WIKI_GROUP = {
    "w": "wikipedia",
    "wikipedia": "wikipedia",
    "wikt": "wiktionary",
    "wiktionary": "wiktionary",
    "b": "wikibooks",
    "wikibooks": "wikibooks",
    "n": "wikinews",
    "wikinews": "wikinews",
    "q": "wikiquote",
    "wikiquote": "wikiquote",
    "s": "wikisource",
    "wikisource": "wikisource",
    "v": "wikiversity",
    "wikiversity": "wikiversity",
    "voy": "wikivoyage",
    "wikivoyage": "wikivoyage",
    "d": "wikidata",
    "wikidata": "wikidata",
    "commons": "commons",
    "c": "commons",
}
_INTERWIKI_PREFIX_TO_SPECIAL_DOMAIN = {
    "meta": _META_DOMAIN,
    "m": _META_DOMAIN,
    "incubator": _INCUBATOR_DOMAIN,
}
_SPECIAL_WIKI_TOKEN_ALIASES = {
    "commons": "commons",
    "c": "commons",
    "wikidata": "wikidata",
    "d": "wikidata",
    "meta": "meta",
    "m": "meta",
    "incubator": "incubator",
}
_SPECIAL_WIKI_TOKEN_TO_DOMAIN = {
    "commons": _COMMONS_DOMAIN,
    "wikidata": _WIKIDATA_DOMAIN,
    "meta": _META_DOMAIN,
    "incubator": _INCUBATOR_DOMAIN,
}
_SPECIAL_WIKI_TOKEN_BY_DOMAIN = {
    _COMMONS_DOMAIN: "commons",
    _WIKIDATA_DOMAIN: "wikidata",
    "wikidata.org": "wikidata",
    _META_DOMAIN: "meta",
    _INCUBATOR_DOMAIN: "incubator",
}
_WIKI_INPUT_ERROR = (
    "wiki must contain valid short Wikimedia wiki identifiers such as "
    "fi, w:fi, b:fi, commons, wikidata, meta, or incubator, "
    "full hostnames such as fi.wikipedia.org, or wildcard hostnames such as *.wikipedia.org."
)
_MAX_API_ROWS_PER_WIKI = 100
_MAX_API_PAGEIDS_PER_BATCH = 50
_MAX_API_FULL_SCAN_WIKI_COUNT = 10
_MIN_WIKI_COUNT_FOR_ACTIVE_USER_FILTER = 10
_MAX_EDITED_PAGES_WINDOW = timedelta(days=60)
_MAX_RECENTCHANGES_PRECHECK_WINDOW = timedelta(days=30)
pymysql = cast(Any, enrichment_sql.pymysql)


@dataclass(frozen=True)
class _WikiDescriptor:
    domain: str
    dbname: str
    lang_code: str
    wiki_group: str
    site_url: str
    site_code: str = ""


@dataclass(frozen=True)
class _SiteInfo:
    article_path: str
    lang_code: str
    namespace_names: Dict[int, str]
    namespace_aliases: Dict[int, Tuple[str, ...]]


@dataclass(frozen=True)
class _UserListPageRef:
    domain: str
    page_title: str
    canonical_ref: str


def _console_log(message: str) -> None:
    print("[newpages-api] {}".format(message), flush=True)


def _sanitize_for_log(value: str) -> str:
    return "".join(character if character.isprintable() and character not in "\r\n\t" else "?" for character in value)


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


def normalize_include_edited_pages(value: Any) -> bool:
    if isinstance(value, (list, tuple, set)):
        candidates = [item for item in value if str(item).strip()]
        value = candidates[-1] if candidates else None
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return False
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError("include_edited_pages must be a boolean.")


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


def _is_valid_wiki_wildcard(pattern: str) -> bool:
    text = str(pattern or "").strip().lower().rstrip(".")
    if not text.startswith("*."):
        return False
    return _is_valid_hostname(text[2:])


def _is_supported_wiki_descriptor(descriptor: _WikiDescriptor) -> bool:
    return descriptor.wiki_group in _ALLOWED_WIKI_GROUPS or descriptor.domain in _ALLOWED_SPECIAL_DOMAINS


def _is_valid_short_wiki_code(value: str) -> bool:
    text = str(value or "").strip().lower()
    return bool(text) and _HOST_LABEL_RE.fullmatch(text) is not None


def _canonical_wiki_token_for_descriptor(descriptor: _WikiDescriptor) -> str:
    special_token = _SPECIAL_WIKI_TOKEN_BY_DOMAIN.get(descriptor.domain)
    if special_token is not None:
        return special_token

    lang_code = str(descriptor.lang_code or "").strip().lower()
    if descriptor.wiki_group == "wikipedia" and _is_valid_short_wiki_code(lang_code):
        return lang_code

    interwiki_prefix = _WIKI_GROUP_TO_INTERWIKI_PREFIX.get(descriptor.wiki_group, "")
    if interwiki_prefix and _is_valid_short_wiki_code(lang_code):
        return "{}:{}".format(interwiki_prefix, lang_code)
    return descriptor.domain


@lru_cache(maxsize=1)
def _known_wiki_domains_by_token() -> Dict[str, str]:
    token_to_domain: Dict[str, str] = {}
    for descriptor in _known_wikis_by_domain().values():
        token = _canonical_wiki_token_for_descriptor(descriptor)
        if token and token not in token_to_domain:
            token_to_domain[token] = descriptor.domain
    return token_to_domain


def _normalize_single_wiki_token(token: str) -> str:
    normalized = str(token or "").strip().lower().rstrip(".")
    if not normalized:
        raise ValueError(_WIKI_INPUT_ERROR)

    special_token = _SPECIAL_WIKI_TOKEN_ALIASES.get(normalized)
    if special_token is not None:
        return special_token

    if _is_valid_hostname(normalized):
        special_domain_token = _SPECIAL_WIKI_TOKEN_BY_DOMAIN.get(normalized)
        if special_domain_token is not None:
            return special_domain_token
        for wiki_group, domain_suffix in _WIKI_GROUP_TO_DOMAIN_SUFFIX.items():
            if not normalized.endswith(domain_suffix):
                continue
            lang_code = normalized[: -len(domain_suffix)]
            if not _is_valid_short_wiki_code(lang_code):
                return normalized
            if wiki_group == "wikipedia":
                return lang_code
            return "{}:{}".format(_WIKI_GROUP_TO_INTERWIKI_PREFIX[wiki_group], lang_code)
        return normalized

    if ":" in normalized:
        parts = normalized.split(":")
        if len(parts) != 2:
            raise ValueError(_WIKI_INPUT_ERROR)
        prefix = str(parts[0] or "").strip().lower()
        lang_code = str(parts[1] or "").strip().lower()
        prefix_wiki_group = _INTERWIKI_PREFIX_TO_WIKI_GROUP.get(prefix)
        if prefix_wiki_group is None or prefix_wiki_group not in _WIKI_GROUP_TO_DOMAIN_SUFFIX:
            raise ValueError(_WIKI_INPUT_ERROR)
        if not _is_valid_short_wiki_code(lang_code):
            raise ValueError(_WIKI_INPUT_ERROR)
        if prefix_wiki_group == "wikipedia":
            return lang_code
        return "{}:{}".format(_WIKI_GROUP_TO_INTERWIKI_PREFIX[prefix_wiki_group], lang_code)

    if _is_valid_short_wiki_code(normalized):
        return normalized

    raise ValueError(_WIKI_INPUT_ERROR)


def _wiki_domain_for_token(token: str) -> str:
    normalized = str(token or "").strip().lower().rstrip(".")
    if not normalized:
        return normalized

    special_token = _SPECIAL_WIKI_TOKEN_ALIASES.get(normalized)
    if special_token is not None:
        return _SPECIAL_WIKI_TOKEN_TO_DOMAIN[special_token]

    if _is_valid_hostname(normalized):
        return _WIKIDATA_DOMAIN if normalized == "wikidata.org" else normalized

    if ":" in normalized:
        parts = normalized.split(":")
        if len(parts) == 2:
            prefix = str(parts[0] or "").strip().lower()
            lang_code = str(parts[1] or "").strip().lower()
            wiki_group = _INTERWIKI_PREFIX_TO_WIKI_GROUP.get(prefix)
            if wiki_group is not None and wiki_group in _WIKI_GROUP_TO_DOMAIN_SUFFIX and _is_valid_short_wiki_code(
                lang_code
            ):
                return "{}{}".format(lang_code, _WIKI_GROUP_TO_DOMAIN_SUFFIX[wiki_group])
        return normalized

    if _is_valid_short_wiki_code(normalized):
        return "{}{}".format(normalized, _WIKI_GROUP_TO_DOMAIN_SUFFIX["wikipedia"])
    return normalized


def normalize_wikis(value: Any) -> List[str]:
    if value is None:
        return []

    raw_values: List[str]
    if isinstance(value, (list, tuple, set)):
        raw_values = [str(item) for item in value]
    else:
        raw_values = [str(value)]

    known_wikis: Optional[Mapping[str, _WikiDescriptor]] = None
    wiki_tokens: List[str] = []
    seen = set()

    for raw_value in raw_values:
        for part in raw_value.split(","):
            token = str(part).strip().lower().rstrip(".")
            if not token:
                continue
            if token.startswith("*."):
                if not _is_valid_wiki_wildcard(token):
                    raise ValueError(_WIKI_INPUT_ERROR)
                if known_wikis is None:
                    known_wikis = _known_wikis_by_domain()
                suffix = token[1:]
                matched_domains = [
                    known_domain
                    for known_domain, descriptor in known_wikis.items()
                    if known_domain.endswith(suffix) and _is_supported_wiki_descriptor(descriptor)
                ]
                if not matched_domains:
                    raise ValueError("Unknown wiki wildcard: {}.".format(token))
                for matched_domain in sorted(matched_domains):
                    canonical_token = _canonical_wiki_token_for_descriptor(known_wikis[matched_domain])
                    if canonical_token in seen:
                        continue
                    seen.add(canonical_token)
                    wiki_tokens.append(canonical_token)
                continue
            canonical_token = _normalize_single_wiki_token(token)
            if canonical_token in seen:
                continue
            seen.add(canonical_token)
            wiki_tokens.append(canonical_token)

    wiki_tokens.sort()
    return wiki_tokens


def normalize_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if not text.isdigit():
        raise ValueError("timestamp must contain digits only.")
    if len(text) not in _TIMESTAMP_LENGTHS:
        raise ValueError(
            "timestamp must use YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHH, YYYYMMDDHHMM, or YYYYMMDDHHMMSS."
        )
    return text.ljust(14, "0")


def _parse_normalized_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def _lower_bound_datetime_for_timestamp(value: str) -> datetime:
    text = str(value or "").strip()
    if len(text) != 14 or not text.isdigit():
        raise ValueError("timestamp must use YYYYMMDDHHMMSS after normalization.")

    year = int(text[0:4])
    month = int(text[4:6]) or 1
    day = int(text[6:8]) or 1
    hour = int(text[8:10])
    minute = int(text[10:12])
    second = int(text[12:14])
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def _api_timestamp(value: str) -> str:
    return _lower_bound_datetime_for_timestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validate_include_edited_pages_request(
    include_edited_pages: bool,
    timestamp: Optional[str],
    user_list_page: Optional[str],
) -> None:
    if not include_edited_pages:
        return
    if user_list_page is None:
        raise ValueError("include_edited_pages requires user_list_page.")
    if timestamp is None:
        raise ValueError("include_edited_pages requires timestamp.")
    threshold = _lower_bound_datetime_for_timestamp(timestamp)
    if threshold < (datetime.now(timezone.utc) - _MAX_EDITED_PAGES_WINDOW):
        raise ValueError("timestamp must be within the last 60 days when include_edited_pages is enabled.")


def _normalize_user_name(value: Any) -> Optional[str]:
    text = str(value or "").strip().replace("_", " ")
    return text or None


def _page_title_from_wikimedia_url(raw_value: Any) -> Optional[Tuple[str, str]]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    parsed = urlsplit(text)
    if str(parsed.scheme or "").strip().lower() not in {"http", "https"}:
        return None

    domain = str(parsed.hostname or "").strip().lower()
    if not _is_valid_hostname(domain):
        return None

    title = ""
    if parsed.path.startswith("/wiki/"):
        title = unquote(parsed.path[len("/wiki/") :])
    else:
        parsed_query = parse_qs(parsed.query, keep_blank_values=False)
        title_values = [str(value).strip() for value in parsed_query.get("title", []) if str(value).strip()]
        if title_values:
            title = unquote(title_values[-1])

    normalized_title = normalize_page_title(title)
    if not normalized_title:
        return None
    return domain, normalized_title


def _canonical_user_list_page_ref(domain: str, page_title: str) -> str:
    normalized_title = normalize_page_title(page_title)
    if domain == _INCUBATOR_DOMAIN:
        return ":incubator:{}".format(normalized_title)
    if domain == _META_DOMAIN:
        return ":meta:{}".format(normalized_title)
    if domain == "commons.wikimedia.org":
        return ":commons:{}".format(normalized_title)
    if domain in {"www.wikidata.org", "wikidata.org"}:
        return ":d:{}".format(normalized_title)
    for wiki_group, domain_suffix in _WIKI_GROUP_TO_DOMAIN_SUFFIX.items():
        if not domain.endswith(domain_suffix):
            continue
        lang_code = domain[: -len(domain_suffix)]
        interwiki_prefix = _WIKI_GROUP_TO_INTERWIKI_PREFIX.get(wiki_group, "")
        if not lang_code or not interwiki_prefix:
            break
        return ":{}:{}:{}".format(interwiki_prefix, lang_code, normalized_title)
    descriptor = _known_wikis_by_domain().get(domain)
    if descriptor is not None:
        normalized_site_code = str(descriptor.site_code or "").strip().lower()
        if normalized_site_code:
            return ":{}:{}".format(normalized_site_code, normalized_title)
    raise ValueError("Unsupported user_list_page wiki domain: {}.".format(domain))


def _resolve_user_list_page(value: Any) -> Optional[_UserListPageRef]:
    text = str(value or "").strip()
    if not text:
        return None

    url_reference = _page_title_from_wikimedia_url(text)
    if url_reference is not None:
        domain, page_title = url_reference
        return _UserListPageRef(
            domain=domain,
            page_title=page_title,
            canonical_ref=_canonical_user_list_page_ref(domain, page_title),
        )

    normalized = text.lstrip(":").strip()
    if not normalized:
        return None
    parts = [segment.strip() for segment in normalized.split(":")]
    if len(parts) < 2:
        raise ValueError(
            "user_list_page must be a Wikimedia wiki page in interwiki form or a direct https://.../wiki/... link."
        )

    prefix = str(parts[0] or "").strip().lower()
    special_domain = _INTERWIKI_PREFIX_TO_SPECIAL_DOMAIN.get(prefix)
    if special_domain is not None:
        page_title = normalize_page_title(":".join(parts[1:]))
        if not page_title:
            raise ValueError("user_list_page must include a page title after the interwiki prefix.")
        return _UserListPageRef(
            domain=special_domain,
            page_title=page_title,
            canonical_ref=_canonical_user_list_page_ref(special_domain, page_title),
        )

    wiki_group = _INTERWIKI_PREFIX_TO_WIKI_GROUP.get(prefix)
    if wiki_group is not None:
        if len(parts) < 3:
            raise ValueError(
                "user_list_page must be a Wikimedia wiki page in interwiki form or a direct https://.../wiki/... link."
            )

        lang_code = str(parts[1] or "").strip().lower()
        page_title = normalize_page_title(":".join(parts[2:]))
        if not lang_code or not page_title:
            raise ValueError("user_list_page must include both a language code and page title.")

        return _UserListPageRef(
            domain="{}{}".format(lang_code, _WIKI_GROUP_TO_DOMAIN_SUFFIX[wiki_group]),
            page_title=page_title,
            canonical_ref=":{}:{}:{}".format(
                _WIKI_GROUP_TO_INTERWIKI_PREFIX[wiki_group],
                lang_code,
                page_title,
            ),
        )

    dynamic_special_domain = _user_list_source_domain_for_prefix(prefix)
    if dynamic_special_domain is not None:
        page_title = normalize_page_title(":".join(parts[1:]))
        if not page_title:
            raise ValueError("user_list_page must include a page title after the interwiki prefix.")
        return _UserListPageRef(
            domain=dynamic_special_domain,
            page_title=page_title,
            canonical_ref=_canonical_user_list_page_ref(dynamic_special_domain, page_title),
        )

    raise ValueError(
        "user_list_page must be a Wikimedia wiki page in interwiki form or a direct https://.../wiki/... link."
    )


def normalize_user_list_page(value: Any) -> Optional[str]:
    resolved = _resolve_user_list_page(value)
    if resolved is None:
        return None
    return resolved.canonical_ref


def normalize_source_params(params: Optional[Mapping[str, Any]]) -> Dict[str, List[str]]:
    normalized = {}  # type: Dict[str, List[str]]
    if not params or not isinstance(params, Mapping):
        return normalized

    for key, raw_value in params.items():
        text_key = str(key).strip()
        if not text_key or text_key not in _SOURCE_PARAM_KEYS:
            continue

        if text_key == "wiki":
            wiki_domains = normalize_wikis(raw_value)
            if wiki_domains:
                normalized[text_key] = wiki_domains
            continue

        if text_key == "limit":
            limit_value = raw_value
            if isinstance(raw_value, (list, tuple, set)):
                limit_candidates = [str(value).strip() for value in raw_value if str(value).strip()]
                limit_value = limit_candidates[-1] if limit_candidates else None
            limit = normalize_load_limit(limit_value)
            if limit is not None:
                normalized[text_key] = [str(limit)]
            continue

        if text_key == "timestamp":
            timestamp_value = raw_value
            if isinstance(raw_value, (list, tuple, set)):
                timestamp_candidates = [str(value).strip() for value in raw_value if str(value).strip()]
                timestamp_value = timestamp_candidates[-1] if timestamp_candidates else None
            timestamp = normalize_timestamp(timestamp_value)
            if timestamp is not None:
                normalized[text_key] = [timestamp]
            continue

        if text_key == "include_edited_pages":
            if normalize_include_edited_pages(raw_value):
                normalized[text_key] = ["1"]
            continue

        if text_key == "user_list_page":
            user_list_page_value = raw_value
            if isinstance(raw_value, (list, tuple, set)):
                page_candidates = [str(value).strip() for value in raw_value if str(value).strip()]
                user_list_page_value = page_candidates[-1] if page_candidates else None
            normalized_user_list_page = normalize_user_list_page(user_list_page_value)
            if normalized_user_list_page is not None:
                normalized[text_key] = [normalized_user_list_page]
            continue

    return normalized


def newpages_lookup_backend() -> str:
    return service_links.wikidata_lookup_backend()


def _sitematrix_api_url() -> str:
    endpoint = str(
        getattr(settings, "NEWPAGES_SITEMATRIX_API_ENDPOINT", _DEFAULT_SITEMATRIX_API_URL)
    ).strip()
    return endpoint or _DEFAULT_SITEMATRIX_API_URL


def _wiki_api_url(domain: str) -> str:
    return "https://{}/w/api.php".format(domain)


def _request_json(request_url: str) -> Dict[str, Any]:
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
            "Failed to fetch new-page API data: {}".format(exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
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


def _chunked_page_ids(page_ids: List[int], size: int) -> List[List[int]]:
    chunk_size = max(1, size)
    return [page_ids[index : index + chunk_size] for index in range(0, len(page_ids), chunk_size)]


def _wiki_group_for_site_code(site_code: str) -> str:
    normalized = str(site_code or "").strip().lower()
    mapping = {
        "wiki": "wikipedia",
        "wiktionary": "wiktionary",
        "wikiquote": "wikiquote",
        "wikibooks": "wikibooks",
        "wikinews": "wikinews",
        "wikiversity": "wikiversity",
        "wikivoyage": "wikivoyage",
        "wikisource": "wikisource",
        "commons": "commons",
        "wikidata": "wikidata",
        "meta": "wikimedia",
        "mediawiki": "wikimedia",
        "species": "wikimedia",
        "outreach": "wikimedia",
        "incubator": "wikimedia",
    }
    return mapping.get(normalized, normalized or "wikimedia")


def _normalize_site_root_url(raw_url: Any) -> Optional[Tuple[str, str]]:
    text = str(raw_url or "").strip()
    if not text:
        return None
    parsed = urlsplit(text)
    hostname = str(parsed.hostname or "").strip().lower()
    if not _is_valid_hostname(hostname):
        return None
    scheme = str(parsed.scheme or "https").strip().lower()
    if scheme not in {"http", "https"}:
        return None
    return hostname, "{}://{}/".format(scheme, hostname)


def _normalize_db_page_title(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    return normalize_page_title(value)


def _split_incubator_title(page_title: str) -> Tuple[str, str, str]:
    parts = str(page_title or "").split("/", 2)
    wiki_project = parts[0] if len(parts) >= 1 else ""
    lang_code = parts[1] if len(parts) >= 2 else ""
    page_name = parts[2] if len(parts) >= 3 else parts[-1] if parts else ""
    return wiki_project, lang_code, page_name


def _incubator_wiki_group_for_title(page_title: str) -> str:
    wiki_project, _lang_code, _page_name = _split_incubator_title(page_title)
    if not wiki_project:
        return "wikimedia"
    return _INCUBATOR_WIKI_GROUP_BY_CODE.get(wiki_project, str(wiki_project).strip().lower() or "wikimedia")


def _incubator_site_url_for_title(page_title: str) -> Optional[str]:
    wiki_project, lang_code, _page_name = _split_incubator_title(page_title)
    normalized_project = str(wiki_project or "").strip()
    normalized_lang = str(lang_code or "").strip()
    if not normalized_project or not normalized_lang:
        return None
    return "https://{}/wiki/{}".format(
        _INCUBATOR_DOMAIN,
        _quote_page_path("{}/{}/".format(normalized_project, normalized_lang)),
    )


def _normalize_api_page_title(title: Any, namespace_id: int, siteinfo: _SiteInfo) -> str:
    normalized_title = normalize_page_title(title)
    if not normalized_title or namespace_id <= 0:
        return normalized_title

    namespace_prefix = str(siteinfo.namespace_names.get(namespace_id, "") or "").strip()
    if not namespace_prefix:
        return normalized_title

    prefix = "{}:".format(namespace_prefix)
    if normalized_title.startswith(prefix):
        remainder = normalized_title[len(prefix) :].strip()
        if remainder:
            return normalize_page_title(remainder)
    return normalized_title


def _supported_wiki_projects_text() -> str:
    return ", ".join(_SUPPORTED_WIKI_PROJECT_NAMES)


@lru_cache(maxsize=1)
def _known_wikis_by_domain() -> Dict[str, _WikiDescriptor]:
    request_url = "{}?{}".format(
        _sitematrix_api_url(),
        urlencode(
            {
                "action": "sitematrix",
                "format": "json",
                "smtype": "language|special",
                "smsiteprop": "url|dbname|code",
            }
        ),
    )
    payload = _request_json(request_url)
    sitematrix = payload.get("sitematrix")
    if not isinstance(sitematrix, Mapping):
        raise PetscanServiceError("SiteMatrix API returned no sitematrix payload.")

    descriptors: Dict[str, _WikiDescriptor] = {}

    def _add_descriptor(site_entry: Mapping[str, Any], lang_code: str = "") -> None:
        site_root = _normalize_site_root_url(site_entry.get("url"))
        if site_root is None:
            return
        domain, site_url = site_root
        dbname = str(site_entry.get("dbname", "") or "").strip().lower()
        site_code = str(site_entry.get("code", "") or "").strip().lower()
        if not domain or not dbname:
            return
        descriptors[domain] = _WikiDescriptor(
            domain=domain,
            dbname=dbname,
            lang_code=str(lang_code or "").strip().lower(),
            wiki_group=_wiki_group_for_site_code(site_code),
            site_url=site_url,
            site_code=site_code,
        )

    for key, value in sitematrix.items():
        if key == "specials":
            if isinstance(value, list):
                for site_entry in value:
                    if isinstance(site_entry, Mapping):
                        _add_descriptor(site_entry)
            continue
        if not isinstance(value, Mapping):
            continue
        lang_code = str(value.get("code", "") or "").strip().lower()
        sites = value.get("site")
        if not isinstance(sites, list):
            continue
        for site_entry in sites:
            if isinstance(site_entry, Mapping):
                _add_descriptor(site_entry, lang_code=lang_code)

    return descriptors


@lru_cache(maxsize=1)
def _user_list_source_domain_for_prefix(prefix: str) -> Optional[str]:
    normalized_prefix = str(prefix or "").strip().lower()
    if not normalized_prefix:
        return None
    for descriptor in _known_wikis_by_domain().values():
        if descriptor.lang_code:
            continue
        normalized_site_code = str(descriptor.site_code or "").strip().lower()
        if normalized_site_code == normalized_prefix:
            return descriptor.domain
    return None


def _selected_wiki_descriptors(wiki_tokens: List[str]) -> List[_WikiDescriptor]:
    known_wikis = _known_wikis_by_domain()
    known_domains_by_token = _known_wiki_domains_by_token()
    selected: List[_WikiDescriptor] = []
    unknown: List[str] = []
    unsupported: List[str] = []
    seen_domains: set[str] = set()

    for token in wiki_tokens:
        lookup_domain = _wiki_domain_for_token(token)
        descriptor = known_wikis.get(lookup_domain)
        if descriptor is None:
            resolved_domain = known_domains_by_token.get(str(token or "").strip().lower())
            if resolved_domain is not None:
                descriptor = known_wikis.get(resolved_domain)
                if descriptor is not None:
                    lookup_domain = resolved_domain
        if descriptor is None:
            unknown.append(lookup_domain)
            continue
        if not _is_supported_wiki_descriptor(descriptor):
            unsupported.append(lookup_domain)
            continue
        if descriptor.domain in seen_domains:
            continue
        seen_domains.add(descriptor.domain)
        selected.append(descriptor)

    if unknown:
        if len(unknown) == 1:
            raise ValueError("Unknown wiki domain: {}.".format(unknown[0]))
        raise ValueError("Unknown wiki domains: {}.".format(", ".join(unknown)))
    if unsupported:
        supported_projects = _supported_wiki_projects_text()
        if len(unsupported) == 1:
            raise ValueError(
                "Unsupported wiki domain: {}. Supported projects are: {}.".format(
                    unsupported[0], supported_projects
                )
            )
        raise ValueError(
            "Unsupported wiki domains: {}. Supported projects are: {}.".format(
                ", ".join(unsupported), supported_projects
            )
        )

    return selected


def _siteinfo_from_query_payload(query_payload: Mapping[str, Any]) -> _SiteInfo:
    general = query_payload.get("general")
    article_path = "/wiki/$1"
    lang_code = ""
    if isinstance(general, Mapping):
        configured_article_path = str(general.get("articlepath", "") or "").strip()
        if configured_article_path:
            article_path = configured_article_path
        lang_code = str(general.get("lang", "") or "").strip().lower()

    namespace_names: Dict[int, str] = {}
    namespace_alias_sets: Dict[int, set[str]] = {}
    namespaces = query_payload.get("namespaces")
    if isinstance(namespaces, Mapping):
        for raw_id, payload_value in namespaces.items():
            try:
                namespace_id = int(str(raw_id).strip())
            except (TypeError, ValueError):
                continue
            if not isinstance(payload_value, Mapping):
                continue
            namespace_name = str(payload_value.get("*", "") or "").strip()
            canonical_name = str(payload_value.get("canonical", "") or "").strip()
            normalized_namespace_name = normalize_page_title(namespace_name) if namespace_name else ""
            namespace_names[namespace_id] = normalized_namespace_name

            alias_set = namespace_alias_sets.setdefault(namespace_id, set())
            for candidate in (namespace_name, canonical_name):
                normalized_candidate = normalize_page_title(candidate) if candidate else ""
                if normalized_candidate:
                    alias_set.add(normalized_candidate)

    namespace_aliases = query_payload.get("namespacealiases")
    if isinstance(namespace_aliases, list):
        for alias_payload in namespace_aliases:
            if not isinstance(alias_payload, Mapping):
                continue
            try:
                namespace_id = int(str(alias_payload.get("id", "")).strip())
            except (TypeError, ValueError):
                continue
            alias_name = str(alias_payload.get("*", "") or alias_payload.get("alias", "") or "").strip()
            normalized_alias = normalize_page_title(alias_name) if alias_name else ""
            if normalized_alias:
                namespace_alias_sets.setdefault(namespace_id, set()).add(normalized_alias)

    normalized_namespace_aliases: Dict[int, Tuple[str, ...]] = {}
    for namespace_id, alias_values in namespace_alias_sets.items():
        normalized_namespace_aliases[namespace_id] = tuple(sorted(alias_values))

    return _SiteInfo(
        article_path=article_path,
        lang_code=lang_code,
        namespace_names=namespace_names,
        namespace_aliases=normalized_namespace_aliases,
    )


def _siteinfo_to_snapshot_entry(siteinfo: _SiteInfo) -> Dict[str, Any]:
    return {
        "article_path": siteinfo.article_path,
        "lang_code": siteinfo.lang_code,
        "namespace_names": {
            str(namespace_id): namespace_name
            for namespace_id, namespace_name in sorted(siteinfo.namespace_names.items())
        },
        "namespace_aliases": {
            str(namespace_id): list(alias_values)
            for namespace_id, alias_values in sorted(siteinfo.namespace_aliases.items())
        },
    }


def _siteinfo_from_snapshot_entry(payload: Mapping[str, Any]) -> Optional[_SiteInfo]:
    article_path = str(payload.get("article_path", "") or "").strip() or "/wiki/$1"
    lang_code = str(payload.get("lang_code", "") or "").strip().lower()

    namespace_names_payload = payload.get("namespace_names")
    namespace_names: Dict[int, str] = {}
    if isinstance(namespace_names_payload, Mapping):
        for raw_id, raw_name in namespace_names_payload.items():
            try:
                namespace_id = int(str(raw_id).strip())
            except (TypeError, ValueError):
                continue
            namespace_names[namespace_id] = normalize_page_title(raw_name)

    namespace_aliases_payload = payload.get("namespace_aliases")
    namespace_aliases: Dict[int, Tuple[str, ...]] = {}
    if isinstance(namespace_aliases_payload, Mapping):
        for raw_id, raw_values in namespace_aliases_payload.items():
            try:
                namespace_id = int(str(raw_id).strip())
            except (TypeError, ValueError):
                continue
            if not isinstance(raw_values, list):
                continue
            normalized_aliases = sorted(
                {
                    normalize_page_title(raw_value)
                    for raw_value in raw_values
                    if normalize_page_title(raw_value)
                }
            )
            namespace_aliases[namespace_id] = tuple(normalized_aliases)

    return _SiteInfo(
        article_path=article_path,
        lang_code=lang_code,
        namespace_names=namespace_names,
        namespace_aliases=namespace_aliases,
    )


@lru_cache(maxsize=1)
def _siteinfo_snapshot_by_domain() -> Dict[str, _SiteInfo]:
    if not _SITEINFO_SNAPSHOT_PATH.exists():
        return {}

    try:
        payload = json.loads(_SITEINFO_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to load siteinfo snapshot from {}: {}".format(_SITEINFO_SNAPSHOT_PATH, exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        ) from exc

    if not isinstance(payload, Mapping):
        return {}

    raw_siteinfo_by_domain = payload.get("siteinfo", payload)
    if not isinstance(raw_siteinfo_by_domain, Mapping):
        return {}

    siteinfo_by_domain: Dict[str, _SiteInfo] = {}
    for raw_domain, raw_siteinfo in raw_siteinfo_by_domain.items():
        domain = str(raw_domain or "").strip().lower()
        if not _is_valid_hostname(domain):
            continue
        if not isinstance(raw_siteinfo, Mapping):
            continue
        siteinfo = _siteinfo_from_snapshot_entry(raw_siteinfo)
        if siteinfo is not None:
            siteinfo_by_domain[domain] = siteinfo
    return siteinfo_by_domain


def _fetch_siteinfo_from_api(domain: str) -> _SiteInfo:
    request_url = (
        "https://{}/w/api.php?action=query&meta=siteinfo&siprop=general|namespaces|namespacealiases&format=json"
    ).format(
        domain
    )
    payload = _request_json(request_url)
    query_payload = payload.get("query")
    if not isinstance(query_payload, Mapping):
        raise PetscanServiceError("Siteinfo API returned no query payload.")
    return _siteinfo_from_query_payload(query_payload)


@lru_cache(maxsize=128)
def _siteinfo_for_domain(domain: str) -> _SiteInfo:
    snapshot_siteinfo = _siteinfo_snapshot_by_domain().get(domain)
    if snapshot_siteinfo is not None:
        return snapshot_siteinfo
    return _fetch_siteinfo_from_api(domain)


@lru_cache(maxsize=256)
def _quote_page_path(path: str) -> str:
    return quote(path, safe=_URL_SAFE_CHARS)


def _title_with_namespace(db_title: Any, namespace_id: int, siteinfo: _SiteInfo) -> str:
    normalized_title = _normalize_db_page_title(db_title)
    if not normalized_title:
        return ""
    if namespace_id <= 0:
        return normalized_title
    namespace_prefix = str(siteinfo.namespace_names.get(namespace_id, "") or "").strip()
    if not namespace_prefix:
        return normalized_title
    return "{}:{}".format(namespace_prefix, normalized_title)


def _page_url(domain: str, siteinfo: _SiteInfo, namespace_id: int, db_title: Any) -> str:
    full_title = _title_with_namespace(db_title, namespace_id, siteinfo)
    if not full_title:
        return ""

    article_path = str(siteinfo.article_path or "/wiki/$1").strip() or "/wiki/$1"
    if not article_path.startswith("/"):
        article_path = "/{}".format(article_path)
    encoded_title = _quote_page_path(full_title)

    if "$1" in article_path:
        path_prefix, path_suffix = article_path.split("$1", 1)
        return "https://{}{}{}".format(domain, path_prefix, encoded_title) + path_suffix

    return "https://{}{}/{}".format(domain, article_path.rstrip("/"), encoded_title)


def _source_url_for_descriptors(descriptors: List[_WikiDescriptor], include_edited_pages: bool = False) -> str:
    if len(descriptors) == 1:
        if include_edited_pages:
            return "https://{}/wiki/Special:Contributions".format(descriptors[0].domain)
        return "https://{}/wiki/Special:RecentChanges".format(descriptors[0].domain)
    return SITEMATRIX_SOURCE_URL


def _api_source_url_for_descriptors(descriptors: List[_WikiDescriptor], include_edited_pages: bool = False) -> str:
    if len(descriptors) == 1:
        if include_edited_pages:
            return "https://{}/wiki/Special:Contributions".format(descriptors[0].domain)
        return "https://{}/wiki/Special:Log/create".format(descriptors[0].domain)
    return SITEMATRIX_SOURCE_URL


def _user_name_from_namespace_title(title: Any, siteinfo: _SiteInfo) -> Optional[str]:
    normalized_title = normalize_page_title(title)
    if ":" not in normalized_title:
        return None
    namespace_prefix, remainder = normalized_title.split(":", 1)
    if not remainder:
        return None
    user_name = remainder.split("/", 1)[0].strip()
    if not user_name:
        return None
    user_namespace_prefixes = {
        str(prefix).strip().lower()
        for prefix in siteinfo.namespace_aliases.get(2, ())
        if str(prefix).strip()
    }
    if namespace_prefix.strip().lower() not in user_namespace_prefixes:
        return None
    return _normalize_user_name(user_name)


def _user_name_from_user_page_url(raw_url: Any) -> Optional[str]:
    resolved = _page_title_from_wikimedia_url(raw_url)
    if resolved is None:
        return None
    domain, page_title = resolved
    if domain not in _known_wikis_by_domain():
        return None
    return _user_name_from_namespace_title(page_title, _siteinfo_for_domain(domain))


def _append_unique_user_name(user_names: List[str], seen_user_names: set[str], candidate: Any) -> None:
    normalized_candidate = _normalize_user_name(candidate)
    if normalized_candidate is None:
        return
    lookup_key = normalized_candidate.casefold()
    if lookup_key in seen_user_names:
        return
    seen_user_names.add(lookup_key)
    user_names.append(normalized_candidate)


def _normalized_text_value(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return str(value or "")


def _fetched_row_list(value: Any) -> List[Tuple[Any, ...] | List[Any]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [row for row in value if isinstance(row, (tuple, list))]


def _namespace_and_db_title_for_full_title(full_title: str, siteinfo: _SiteInfo) -> Tuple[int, str]:
    normalized_title = normalize_page_title(full_title)
    if ":" not in normalized_title:
        return 0, normalized_title

    namespace_prefix, remainder = normalized_title.split(":", 1)
    normalized_remainder = normalize_page_title(remainder)
    if not namespace_prefix or not normalized_remainder:
        return 0, normalized_title

    normalized_prefix = namespace_prefix.strip().lower()
    for namespace_id, aliases in siteinfo.namespace_aliases.items():
        alias_values = {
            str(alias).strip().lower()
            for alias in aliases
            if str(alias).strip()
        }
        if normalized_prefix in alias_values:
            return namespace_id, normalized_remainder

    return 0, normalized_title


def _interwiki_target_url(iw_url: Any, page_title: str) -> Optional[str]:
    template = _normalized_text_value(iw_url).strip()
    normalized_title = normalize_page_title(page_title)
    if not template or not normalized_title:
        return None

    encoded_title = quote(normalized_title, safe=_URL_SAFE_CHARS)
    target_url = template.replace("$1", encoded_title)
    if target_url.startswith("//"):
        target_url = "https:{}".format(target_url)
    if target_url.startswith("http://") or target_url.startswith("https://"):
        return target_url
    return None


def _interwiki_target_domain_and_title(
    prefix: Any, title: Any, source_descriptor: _WikiDescriptor
) -> Optional[Tuple[str, str]]:
    normalized_prefix = str(prefix or "").strip().lower()
    normalized_title = _normalize_db_page_title(title)
    if not normalized_prefix or not normalized_title:
        return None

    special_domain = _INTERWIKI_PREFIX_TO_SPECIAL_DOMAIN.get(normalized_prefix)
    if special_domain is not None:
        return special_domain, normalized_title

    wiki_group = _INTERWIKI_PREFIX_TO_WIKI_GROUP.get(normalized_prefix)
    if wiki_group is not None:
        if wiki_group == "commons":
            return "commons.wikimedia.org", normalized_title
        if wiki_group == "wikidata":
            for wikidata_domain in ("www.wikidata.org", "wikidata.org"):
                if wikidata_domain in _known_wikis_by_domain():
                    return wikidata_domain, normalized_title
            return None

        domain_suffix = _WIKI_GROUP_TO_DOMAIN_SUFFIX.get(wiki_group)
        if domain_suffix is None or ":" not in normalized_title:
            return None
        lang_code, remainder = normalized_title.split(":", 1)
        normalized_lang_code = str(lang_code or "").strip().lower()
        normalized_remainder = normalize_page_title(remainder)
        if not normalized_lang_code or not normalized_remainder:
            return None
        return "{}{}".format(normalized_lang_code, domain_suffix), normalized_remainder

    dynamic_special_domain = _user_list_source_domain_for_prefix(normalized_prefix)
    if dynamic_special_domain is not None:
        return dynamic_special_domain, normalized_title

    domain_suffix = _WIKI_GROUP_TO_DOMAIN_SUFFIX.get(source_descriptor.wiki_group)
    if domain_suffix is None:
        return None
    return "{}{}".format(normalized_prefix, domain_suffix), normalized_title


def _user_name_from_interwiki_link(prefix: Any, title: Any, source_descriptor: _WikiDescriptor) -> Optional[str]:
    resolved_target = _interwiki_target_domain_and_title(prefix, title, source_descriptor)
    if resolved_target is None:
        return None
    target_domain, target_title = resolved_target
    if target_domain not in _known_wikis_by_domain():
        return None
    return _user_name_from_namespace_title(target_title, _siteinfo_for_domain(target_domain))


def _user_name_from_local_user_linktarget(title: Any) -> Optional[str]:
    normalized_title = _normalize_db_page_title(title)
    if not normalized_title:
        return None
    return _normalize_user_name(normalized_title.split("/", 1)[0])


def _fetch_user_names_for_page_sql(ref: _UserListPageRef) -> List[str]:
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        )

    descriptor = _known_wikis_by_domain().get(ref.domain)
    if descriptor is None:
        raise ValueError("user_list_page could not be resolved to an existing Wikimedia page.")

    source_siteinfo = _siteinfo_for_domain(ref.domain)
    initial_namespace_id, initial_db_title = _namespace_and_db_title_for_full_title(ref.page_title, source_siteinfo)
    if not initial_db_title:
        raise ValueError("user_list_page could not be resolved to an existing Wikimedia page.")

    user_names: List[str] = []
    seen_user_names: set[str] = set()
    connection = None

    try:
        connection = pymysql.connect(**_replica_connect_kwargs(descriptor.dbname))
        with connection.cursor() as cursor:
            page_namespace = initial_namespace_id
            page_db_title = initial_db_title
            resolved_page_id: Optional[int] = None

            for _ in range(5):
                cursor.execute(
                    (
                        "SELECT p.page_id, rd.rd_namespace, rd.rd_title "
                        "FROM page AS p "
                        "LEFT JOIN redirect AS rd ON rd.rd_from = p.page_id "
                        "WHERE p.page_namespace = %s AND p.page_title = %s "
                        "LIMIT 1"
                    ),
                    [page_namespace, page_db_title],
                )
                rows = _fetched_row_list(cursor.fetchall())
                if not rows:
                    break

                row = rows[0]
                try:
                    resolved_page_id = int(row[0])
                except (TypeError, ValueError, IndexError):
                    resolved_page_id = None
                    break

                redirect_namespace = row[1] if len(row) > 1 else None
                redirect_title = row[2] if len(row) > 2 else None
                if redirect_namespace is None or redirect_title in {None, ""}:
                    break

                try:
                    page_namespace = int(redirect_namespace)
                except (TypeError, ValueError):
                    break
                page_db_title = _normalize_db_page_title(redirect_title)
                if not page_db_title:
                    break

            if resolved_page_id is None:
                raise ValueError(
                    "user_list_page could not be resolved to an existing Wikimedia page "
                    "(ref={}, domain={}, namespace={}, db_title={}).".format(
                        ref.canonical_ref,
                        descriptor.domain,
                        initial_namespace_id,
                        initial_db_title,
                    )
                )

            cursor.execute(
                (
                    "SELECT lt.lt_title "
                    "FROM pagelinks AS pl "
                    "JOIN linktarget AS lt ON lt.lt_id = pl.pl_target_id "
                    "WHERE pl.pl_from = %s AND lt.lt_namespace = 2"
                ),
                [resolved_page_id],
            )
            local_rows = _fetched_row_list(cursor.fetchall())
            for row in local_rows:
                if not row:
                    continue
                _append_unique_user_name(
                    user_names,
                    seen_user_names,
                    _user_name_from_local_user_linktarget(row[0]),
                )

            cursor.execute(
                (
                    "SELECT iwl.iwl_prefix, iwl.iwl_title "
                    "FROM iwlinks AS iwl "
                    "WHERE iwl.iwl_from = %s"
                ),
                [resolved_page_id],
            )
            interwiki_rows = _fetched_row_list(cursor.fetchall())
            for row in interwiki_rows:
                if len(row) < 2:
                    continue
                interwiki_title = _normalize_db_page_title(row[1])
                if not interwiki_title:
                    continue

                candidate_user_name = _user_name_from_interwiki_link(row[0], interwiki_title, descriptor)
                _append_unique_user_name(user_names, seen_user_names, candidate_user_name)
    except ValueError:
        raise
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch user list page replica data for {}: {}".format(ref.domain, exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    if not user_names:
        raise ValueError("user_list_page must link to at least one Wikimedia user page.")
    return user_names


def _fetch_user_names_for_page(ref: _UserListPageRef) -> List[str]:
    source_siteinfo = _siteinfo_for_domain(ref.domain)
    user_names: List[str] = []
    seen_user_names: set[str] = set()
    continuation_params: Dict[str, str] = {}
    page_found = False

    while True:
        params: Dict[str, str] = {
            "action": "query",
            "titles": ref.page_title,
            "redirects": "1",
            "prop": "links|iwlinks",
            "plnamespace": "2",
            "pllimit": "max",
            "iwlimit": "max",
            "iwprop": "url",
            "format": "json",
            "formatversion": "2",
        }
        params.update(continuation_params)

        payload = _request_json("{}?{}".format(_wiki_api_url(ref.domain), urlencode(params)))
        query_payload = payload.get("query")
        if not isinstance(query_payload, Mapping):
            raise PetscanServiceError("User list page API returned no query payload.")

        pages = query_payload.get("pages")
        if not isinstance(pages, list):
            raise PetscanServiceError("User list page API returned no pages payload.")

        for page in pages:
            if not isinstance(page, Mapping):
                continue
            if page.get("missing") is True:
                continue
            page_found = True

            links = page.get("links")
            if isinstance(links, list):
                for link in links:
                    if not isinstance(link, Mapping):
                        continue
                    _append_unique_user_name(
                        user_names,
                        seen_user_names,
                        _user_name_from_namespace_title(link.get("title"), source_siteinfo),
                    )

            iwlinks = page.get("iwlinks")
            if isinstance(iwlinks, list):
                for iwlink in iwlinks:
                    if not isinstance(iwlink, Mapping):
                        continue
                    _append_unique_user_name(
                        user_names,
                        seen_user_names,
                        _user_name_from_user_page_url(iwlink.get("url")),
                    )

        raw_continue = payload.get("continue")
        if not isinstance(raw_continue, Mapping):
            break

        next_params = {}
        for key, raw_value in raw_continue.items():
            if str(key).strip() == "continue":
                continue
            text_value = str(raw_value or "").strip()
            if text_value:
                next_params[str(key).strip()] = text_value
        if not next_params:
            break
        continuation_params = next_params

    if not page_found:
        raise ValueError("user_list_page could not be resolved to an existing Wikimedia page.")
    if not user_names:
        raise ValueError("user_list_page must link to at least one Wikimedia user page.")
    existing_user_names = [user_name for user_name in user_names if _centralauth_user_exists(user_name)]
    if not existing_user_names:
        raise ValueError("user_list_page must link to at least one CentralAuth user page.")
    return existing_user_names


@lru_cache(maxsize=512)
def _centralauth_user_summary(user_name: str) -> Tuple[bool, Tuple[str, ...]]:
    normalized_user_name = _normalize_user_name(user_name)
    if normalized_user_name is None:
        return False, ()

    params = {
        "action": "query",
        "meta": "globaluserinfo",
        "guiuser": normalized_user_name,
        "guiprop": "merged",
        "format": "json",
        "formatversion": "2",
    }
    payload = _request_json("{}?{}".format(_wiki_api_url(_META_DOMAIN), urlencode(params)))
    query_payload = payload.get("query")
    if not isinstance(query_payload, Mapping):
        raise PetscanServiceError("Globaluserinfo API returned no query payload.")
    globaluserinfo = query_payload.get("globaluserinfo")
    if not isinstance(globaluserinfo, Mapping):
        raise PetscanServiceError("Globaluserinfo API returned no globaluserinfo payload.")

    missing_flag = globaluserinfo.get("missing")
    user_exists = missing_flag is not True and missing_flag != ""
    active_dbnames: set[str] = set()
    merged_accounts = globaluserinfo.get("merged")
    if not isinstance(merged_accounts, list):
        return user_exists, ()

    for merged_account in merged_accounts:
        if not isinstance(merged_account, Mapping):
            continue
        dbname = str(merged_account.get("wiki", "") or "").strip().lower()
        if not dbname:
            continue
        raw_editcount = merged_account.get("editcount")
        if raw_editcount is None:
            active_dbnames.add(dbname)
            continue
        try:
            if int(str(raw_editcount).strip()) > 0:
                active_dbnames.add(dbname)
        except (TypeError, ValueError):
            active_dbnames.add(dbname)

    return user_exists, tuple(sorted(active_dbnames))


def _centralauth_user_exists(user_name: str) -> bool:
    return _centralauth_user_summary(user_name)[0]


def _centralauth_localuser_summary_sql(user_names: List[str]) -> Tuple[List[str], Dict[str, Tuple[str, ...]]]:
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        )

    normalized_user_names = _actor_user_names(user_names)
    if not normalized_user_names:
        return [], {}

    connection = None
    user_name_by_casefold = {user_name.casefold(): user_name for user_name in normalized_user_names}
    dbnames_by_user: Dict[str, set[str]] = {}

    try:
        connection = pymysql.connect(**_replica_connect_kwargs("centralauth"))
        with connection.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(normalized_user_names))
            cursor.execute(
                "SELECT lu_name, lu_wiki FROM localuser WHERE lu_name IN ({})".format(placeholders),
                normalized_user_names,
            )
            rows = _fetched_row_list(cursor.fetchall())
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch CentralAuth localuser data: {}".format(exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    if not rows:
        return [], {}

    for row in rows:
        if len(row) < 2:
            continue
        normalized_user_name = _normalize_user_name(_normalized_text_value(row[0]))
        dbname = _normalized_text_value(row[1]).strip().lower()
        if normalized_user_name is None or not dbname:
            continue
        canonical_user_name = user_name_by_casefold.get(normalized_user_name.casefold(), normalized_user_name)
        dbnames_by_user.setdefault(canonical_user_name, set()).add(dbname)

    existing_user_names = [
        user_name for user_name in normalized_user_names if user_name in dbnames_by_user and dbnames_by_user[user_name]
    ]
    return existing_user_names, {
        user_name: tuple(sorted(dbnames))
        for user_name, dbnames in dbnames_by_user.items()
        if dbnames
    }


@lru_cache(maxsize=512)
def _active_user_wiki_dbnames_for_user(user_name: str) -> Tuple[str, ...]:
    return _centralauth_user_summary(user_name)[1]


def _registered_user_names_for_descriptor(
    descriptor: _WikiDescriptor,
    user_names: List[str],
    dbnames_by_user: Mapping[str, Tuple[str, ...]],
) -> List[str]:
    return [
        user_name
        for user_name in user_names
        if descriptor.dbname in set(dbnames_by_user.get(user_name, ()))
    ]


def _recent_user_activity_threshold(timestamp: Optional[str]) -> str:
    replica_window_floor = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y%m%d%H%M%S")
    if timestamp is None:
        return replica_window_floor
    return timestamp if timestamp > replica_window_floor else replica_window_floor


def _timestamp_is_within_recentchanges_window(timestamp: str) -> bool:
    return _lower_bound_datetime_for_timestamp(timestamp) >= (
        datetime.now(timezone.utc) - _MAX_RECENTCHANGES_PRECHECK_WINDOW
    )


def _should_use_recentchanges_precheck_sql(
    descriptor_count: int,
    include_edited_pages: bool,
    timestamp: Optional[str],
) -> bool:
    if descriptor_count < _MIN_WIKI_COUNT_FOR_ACTIVE_USER_FILTER:
        return False
    if not include_edited_pages:
        return True
    if timestamp is None:
        return False
    return _timestamp_is_within_recentchanges_window(timestamp)


def _descriptor_has_recentchanges_for_users_sql(
    descriptor: _WikiDescriptor,
    user_names: List[str],
    rc_sources: List[str],
    timestamp: Optional[str],
) -> bool:
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        )

    normalized_user_names = _actor_user_names(user_names)
    normalized_rc_sources = [str(source or "").strip() for source in rc_sources if str(source or "").strip()]
    if not normalized_user_names or not normalized_rc_sources:
        return False

    connection = None
    try:
        connection = pymysql.connect(**_replica_connect_kwargs(descriptor.dbname))
        with connection.cursor() as cursor:
            actor_placeholders = ", ".join(["%s"] * len(normalized_user_names))
            if len(normalized_rc_sources) == 1:
                sql = (
                    "SELECT 1 "
                    "FROM recentchanges_userindex AS rc "
                    "JOIN actor_recentchanges AS a ON rc.rc_actor = a.actor_id "
                    "WHERE rc.rc_source = %s "
                    "AND a.actor_name IN ({})"
                ).format(actor_placeholders)
            else:
                source_placeholders = ", ".join(["%s"] * len(normalized_rc_sources))
                sql = (
                    "SELECT 1 "
                    "FROM recentchanges_userindex AS rc "
                    "JOIN actor_recentchanges AS a ON rc.rc_actor = a.actor_id "
                    "WHERE rc.rc_source IN ({}) "
                    "AND a.actor_name IN ({})"
                ).format(source_placeholders, actor_placeholders)

            params: List[object] = list(normalized_rc_sources)
            params.extend(normalized_user_names)
            if descriptor.domain == _COMMONS_DOMAIN:
                sql += " AND rc.rc_namespace <> %s"
                params.append(_COMMONS_FILE_NAMESPACE)
            if timestamp is not None:
                sql += " AND rc.rc_timestamp >= %s"
                params.append(timestamp)
            sql += " LIMIT 1"

            cursor.execute(sql, params)
            rows = _fetched_row_list(cursor.fetchall())
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to precheck recentchanges activity for {}: {}".format(descriptor.domain, exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    return bool(rows)


def _filter_user_names_for_recent_activity_sql(
    descriptor: _WikiDescriptor,
    user_names: List[str],
    activity_threshold: str,
) -> List[str]:
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        )

    normalized_user_names = _actor_user_names(user_names)
    if not normalized_user_names:
        return []

    connection = None
    recently_active_user_names: List[str] = []
    user_name_by_casefold = {user_name.casefold(): user_name for user_name in normalized_user_names}

    try:
        connection = pymysql.connect(**_replica_connect_kwargs(descriptor.dbname))
        with connection.cursor() as cursor:
            # Toolforge note:
            # - actor_revision is an actor-shaped table/view limited to actors that appear in revision rows.
            # - revision_userindex is the revision event table that contains rev_timestamp and rev_actor.
            # So actor_revision is only used to shrink the actor side; it is not a replacement for revision_userindex.
            placeholders = ", ".join(["%s"] * len(normalized_user_names))
            cursor.execute(
                (
                    "SELECT u.user_name "
                    "FROM user AS u "
                    "WHERE u.user_name IN ({}) "
                    "AND EXISTS ("
                    "SELECT 1 "
                    "FROM actor_revision AS a "
                    "JOIN revision_userindex AS r ON r.rev_actor = a.actor_id "
                    "WHERE a.actor_user = u.user_id "
                    "AND r.rev_timestamp >= %s"
                    ")"
                ).format(placeholders),
                normalized_user_names + [activity_threshold],
            )
            rows = _fetched_row_list(cursor.fetchall())
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch recent user activity for {}: {}".format(descriptor.domain, exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    if not rows:
        return []

    seen_recently_active: set[str] = set()
    for row in rows:
        if not row:
            continue
        normalized_user_name = _normalize_user_name(_normalized_text_value(row[0]))
        if normalized_user_name is None:
            continue
        canonical_user_name = user_name_by_casefold.get(normalized_user_name.casefold(), normalized_user_name)
        if canonical_user_name in seen_recently_active:
            continue
        seen_recently_active.add(canonical_user_name)
        recently_active_user_names.append(canonical_user_name)

    return recently_active_user_names


def _filter_descriptors_for_active_user_wikis(
    descriptors: List[_WikiDescriptor],
    user_names: Optional[List[str]],
) -> List[_WikiDescriptor]:
    if (
        not user_names
        or len(descriptors) <= 1
        or len(descriptors) < _MIN_WIKI_COUNT_FOR_ACTIVE_USER_FILTER
    ):
        return descriptors

    active_dbnames: set[str] = set()
    for user_name in user_names:
        active_dbnames.update(_active_user_wiki_dbnames_for_user(user_name))

    if not active_dbnames:
        return []

    return [descriptor for descriptor in descriptors if descriptor.dbname in active_dbnames]


def _replica_connect_kwargs(dbname: str) -> Dict[str, Any]:
    replica_cnf = str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip()
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    connect_kwargs: Dict[str, Any] = {
        "host": "{}.web.db.svc.wikimedia.cloud".format(dbname),
        "database": "{}_p".format(dbname),
        "charset": "utf8mb4",
        "connect_timeout": timeout,
        "read_timeout": timeout,
        "write_timeout": timeout,
        "autocommit": True,
    }
    if replica_cnf:
        connect_kwargs["read_default_file"] = os.path.expanduser(os.path.expandvars(replica_cnf))
    return connect_kwargs


def _actor_user_names(user_names: Optional[List[str]]) -> List[str]:
    normalized_names: List[str] = []
    seen: set[str] = set()
    for raw_name in user_names or []:
        normalized_name = _normalize_user_name(raw_name)
        if normalized_name is None:
            continue
        if normalized_name in seen:
            continue
        seen.add(normalized_name)
        normalized_names.append(normalized_name)
    return normalized_names


def _fetch_revision_rows_for_wiki(
    descriptor: _WikiDescriptor,
    timestamp: str,
    limit: Optional[int],
    user_names: List[str],
) -> List[Tuple[Any, Any, Any, Any, Any]]:
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        )

    effective_limit = _DEFAULT_SQL_LIMIT if limit is None else min(limit, _DEFAULT_SQL_LIMIT)
    actor_user_names = _actor_user_names(user_names)
    if not actor_user_names:
        return []

    actor_placeholders = ", ".join(["%s"] * len(actor_user_names))

    def _finalize_revision_sql(sql: str, params: List[object], qid_expression: str) -> Tuple[str, List[object]]:
        finalized_params = list(params)
        sql += " WHERE a.actor_name IN ({})".format(actor_placeholders)
        finalized_params.extend(actor_user_names)
        sql += " AND rev.rev_timestamp >= %s"
        finalized_params.append(timestamp)
        if descriptor.domain == "commons.wikimedia.org":
            sql += " AND p.page_namespace <> %s"
            finalized_params.append(_COMMONS_FILE_NAMESPACE)
        sql += " GROUP BY p.page_id, p.page_title, p.page_namespace, {}".format(qid_expression)
        sql += " ORDER BY matched_timestamp DESC, p.page_title ASC"
        sql += " LIMIT %s"
        finalized_params.append(effective_limit)
        return sql, finalized_params

    if descriptor.domain != _INCUBATOR_DOMAIN:
        sql = (
            "SELECT p.page_id AS page_id, p.page_title, p.page_namespace, pp.pp_value, "
            "MAX(rev.rev_timestamp) AS matched_timestamp "
            # Toolforge note:
            # - actor_revision is actor-table shaped and only narrows the actor rows to users who appear in revision.
            # - revision_userindex is still the real revision event source with rev_page/rev_timestamp.
            "FROM actor_revision AS a "
            "JOIN revision_userindex AS rev ON rev.rev_actor = a.actor_id "
            "JOIN page AS p ON p.page_id = rev.rev_page "
            "JOIN page_props AS pp ON pp.pp_page = p.page_id "
            "AND pp.pp_propname = %s"
        )
        sql, params = _finalize_revision_sql(sql, ["wikibase_item"], "pp.pp_value")
    else:
        primary_sql = (
            "SELECT p.page_id AS page_id, p.page_title, p.page_namespace, pp.pp_value, "
            "MAX(rev.rev_timestamp) AS matched_timestamp "
            "FROM actor_revision AS a "
            "JOIN revision_userindex AS rev ON rev.rev_actor = a.actor_id "
            "JOIN page AS p ON p.page_id = rev.rev_page "
            "JOIN page_props AS pp ON pp.pp_page = p.page_id "
            "AND pp.pp_propname = %s"
        )
        primary_sql, primary_params = _finalize_revision_sql(primary_sql, ["wikibase_item"], "pp.pp_value")

        fallback_sql = (
            "SELECT p.page_id AS page_id, p.page_title, p.page_namespace, cl.cl_sortkey_prefix AS qid, "
            "MAX(rev.rev_timestamp) AS matched_timestamp "
            "FROM actor_revision AS a "
            "JOIN revision_userindex AS rev ON rev.rev_actor = a.actor_id "
            "JOIN page AS p ON p.page_id = rev.rev_page "
            "LEFT JOIN page_props AS pp ON pp.pp_page = p.page_id "
            "AND pp.pp_propname = %s "
            "JOIN linktarget AS lt ON lt.lt_namespace = 14 "
            "AND lt.lt_title = %s "
            "JOIN categorylinks AS cl ON cl.cl_from = p.page_id "
            "AND cl.cl_target_id = lt.lt_id"
        )
        fallback_sql, fallback_params = _finalize_revision_sql(
            fallback_sql + " AND 1=1",
            ["wikibase_item", _INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE],
            "cl.cl_sortkey_prefix",
        )
        fallback_sql = fallback_sql.replace(
            " WHERE ",
            " WHERE pp.pp_value IS NULL AND cl.cl_sortkey_prefix IS NOT NULL AND ",
            1,
        )

    connection = None
    try:
        connection = pymysql.connect(**_replica_connect_kwargs(descriptor.dbname))
        with connection.cursor() as cursor:
            if descriptor.domain != _INCUBATOR_DOMAIN:
                cursor.execute(sql, params)
                rows = _fetched_row_list(cursor.fetchall())
            else:
                cursor.execute(primary_sql, primary_params)
                primary_rows = _fetched_row_list(cursor.fetchall())
                cursor.execute(fallback_sql, fallback_params)
                fallback_rows = _fetched_row_list(cursor.fetchall())
                rows = []
                rows.extend(primary_rows)
                rows.extend(fallback_rows)
                rows.sort(
                    key=lambda row: (
                        -int(_numeric_timestamp(row[4]) or 0),
                        str(row[1] if len(row) > 1 else ""),
                    )
                )
                rows = rows[:effective_limit]
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch revision replica data for {}: {}".format(descriptor.domain, exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    return [cast(Tuple[Any, Any, Any, Any, Any], row) for row in rows]


def _fetch_rows_for_wiki(
    descriptor: _WikiDescriptor,
    timestamp: Optional[str],
    limit: Optional[int],
    user_names: Optional[List[str]] = None,
    include_edited_pages: bool = False,
) -> List[Tuple[Any, Any, Any, Any, Any]]:
    if include_edited_pages:
        if timestamp is None:
            raise ValueError("include_edited_pages requires timestamp.")
        return _fetch_revision_rows_for_wiki(
            descriptor,
            timestamp,
            limit,
            user_names or [],
        )

    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is not installed. Install dependencies from requirements.txt first.",
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        )

    effective_limit = _DEFAULT_SQL_LIMIT if limit is None else min(limit, _DEFAULT_SQL_LIMIT)
    actor_user_names = _actor_user_names(user_names)
    # Toolforge note:
    # - recentchanges_userindex is the recentchanges event table we actually query for rc_* fields.
    # - actor_recentchanges is not a recentchanges table; it is actor-table shaped and only contains
    #   actor rows referenced by recentchanges. We join it only to filter actor_name efficiently.
    rc_table_name = "recentchanges_userindex"

    def _finalize_sql(sql: str, params: List[object]) -> Tuple[str, List[object]]:
        finalized_params = list(params)
        if actor_user_names:
            sql += " AND a.actor_name IN ({})".format(", ".join(["%s"] * len(actor_user_names)))
            finalized_params.extend(actor_user_names)
        if descriptor.domain == "commons.wikimedia.org":
            sql += " AND rc.rc_namespace <> %s"
            finalized_params.append(_COMMONS_FILE_NAMESPACE)
        if timestamp is not None:
            sql += " AND rc.rc_timestamp >= %s"
            finalized_params.append(timestamp)
        sql += " ORDER BY rc.rc_timestamp DESC"
        sql += " LIMIT %s"
        finalized_params.append(effective_limit)
        return sql, finalized_params

    if descriptor.domain != _INCUBATOR_DOMAIN:
        sql = (
            "SELECT rc.rc_cur_id AS page_id, p.page_title, p.page_namespace, pp.pp_value, rc.rc_timestamp "
            + "FROM {} AS rc ".format(rc_table_name)
            + ("JOIN actor_recentchanges AS a ON rc.rc_actor = a.actor_id " if actor_user_names else "")
            + "JOIN page AS p ON p.page_id = rc.rc_cur_id "
            + "JOIN page_props AS pp ON pp.pp_page = rc.rc_cur_id "
            + "AND pp.pp_propname = %s "
            + "WHERE rc.rc_source = %s"
        )
        sql, params = _finalize_sql(sql, ["wikibase_item", "mw.new"])
    else:
        primary_sql = (
            "SELECT rc.rc_cur_id AS page_id, p.page_title, p.page_namespace, pp.pp_value, rc.rc_timestamp "
            + "FROM {} AS rc ".format(rc_table_name)
            + ("JOIN actor_recentchanges AS a ON rc.rc_actor = a.actor_id " if actor_user_names else "")
            + "JOIN page AS p ON p.page_id = rc.rc_cur_id "
            + "JOIN page_props AS pp ON pp.pp_page = rc.rc_cur_id "
            + "AND pp.pp_propname = %s "
            + "WHERE rc.rc_source = %s"
        )
        primary_sql, primary_params = _finalize_sql(primary_sql, ["wikibase_item", "mw.new"])

        fallback_sql = (
            "SELECT rc.rc_cur_id AS page_id, p.page_title, p.page_namespace, cl.cl_sortkey_prefix AS qid, rc.rc_timestamp "
            + "FROM {} AS rc ".format(rc_table_name)
            + ("JOIN actor_recentchanges AS a ON rc.rc_actor = a.actor_id " if actor_user_names else "")
            + "JOIN page AS p ON p.page_id = rc.rc_cur_id "
            + "LEFT JOIN page_props AS pp ON pp.pp_page = rc.rc_cur_id "
            + "AND pp.pp_propname = %s "
            + "JOIN linktarget AS lt ON lt.lt_namespace = 14 "
            + "AND lt.lt_title = %s "
            + "JOIN categorylinks AS cl ON cl.cl_from = rc.rc_cur_id "
            + "AND cl.cl_target_id = lt.lt_id "
            + "WHERE rc.rc_source = %s "
            + "AND pp.pp_value IS NULL "
            + "AND cl.cl_sortkey_prefix IS NOT NULL"
        )
        fallback_sql, fallback_params = _finalize_sql(
            fallback_sql,
            [
                "wikibase_item",
                _INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE,
                "mw.new",
            ],
        )

    connection = None
    try:
        connection = pymysql.connect(**_replica_connect_kwargs(descriptor.dbname))
        with connection.cursor() as cursor:
            if descriptor.domain != _INCUBATOR_DOMAIN:
                cursor.execute(sql, params)
                rows = _fetched_row_list(cursor.fetchall())
            else:
                cursor.execute(primary_sql, primary_params)
                primary_rows = _fetched_row_list(cursor.fetchall())
                cursor.execute(fallback_sql, fallback_params)
                fallback_rows = _fetched_row_list(cursor.fetchall())
                rows = []
                rows.extend(primary_rows)
                rows.extend(fallback_rows)
                rows.sort(
                    key=lambda row: (
                        -int(_numeric_timestamp(row[4]) or 0),
                        str(row[1] if len(row) > 1 else ""),
                    )
                )
                rows = rows[:effective_limit]
    except Exception as exc:
        raise PetscanServiceError(
            "Failed to fetch new-page replica data for {}: {}".format(descriptor.domain, exc),
            public_message=_NEWPAGES_FETCH_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    return [cast(Tuple[Any, Any, Any, Any, Any], row) for row in rows]


def _fetch_creation_log_entries_api(
    descriptor: _WikiDescriptor,
    limit: Optional[int],
) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
    effective_limit = _MAX_API_ROWS_PER_WIKI if limit is None else min(limit, _MAX_API_ROWS_PER_WIKI)
    return _fetch_creation_log_entries_api_page(descriptor, effective_limit)


def _fetch_creation_log_entries_api_page(
    descriptor: _WikiDescriptor,
    limit: int,
    continue_token: Optional[str] = None,
) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
    params: Dict[str, str] = {
        "action": "query",
        "list": "logevents",
        "leprop": "title|timestamp|ids|user",
        "letype": "create",
        "ledir": "older",
        "lelimit": str(max(1, limit)),
        "format": "json",
        "formatversion": "2",
    }
    if continue_token:
        params["lecontinue"] = continue_token

    payload = _request_json("{}?{}".format(_wiki_api_url(descriptor.domain), urlencode(params)))
    query_payload = payload.get("query")
    if not isinstance(query_payload, Mapping):
        raise PetscanServiceError("Logevents API returned no query payload.")
    logevents = query_payload.get("logevents")
    if not isinstance(logevents, list):
        raise PetscanServiceError("Logevents API returned no logevents payload.")
    continuation = payload.get("continue")
    next_continue: Optional[str] = None
    if isinstance(continuation, Mapping):
        raw_continue = continuation.get("lecontinue")
        text_continue = str(raw_continue or "").strip()
        if text_continue:
            next_continue = text_continue
    return [entry for entry in logevents if isinstance(entry, Mapping)], next_continue


def _fetch_usercontrib_entries_api_page(
    descriptor: _WikiDescriptor,
    user_name: str,
    timestamp: str,
    continue_token: Optional[str] = None,
) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
    params: Dict[str, str] = {
        "action": "query",
        "list": "usercontribs",
        "ucuser": user_name,
        "ucprop": "title|timestamp|ids",
        "ucdir": "older",
        "uclimit": "max",
        "ucend": _api_timestamp(timestamp),
        "format": "json",
        "formatversion": "2",
    }
    if continue_token:
        params["uccontinue"] = continue_token

    payload = _request_json("{}?{}".format(_wiki_api_url(descriptor.domain), urlencode(params)))
    query_payload = payload.get("query")
    if not isinstance(query_payload, Mapping):
        raise PetscanServiceError("Usercontribs API returned no query payload.")
    usercontribs = query_payload.get("usercontribs")
    if not isinstance(usercontribs, list):
        raise PetscanServiceError("Usercontribs API returned no usercontribs payload.")
    continuation = payload.get("continue")
    next_continue: Optional[str] = None
    if isinstance(continuation, Mapping):
        raw_continue = continuation.get("uccontinue")
        text_continue = str(raw_continue or "").strip()
        if text_continue:
            next_continue = text_continue
    return [entry for entry in usercontribs if isinstance(entry, Mapping)], next_continue


def _fetch_pageprops_qids_api(domain: str, page_ids: List[int]) -> Dict[int, str]:
    qids_by_page_id: Dict[int, str] = {}
    for batch in _chunked_page_ids(page_ids, _MAX_API_PAGEIDS_PER_BATCH):
        params = {
            "action": "query",
            "prop": "pageprops",
            "pageids": "|".join(str(page_id) for page_id in batch),
            "ppprop": "wikibase_item",
            "format": "json",
            "formatversion": "2",
        }
        payload = _request_json("{}?{}".format(_wiki_api_url(domain), urlencode(params)))
        query_payload = payload.get("query")
        if not isinstance(query_payload, Mapping):
            raise PetscanServiceError("Pageprops API returned no query payload.")
        pages = query_payload.get("pages")
        if not isinstance(pages, list):
            raise PetscanServiceError("Pageprops API returned no pages payload.")

        for page in pages:
            if not isinstance(page, Mapping):
                continue
            raw_page_id = page.get("pageid")
            if raw_page_id is None:
                continue
            try:
                page_id = int(raw_page_id)
            except (TypeError, ValueError):
                continue
            pageprops = page.get("pageprops")
            if not isinstance(pageprops, Mapping):
                continue
            qid = normalize_qid(pageprops.get("wikibase_item"))
            if qid is not None:
                qids_by_page_id[page_id] = qid

    return qids_by_page_id


def _fetch_incubator_sortkey_qids_api(page_ids: List[int]) -> Dict[int, str]:
    qids_by_page_id: Dict[int, str] = {}
    for batch in _chunked_page_ids(page_ids, _MAX_API_PAGEIDS_PER_BATCH):
        params = {
            "action": "query",
            "prop": "categories",
            "pageids": "|".join(str(page_id) for page_id in batch),
            "clcategories": _INCUBATOR_WIKIDATA_CATEGORY_PAGE,
            "clprop": "sortkey",
            "cllimit": "max",
            "format": "json",
            "formatversion": "2",
        }
        payload = _request_json("{}?{}".format(_wiki_api_url(_INCUBATOR_DOMAIN), urlencode(params)))
        query_payload = payload.get("query")
        if not isinstance(query_payload, Mapping):
            raise PetscanServiceError("Categories API returned no query payload.")
        pages = query_payload.get("pages")
        if not isinstance(pages, list):
            raise PetscanServiceError("Categories API returned no pages payload.")

        for page in pages:
            if not isinstance(page, Mapping):
                continue
            raw_page_id = page.get("pageid")
            if raw_page_id is None:
                continue
            try:
                page_id = int(raw_page_id)
            except (TypeError, ValueError):
                continue
            categories = page.get("categories")
            if not isinstance(categories, list):
                continue
            for category in categories:
                if not isinstance(category, Mapping):
                    continue
                qid = normalize_qid(category.get("sortkeyprefix"))
                if qid is None:
                    continue
                qids_by_page_id[page_id] = qid
                break

    return qids_by_page_id


def _numeric_timestamp(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit() and len(text) == 14:
        try:
            return int(text)
        except ValueError:
            return None

    normalized = normalize_datetime_xsd(text)
    if normalized is None or len(normalized) < 19:
        return None
    digits = "".join(character for character in normalized if character.isdigit())
    if len(digits) < 14:
        return None
    try:
        return int(digits[:14])
    except ValueError:
        return None


def _record_sort_key(row: Mapping[str, Any]) -> Tuple[int, str, str]:
    return (
        -int(row.get("_created_sort", 0)),
        str(row.get("wiki_domain", "")),
        str(row.get("page_title", "")),
    )


def _finalize_records(
    records: List[Dict[str, Any]],
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    finalized = sorted(records, key=_record_sort_key)

    if limit is not None:
        finalized = finalized[:limit]

    for row in finalized:
        row.pop("_created_sort", None)

    return finalized


def _fetch_newpage_records_api(
    limit: Optional[int],
    descriptors: List[_WikiDescriptor],
    timestamp: Optional[str],
    user_names: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    records: List[Dict[str, Any]] = []
    threshold = int(timestamp) if timestamp is not None else None
    fully_scan_timestamp_window = threshold is not None and len(descriptors) < _MAX_API_FULL_SCAN_WIKI_COUNT
    per_wiki_limit: Optional[int]
    if fully_scan_timestamp_window:
        per_wiki_limit = limit
    else:
        per_wiki_limit = _MAX_API_ROWS_PER_WIKI if limit is None else min(limit, _MAX_API_ROWS_PER_WIKI)
    allowed_user_names = {
        str(name).casefold()
        for name in (user_names or [])
        if str(name).strip()
    }

    for descriptor in descriptors:
        siteinfo = _siteinfo_for_domain(descriptor.domain)
        descriptor_records: List[Dict[str, Any]] = []
        continue_token: Optional[str] = None
        inspected_rows = 0
        reached_threshold = False

        while True:
            if not fully_scan_timestamp_window and inspected_rows >= _MAX_API_ROWS_PER_WIKI:
                break
            if per_wiki_limit is not None and len(descriptor_records) >= per_wiki_limit:
                break

            if fully_scan_timestamp_window:
                batch_limit = _MAX_API_ROWS_PER_WIKI
            else:
                assert per_wiki_limit is not None
                remaining_raw_budget = _MAX_API_ROWS_PER_WIKI - inspected_rows
                batch_limit = min(per_wiki_limit, remaining_raw_budget)
            entries, continue_token = _fetch_creation_log_entries_api_page(
                descriptor,
                batch_limit,
                continue_token=continue_token,
            )
            if not entries:
                break

            filtered_entries: List[Mapping[str, Any]] = []
            page_ids: List[int] = []

            for entry in entries:
                inspected_rows += 1
                timestamp_value = _numeric_timestamp(entry.get("timestamp"))
                if threshold is not None and (timestamp_value is None or timestamp_value < threshold):
                    reached_threshold = True
                    break

                raw_page_id = entry.get("pageid")
                raw_namespace_id = entry.get("ns", 0)
                if raw_page_id is None:
                    continue
                try:
                    page_id = int(raw_page_id)
                    namespace_id = int(raw_namespace_id)
                except (TypeError, ValueError):
                    continue
                if (
                    descriptor.domain == "commons.wikimedia.org"
                    and namespace_id == _COMMONS_FILE_NAMESPACE
                ):
                    continue
                user_name = _normalize_user_name(entry.get("user"))
                if allowed_user_names and (user_name is None or user_name.casefold() not in allowed_user_names):
                    continue

                filtered_entries.append(
                    {
                        "pageid": page_id,
                        "ns": namespace_id,
                        "title": _normalize_api_page_title(entry.get("title"), namespace_id, siteinfo),
                        "timestamp": entry.get("timestamp"),
                        "user": user_name,
                    }
                )
                page_ids.append(page_id)

            qids_by_page_id = _fetch_pageprops_qids_api(descriptor.domain, page_ids)
            if descriptor.domain == _INCUBATOR_DOMAIN:
                missing_page_ids = [page_id for page_id in page_ids if page_id not in qids_by_page_id]
                if missing_page_ids:
                    qids_by_page_id.update(_fetch_incubator_sortkey_qids_api(missing_page_ids))
            for entry in filtered_entries:
                qid = qids_by_page_id.get(int(entry["pageid"]))
                if qid is None:
                    continue
                record = _build_record(
                    descriptor,
                    siteinfo,
                    (
                        entry["pageid"],
                        entry["title"],
                        entry["ns"],
                        qid,
                        entry["timestamp"],
                    ),
                )
                if record is not None:
                    descriptor_records.append(record)
                    if per_wiki_limit is not None and len(descriptor_records) >= per_wiki_limit:
                        break

            if per_wiki_limit is not None and len(descriptor_records) >= per_wiki_limit:
                break
            if reached_threshold or continue_token is None:
                break

        records.extend(descriptor_records)

    return _finalize_records(records, limit), _api_source_url_for_descriptors(descriptors)


def _fetch_edited_records_api(
    limit: Optional[int],
    descriptors: List[_WikiDescriptor],
    timestamp: str,
    user_names: List[str],
) -> Tuple[List[Dict[str, Any]], str]:
    records: List[Dict[str, Any]] = []

    for descriptor in descriptors:
        siteinfo = _siteinfo_for_domain(descriptor.domain)
        latest_by_page_id: Dict[int, Dict[str, Any]] = {}

        for user_name in _actor_user_names(user_names):
            continue_token: Optional[str] = None

            while True:
                entries, continue_token = _fetch_usercontrib_entries_api_page(
                    descriptor,
                    user_name,
                    timestamp,
                    continue_token=continue_token,
                )
                if not entries:
                    break

                for entry in entries:
                    raw_page_id = entry.get("pageid")
                    raw_namespace_id = entry.get("ns", 0)
                    if raw_page_id is None:
                        continue
                    try:
                        page_id = int(raw_page_id)
                        namespace_id = int(raw_namespace_id)
                    except (TypeError, ValueError):
                        continue
                    if descriptor.domain == "commons.wikimedia.org" and namespace_id == _COMMONS_FILE_NAMESPACE:
                        continue

                    title = _normalize_api_page_title(entry.get("title"), namespace_id, siteinfo)
                    timestamp_value = entry.get("timestamp")
                    sort_value = _numeric_timestamp(timestamp_value)
                    if sort_value is None:
                        continue
                    current = latest_by_page_id.get(page_id)
                    if current is not None and int(current.get("_created_sort", 0)) >= sort_value:
                        continue
                    latest_by_page_id[page_id] = {
                        "pageid": page_id,
                        "ns": namespace_id,
                        "title": title,
                        "timestamp": timestamp_value,
                        "_created_sort": sort_value,
                    }

                if continue_token is None:
                    break

        if not latest_by_page_id:
            continue

        page_ids = sorted(latest_by_page_id.keys())
        qids_by_page_id = _fetch_pageprops_qids_api(descriptor.domain, page_ids)
        if descriptor.domain == _INCUBATOR_DOMAIN:
            missing_page_ids = [page_id for page_id in page_ids if page_id not in qids_by_page_id]
            if missing_page_ids:
                qids_by_page_id.update(_fetch_incubator_sortkey_qids_api(missing_page_ids))

        for page_id in page_ids:
            entry = latest_by_page_id[page_id]
            qid = qids_by_page_id.get(page_id)
            if qid is None:
                continue
            record = _build_record(
                descriptor,
                siteinfo,
                (
                    page_id,
                    entry["title"],
                    entry["ns"],
                    qid,
                    entry["timestamp"],
                ),
                timestamp_key="current_timestamp",
            )
            if record is not None:
                records.append(record)

    return _finalize_records(records, limit), _api_source_url_for_descriptors(descriptors, include_edited_pages=True)


def _build_record(
    descriptor: _WikiDescriptor,
    siteinfo: _SiteInfo,
    row: Tuple[Any, Any, Any, Any, Any],
    timestamp_key: str = "created_timestamp",
) -> Optional[Dict[str, Any]]:
    if len(row) < 5:
        return None

    try:
        page_id = int(row[0])
        namespace_id = int(row[2])
    except (TypeError, ValueError):
        return None

    core_title = _normalize_db_page_title(row[1])
    full_title = _title_with_namespace(row[1], namespace_id, siteinfo)
    page_url = _page_url(descriptor.domain, siteinfo, namespace_id, row[1])
    if not full_title or not page_url:
        return None

    normalized_timestamp = normalize_datetime_xsd(row[4])
    qid = normalize_qid(row[3])
    if qid is None:
        return None

    lang_code = siteinfo.lang_code or descriptor.lang_code
    site_url = descriptor.site_url
    wiki_group = descriptor.wiki_group
    if descriptor.domain == _INCUBATOR_DOMAIN:
        incubator_project, incubator_lang_code, _page_name = _split_incubator_title(core_title)
        if incubator_lang_code:
            lang_code = incubator_lang_code
        incubator_site_url = _incubator_site_url_for_title(core_title)
        if incubator_site_url is not None:
            site_url = incubator_site_url
        if incubator_project:
            wiki_group = _incubator_wiki_group_for_title(core_title)
    record: Dict[str, Any] = {
        "page_id": page_id,
        "page_title": full_title,
        "page_label": full_title.replace("_", " "),
        "namespace": namespace_id,
        "page_url": page_url,
        "site_url": site_url,
        "wiki_domain": descriptor.domain,
        "wiki_dbname": descriptor.dbname,
        "wiki_group": wiki_group,
        "wikidata_id": qid,
        "wikidata_entity": "http://www.wikidata.org/entity/{}".format(qid),
    }
    if lang_code:
        record["lang_code"] = lang_code
    if normalized_timestamp is not None:
        record[timestamp_key] = normalized_timestamp
        created_sort = _numeric_timestamp(row[4])
        if created_sort is not None:
            record["_created_sort"] = created_sort
    return record


def fetch_newpage_records(
    limit: Optional[int] = None,
    wiki_domains: Optional[List[str]] = None,
    timestamp: Optional[str] = None,
    user_list_page: Any = None,
    include_edited_pages: Any = False,
) -> Tuple[List[Dict[str, Any]], str]:
    normalized_limit = normalize_load_limit(limit)
    normalized_wikis = normalize_wikis(wiki_domains)
    normalized_timestamp = normalize_timestamp(timestamp)
    normalized_user_list_page = normalize_user_list_page(user_list_page)
    normalized_include_edited_pages = normalize_include_edited_pages(include_edited_pages)
    backend = newpages_lookup_backend()

    if not normalized_wikis:
        raise ValueError("wiki must include at least one known Wikimedia wiki.")
    _validate_include_edited_pages_request(
        normalized_include_edited_pages,
        normalized_timestamp,
        normalized_user_list_page,
    )

    descriptors = _selected_wiki_descriptors(normalized_wikis)
    filtered_user_names = None  # type: Optional[List[str]]
    descriptor_user_names_by_dbname: Dict[str, List[str]] = {}
    if normalized_user_list_page is not None:
        resolved_user_list_page = _resolve_user_list_page(normalized_user_list_page)
        if resolved_user_list_page is None:
            raise ValueError("user_list_page must be a Wikimedia wiki page reference.")
        if backend == LOOKUP_BACKEND_TOOLFORGE_SQL:
            filtered_user_names = _fetch_user_names_for_page_sql(resolved_user_list_page)
            filtered_user_names, dbnames_by_user = _centralauth_localuser_summary_sql(filtered_user_names)
            if not filtered_user_names:
                raise ValueError("user_list_page must link to at least one CentralAuth user page.")

            use_recentchanges_precheck = _should_use_recentchanges_precheck_sql(
                len(descriptors),
                normalized_include_edited_pages,
                normalized_timestamp,
            )
            precheck_sources = ["mw.new", "mw.edit"] if normalized_include_edited_pages else ["mw.new"]
            filtered_descriptors: List[_WikiDescriptor] = []
            for descriptor in descriptors:
                registered_user_names = _registered_user_names_for_descriptor(
                    descriptor,
                    filtered_user_names,
                    dbnames_by_user,
                )
                if not registered_user_names:
                    continue
                if use_recentchanges_precheck and not _descriptor_has_recentchanges_for_users_sql(
                    descriptor,
                    registered_user_names,
                    precheck_sources,
                    normalized_timestamp,
                ):
                    continue
                if normalized_include_edited_pages:
                    activity_threshold = _recent_user_activity_threshold(normalized_timestamp)
                    registered_user_names = _filter_user_names_for_recent_activity_sql(
                        descriptor,
                        registered_user_names,
                        activity_threshold,
                    )
                    if not registered_user_names:
                        continue
                descriptor_user_names_by_dbname[descriptor.dbname] = registered_user_names
                filtered_descriptors.append(descriptor)
            descriptors = filtered_descriptors
        else:
            filtered_user_names = _fetch_user_names_for_page(resolved_user_list_page)
            descriptors = _filter_descriptors_for_active_user_wikis(descriptors, filtered_user_names)
    _console_log(
        "backend={} limit={} timestamp={} user_list_page={} include_edited_pages={} wikis={}".format(
            backend,
            normalized_limit if normalized_limit is not None else "all",
            normalized_timestamp if normalized_timestamp is not None else "any",
            normalized_user_list_page if normalized_user_list_page is not None else "any",
            "1" if normalized_include_edited_pages else "0",
            ",".join(_sanitize_for_log(descriptor.domain) for descriptor in descriptors),
        )
    )

    if normalized_include_edited_pages:
        assert filtered_user_names is not None
        assert normalized_timestamp is not None
        if backend == LOOKUP_BACKEND_API:
            return _fetch_edited_records_api(
                limit=normalized_limit,
                descriptors=descriptors,
                timestamp=normalized_timestamp,
                user_names=filtered_user_names,
            )
        edited_records: List[Dict[str, Any]] = []
        for descriptor in descriptors:
            siteinfo = _siteinfo_for_domain(descriptor.domain)
            descriptor_user_names = descriptor_user_names_by_dbname.get(descriptor.dbname, filtered_user_names or [])
            for replica_row in _fetch_rows_for_wiki(
                descriptor,
                normalized_timestamp,
                normalized_limit,
                user_names=descriptor_user_names,
                include_edited_pages=True,
            ):
                record = _build_record(descriptor, siteinfo, replica_row, timestamp_key="current_timestamp")
                if record is not None:
                    edited_records.append(record)
        return _finalize_records(edited_records, normalized_limit), _source_url_for_descriptors(
            descriptors,
            include_edited_pages=True,
        )

    if backend == LOOKUP_BACKEND_API:
        return _fetch_newpage_records_api(
            limit=normalized_limit,
            descriptors=descriptors,
            timestamp=normalized_timestamp,
            user_names=filtered_user_names,
        )

    records: List[Dict[str, Any]] = []
    per_wiki_limit = normalized_limit
    for descriptor in descriptors:
        siteinfo = _siteinfo_for_domain(descriptor.domain)
        descriptor_user_names = descriptor_user_names_by_dbname.get(descriptor.dbname, filtered_user_names or [])
        for replica_row in _fetch_rows_for_wiki(
            descriptor,
            normalized_timestamp,
            per_wiki_limit,
            user_names=descriptor_user_names,
        ):
            record = _build_record(descriptor, siteinfo, replica_row)
            if record is not None:
                records.append(record)
    return _finalize_records(records, normalized_limit), _source_url_for_descriptors(descriptors)
