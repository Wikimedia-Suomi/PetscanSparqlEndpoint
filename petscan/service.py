"""Service-layer orchestration for PetScan ingestion and SPARQL execution.

Responsibilities:
- fetch and normalize PetScan datasets
- enrich GIL links with Wikidata IDs
- build/load Oxigraph stores with metadata cache
- execute and serialize read-only SPARQL queries
"""

import json
import re
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TypedDict, cast
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

from django.conf import settings

from . import enrichment_sql as _enrichment_sql
from .enrichment_api import fetch_wikibase_items_for_site_api as _api_fetch_wikibase_items_for_site

_sql_fetch_wikibase_items_for_site = _enrichment_sql.fetch_wikibase_items_for_site_sql
pymysql = _enrichment_sql.pymysql

try:
    from pyoxigraph import BlankNode, DefaultGraph, Literal, NamedNode, Quad, Store
except ImportError:  # pragma: no cover - dependency check at runtime
    BlankNode = None  # type: ignore[misc,assignment]
    DefaultGraph = None  # type: ignore[misc,assignment]
    Literal = None  # type: ignore[misc,assignment]
    NamedNode = None  # type: ignore[misc,assignment]
    Quad = None  # type: ignore[misc,assignment]
    Store = None  # type: ignore[misc,assignment]

PREDICATE_BASE = "https://petscan.wmcloud.org/ontology/"
ITEM_BASE = "https://petscan.wmcloud.org/psid"
RDF_TYPE_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_INTEGER_IRI = "http://www.w3.org/2001/XMLSchema#integer"
XSD_DOUBLE_IRI = "http://www.w3.org/2001/XMLSchema#double"
XSD_BOOLEAN_IRI = "http://www.w3.org/2001/XMLSchema#boolean"
XSD_DATE_TIME_IRI = "http://www.w3.org/2001/XMLSchema#dateTime"
HTTP_USER_AGENT = "PetscanSparqlEndpoint (https://meta.wikimedia.org/wiki/user:Zache)"

_QUERY_TYPES = {"SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}
_FIELD_NAME_RE = re.compile(r"[^0-9A-Za-z_]+")
_QID_RE = re.compile(r"Q([1-9][0-9]*)", re.IGNORECASE)
_SITE_TOKEN_RE = re.compile(r"^[a-z0-9_-]+$")
_HOST_LABEL_RE = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
_SPARQL_COMMENT_LINE_RE = re.compile(r"(?m)^\s*#.*$")
_SPARQL_PREFIX_PROLOGUE_RE = re.compile(r"(?is)\A\s*PREFIX\s+[A-Za-z][A-Za-z0-9._-]*:\s*<[^>]*>")
_SPARQL_BASE_PROLOGUE_RE = re.compile(r"(?is)\A\s*BASE\s*<[^>]*>")
_SPARQL_QUERY_FORM_RE = re.compile(r"(?is)\A\s*(SELECT|ASK|CONSTRUCT|DESCRIBE)\b")
_SERVICE_CLAUSE_RE = re.compile(
    r"(?is)\bSERVICE\b(?:\s+SILENT\b)?\s*(?:<[^>]+>|[?$][A-Za-z_][A-Za-z0-9_]*|[A-Za-z][A-Za-z0-9_-]*:[^\s{>]*)\s*\{"
)
_MAX_TITLES_PER_MEDIAWIKI_BATCH = 50
_FIELD_RENAMES = {
    "id": "page_id",
}
_LOOKUP_BACKEND_API = "api"
_LOOKUP_BACKEND_TOOLFORGE_SQL = "toolforge_sql"
_PETSCAN_RESERVED_QUERY_PARAMS = {"psid", "format", "query", "refresh"}

_lock_guard = threading.Lock()
_psid_locks = {}  # type: Dict[int, threading.Lock]


class PetscanServiceError(RuntimeError):
    pass


class StructureField(TypedDict):
    source_key: str
    predicate: str
    present_in_rows: int
    primary_type: str
    observed_types: List[str]


class StructureSummary(TypedDict):
    row_count: int
    field_count: int
    fields: List[StructureField]


class StoreMeta(TypedDict):
    psid: int
    records: int
    source_url: str
    source_params: Dict[str, List[str]]
    loaded_at: str
    structure: StructureSummary


class QueryExecution(TypedDict, total=False):
    query_type: str
    result_format: str
    sparql_json: Dict[str, Any]
    ntriples: str
    meta: StoreMeta


@dataclass(frozen=True)
class StoreMetaModel:
    psid: int
    records: int
    source_url: str
    source_params: Dict[str, List[str]]
    loaded_at: str
    structure: StructureSummary

    def to_dict(self) -> StoreMeta:
        return {
            "psid": self.psid,
            "records": self.records,
            "source_url": self.source_url,
            "source_params": self.source_params,
            "loaded_at": self.loaded_at,
            "structure": self.structure,
        }


@dataclass(frozen=True)
class QueryExecutionModel:
    query_type: str
    result_format: str
    meta: StoreMeta
    sparql_json: Optional[Dict[str, Any]] = None
    ntriples: Optional[str] = None

    def to_dict(self) -> QueryExecution:
        payload: QueryExecution = {
            "query_type": self.query_type,
            "result_format": self.result_format,
            "meta": self.meta,
        }
        if self.result_format == "sparql-json":
            payload["sparql_json"] = self.sparql_json if self.sparql_json is not None else {}
        else:
            payload["ntriples"] = self.ntriples if self.ntriples is not None else ""
        return payload


def _ensure_oxigraph() -> None:
    if Store is None:
        raise PetscanServiceError(
            "pyoxigraph is not installed. Install dependencies from requirements.txt first."
        )


def _get_psid_lock(psid: int) -> threading.Lock:
    with _lock_guard:
        if psid not in _psid_locks:
            _psid_locks[psid] = threading.Lock()
        return _psid_locks[psid]


def _store_root() -> Path:
    path = Path(settings.OXIGRAPH_BASE_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _store_path(psid: int) -> Path:
    return _store_root() / str(psid)


def _meta_path(psid: int) -> Path:
    return _store_path(psid) / "meta.json"


def _normalize_petscan_params(params: Optional[Mapping[str, Any]]) -> Dict[str, List[str]]:
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
                text_value = str(item).strip()
                if text_value:
                    values.append(text_value)
        else:
            text_value = str(raw_value).strip()
            if text_value:
                values.append(text_value)

        if values:
            normalized[text_key] = values

    return normalized


def _build_petscan_url(psid: int, petscan_params: Optional[Mapping[str, Any]] = None) -> str:
    endpoint = str(settings.PETSCAN_ENDPOINT).rstrip("/")
    normalized_params = _normalize_petscan_params(petscan_params)
    query_pairs = [("psid", str(psid)), ("format", "json")]
    for key in sorted(normalized_params.keys()):
        for value in normalized_params[key]:
            query_pairs.append((key, value))
    query = urlencode(query_pairs)
    return "{}/?{}".format(endpoint, query)


def _fetch_petscan_json(
    psid: int,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], str]:
    source_url = _build_petscan_url(psid, petscan_params=petscan_params)
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
        raise PetscanServiceError("Failed to fetch PetScan data: {}".format(exc)) from exc

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
    keys_of_interest = {
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
    found_keys = set()
    for row in records[:20]:
        found_keys.update(set(row.keys()) & keys_of_interest)
    return len(records) * 10 + len(found_keys)


def _extract_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
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


def _field_name(key: str) -> str:
    canonical = _FIELD_RENAMES.get(key, key)
    cleaned = _FIELD_NAME_RE.sub("_", canonical).strip("_")
    if not cleaned:
        cleaned = "field"
    if cleaned[0].isdigit():
        cleaned = "field_{}".format(cleaned)
    return cleaned


def _predicate_for(key: str):
    return NamedNode(PREDICATE_BASE + _field_name(key))


def _literal_for(value: Any):
    if isinstance(value, bool):
        return Literal("true" if value else "false", datatype=NamedNode(XSD_BOOLEAN_IRI))
    if isinstance(value, int):
        return Literal(str(value), datatype=NamedNode(XSD_INTEGER_IRI))
    if isinstance(value, float):
        return Literal(repr(value), datatype=NamedNode(XSD_DOUBLE_IRI))
    return Literal(str(value))


def _record_identifier(record: Mapping[str, Any], index: int) -> str:
    for key in ("id", "pageid", "title", "qid", "wikidata"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return str(index)


def _record_page_id(record: Mapping[str, Any]) -> Optional[int]:
    for key in ("id", "pageid"):
        value = record.get(key)
        if value is None:
            continue
        try:
            page_id = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if page_id > 0:
            return page_id
    return None


def _is_commons_file_record(record: Mapping[str, Any]) -> bool:
    wiki_value = str(record.get("wiki", "")).strip().lower()
    if wiki_value and wiki_value in {"commonswiki", "commons.wikimedia.org"}:
        return True

    namespace_value = record.get("namespace")
    try:
        namespace = int(namespace_value) if namespace_value is not None else None
    except Exception:
        namespace = None

    nstext = str(record.get("nstext", "")).strip().lower()
    has_image_metadata = any(str(key).startswith("img_") for key in record.keys())

    # PetScan media rows often omit explicit wiki while still representing Commons files.
    if namespace == 6 and nstext == "file" and has_image_metadata:
        return True

    return False


def _item_subject(psid: int, record: Mapping[str, Any], index: int):
    page_id = _record_page_id(record)
    if page_id is not None and _is_commons_file_record(record):
        return NamedNode("https://commons.wikimedia.org/entity/M{}".format(page_id))

    qid = _extract_qid(record)
    if qid is not None:
        return NamedNode("http://www.wikidata.org/entity/{}".format(qid))

    identifier = quote(_record_identifier(record, index), safe="")
    return NamedNode("{}/{}/item/{}".format(ITEM_BASE, psid, identifier))


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


def _build_gil_link_wikidata_map(records: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    link_targets = {}  # type: Dict[str, Tuple[str, int, str, str]]
    site_to_targets = {}  # type: Dict[str, set]
    link_to_qid = {}  # type: Dict[str, str]

    for row in records:
        for link_uri, site, namespace, api_title, db_title in _iter_gil_link_targets(row):
            link_targets[link_uri] = (site, namespace, api_title, db_title)
            direct_qid = _direct_wikidata_qid_for_target(site, namespace, api_title, db_title)
            if direct_qid is not None:
                link_to_qid[link_uri] = direct_qid
                continue
            site_to_targets.setdefault(site, set()).add((namespace, api_title, db_title))

    site_title_to_qid = {}  # type: Dict[Tuple[str, str], str]
    backend = _wikidata_lookup_backend()
    for site, targets in site_to_targets.items():
        ordered_targets = sorted(targets, key=lambda item: (item[0], item[1], item[2]))
        result = _fetch_wikibase_items_for_site(site, ordered_targets, backend=backend)
        for title, qid in result.items():
            normalized_title = _normalize_page_title(title)
            normalized_qid = _normalize_qid(qid)
            if normalized_title and normalized_qid:
                site_title_to_qid[(site, normalized_title)] = normalized_qid

    for link_uri, (site, _namespace, api_title, _db_title) in link_targets.items():
        if link_uri in link_to_qid:
            continue
        resolved_qid = site_title_to_qid.get((site, _normalize_page_title(api_title)))
        if resolved_qid is not None:
            link_to_qid[link_uri] = resolved_qid

    return link_to_qid


def _thumbnail_url(image_name: str, width: int = 320) -> str:
    normalized = image_name.strip().replace(" ", "_")
    encoded = quote(normalized, safe="_-().,:")
    return "https://commons.wikimedia.org/wiki/Special:FilePath/{}?width={}".format(
        encoded,
        width,
    )


def _parse_coordinates(value: Any) -> Optional[Tuple[float, float]]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    parts = re.split(r"\s*[/,;]\s*", text)
    if len(parts) < 2:
        return None
    try:
        lat = float(parts[0])
        lon = float(parts[1])
    except Exception:
        return None

    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def _iter_scalar_fields(
    record: Mapping[str, Any],
    gil_link_wikidata_map: Optional[Mapping[str, str]] = None,
) -> Iterable[Tuple[str, Any]]:
    qid = _extract_qid(record)
    if qid is not None:
        yield "qid", qid
        yield "wikidata_entity", "https://www.wikidata.org/entity/{}".format(qid)

    metadata = record.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}

    image_name = metadata_map.get("image")
    if isinstance(image_name, str) and image_name.strip():
        normalized_image_name = image_name.strip()
        yield "thumbnail_image", _thumbnail_url(normalized_image_name)
        yield "thumbnail_image_file", normalized_image_name

    coordinates_value = metadata_map.get("coordinates")
    parsed_coordinates = _parse_coordinates(coordinates_value)
    if parsed_coordinates is not None:
        lat, lon = parsed_coordinates
        yield "coordinates", str(coordinates_value).strip()
        yield "coordinate_lat", lat
        yield "coordinate_lon", lon

    for key, value in record.items():
        if value is None:
            continue

        if key == "gil" and isinstance(value, str):
            # Keep raw field at item-level; URI link relationships are emitted as dedicated quads.
            yield key, value
            gil_links = _iter_gil_link_uris(record)
            yield "gil_link_count", len(gil_links)
            continue

        if isinstance(value, (str, int, float, bool)):
            yield key, value
            continue
        if isinstance(value, list):
            scalar_values = [
                str(item)
                for item in value
                if isinstance(item, (str, int, float, bool)) and str(item).strip()
            ]
            if scalar_values:
                yield key, "; ".join(scalar_values)


def _value_kind(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return "other"


def _summarize_structure(
    records: Sequence[Mapping[str, Any]],
    gil_link_wikidata_map: Optional[Mapping[str, str]] = None,
) -> StructureSummary:
    field_info = {}  # type: Dict[str, Dict[str, Any]]

    for row in records:
        row_fields = {}  # type: Dict[str, List[Any]]
        for key, value in _iter_scalar_fields(row, gil_link_wikidata_map=gil_link_wikidata_map):
            row_fields.setdefault(key, []).append(value)
        for link_uri, qid in _iter_gil_link_enrichment(
            row,
            gil_link_wikidata_map=gil_link_wikidata_map,
        ):
            row_fields.setdefault("gil_link", []).append(link_uri)
            if qid is not None:
                row_fields.setdefault("gil_link_wikidata_id", []).append(qid)
                row_fields.setdefault("gil_link_wikidata_entity", []).append(
                    "http://www.wikidata.org/entity/{}".format(qid)
                )

        for key, values in row_fields.items():
            info = field_info.setdefault(
                key,
                {
                    "source_key": key,
                    "predicate": PREDICATE_BASE + _field_name(key),
                    "present_in_rows": 0,
                    "type_counts": {},
                },
            )
            info["present_in_rows"] += 1

            type_counts = info["type_counts"]
            for kind in sorted({_value_kind(v) for v in values}):
                type_counts[kind] = int(type_counts.get(kind, 0)) + 1

    fields: List[StructureField] = []
    for key in sorted(field_info.keys()):
        info = field_info[key]
        type_counts = info["type_counts"]
        observed_types = sorted(type_counts.keys())
        primary_type = max(
            observed_types,
            key=lambda kind: (int(type_counts.get(kind, 0)), kind),
        )
        fields.append(
            {
                "source_key": info["source_key"],
                "predicate": info["predicate"],
                "present_in_rows": info["present_in_rows"],
                "primary_type": primary_type,
                "observed_types": observed_types,
            }
        )

    return {
        "row_count": len(records),
        "field_count": len(fields),
        "fields": fields,
    }


def _build_store(
    psid: int,
    records: Sequence[Mapping[str, Any]],
    source_url: str,
    source_params: Optional[Mapping[str, Any]] = None,
) -> StoreMeta:
    store_path = _store_path(psid)
    if store_path.exists():
        shutil.rmtree(store_path)
    store_path.mkdir(parents=True, exist_ok=True)

    store = Store(str(store_path))

    page_class = NamedNode(PREDICATE_BASE + "Page")
    rdf_type = NamedNode(RDF_TYPE_IRI)
    psid_predicate = NamedNode(PREDICATE_BASE + "psid")
    position_predicate = NamedNode(PREDICATE_BASE + "position")
    loaded_at_predicate = NamedNode(PREDICATE_BASE + "loadedAt")
    gil_link_predicate = NamedNode(PREDICATE_BASE + "gil_link")
    gil_link_wikidata_id_predicate = NamedNode(PREDICATE_BASE + "gil_link_wikidata_id")
    gil_link_wikidata_entity_predicate = NamedNode(PREDICATE_BASE + "gil_link_wikidata_entity")
    gil_link_wikidata_map = _build_gil_link_wikidata_map(records)

    loaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for index, row in enumerate(records):
        subject = _item_subject(psid, row, index)
        store.add(Quad(subject, rdf_type, page_class, DefaultGraph()))
        store.add(
            Quad(
                subject,
                psid_predicate,
                Literal(str(psid), datatype=NamedNode(XSD_INTEGER_IRI)),
                DefaultGraph(),
            )
        )
        store.add(
            Quad(
                subject,
                position_predicate,
                Literal(str(index), datatype=NamedNode(XSD_INTEGER_IRI)),
                DefaultGraph(),
            )
        )
        store.add(
            Quad(
                subject,
                loaded_at_predicate,
                Literal(loaded_at, datatype=NamedNode(XSD_DATE_TIME_IRI)),
                DefaultGraph(),
            )
        )

        for key, value in _iter_scalar_fields(row, gil_link_wikidata_map=gil_link_wikidata_map):
            predicate = _predicate_for(key)
            literal = _literal_for(value)
            store.add(Quad(subject, predicate, literal, DefaultGraph()))

        for link_uri, qid in _iter_gil_link_enrichment(
            row,
            gil_link_wikidata_map=gil_link_wikidata_map,
        ):
            link_node = NamedNode(link_uri)
            store.add(Quad(subject, gil_link_predicate, link_node, DefaultGraph()))
            if qid is not None:
                store.add(
                    Quad(
                        link_node,
                        gil_link_wikidata_id_predicate,
                        Literal(qid),
                        DefaultGraph(),
                    )
                )
                store.add(
                    Quad(
                        link_node,
                        gil_link_wikidata_entity_predicate,
                        NamedNode("http://www.wikidata.org/entity/{}".format(qid)),
                        DefaultGraph(),
                    )
                )

    meta_model = StoreMetaModel(
        psid=psid,
        records=len(records),
        source_url=source_url,
        source_params=_normalize_petscan_params(source_params),
        loaded_at=loaded_at,
        structure=_summarize_structure(records, gil_link_wikidata_map=gil_link_wikidata_map),
    )
    meta = meta_model.to_dict()
    _meta_path(psid).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta


def _read_meta(psid: int) -> Dict[str, Any]:
    path = _meta_path(psid)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _has_existing_store(psid: int) -> bool:
    return _meta_path(psid).exists()


def _meta_has_matching_source_params(meta: Mapping[str, Any], petscan_params: Mapping[str, Any]) -> bool:
    expected = _normalize_petscan_params(petscan_params)
    actual = _normalize_petscan_params(meta.get("source_params") if isinstance(meta, Mapping) else {})
    return expected == actual


def _meta_is_usable(meta: Mapping[str, Any], psid: int) -> bool:
    if not isinstance(meta, Mapping) or not meta:
        return False

    meta_psid = meta.get("psid")
    if not isinstance(meta_psid, int) or isinstance(meta_psid, bool) or meta_psid != psid:
        return False

    records = meta.get("records")
    if not isinstance(records, int) or isinstance(records, bool) or records < 0:
        return False

    source_url = meta.get("source_url")
    if not isinstance(source_url, str) or not source_url.strip():
        return False

    loaded_at = meta.get("loaded_at")
    if not isinstance(loaded_at, str) or not loaded_at.strip():
        return False

    source_params = meta.get("source_params", {})
    if not isinstance(source_params, Mapping):
        return False

    return True


def ensure_loaded(
    psid: int,
    refresh: bool = False,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> StoreMeta:
    _ensure_oxigraph()
    lock = _get_psid_lock(psid)
    normalized_params = _normalize_petscan_params(petscan_params)

    with lock:
        if not refresh and _has_existing_store(psid):
            meta = _read_meta(psid)
            if _meta_is_usable(meta, psid) and _meta_has_matching_source_params(meta, normalized_params):
                return cast(StoreMeta, meta)

        payload, source_url = _fetch_petscan_json(psid, petscan_params=normalized_params)
        records = _extract_records(payload)
        if not records:
            raise PetscanServiceError("PetScan returned zero rows for psid {}.".format(psid))

        return _build_store(psid, records, source_url, source_params=normalized_params)


def _query_type(query: str) -> str:
    remaining = _SPARQL_COMMENT_LINE_RE.sub("", query)

    # Strip SPARQL prologue declarations to avoid matching query-form keywords
    # inside prefixed names (for example `PREFIX select: <...>`).
    while True:
        prefix_match = _SPARQL_PREFIX_PROLOGUE_RE.match(remaining)
        if prefix_match is not None:
            remaining = remaining[prefix_match.end() :]
            continue

        base_match = _SPARQL_BASE_PROLOGUE_RE.match(remaining)
        if base_match is not None:
            remaining = remaining[base_match.end() :]
            continue

        break

    form_match = _SPARQL_QUERY_FORM_RE.match(remaining)
    if form_match is not None:
        query_type = str(form_match.group(1)).upper()
        if query_type in _QUERY_TYPES:
            return query_type

    raise PetscanServiceError("SPARQL query must contain SELECT, ASK, CONSTRUCT, or DESCRIBE.")


def _contains_service_clause(query: str) -> bool:
    clean_query = re.sub(r"(?m)^\s*#.*$", "", query)
    return bool(_SERVICE_CLAUSE_RE.search(clean_query))


def _variable_name(value: Any) -> str:
    text = str(value)
    return text[1:] if text.startswith("?") else text


def _is_named_node(term: Any) -> bool:
    return term is not None and term.__class__.__name__ == "NamedNode"


def _is_blank_node(term: Any) -> bool:
    return term is not None and term.__class__.__name__ == "BlankNode"


def _is_literal(term: Any) -> bool:
    return term is not None and term.__class__.__name__ == "Literal"


def _term_value(term: Any) -> str:
    value = getattr(term, "value", None)
    return str(value if value is not None else term)


def _term_to_sparql_binding(term: Any) -> Dict[str, Any]:
    if _is_named_node(term):
        return {"type": "uri", "value": _term_value(term)}

    if _is_blank_node(term):
        raw = _term_value(term)
        return {
            "type": "bnode",
            "value": raw[2:] if raw.startswith("_:") else raw,
        }

    if _is_literal(term):
        data = {"type": "literal", "value": _term_value(term)}
        language = getattr(term, "language", None)
        datatype = getattr(term, "datatype", None)
        if language:
            data["xml:lang"] = str(language)
        elif datatype:
            datatype_iri = _term_value(datatype)
            if datatype_iri != "http://www.w3.org/2001/XMLSchema#string":
                data["datatype"] = datatype_iri
        return data

    return {"type": "literal", "value": str(term)}


def _term_to_ntriples(term: Any) -> str:
    if _is_named_node(term):
        return "<{}>".format(_term_value(term))

    if _is_blank_node(term):
        text = _term_value(term)
        return text if text.startswith("_:") else "_:{}".format(text)

    if _is_literal(term):
        escaped = (
            _term_value(term)
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace('"', '\\"')
        )
        language = getattr(term, "language", None)
        datatype = getattr(term, "datatype", None)
        if language:
            return '"{}"@{}'.format(escaped, language)
        if datatype:
            return '"{}"^^<{}>'.format(escaped, _term_value(datatype))
        return '"{}"'.format(escaped)

    return '"{}"'.format(str(term).replace('"', '\\"'))


def _serialize_select(result: Any) -> Dict[str, Any]:
    variables = [_variable_name(v) for v in getattr(result, "variables", [])]
    rows = []  # type: List[Dict[str, Any]]

    for solution in result:
        bindings = {}  # type: Dict[str, Any]
        items = []
        if hasattr(solution, "items"):
            items = list(solution.items())

        if items:
            for variable, term in items:
                bindings[_variable_name(variable)] = _term_to_sparql_binding(term)
        else:
            for variable in variables:
                try:
                    term = solution[variable]
                except (KeyError, TypeError, IndexError):
                    continue
                bindings[variable] = _term_to_sparql_binding(term)

        rows.append(bindings)

    return {
        "head": {"vars": variables},
        "results": {"bindings": rows},
    }


def _serialize_ask(result: Any) -> Dict[str, Any]:
    if isinstance(result, bool):
        return {"head": {}, "boolean": result}

    if result is not None and result.__class__.__name__ == "QueryBoolean":
        return {"head": {}, "boolean": bool(result)}

    # Some implementations expose ASK as iterable with one row; fallback handles that.
    try:
        first = next(iter(result))
    except Exception:
        first = None

    if isinstance(first, bool):
        return {"head": {}, "boolean": first}

    raise PetscanServiceError("ASK result could not be serialized.")


def _serialize_graph(result: Any) -> str:
    lines = []  # type: List[str]

    for triple in result:
        subject = getattr(triple, "subject", None)
        predicate = getattr(triple, "predicate", None)
        object_term = getattr(triple, "object", None)

        if subject is None or predicate is None or object_term is None:
            if isinstance(triple, tuple) and len(triple) == 3:
                subject, predicate, object_term = triple
            else:
                continue

        lines.append(
            "{} {} {} .".format(
                _term_to_ntriples(subject),
                _term_to_ntriples(predicate),
                _term_to_ntriples(object_term),
            )
        )

    return "\n".join(lines) + ("\n" if lines else "")


def execute_query(
    psid: int,
    query: str,
    refresh: bool = False,
    petscan_params: Optional[Mapping[str, Any]] = None,
) -> QueryExecution:
    _ensure_oxigraph()
    if _contains_service_clause(query):
        raise ValueError("SERVICE clauses are not allowed in this endpoint.")

    meta = ensure_loaded(psid, refresh=refresh, petscan_params=petscan_params)

    store = Store(str(_store_path(psid)))
    qtype = _query_type(query)

    try:
        raw_result = store.query(query)
    except Exception as exc:
        raise PetscanServiceError("SPARQL query failed: {}".format(exc)) from exc

    if qtype == "SELECT":
        result = QueryExecutionModel(
            query_type=qtype,
            result_format="sparql-json",
            sparql_json=_serialize_select(raw_result),
            meta=meta,
        )
        return result.to_dict()

    if qtype == "ASK":
        result = QueryExecutionModel(
            query_type=qtype,
            result_format="sparql-json",
            sparql_json=_serialize_ask(raw_result),
            meta=meta,
        )
        return result.to_dict()

    result = QueryExecutionModel(
        query_type=qtype,
        result_format="n-triples",
        ntriples=_serialize_graph(raw_result),
        meta=meta,
    )
    return result.to_dict()
