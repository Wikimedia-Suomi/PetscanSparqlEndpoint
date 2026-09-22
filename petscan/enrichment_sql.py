import os
import re
from time import perf_counter
from types import ModuleType
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence, Tuple, cast

from .normalization import normalize_page_title, normalize_qid
from .service_errors import GilLinkEnrichmentError, PetscanServiceError

_REPLICA_ENRICHMENT_PUBLIC_MESSAGE = "Failed to enrich linked pages from the replica database."
_USER_REGISTRATION_PUBLIC_MESSAGE = (
    "Failed to enrich file uploader registration data from CentralAuth."
)
_pymysql_module: Optional[ModuleType]
try:
    import pymysql as _pymysql_module
except ImportError:  # pragma: no cover - optional dependency
    _pymysql_module = None

pymysql = cast(Any, _pymysql_module)
_SITE_TOKEN_RE = re.compile(r"^[a-z0-9_-]+$")
_REPLICA_DOMAIN_SUFFIX = "web.db.svc.wikimedia.cloud"
_GLOBAL_USER_SQL_BATCH_SIZE = 500


def _chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(values), size):
        yield list(values[index : index + size])


def _normalize_text(value: object) -> str:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    return str(value or "").strip()


def fetch_global_user_registrations_sql(
    user_names: Sequence[str],
    timeout_seconds: int,
    replica_cnf: Optional[str] = None,
) -> Dict[str, str]:
    if not user_names:
        return {}
    if pymysql is None:
        raise PetscanServiceError(
            "PyMySQL is required for CentralAuth registration enrichment.",
            public_message=_USER_REGISTRATION_PUBLIC_MESSAGE,
        )

    connect_kwargs = {
        "host": "centralauth.{}".format(_REPLICA_DOMAIN_SUFFIX),
        "database": "centralauth_p",
        "charset": "utf8mb4",
        "connect_timeout": timeout_seconds,
        "read_timeout": timeout_seconds,
        "write_timeout": timeout_seconds,
        "autocommit": True,
    }
    if replica_cnf:
        connect_kwargs["read_default_file"] = os.path.expanduser(os.path.expandvars(replica_cnf))

    registrations: Dict[str, str] = {}
    connection = None
    try:
        connection = pymysql.connect(**cast(Any, connect_kwargs))
        with connection.cursor() as cursor:
            for batch in _chunked(user_names, _GLOBAL_USER_SQL_BATCH_SIZE):
                placeholders = ", ".join(["%s"] * len(batch))
                sql = (  # nosec B608
                    "SELECT gu_name, gu_registration "
                    "FROM globaluser "
                    "WHERE gu_name IN ({})"
                ).format(placeholders)
                cursor.execute(sql, list(batch))
                for row in cursor.fetchall():
                    if not isinstance(row, (tuple, list)) or len(row) < 2:
                        continue
                    user_name = _normalize_text(row[0])
                    registration = _normalize_text(row[1])
                    if user_name and registration:
                        registrations[user_name] = registration
    except Exception as exc:
        raise PetscanServiceError(
            "CentralAuth globaluser SQL query failed: {}".format(exc),
            public_message=_USER_REGISTRATION_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    return registrations


def _normalize_db_title(value: object) -> str:
    """Normalize replica page_title values that may arrive as bytes (VARBINARY)."""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    return normalize_page_title(value)


def _replica_host_for_site(site: str) -> Optional[str]:
    normalized_site = str(site or "").strip().lower()
    if not normalized_site or not _SITE_TOKEN_RE.fullmatch(normalized_site):
        return None
    return "{}.{}".format(normalized_site, _REPLICA_DOMAIN_SUFFIX)


def _normalize_revision_timestamp(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    text = str(value).strip()
    if not text:
        return None
    return text


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


def fetch_wikibase_items_for_site_sql(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
    timeout_seconds: int,
    replica_cnf: Optional[str] = None,
    lookup_stats: Optional[MutableMapping[str, float]] = None,
) -> Dict[str, Dict[str, Any]]:
    if not targets or pymysql is None:
        return {}

    replica_host = _replica_host_for_site(site)
    if replica_host is None:
        return {}
    replica_db = "{}_p".format(str(site or "").strip().lower())
    connect_kwargs = {
        "host": replica_host,
        "database": replica_db,
        "charset": "utf8mb4",
        "connect_timeout": timeout_seconds,
        "read_timeout": timeout_seconds,
        "write_timeout": timeout_seconds,
        "autocommit": True,
    }
    if replica_cnf:
        connect_kwargs["read_default_file"] = os.path.expanduser(os.path.expandvars(replica_cnf))

    unique_pairs = []
    seen_pairs = set()
    for namespace, _api_title, db_title in targets:
        normalized_db_title = _normalize_db_title(db_title)
        key = (int(namespace), normalized_db_title)
        if not normalized_db_title or key in seen_pairs:
            continue
        seen_pairs.add(key)
        unique_pairs.append(key)

    if not unique_pairs:
        return {}

    placeholders = ", ".join(["(%s, %s)"] * len(unique_pairs))
    sql = (  # nosec B608
        "SELECT p.page_namespace, p.page_title, pp.pp_value, p.page_len, r.rev_timestamp "
        "FROM page AS p "
        "LEFT JOIN page_props AS pp "
        "ON pp.pp_page = p.page_id AND pp.pp_propname = %s "
        "LEFT JOIN revision AS r "
        "ON r.rev_id = p.page_latest "
        "WHERE (p.page_namespace, p.page_title) IN ({})"
    ).format(placeholders)
    params: List[Any] = ["wikibase_item"]
    for namespace, db_title in unique_pairs:
        params.extend([namespace, db_title])

    started_at = perf_counter()
    connection = None
    try:
        connection = pymysql.connect(**cast(Any, connect_kwargs))
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    except Exception as exc:
        elapsed_ms = (perf_counter() - started_at) * 1000.0
        if lookup_stats is not None:
            lookup_stats["sql_calls"] = float(lookup_stats.get("sql_calls", 0.0)) + 1.0
            lookup_stats["sql_ms_total"] = float(lookup_stats.get("sql_ms_total", 0.0)) + elapsed_ms
        raise GilLinkEnrichmentError(
            "Wikibase enrichment SQL query failed for site {}: {}".format(site, exc),
            public_message=_REPLICA_ENRICHMENT_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    elapsed_ms = (perf_counter() - started_at) * 1000.0
    if lookup_stats is not None:
        lookup_stats["sql_calls"] = float(lookup_stats.get("sql_calls", 0.0)) + 1.0
        lookup_stats["sql_ms_total"] = float(lookup_stats.get("sql_ms_total", 0.0)) + elapsed_ms

    enrichment_by_pair = {}  # type: Dict[Tuple[int, str], Dict[str, Any]]
    for row in rows:
        if not isinstance(row, (tuple, list)) or len(row) < 5:
            continue
        namespace = int(row[0])
        db_title = _normalize_db_title(row[1])
        qid = normalize_qid(row[2])
        page_len = _normalize_page_len(row[3])
        rev_timestamp = _normalize_revision_timestamp(row[4])
        if db_title:
            enrichment_by_pair[(namespace, db_title)] = {
                "wikidata_id": qid,
                "page_len": page_len,
                "rev_timestamp": rev_timestamp,
            }

    resolved = {}  # type: Dict[str, Dict[str, Any]]
    for namespace, api_title, db_title in targets:
        key = (int(namespace), _normalize_db_title(db_title))
        enrichment = enrichment_by_pair.get(key)
        normalized_api_title = normalize_page_title(api_title)
        if normalized_api_title and enrichment is not None:
            resolved[normalized_api_title] = enrichment
    return resolved
