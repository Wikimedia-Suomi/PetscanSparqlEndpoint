import os
import re
from time import perf_counter
from types import ModuleType
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast

from .normalization import normalize_page_title, normalize_qid

_pymysql_module: Optional[ModuleType]
try:
    import pymysql as _pymysql_module
except ImportError:  # pragma: no cover - optional dependency
    _pymysql_module = None

pymysql = cast(Any, _pymysql_module)
_SITE_TOKEN_RE = re.compile(r"^[a-z0-9_-]+$")
_REPLICA_DOMAIN_SUFFIX = "web.db.svc.wikimedia.cloud"


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


def fetch_wikibase_items_for_site_sql(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
    timeout_seconds: int,
    replica_cnf: Optional[str] = None,
) -> Dict[str, str]:
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
        "SELECT p.page_namespace, p.page_title, pp.pp_value "
        "FROM page AS p "
        "LEFT JOIN page_props AS pp "
        "ON pp.pp_page = p.page_id AND pp.pp_propname = %s "
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
        print(
            "[wikimedia-sql] ERROR {:.1f} ms site={} db={}".format(
                elapsed_ms,
                site,
                replica_db,
            ),
            flush=True,
        )
        print("[wikimedia-sql] ERROR_DETAILS {}".format(exc), flush=True)
        return {}
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception as exc:
                print("[wikimedia-sql] CLOSE_ERROR {}".format(exc), flush=True)

    elapsed_ms = (perf_counter() - started_at) * 1000.0
    print(
        "[wikimedia-sql] DONE {:.1f} ms site={} db={} rows={}".format(
            elapsed_ms,
            site,
            replica_db,
            len(rows),
        ),
        flush=True,
    )

    qid_by_pair = {}  # type: Dict[Tuple[int, str], str]
    for row in rows:
        if not isinstance(row, (tuple, list)) or len(row) < 3:
            continue
        namespace = int(row[0])
        db_title = _normalize_db_title(row[1])
        qid = normalize_qid(row[2])
        if db_title and qid is not None:
            qid_by_pair[(namespace, db_title)] = qid

    resolved = {}  # type: Dict[str, str]
    for namespace, api_title, db_title in targets:
        key = (int(namespace), _normalize_db_title(db_title))
        qid = qid_by_pair.get(key)
        normalized_api_title = normalize_page_title(api_title)
        if normalized_api_title and qid is not None:
            resolved[normalized_api_title] = qid
    return resolved
