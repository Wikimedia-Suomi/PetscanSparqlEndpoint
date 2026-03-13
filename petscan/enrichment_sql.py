import os
import re
from time import perf_counter
from typing import Dict, Optional, Sequence, Tuple

try:
    import pymysql
except ImportError:  # pragma: no cover - optional dependency
    pymysql = None  # type: ignore[assignment]


_QID_RE = re.compile(r"Q([1-9][0-9]*)", re.IGNORECASE)


def _normalize_page_title(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.lstrip(":").replace(" ", "_")


def _normalize_qid(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _QID_RE.search(text)
    if not match:
        return None
    return "Q{}".format(match.group(1))


def fetch_wikibase_items_for_site_sql(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
    timeout_seconds: int,
    replica_host: str,
    replica_cnf: str = "",
    replica_user: str = "",
    replica_password: str = "",
) -> Dict[str, str]:
    if not targets or pymysql is None:
        return {}

    replica_db = "{}_p".format(site)
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
    if replica_user:
        connect_kwargs["user"] = replica_user
    if replica_password:
        connect_kwargs["password"] = replica_password

    unique_pairs = []
    seen_pairs = set()
    for namespace, _api_title, db_title in targets:
        normalized_db_title = _normalize_page_title(db_title)
        key = (int(namespace), normalized_db_title)
        if not normalized_db_title or key in seen_pairs:
            continue
        seen_pairs.add(key)
        unique_pairs.append(key)

    if not unique_pairs:
        return {}

    placeholders = ", ".join(["(%s, %s)"] * len(unique_pairs))
    sql = (
        "SELECT p.page_namespace, p.page_title, pp.pp_value "
        "FROM page AS p "
        "LEFT JOIN page_props AS pp "
        "ON pp.pp_page = p.page_id AND pp.pp_propname = %s "
        "WHERE (p.page_namespace, p.page_title) IN ({})"
    ).format(placeholders)
    params = ["wikibase_item"]
    for namespace, db_title in unique_pairs:
        params.extend([namespace, db_title])

    started_at = perf_counter()
    connection = None
    try:
        connection = pymysql.connect(**connect_kwargs)
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
            except Exception:
                pass

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
        db_title = _normalize_page_title(row[1])
        qid = _normalize_qid(row[2])
        if db_title and qid is not None:
            qid_by_pair[(namespace, db_title)] = qid

    resolved = {}  # type: Dict[str, str]
    for namespace, api_title, db_title in targets:
        key = (int(namespace), _normalize_page_title(db_title))
        qid = qid_by_pair.get(key)
        normalized_api_title = _normalize_page_title(api_title)
        if normalized_api_title and qid is not None:
            resolved[normalized_api_title] = qid
    return resolved
