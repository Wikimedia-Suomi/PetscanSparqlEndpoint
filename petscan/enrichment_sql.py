import os
import re
from time import perf_counter
from types import ModuleType
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)

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
_GIL_CATEGORY_SQL_BATCH_SIZE = 500


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
        "SELECT p.page_namespace, p.page_title, p.page_id, pp.pp_value, "
        "p.page_len, r.rev_timestamp "
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
        if not isinstance(row, (tuple, list)) or len(row) < 6:
            continue
        namespace = int(row[0])
        db_title = _normalize_db_title(row[1])
        try:
            page_id = int(row[2])
        except (TypeError, ValueError):
            page_id = 0
        qid = normalize_qid(row[3])
        page_len = _normalize_page_len(row[4])
        rev_timestamp = _normalize_revision_timestamp(row[5])
        if db_title:
            enrichment_by_pair[(namespace, db_title)] = {
                "wikidata_id": qid,
                "page_len": page_len,
                "rev_timestamp": rev_timestamp,
            }
            if page_id > 0:
                enrichment_by_pair[(namespace, db_title)]["page_id"] = page_id

    resolved = {}  # type: Dict[str, Dict[str, Any]]
    for namespace, api_title, db_title in targets:
        key = (int(namespace), _normalize_db_title(db_title))
        enrichment = enrichment_by_pair.get(key)
        normalized_api_title = normalize_page_title(api_title)
        if normalized_api_title and enrichment is not None:
            resolved[normalized_api_title] = enrichment
    return resolved


def _categorylinks_replica_host_for_site(site: str) -> Optional[str]:
    normalized_site = str(site or "").strip().lower()
    if normalized_site == "commonswiki":
        return "links.commonswiki.{}".format(_REPLICA_DOMAIN_SUFFIX)
    return _replica_host_for_site(normalized_site)


def _replica_connect_kwargs(
    *,
    host: str,
    database: str,
    timeout_seconds: int,
    replica_cnf: Optional[str],
) -> Dict[str, Any]:
    connect_kwargs: Dict[str, Any] = {
        "host": host,
        "database": database,
        "charset": "utf8mb4",
        "connect_timeout": timeout_seconds,
        "read_timeout": timeout_seconds,
        "write_timeout": timeout_seconds,
        "autocommit": True,
    }
    if replica_cnf:
        connect_kwargs["read_default_file"] = os.path.expanduser(
            os.path.expandvars(replica_cnf)
        )
    return connect_kwargs


def fetch_page_categories_with_wikidata_sql(
    site: str,
    targets: Sequence[Tuple[int, str, str]],
    timeout_seconds: int,
    replica_cnf: Optional[str] = None,
    lookup_stats: Optional[MutableMapping[str, float]] = None,
    page_ids_by_title: Optional[Mapping[str, int]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch categories, hidden-category flags, and Wikidata QIDs from replicas."""
    if not targets or pymysql is None:
        return {}

    normalized_site = str(site or "").strip().lower()
    core_host = _replica_host_for_site(normalized_site)
    categorylinks_host = _categorylinks_replica_host_for_site(normalized_site)
    if core_host is None or categorylinks_host is None:
        return {}
    replica_db = "{}_p".format(normalized_site)

    normalized_page_ids_by_title: Dict[str, int] = {}
    if page_ids_by_title:
        for raw_title, raw_page_id in page_ids_by_title.items():
            normalized_title = normalize_page_title(raw_title)
            try:
                parsed_page_id = int(raw_page_id)
            except (TypeError, ValueError):
                continue
            if normalized_title and parsed_page_id > 0:
                normalized_page_ids_by_title[normalized_title] = parsed_page_id

    unique_page_ids: List[int] = []
    seen_page_ids: set[int] = set()
    unique_pairs: List[Tuple[int, str]] = []
    seen_pairs: set[Tuple[int, str]] = set()
    for namespace, api_title, db_title in targets:
        normalized_api_title = normalize_page_title(api_title)
        target_page_id = normalized_page_ids_by_title.get(normalized_api_title)
        if target_page_id is not None:
            if target_page_id not in seen_page_ids:
                seen_page_ids.add(target_page_id)
                unique_page_ids.append(target_page_id)
            continue
        normalized_db_title = _normalize_db_title(db_title)
        pair = (int(namespace), normalized_db_title)
        if not normalized_db_title or pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        unique_pairs.append(pair)
    if not unique_page_ids and not unique_pairs:
        return {}

    category_property_rows: List[Any] = []
    categories_by_page_id: Dict[int, List[Tuple[str, Optional[int]]]] = {}
    categories_by_pair: Dict[Tuple[int, str], List[Tuple[str, Optional[int]]]] = {}
    seen_category_titles_by_page_id: Dict[int, set[str]] = {}
    seen_category_titles_by_pair: Dict[Tuple[int, str], set[str]] = {}
    category_connection = None
    core_connection = None
    started_at = perf_counter()

    def _fetch_category_property_rows(cursor: Any, category_page_ids: Sequence[int]) -> None:
        for index in range(0, len(category_page_ids), _GIL_CATEGORY_SQL_BATCH_SIZE):
            batch = list(category_page_ids[index : index + _GIL_CATEGORY_SQL_BATCH_SIZE])
            placeholders = ", ".join(["%s"] * len(batch))
            sql = (  # nosec B608
                "SELECT pp.pp_page, pp.pp_propname, pp.pp_value "
                "FROM page_props AS pp "
                "WHERE pp.pp_propname IN (%s, %s) "
                "AND pp.pp_page IN ({})"
            ).format(placeholders)
            cursor.execute(sql, ["wikibase_item", "hiddencat", *batch])
            category_property_rows.extend(cursor.fetchall())

    try:
        category_connection = pymysql.connect(
            **cast(
                Any,
                _replica_connect_kwargs(
                    host=categorylinks_host,
                    database=replica_db,
                    timeout_seconds=timeout_seconds,
                    replica_cnf=replica_cnf,
                ),
            )
        )
        with category_connection.cursor() as cursor:
            if unique_page_ids:
                placeholders = ", ".join(["%s"] * len(unique_page_ids))
                sql = (  # nosec B608
                    "SELECT cl.cl_from, cl.cl_target_id "
                    "FROM categorylinks AS cl "
                    "WHERE cl.cl_from IN ({})"
                ).format(placeholders)
                cursor.execute(sql, unique_page_ids)

                category_target_ids_by_page_id: Dict[int, List[int]] = {}
                unique_category_target_ids: List[int] = []
                seen_category_target_ids: set[int] = set()
                for row in cursor.fetchall():
                    if not isinstance(row, (tuple, list)) or len(row) < 2:
                        continue
                    try:
                        source_page_id = int(row[0])
                        category_target_id = int(row[1])
                    except (TypeError, ValueError):
                        continue
                    if source_page_id <= 0 or category_target_id <= 0:
                        continue
                    category_target_ids_by_page_id.setdefault(source_page_id, []).append(
                        category_target_id
                    )
                    if category_target_id not in seen_category_target_ids:
                        seen_category_target_ids.add(category_target_id)
                        unique_category_target_ids.append(category_target_id)

                categories_by_target_id: Dict[int, Tuple[str, Optional[int]]] = {}
                for index in range(
                    0,
                    len(unique_category_target_ids),
                    _GIL_CATEGORY_SQL_BATCH_SIZE,
                ):
                    target_id_batch = unique_category_target_ids[
                        index : index + _GIL_CATEGORY_SQL_BATCH_SIZE
                    ]
                    placeholders = ", ".join(["%s"] * len(target_id_batch))
                    sql = (  # nosec B608
                        "SELECT lt.lt_id, lt.lt_title, category_page.page_id "
                        "FROM linktarget AS lt "
                        "LEFT JOIN page AS category_page "
                        "ON category_page.page_namespace = 14 "
                        "AND category_page.page_title = lt.lt_title "
                        "WHERE lt.lt_namespace = 14 "
                        "AND lt.lt_id IN ({})"
                    ).format(placeholders)
                    cursor.execute(sql, target_id_batch)
                    for row in cursor.fetchall():
                        if not isinstance(row, (tuple, list)) or len(row) < 3:
                            continue
                        try:
                            category_target_id = int(row[0])
                        except (TypeError, ValueError):
                            continue
                        category_title = _normalize_db_title(row[1])
                        try:
                            category_page_id = (
                                int(row[2]) if row[2] is not None else None
                            )
                        except (TypeError, ValueError):
                            category_page_id = None
                        if category_target_id <= 0 or not category_title:
                            continue
                        categories_by_target_id[category_target_id] = (
                            category_title,
                            category_page_id,
                        )

                for source_page_id, category_target_ids in (
                    category_target_ids_by_page_id.items()
                ):
                    page_categories = categories_by_page_id.setdefault(source_page_id, [])
                    seen_category_titles = seen_category_titles_by_page_id.setdefault(
                        source_page_id,
                        set(),
                    )
                    for category_target_id in category_target_ids:
                        category = categories_by_target_id.get(category_target_id)
                        if category is None:
                            continue
                        category_title, category_page_id = category
                        if category_title in seen_category_titles:
                            continue
                        seen_category_titles.add(category_title)
                        page_categories.append((category_title, category_page_id))

            for index in range(0, len(unique_pairs), _GIL_CATEGORY_SQL_BATCH_SIZE):
                pair_batch = unique_pairs[index : index + _GIL_CATEGORY_SQL_BATCH_SIZE]
                placeholders = ", ".join(["(%s, %s)"] * len(pair_batch))
                sql = (  # nosec B608
                    "SELECT p.page_namespace, p.page_title, lt.lt_title, "
                    "category_page.page_id "
                    "FROM page AS p "
                    "JOIN categorylinks AS cl ON cl.cl_from = p.page_id "
                    "JOIN linktarget AS lt ON lt.lt_id = cl.cl_target_id "
                    "LEFT JOIN page AS category_page "
                    "ON category_page.page_namespace = 14 "
                    "AND category_page.page_title = lt.lt_title "
                    "WHERE lt.lt_namespace = 14 "
                    "AND (p.page_namespace, p.page_title) IN ({})"
                ).format(placeholders)
                params: List[Any] = []
                for namespace, db_title in pair_batch:
                    params.extend([namespace, db_title])
                cursor.execute(sql, params)
                for row in cursor.fetchall():
                    if not isinstance(row, (tuple, list)) or len(row) < 4:
                        continue
                    pair = (int(row[0]), _normalize_db_title(row[1]))
                    category_title = _normalize_db_title(row[2])
                    try:
                        category_page_id = int(row[3]) if row[3] is not None else None
                    except (TypeError, ValueError):
                        category_page_id = None
                    if not pair[1] or not category_title:
                        continue
                    pair_categories = categories_by_pair.setdefault(pair, [])
                    seen_category_titles = seen_category_titles_by_pair.setdefault(pair, set())
                    if category_title in seen_category_titles:
                        continue
                    seen_category_titles.add(category_title)
                    pair_categories.append((category_title, category_page_id))

            category_page_ids = sorted(
                {
                    category_page_id
                    for page_categories in categories_by_page_id.values()
                    for _category_title, category_page_id in page_categories
                    if category_page_id is not None and category_page_id > 0
                }
                | {
                    category_page_id
                    for pair_categories in categories_by_pair.values()
                    for _category_title, category_page_id in pair_categories
                    if category_page_id is not None and category_page_id > 0
                }
            )
            if normalized_site != "commonswiki" and category_page_ids:
                _fetch_category_property_rows(cursor, category_page_ids)

        if normalized_site == "commonswiki" and category_page_ids:
            core_connection = pymysql.connect(
                **cast(
                    Any,
                    _replica_connect_kwargs(
                        host=core_host,
                        database=replica_db,
                        timeout_seconds=timeout_seconds,
                        replica_cnf=replica_cnf,
                    ),
                )
            )
            with core_connection.cursor() as cursor:
                _fetch_category_property_rows(cursor, category_page_ids)
    except Exception as exc:
        elapsed_ms = (perf_counter() - started_at) * 1000.0
        if lookup_stats is not None:
            lookup_stats["sql_calls"] = float(lookup_stats.get("sql_calls", 0.0)) + 1.0
            lookup_stats["sql_ms_total"] = (
                float(lookup_stats.get("sql_ms_total", 0.0)) + elapsed_ms
            )
        raise GilLinkEnrichmentError(
            "GIL category SQL query failed for site {}: {}".format(site, exc),
            public_message=_REPLICA_ENRICHMENT_PUBLIC_MESSAGE,
        ) from exc
    finally:
        if core_connection is not None:
            core_connection.close()
        if category_connection is not None:
            category_connection.close()

    elapsed_ms = (perf_counter() - started_at) * 1000.0
    if lookup_stats is not None:
        lookup_stats["sql_calls"] = float(lookup_stats.get("sql_calls", 0.0)) + 1.0
        lookup_stats["sql_ms_total"] = float(lookup_stats.get("sql_ms_total", 0.0)) + elapsed_ms

    qid_by_category_page_id: Dict[int, Optional[str]] = {}
    hidden_category_page_ids: set[int] = set()
    for row in category_property_rows:
        if not isinstance(row, (tuple, list)) or len(row) < 3:
            continue
        try:
            property_page_id = int(row[0])
        except (TypeError, ValueError):
            continue
        property_name = _normalize_text(row[1])
        if property_name == "wikibase_item":
            qid_by_category_page_id[property_page_id] = normalize_qid(row[2])
        elif property_name == "hiddencat":
            hidden_category_page_ids.add(property_page_id)

    resolved: Dict[str, List[Dict[str, Any]]] = {}
    for namespace, api_title, db_title in targets:
        normalized_api_title = normalize_page_title(api_title)
        if not normalized_api_title:
            continue
        target_page_id = normalized_page_ids_by_title.get(normalized_api_title)
        if target_page_id is not None:
            categories = categories_by_page_id.get(target_page_id, [])
        else:
            pair = (int(namespace), _normalize_db_title(db_title))
            categories = categories_by_pair.get(pair, [])
        resolved[normalized_api_title] = [
            {
                "title": "Category:{}".format(category_title),
                "wikidata_id": (
                    qid_by_category_page_id.get(category_page_id)
                    if category_page_id is not None
                    else None
                ),
                "hiddencat": (
                    category_page_id is not None
                    and category_page_id in hidden_category_page_ids
                ),
            }
            for category_title, category_page_id in categories
        ]
    return resolved
