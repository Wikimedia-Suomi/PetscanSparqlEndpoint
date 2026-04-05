from argparse import ArgumentParser
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, cast

from django.core.management.base import BaseCommand, CommandError

from newpages import service_source as source


@dataclass(frozen=True)
class _SqlUserNameMatchRow:
    wiki_domain: str
    user_name: str
    actor_exact_values: Tuple[str, ...]
    rc_exact_values: Tuple[str, ...]
    rc_exact_hits: int
    rc_exact_latest: str


def _timestamp_days_ago(days: int) -> str:
    if days <= 0:
        raise ValueError("days must be greater than zero.")
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d%H%M%S")


def _user_lookup_key(value: Any) -> Optional[str]:
    normalized = source._normalize_user_name(value)
    if normalized is None:
        return None
    return normalized.casefold()


def _parse_cli_user_names(raw_values: Sequence[str]) -> List[str]:
    user_names: List[str] = []
    seen_keys: set[str] = set()
    for raw_value in raw_values:
        for part in str(raw_value or "").split(","):
            normalized = source._normalize_user_name(part)
            if normalized is None:
                continue
            lookup_key = normalized.casefold()
            if lookup_key in seen_keys:
                continue
            seen_keys.add(lookup_key)
            user_names.append(normalized)
    return user_names


def _resolve_target_user_names(
    raw_user_values: Sequence[str],
    user_list_page: Optional[str],
) -> List[str]:
    user_names = _parse_cli_user_names(raw_user_values)
    seen_keys = {user_name.casefold() for user_name in user_names}

    normalized_user_list_page = source.normalize_user_list_page(user_list_page)
    if normalized_user_list_page is not None:
        resolved = source._resolve_user_list_page(normalized_user_list_page)
        if resolved is None:
            raise CommandError("user_list_page must be a Wikimedia wiki page reference.")
        try:
            listed_user_names = source._fetch_user_names_for_page(resolved)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        for user_name in listed_user_names:
            lookup_key = user_name.casefold()
            if lookup_key in seen_keys:
                continue
            seen_keys.add(lookup_key)
            user_names.append(user_name)

    if not user_names:
        raise CommandError("At least one --user or --user-list-page value is required.")
    return user_names


def _search_values_for_user_names(user_names: Sequence[str]) -> List[str]:
    values: List[str] = []
    seen_values: set[str] = set()
    for user_name in user_names:
        candidate = user_name
        if not candidate or candidate in seen_values:
            continue
        seen_values.add(candidate)
        values.append(candidate)
    return values


def _fetch_actor_match_values(cursor: Any, search_values: Sequence[str]) -> List[str]:
    if not search_values:
        return []
    sql = "SELECT actor_name FROM actor WHERE actor_name IN ({})".format(", ".join(["%s"] * len(search_values)))
    cursor.execute(sql, list(search_values))
    rows = cursor.fetchall()
    if not isinstance(rows, list):
        return []
    return [str(row[0]) for row in rows if isinstance(row, (tuple, list)) and row]


def _fetch_recentchange_match_rows(
    cursor: Any,
    search_values: Sequence[str],
    threshold_timestamp: str,
) -> List[Tuple[str, int, str]]:
    if not search_values:
        return []
    sql = """
SELECT a.actor_name, COUNT(*), MAX(rc.rc_timestamp)
FROM actor_recentchanges AS rc
JOIN actor AS a ON rc.rc_actor = a.actor_id
WHERE rc.rc_timestamp >= %s
  AND a.actor_name IN ({})
GROUP BY a.actor_name
""".strip().format(", ".join(["%s"] * len(search_values)))
    cursor.execute(sql, [threshold_timestamp] + list(search_values))
    rows = cursor.fetchall()
    if not isinstance(rows, list):
        return []

    normalized_rows: List[Tuple[str, int, str]] = []
    for row in rows:
        if not isinstance(row, (tuple, list)) or len(row) < 3:
            continue
        try:
            hit_count = int(row[1])
        except (TypeError, ValueError):
            continue
        normalized_rows.append((str(row[0]), hit_count, str(row[2] or "")))
    return normalized_rows


def _group_actor_matches_by_user(
    matched_values: Sequence[str],
    target_user_names: Sequence[str],
) -> Dict[str, Tuple[str, ...]]:
    known_keys = {user_name.casefold() for user_name in target_user_names}
    values_by_key: Dict[str, List[str]] = {}
    for matched_value in matched_values:
        lookup_key = _user_lookup_key(matched_value)
        if lookup_key is None or lookup_key not in known_keys:
            continue
        values_by_key.setdefault(lookup_key, [])
        if matched_value not in values_by_key[lookup_key]:
            values_by_key[lookup_key].append(matched_value)
    return {key: tuple(values) for key, values in values_by_key.items()}


def _group_recentchange_matches_by_user(
    matched_rows: Sequence[Tuple[str, int, str]],
    target_user_names: Sequence[str],
) -> Dict[str, Tuple[Tuple[str, ...], int, str]]:
    known_keys = {user_name.casefold() for user_name in target_user_names}
    values_by_key: Dict[str, List[str]] = {}
    hits_by_key: Dict[str, int] = {}
    latest_by_key: Dict[str, str] = {}

    for raw_user_text, hit_count, latest_timestamp in matched_rows:
        lookup_key = _user_lookup_key(raw_user_text)
        if lookup_key is None or lookup_key not in known_keys:
            continue
        values_by_key.setdefault(lookup_key, [])
        if raw_user_text not in values_by_key[lookup_key]:
            values_by_key[lookup_key].append(raw_user_text)
        hits_by_key[lookup_key] = hits_by_key.get(lookup_key, 0) + hit_count
        current_latest = latest_by_key.get(lookup_key, "")
        if latest_timestamp > current_latest:
            latest_by_key[lookup_key] = latest_timestamp

    return {
        lookup_key: (
            tuple(values_by_key.get(lookup_key, [])),
            hits_by_key.get(lookup_key, 0),
            latest_by_key.get(lookup_key, ""),
        )
        for lookup_key in known_keys
        if lookup_key in values_by_key or lookup_key in hits_by_key or lookup_key in latest_by_key
    }


def _load_user_name_match_rows(
    wiki_values: Sequence[str],
    user_names: Sequence[str],
    days: int,
) -> Tuple[str, List[_SqlUserNameMatchRow]]:
    if source.pymysql is None:
        raise CommandError("PyMySQL is required for SQL-backed user-name checks.")

    normalized_wikis = source.normalize_wikis(list(wiki_values))
    if not normalized_wikis:
        raise CommandError("At least one --wiki value is required.")

    try:
        descriptors = source._selected_wiki_descriptors(normalized_wikis)
    except ValueError as exc:
        raise CommandError(str(exc)) from exc

    threshold_timestamp = _timestamp_days_ago(days)
    exact_search_values = _search_values_for_user_names(user_names)
    rows: List[_SqlUserNameMatchRow] = []

    for descriptor in descriptors:
        connection = None
        try:
            connection = source.pymysql.connect(**source._replica_connect_kwargs(descriptor.dbname))
            with connection.cursor() as cursor:
                actor_exact_values = _fetch_actor_match_values(cursor, exact_search_values)
                rc_exact_rows = _fetch_recentchange_match_rows(cursor, exact_search_values, threshold_timestamp)
        except Exception as exc:
            raise CommandError(
                "Failed to inspect SQL user-name matches for {}: {}".format(descriptor.domain, exc)
            ) from exc
        finally:
            if connection is not None:
                connection.close()

        actor_exact_by_user = _group_actor_matches_by_user(actor_exact_values, user_names)
        rc_exact_by_user = _group_recentchange_matches_by_user(rc_exact_rows, user_names)

        for user_name in user_names:
            lookup_key = user_name.casefold()
            rc_exact_values_for_user, rc_exact_hits, rc_exact_latest = rc_exact_by_user.get(
                lookup_key,
                ((), 0, ""),
            )
            rows.append(
                _SqlUserNameMatchRow(
                    wiki_domain=descriptor.domain,
                    user_name=user_name,
                    actor_exact_values=actor_exact_by_user.get(lookup_key, ()),
                    rc_exact_values=rc_exact_values_for_user,
                    rc_exact_hits=rc_exact_hits,
                    rc_exact_latest=rc_exact_latest,
                )
            )

    return threshold_timestamp, rows


def _row_status(row: _SqlUserNameMatchRow) -> str:
    if row.actor_exact_values or row.rc_exact_hits > 0:
        return "match"
    return "not_found"


def _format_values(values: Sequence[str]) -> str:
    if not values:
        return "-"
    return ", ".join(values)


@contextmanager
def _suppress_source_console_log() -> Iterator[None]:
    original_console_log = cast(Callable[[str], None], source._console_log)
    source._console_log = cast(Any, lambda message: None)
    try:
        yield
    finally:
        source._console_log = cast(Any, original_console_log)


class Command(BaseCommand):  # type: ignore[misc]
    help = (
        "Check whether the SQL-side user-name assumptions used by newpages filtering hold for "
        "the selected wikis and user names."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--wiki",
            action="append",
            required=True,
            help="One or more wiki hostnames. Accepts comma-separated values and repeated flags.",
        )
        parser.add_argument(
            "--user",
            action="append",
            default=[],
            help="One or more user names. Accepts comma-separated values and repeated flags.",
        )
        parser.add_argument(
            "--user-list-page",
            default="",
            help="Optional Wikimedia wiki page reference used to resolve the user names to test.",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=30,
            help="How many days back recentchanges should be checked (default: 30).",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        days = int(options["days"])
        if days <= 0:
            raise CommandError("--days must be greater than zero.")

        wiki_values = [str(value) for value in (options.get("wiki") or []) if str(value).strip()]
        raw_user_values = [str(value) for value in (options.get("user") or []) if str(value).strip()]
        user_list_page = str(options.get("user_list_page") or "").strip()

        with _suppress_source_console_log():
            user_names = _resolve_target_user_names(raw_user_values, user_list_page)
            threshold_timestamp, rows = _load_user_name_match_rows(wiki_values, user_names, days)

        self.stdout.write("target_wikis={}".format(len(source.normalize_wikis(wiki_values))))
        self.stdout.write("target_users={}".format(len(user_names)))
        self.stdout.write("threshold_timestamp={}".format(threshold_timestamp))

        failing_rows: List[_SqlUserNameMatchRow] = []
        for row in rows:
            status = _row_status(row)
            self.stdout.write(
                "wiki={} user={} status={}".format(row.wiki_domain, row.user_name, status)
            )
            self.stdout.write(
                "  actor_exact:      {} raw={}".format(
                    "yes" if row.actor_exact_values else "no",
                    _format_values(row.actor_exact_values),
                )
            )
            self.stdout.write(
                "  rc_exact:         hits={} latest={} raw={}".format(
                    row.rc_exact_hits,
                    row.rc_exact_latest or "-",
                    _format_values(row.rc_exact_values),
                )
            )
            if status != "match":
                failing_rows.append(row)

        if failing_rows:
            for row in failing_rows:
                status = _row_status(row)
                self.stderr.write(
                    "wiki={} user={} status={} actor_exact_raw={} rc_exact_hits={}".format(
                        row.wiki_domain,
                        row.user_name,
                        status,
                        _format_values(row.actor_exact_values),
                        row.rc_exact_hits,
                    )
                )
            raise CommandError(
                "SQL user-name matching failed for {} user/wiki checks.".format(len(failing_rows))
            )

        self.stdout.write(
            "SQL user-name matching passed for {} user/wiki checks.".format(len(rows))
        )
