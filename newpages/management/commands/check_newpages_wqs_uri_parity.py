import json
from argparse import ArgumentParser
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from newpages import service_source as source
from petscan.normalization import normalize_qid
from petscan.service_source import HTTP_USER_AGENT

WQS_ENDPOINT = "https://query.wikidata.org/sparql"
_API_LOGEVENTS_BATCH_LIMIT = 500
_WQS_BATCH_SIZE = 50
_MAX_MISMATCH_SAMPLES = 20
_AUTO_TARGET_WIKI_GROUPS = frozenset({"wikipedia", "wikivoyage"})
_URI_LABEL_WIDTH = 10


@dataclass(frozen=True)
class _ParityCheckRow:
    page_title: str
    local_uri: str
    wqs_uri: str
    matched: bool


def _timestamp_days_ago(days: int) -> str:
    if days <= 0:
        raise ValueError("days must be greater than zero.")
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d%H%M%S")


def _chunked(values: Sequence[Any], size: int) -> Iterable[List[Any]]:
    chunk_size = max(1, size)
    for index in range(0, len(values), chunk_size):
        yield list(values[index : index + chunk_size])


def _wqs_query_for_pairs(pairs: Sequence[Tuple[str, str]]) -> str:
    values = "\n".join(
        "    (wd:{qid} <{site_url}>)".format(qid=qid, site_url=site_url) for qid, site_url in pairs
    )
    return """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX schema: <http://schema.org/>
SELECT ?item ?site ?article WHERE {{
  VALUES (?item ?site) {{
{values}
  }}
  ?article a schema:Article ;
           schema:about ?item ;
           schema:isPartOf ?site .
}}
""".strip().format(values=values)


def _request_wqs_json(query: str) -> Dict[str, Any]:
    timeout_seconds = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    request = Request(
        WQS_ENDPOINT,
        data=urlencode({"query": query, "format": "json"}).encode("utf-8"),
        headers={
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "User-Agent": HTTP_USER_AGENT,
        },
        method="POST",
    )
    with urlopen(request, timeout=timeout_seconds) as response:  # nosec B310
        raw = response.read()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("WQS returned unexpected payload.")
    return payload


def _fetch_wqs_article_uris(
    sample_records: Sequence[Mapping[str, Any]],
    batch_size: int = _WQS_BATCH_SIZE,
) -> Dict[Tuple[str, str], List[str]]:
    unique_pairs = sorted(
        {
            (str(record.get("wikidata_id", "")).strip(), str(record.get("site_url", "")).strip())
            for record in sample_records
            if str(record.get("wikidata_id", "")).strip() and str(record.get("site_url", "")).strip()
        }
    )
    article_uris_by_key: Dict[Tuple[str, str], List[str]] = {}

    for batch in _chunked(unique_pairs, batch_size):
        payload = _request_wqs_json(_wqs_query_for_pairs(batch))
        results = payload.get("results")
        bindings = results.get("bindings") if isinstance(results, Mapping) else None
        if not isinstance(bindings, list):
            raise RuntimeError("WQS returned no bindings payload.")

        for binding in bindings:
            if not isinstance(binding, Mapping):
                continue
            item_payload = binding.get("item")
            site_payload = binding.get("site")
            article_payload = binding.get("article")
            if not isinstance(item_payload, Mapping):
                continue
            if not isinstance(site_payload, Mapping):
                continue
            if not isinstance(article_payload, Mapping):
                continue

            qid = normalize_qid(item_payload.get("value"))
            site_url = str(site_payload.get("value", "")).strip()
            article_url = str(article_payload.get("value", "")).strip()
            if qid is None or not site_url or not article_url:
                continue
            article_uris_by_key.setdefault((qid, site_url), []).append(article_url)

    return article_uris_by_key


def _sample_recent_non_mainspace_records_sql(
    descriptor: source._WikiDescriptor,
    threshold_timestamp: str,
    sample_size: int,
) -> List[Dict[str, Any]]:
    if source.pymysql is None:
        raise CommandError("PyMySQL is required for SQL-backed namespace parity checks.")

    sql = """
SELECT rc.rc_cur_id, rc.rc_title, rc.rc_namespace, pp.pp_value, rc.rc_timestamp
FROM recentchanges AS rc
JOIN page_props AS pp
  ON pp.pp_page = rc.rc_cur_id
 AND pp.pp_propname = %s
WHERE rc.rc_source = %s
  AND rc.rc_timestamp >= %s
  AND rc.rc_namespace <> 0
ORDER BY rc.rc_timestamp DESC
LIMIT %s
""".strip()
    params: List[object] = ["wikibase_item", "mw.new", threshold_timestamp, sample_size]
    siteinfo = source._siteinfo_for_domain(descriptor.domain)
    records: List[Dict[str, Any]] = []
    connection = None

    try:
        connection = source.pymysql.connect(**source._replica_connect_kwargs(descriptor.dbname))
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    except Exception as exc:
        raise CommandError(
            "Failed to fetch SQL namespace samples for {}: {}".format(descriptor.domain, exc)
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    if not isinstance(rows, list):
        return []

    for row in rows:
        if not isinstance(row, (tuple, list)):
            continue
        record = source._build_record(
            descriptor,
            siteinfo,
            (row[0], row[1], row[2], row[3], row[4]),
        )
        if record is not None:
            records.append(record)
    return records


def _fetch_logevents_page(
    descriptor: source._WikiDescriptor,
    continue_token: Optional[str],
) -> Tuple[List[Mapping[str, Any]], Optional[str]]:
    params: MutableMapping[str, str] = {
        "action": "query",
        "list": "logevents",
        "leprop": "title|timestamp|ids",
        "letype": "create",
        "ledir": "older",
        "lelimit": str(_API_LOGEVENTS_BATCH_LIMIT),
        "format": "json",
        "formatversion": "2",
    }
    if continue_token:
        params["lecontinue"] = continue_token

    payload = source._request_json("{}?{}".format(source._wiki_api_url(descriptor.domain), urlencode(params)))
    query_payload = payload.get("query")
    if not isinstance(query_payload, Mapping):
        raise CommandError("Logevents API returned no query payload for {}.".format(descriptor.domain))
    logevents = query_payload.get("logevents")
    if not isinstance(logevents, list):
        raise CommandError("Logevents API returned no logevents payload for {}.".format(descriptor.domain))

    continuation = payload.get("continue")
    next_continue: Optional[str] = None
    if isinstance(continuation, Mapping):
        raw_continue = continuation.get("lecontinue")
        text_continue = str(raw_continue or "").strip()
        if text_continue:
            next_continue = text_continue

    return [entry for entry in logevents if isinstance(entry, Mapping)], next_continue


def _sample_recent_non_mainspace_records_api(
    descriptor: source._WikiDescriptor,
    threshold_timestamp: str,
    sample_size: int,
) -> List[Dict[str, Any]]:
    threshold = int(threshold_timestamp)
    siteinfo = source._siteinfo_for_domain(descriptor.domain)
    records: List[Dict[str, Any]] = []
    continue_token: Optional[str] = None

    while True:
        entries, continue_token = _fetch_logevents_page(descriptor, continue_token)
        if not entries:
            break

        candidate_entries: List[Tuple[int, int, str, Any]] = []
        reached_threshold = False

        for entry in entries:
            timestamp_value = source._numeric_timestamp(entry.get("timestamp"))
            if timestamp_value is None or timestamp_value < threshold:
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

            if namespace_id <= 0:
                continue

            candidate_entries.append(
                (
                    page_id,
                    namespace_id,
                    source._normalize_api_page_title(entry.get("title"), namespace_id, siteinfo),
                    entry.get("timestamp"),
                )
            )

        qids_by_page_id = source._fetch_pageprops_qids_api(
            descriptor.domain,
            [page_id for page_id, _namespace_id, _title, _timestamp in candidate_entries],
        )
        for page_id, namespace_id, page_title, created_timestamp in candidate_entries:
            if len(records) >= sample_size:
                continue
            qid = qids_by_page_id.get(page_id)
            if qid is None:
                continue
            record = source._build_record(
                descriptor,
                siteinfo,
                (page_id, page_title, namespace_id, qid, created_timestamp),
            )
            if record is not None:
                records.append(record)
                if len(records) >= sample_size:
                    break

        if len(records) >= sample_size or reached_threshold or not continue_token:
            break

    records.sort(
        key=lambda row: (
            -int(row.get("_created_sort", 0)),
            str(row.get("wiki_domain", "")),
            int(row.get("namespace", 0)),
        )
    )
    for record in records:
        record.pop("_created_sort", None)
    return records


def _load_sample_records(
    wiki_values: Sequence[str],
    days: int,
    sample_size: int,
) -> Tuple[str, str, List[Dict[str, Any]]]:
    normalized_wikis = source.normalize_wikis(list(wiki_values))
    if not normalized_wikis:
        raise CommandError("At least one --wiki value is required.")

    threshold_timestamp = _timestamp_days_ago(days)
    backend = source.newpages_lookup_backend()
    try:
        descriptors = source._selected_wiki_descriptors(normalized_wikis)
    except ValueError as exc:
        raise CommandError(str(exc)) from exc

    records: List[Dict[str, Any]] = []
    for descriptor in descriptors:
        if backend == source.LOOKUP_BACKEND_TOOLFORGE_SQL:
            records.extend(
                _sample_recent_non_mainspace_records_sql(
                    descriptor,
                    threshold_timestamp=threshold_timestamp,
                    sample_size=sample_size,
                )
            )
            continue
        records.extend(
            _sample_recent_non_mainspace_records_api(
                descriptor,
                threshold_timestamp=threshold_timestamp,
                sample_size=sample_size,
            )
        )

    return backend, threshold_timestamp, records


def _normalize_start_wiki(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = source.normalize_wikis([text])
    if len(normalized) != 1:
        raise CommandError("--start-wiki must contain exactly one wiki hostname.")
    return source._wiki_domain_for_token(normalized[0])


def _wiki_domains_for_values(wiki_values: Sequence[str]) -> List[str]:
    normalized_tokens = source.normalize_wikis(list(wiki_values))
    seen: set[str] = set()
    domains: List[str] = []
    for token in normalized_tokens:
        domain = source._wiki_domain_for_token(token)
        if domain in seen:
            continue
        seen.add(domain)
        domains.append(domain)
    return domains


def _resolve_target_wikis(wiki_values: Sequence[str], start_wiki: Optional[str]) -> List[str]:
    normalized_start_wiki = _normalize_start_wiki(start_wiki)

    if wiki_values:
        target_wikis = _wiki_domains_for_values(wiki_values)
    else:
        try:
            target_wikis = sorted(
                domain
                for domain, descriptor in source._known_wikis_by_domain().items()
                if descriptor.wiki_group in _AUTO_TARGET_WIKI_GROUPS
            )
        except Exception as exc:
            raise CommandError("Failed to load SiteMatrix wiki list: {}".format(exc)) from exc

    if not target_wikis:
        raise CommandError("No target wikis resolved for the parity check.")

    if normalized_start_wiki is None:
        return target_wikis

    if normalized_start_wiki not in target_wikis:
        raise CommandError("Unknown start wiki: {}.".format(normalized_start_wiki))

    return target_wikis[target_wikis.index(normalized_start_wiki) :]


@contextmanager
def _suppress_source_console_log() -> Iterator[None]:
    original_console_log = cast(Callable[[str], None], source._console_log)
    source._console_log = cast(Any, lambda message: None)
    try:
        yield
    finally:
        source._console_log = cast(Any, original_console_log)


def _group_sample_counts(
    sample_records: Sequence[Mapping[str, Any]],
) -> DefaultDict[Tuple[str, int], int]:
    grouped_counts: DefaultDict[Tuple[str, int], int] = defaultdict(int)
    for record in sample_records:
        wiki_domain = str(record.get("wiki_domain", "")).strip()
        namespace_id = int(record.get("namespace", 0))
        grouped_counts[(wiki_domain, namespace_id)] += 1
    return grouped_counts


def _check_sample_records_against_wqs(
    sample_records: Sequence[Mapping[str, Any]],
) -> Tuple[int, List[str], List[_ParityCheckRow]]:
    try:
        wqs_article_uris = _fetch_wqs_article_uris(sample_records)
    except Exception as exc:
        raise CommandError("Failed to query WQS: {}".format(exc)) from exc

    checked = 0
    mismatches: List[str] = []
    parity_rows: List[_ParityCheckRow] = []
    for record in sample_records:
        qid = str(record.get("wikidata_id", "")).strip()
        site_url = str(record.get("site_url", "")).strip()
        expected_page_url = str(record.get("page_url", "")).strip()
        wiki_domain = str(record.get("wiki_domain", "")).strip()
        page_title = str(record.get("page_title", "")).strip()
        namespace_id = int(record.get("namespace", 0))

        key = (qid, site_url)
        actual_uris = sorted(set(wqs_article_uris.get(key, [])))
        displayed_wqs_uri = expected_page_url if expected_page_url in actual_uris else (
            actual_uris[0] if actual_uris else "(missing)"
        )
        matched = expected_page_url in actual_uris
        parity_rows.append(
            _ParityCheckRow(
                page_title=page_title,
                local_uri=expected_page_url,
                wqs_uri=displayed_wqs_uri,
                matched=matched,
            )
        )
        if not matched:
            mismatches.append(
                "wiki={wiki} namespace={namespace} qid={qid} title={title} expected={expected} wqs={actual}".format(
                    wiki=wiki_domain,
                    namespace=namespace_id,
                    qid=qid,
                    title=page_title,
                    expected=expected_page_url,
                    actual=",".join(actual_uris) if actual_uris else "(missing)",
                )
            )
            continue
        checked += 1

    return checked, mismatches, parity_rows


class Command(BaseCommand):  # type: ignore[misc]
    help = (
        "Check that non-mainspace new-page URIs generated by the newpages source match "
        "the sitelink article URIs exposed by Wikidata Query Service."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--wiki",
            action="append",
            help="One or more wiki identifiers or hostnames. Accepts comma-separated values and repeated flags.",
        )
        parser.add_argument(
            "--start-wiki",
            default="",
            help=(
                "Optional wiki identifier or hostname to resume from. When --wiki is omitted, the command "
                "iterates SiteMatrix Wikipedia and Wikivoyage wikis starting from this wiki (inclusive)."
            ),
        )
        parser.add_argument(
            "--days",
            type=int,
            default=30,
            help="How many days back to inspect for new-page creations (default: 30).",
        )
        parser.add_argument(
            "--sample-size",
            "--per-namespace",
            dest="sample_size",
            type=int,
            default=3,
            help="Maximum sampled non-mainspace pages per wiki (default: 3).",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        days = int(options["days"])
        sample_size = int(options["sample_size"])
        if days <= 0:
            raise CommandError("--days must be greater than zero.")
        if sample_size <= 0:
            raise CommandError("--sample-size must be greater than zero.")

        wiki_values = [str(value) for value in (options.get("wiki") or []) if str(value).strip()]
        start_wiki = str(options.get("start_wiki") or "").strip()
        with _suppress_source_console_log():
            target_wikis = _resolve_target_wikis(wiki_values, start_wiki)
        any_samples_found = False
        total_checked = 0
        printed_run_header = False

        self.stdout.write("target_wikis={}".format(len(target_wikis)))

        for wiki_domain in target_wikis:
            with _suppress_source_console_log():
                backend, threshold_timestamp, sample_records = _load_sample_records(
                    [wiki_domain],
                    days=days,
                    sample_size=sample_size,
                )
            if not printed_run_header:
                self.stdout.write("backend={}".format(backend))
                self.stdout.write("threshold_timestamp={}".format(threshold_timestamp))
                printed_run_header = True

            self.stdout.write("checking wiki={}".format(wiki_domain))
            if not sample_records:
                self.stdout.write(
                    self.style.WARNING(
                        "[SKIP] wiki={} sample_records=0".format(wiki_domain)
                    )
                )
                continue

            any_samples_found = True
            self.stdout.write("sample_records={}".format(len(sample_records)))

            grouped_counts = _group_sample_counts(sample_records)
            for sample_wiki, namespace_id in sorted(grouped_counts.keys()):
                self.stdout.write(
                    "sample wiki={} namespace={} pages={}".format(
                        sample_wiki,
                        namespace_id,
                        grouped_counts[(sample_wiki, namespace_id)],
                    )
                )

            checked, mismatches, parity_rows = _check_sample_records_against_wqs(sample_records)
            for parity_row in parity_rows:
                self.stdout.write("page={}".format(parity_row.page_title))
                self.stdout.write(
                    "  {label:<{width}} {uri}".format(
                        label="local_uri:",
                        width=_URI_LABEL_WIDTH,
                        uri=parity_row.local_uri,
                    )
                )
                self.stdout.write(
                    "  {label:<{width}} {uri}".format(
                        label="wqs_uri:",
                        width=_URI_LABEL_WIDTH,
                        uri=parity_row.wqs_uri,
                    )
                )
            self.stdout.write("checked={}".format(checked))
            total_checked += checked

            if mismatches:
                for mismatch in mismatches[:_MAX_MISMATCH_SAMPLES]:
                    self.stderr.write(self.style.ERROR("[FAIL] {}".format(mismatch)))
                raise CommandError(
                    "WQS URI parity failed for wiki {} ({} sampled page(s)).".format(
                        wiki_domain,
                        len(mismatches),
                    )
                )

            self.stdout.write(
                self.style.SUCCESS(
                    "WQS URI parity passed for wiki={} checked={}.".format(wiki_domain, checked)
                )
            )

        if not any_samples_found:
            raise CommandError("No non-mainspace new pages with Wikidata items were found for the requested wikis.")

        self.stdout.write(
            self.style.SUCCESS(
                "WQS URI parity passed for {} sampled non-mainspace new pages.".format(total_checked)
            )
        )
