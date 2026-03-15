import json
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urlencode, urlparse, urlsplit
from urllib.request import Request, urlopen

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from petscan import enrichment_api
from petscan import service_links as links
from petscan import service_source as source
from petscan.normalization import normalize_page_title

DEFAULT_PETSCAN_URL = "https://petscan.wmcloud.org/?psid=43641756"
_MAX_REASON_SAMPLES = 20
_ALLOWED_MISSING_GIL_LINK_URIS = {
    "https://sat.wikipedia.org/wiki/%E1%B1%A2%E1%B1%A9%E1%B1%AC%E1%B1%A9%E1%B1%9B:%E1%B1%9E%E1%B1%9F_%E1%B1%AF%E1%B1%9F%E1%B1%A1%E1%B1%BD",
    "https://sat.wikipedia.org/wiki/%E1%B1%A2%E1%B1%A9%E1%B1%AC%E1%B1%A9%E1%B1%9B:%E1%B1%AE%E1%B1%9E%E1%B1%9F%E1%B1%9D_%E1%B1%AE%E1%B1%B8%E1%B1%9C%E1%B1%AE%E1%B1%9E",
}


def _normalize_wiki_host(value: Optional[Any]) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if "://" in text:
        parsed = urlparse(text)
        host = (parsed.hostname or "").strip().lower()
        return host or None
    return text


def _extract_psid_and_params_from_url(petscan_url: str) -> Tuple[int, Dict[str, List[str]]]:
    parsed = urlparse(str(petscan_url or "").strip())
    query_pairs = parse_qs(parsed.query, keep_blank_values=False)

    psid_values = [value.strip() for value in query_pairs.get("psid", []) if str(value).strip()]
    if not psid_values:
        raise CommandError("The given PetScan URL must include ?psid=<number>.")

    try:
        psid = int(psid_values[-1])
    except Exception as exc:
        raise CommandError("psid in PetScan URL must be an integer.") from exc
    if psid <= 0:
        raise CommandError("psid must be greater than zero.")

    forwarded: Dict[str, List[str]] = {}
    for key, values in query_pairs.items():
        if key.lower() in {"psid", "format", "query", "refresh"}:
            continue
        normalized_values = [str(value).strip() for value in values if str(value).strip()]
        if normalized_values:
            forwarded[key] = normalized_values

    return psid, forwarded


def _build_api_query_url(api_url: str, titles: Sequence[str]) -> str:
    params = {
        "action": "query",
        "titles": "|".join(titles),
        "prop": "pageprops|info|revisions",
        "ppprop": "wikibase_item",
        "rvprop": "timestamp",
        "redirects": "1",
        "format": "json",
        "formatversion": "2",
    }
    return "{}?{}".format(api_url, urlencode(params))


def _payload_for_title(payload_by_title: Any, title: str) -> Optional[Dict[str, Any]]:
    if not isinstance(payload_by_title, dict):
        return None
    normalized_title = normalize_page_title(title)
    payload = payload_by_title.get(normalized_title)
    if isinstance(payload, dict):
        return payload
    if len(payload_by_title) == 1:
        only_value = next(iter(payload_by_title.values()))
        if isinstance(only_value, dict):
            return only_value
    return None


def _build_filtered_enrichment_map(
    records: Sequence[Mapping[str, Any]],
    wiki_host: Optional[str],
) -> Tuple[Dict[str, links.GilLinkTarget], Dict[str, Dict[str, Any]]]:
    link_targets_by_uri, _all_site_lookup_targets, direct_qids_by_link = links._collect_lookup_inputs(
        records,
        include_direct_lookup_targets=True,
    )

    selected_targets: Dict[str, links.GilLinkTarget] = {}
    selected_site_targets: Dict[str, Set[links.SiteLookupTarget]] = {}
    selected_direct_qids: Dict[str, str] = {}
    for link_uri, target in link_targets_by_uri.items():
        host = (urlsplit(link_uri).hostname or "").lower()
        if wiki_host and host != wiki_host:
            continue
        selected_targets[link_uri] = target
        selected_site_targets.setdefault(target.site, set()).add(
            links.SiteLookupTarget(
                namespace=target.namespace,
                api_title=target.api_title,
                db_title=target.db_title,
            )
        )
        direct_qid = direct_qids_by_link.get(link_uri)
        if direct_qid is not None:
            selected_direct_qids[link_uri] = direct_qid

    resolved_by_site_title = links._resolve_site_title_enrichment(
        selected_site_targets,
        backend=links.LOOKUP_BACKEND_API,
    )
    enrichment_map = links._attach_resolved_enrichment(
        selected_targets,
        selected_direct_qids,
        resolved_by_site_title,
    )
    return selected_targets, enrichment_map


def _probe_target_api_payload(
    target: links.GilLinkTarget,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    api_url = links.site_to_mediawiki_api_url(target.site)
    if api_url is None:
        return None, None

    api_query_url = _build_api_query_url(api_url, [target.api_title])
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    payload_by_title = enrichment_api.fetch_wikibase_items_for_site_api(
        api_url,
        [target.api_title],
        user_agent=source.HTTP_USER_AGENT,
        timeout_seconds=timeout,
    )
    return api_query_url, _payload_for_title(payload_by_title, target.api_title)


def _probe_multi_title_request_error(
    target: links.GilLinkTarget,
    sibling_target: links.GilLinkTarget,
) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    api_url = links.site_to_mediawiki_api_url(target.site)
    if api_url is None:
        return None, None

    titles = [target.api_title, sibling_target.api_title]
    request_url = _build_api_query_url(api_url, titles)
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": source.HTTP_USER_AGENT,
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310
            raw = response.read()
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return request_url, {"code": "transport_error", "info": str(exc)}

    if isinstance(payload, dict):
        error_payload = payload.get("error")
        if isinstance(error_payload, dict):
            code = str(error_payload.get("code", "")).strip() or "unknown"
            info = str(error_payload.get("info", "")).strip() or "unknown"
            return request_url, {"code": code, "info": info}
    return request_url, None


class Command(BaseCommand):
    help = (
        "Validate API enrichment coverage (page_len + rev_timestamp) for all gil_link URIs "
        "from a PetScan result."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--petscan-url",
            default=DEFAULT_PETSCAN_URL,
            help="PetScan URL with psid and optional extra query params.",
        )
        parser.add_argument(
            "--psid",
            type=int,
            default=None,
            help="Override psid from --petscan-url.",
        )
        parser.add_argument(
            "--allow-missing",
            action="store_true",
            help="Do not fail the command if some gil links miss page_len or rev_timestamp.",
        )
        parser.add_argument(
            "--sample-size",
            type=int,
            default=10,
            help="How many missing examples to print (default: 10).",
        )
        parser.add_argument(
            "--wiki",
            default=None,
            help="Optional wiki hostname filter (example: en.wikipedia.org).",
        )

    def handle(self, *args, **options):
        psid_from_url, petscan_params = _extract_psid_and_params_from_url(options["petscan_url"])
        psid = int(options["psid"]) if options["psid"] is not None else psid_from_url
        sample_size = max(1, min(int(options["sample_size"]), _MAX_REASON_SAMPLES))
        wiki_host = _normalize_wiki_host(options.get("wiki"))

        self.stdout.write("Loading PetScan payload for psid={}...".format(psid))
        payload, source_url = source.fetch_petscan_json(psid, petscan_params=petscan_params)
        records = source.extract_records(payload)
        if not records:
            raise CommandError("PetScan returned zero records.")

        self.stdout.write("records={}".format(len(records)))
        self.stdout.write("source_url={}".format(source_url))
        if wiki_host:
            self.stdout.write("wiki_filter={}".format(wiki_host))
        self.stdout.write("Running API enrichment lookup...")

        started_at = perf_counter()
        link_targets_by_uri, enrichment_map = _build_filtered_enrichment_map(records, wiki_host)
        elapsed_ms = (perf_counter() - started_at) * 1000.0
        all_gil_links = sorted(link_targets_by_uri.keys())

        if not all_gil_links:
            raise CommandError(
                "No gil links matched{}.".format(
                    " wiki filter '{}'".format(wiki_host) if wiki_host else ""
                )
            )

        self.stdout.write("gil_links_total={}".format(len(all_gil_links)))

        with_qid = 0
        with_page_len = 0
        with_rev_timestamp = 0
        with_both = 0
        allowed_missing_exceptions = 0
        missing_reasons: List[Tuple[str, str]] = []
        missing_by_site: Dict[str, int] = {}

        for link_uri in all_gil_links:
            payload_for_link = enrichment_map.get(link_uri)
            qid = None
            page_len = None
            rev_timestamp = None
            if isinstance(payload_for_link, dict):
                qid = payload_for_link.get("wikidata_id")
                page_len = payload_for_link.get("page_len")
                rev_timestamp = payload_for_link.get("rev_timestamp")

            has_qid = qid is not None
            has_page_len = page_len is not None
            has_rev_timestamp = rev_timestamp is not None
            has_both_fields = has_page_len and has_rev_timestamp

            with_qid += int(has_qid)
            with_page_len += int(has_page_len)
            with_rev_timestamp += int(has_rev_timestamp)
            with_both += int(has_both_fields)

            if has_both_fields:
                continue

            reason_parts = []
            if payload_for_link is None:
                reason_parts.append("no_enrichment_payload")
            if not has_page_len:
                reason_parts.append("missing_page_len")
            if not has_rev_timestamp:
                reason_parts.append("missing_rev_timestamp")
            reason = ",".join(reason_parts)

            if link_uri in _ALLOWED_MISSING_GIL_LINK_URIS:
                allowed_missing_exceptions += 1
                continue

            missing_reasons.append((link_uri, reason))

            site = (urlsplit(link_uri).hostname or "unknown").lower()
            missing_by_site[site] = int(missing_by_site.get(site, 0)) + 1

            # Fail fast at the first incomplete link and diagnose whether data exists in raw API output.
            target = link_targets_by_uri.get(link_uri)
            api_query_url = None
            api_payload = None
            if target is not None:
                api_query_url, api_payload = _probe_target_api_payload(target)

            self.stderr.write(
                self.style.WARNING(
                    "First incomplete enrichment found at {} [{}]".format(link_uri, reason)
                )
            )

            api_has_page_len = isinstance(api_payload, dict) and api_payload.get("page_len") is not None
            api_has_rev_timestamp = isinstance(api_payload, dict) and api_payload.get("rev_timestamp") is not None
            api_has_both = api_has_page_len and api_has_rev_timestamp

            if api_query_url:
                self.stderr.write("API query URL: {}".format(api_query_url))

            if api_has_both:
                self.stderr.write(
                    self.style.WARNING(
                        "API returned page_len + rev_timestamp, but values were lost after API fetch."
                    )
                )
                sibling_target = None
                if target is not None:
                    for candidate_uri, candidate_target in link_targets_by_uri.items():
                        if candidate_uri == link_uri:
                            continue
                        if candidate_target.site == target.site:
                            sibling_target = candidate_target
                            break

                if target is not None and sibling_target is not None:
                    batch_url, batch_error = _probe_multi_title_request_error(target, sibling_target)
                else:
                    batch_url, batch_error = (None, None)

                if isinstance(batch_error, dict):
                    self.stderr.write(
                        "Likely drop point: enrichment_api multi-title request returns API error "
                        "and is normalized to empty result."
                    )
                    if batch_url:
                        self.stderr.write("Batch API URL: {}".format(batch_url))
                    self.stderr.write(
                        "Batch API error: {} - {}".format(
                            batch_error.get("code"),
                            batch_error.get("info"),
                        )
                    )
                else:
                    self.stderr.write("Likely drop point: link->title attachment in build enrichment map.")
                if isinstance(api_payload, dict):
                    self.stderr.write("API payload sample: {}".format(api_payload))
            else:
                self.stderr.write(
                    self.style.WARNING(
                        "API did not return both page_len and rev_timestamp for this title."
                    )
                )
                if isinstance(api_payload, dict):
                    self.stderr.write("API payload sample: {}".format(api_payload))

            if options["allow_missing"]:
                return
            raise CommandError(
                "API enrichment incomplete: first missing link detected at {}.".format(link_uri)
            )

        self.stdout.write("enrichment_runtime_ms={:.1f}".format(elapsed_ms))
        self.stdout.write("links_with_qid={}".format(with_qid))
        self.stdout.write("links_with_page_len={}".format(with_page_len))
        self.stdout.write("links_with_rev_timestamp={}".format(with_rev_timestamp))
        self.stdout.write("links_with_both={}".format(with_both))
        self.stdout.write("allowed_missing_exceptions={}".format(allowed_missing_exceptions))

        if missing_reasons:
            self.stderr.write(
                self.style.WARNING(
                    "Missing page_len or rev_timestamp for {}/{} links.".format(
                        len(missing_reasons),
                        len(all_gil_links),
                    )
                )
            )

            self.stderr.write("Top missing sites:")
            for site, count in sorted(missing_by_site.items(), key=lambda item: (-item[1], item[0]))[:10]:
                self.stderr.write("  {}: {}".format(site, count))

            self.stderr.write("Sample missing links:")
            for link_uri, reason in missing_reasons[:sample_size]:
                self.stderr.write("  {} [{}]".format(link_uri, reason))

            if not options["allow_missing"]:
                raise CommandError(
                    "API enrichment incomplete: {} of {} gil links miss page_len or rev_timestamp.".format(
                        len(missing_reasons),
                        len(all_gil_links),
                    )
                )
        else:
            self.stdout.write(self.style.SUCCESS("All gil links have page_len and rev_timestamp."))
