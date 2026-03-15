import json
from time import perf_counter
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .normalization import normalize_page_title, normalize_qid


def _resolve_title_alias(title: str, alias_map: Mapping[str, str]) -> str:
    current = title
    seen = set()
    while current in alias_map and current not in seen:
        seen.add(current)
        next_title = alias_map[current]
        if next_title == current:
            break
        current = next_title
    return current


def _normalize_revision_timestamp(value: object) -> Optional[str]:
    if value is None:
        return None
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


def fetch_wikibase_items_for_site_api(
    api_url: str,
    titles: Sequence[str],
    user_agent: str,
    timeout_seconds: int,
    lookup_stats: Optional[MutableMapping[str, float]] = None,
) -> Dict[str, Dict[str, Any]]:
    if not titles:
        return {}

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
    request_url = "{}?{}".format(api_url, urlencode(params))
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": user_agent,
        },
    )
    started_at = perf_counter()

    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # nosec B310
            raw = response.read()
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        elapsed_ms = (perf_counter() - started_at) * 1000.0
        if lookup_stats is not None:
            lookup_stats["api_calls"] = float(lookup_stats.get("api_calls", 0.0)) + 1.0
            lookup_stats["api_ms_total"] = float(lookup_stats.get("api_ms_total", 0.0)) + elapsed_ms
        return {}

    elapsed_ms = (perf_counter() - started_at) * 1000.0
    if lookup_stats is not None:
        lookup_stats["api_calls"] = float(lookup_stats.get("api_calls", 0.0)) + 1.0
        lookup_stats["api_ms_total"] = float(lookup_stats.get("api_ms_total", 0.0)) + elapsed_ms

    if not isinstance(payload, dict):
        return {}

    query = payload.get("query")
    if not isinstance(query, dict):
        return {}

    alias_map = {}  # type: Dict[str, str]
    for mapping_key in ("normalized", "redirects"):
        entries = query.get(mapping_key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            source_title = normalize_page_title(entry.get("from"))
            target_title = normalize_page_title(entry.get("to"))
            if source_title and target_title:
                alias_map[source_title] = target_title

    page_enrichment = {}  # type: Dict[str, Dict[str, Any]]
    pages = query.get("pages")
    if isinstance(pages, list):
        for page in pages:
            if not isinstance(page, Mapping):
                continue
            title = normalize_page_title(page.get("title"))
            if not title:
                continue

            pageprops = page.get("pageprops")
            qid = None
            if isinstance(pageprops, Mapping):
                qid = normalize_qid(pageprops.get("wikibase_item"))

            page_len = _normalize_page_len(page.get("length"))

            rev_timestamp = None
            revisions = page.get("revisions")
            if isinstance(revisions, list) and revisions:
                first_revision = revisions[0]
                if isinstance(first_revision, Mapping):
                    rev_timestamp = _normalize_revision_timestamp(first_revision.get("timestamp"))

            page_enrichment[title] = {
                "wikidata_id": qid,
                "page_len": page_len,
                "rev_timestamp": rev_timestamp,
            }

    resolved = {}  # type: Dict[str, Dict[str, Any]]
    for input_title in titles:
        normalized_input = normalize_page_title(input_title)
        if not normalized_input:
            continue
        final_title = _resolve_title_alias(normalized_input, alias_map)
        enrichment = page_enrichment.get(final_title) or page_enrichment.get(normalized_input)
        if enrichment is not None:
            resolved[normalized_input] = dict(enrichment)

    return resolved
