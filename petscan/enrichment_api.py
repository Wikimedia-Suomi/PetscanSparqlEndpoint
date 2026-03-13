import json
import re
from time import perf_counter
from typing import Dict, Mapping, Optional, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen

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


def fetch_wikibase_items_for_site_api(
    api_url: str,
    titles: Sequence[str],
    user_agent: str,
    timeout_seconds: int,
) -> Dict[str, str]:
    if not titles:
        return {}

    params = {
        "action": "query",
        "titles": "|".join(titles),
        "prop": "pageprops",
        "ppprop": "wikibase_item",
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
    print("[wikimedia-api] GET {}".format(request_url), flush=True)
    started_at = perf_counter()

    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # nosec B310
            raw = response.read()
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        elapsed_ms = (perf_counter() - started_at) * 1000.0
        print("[wikimedia-api] ERROR {:.1f} ms {}".format(elapsed_ms, request_url), flush=True)
        print("[wikimedia-api] ERROR_DETAILS {}".format(exc), flush=True)
        return {}

    elapsed_ms = (perf_counter() - started_at) * 1000.0
    print("[wikimedia-api] DONE {:.1f} ms {}".format(elapsed_ms, request_url), flush=True)

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
            source_title = _normalize_page_title(entry.get("from"))
            target_title = _normalize_page_title(entry.get("to"))
            if source_title and target_title:
                alias_map[source_title] = target_title

    page_qids = {}  # type: Dict[str, str]
    pages = query.get("pages")
    if isinstance(pages, list):
        for page in pages:
            if not isinstance(page, Mapping):
                continue
            title = _normalize_page_title(page.get("title"))
            if not title:
                continue
            pageprops = page.get("pageprops")
            if not isinstance(pageprops, Mapping):
                continue
            qid = _normalize_qid(pageprops.get("wikibase_item"))
            if qid is not None:
                page_qids[title] = qid

    resolved = {}  # type: Dict[str, str]
    for input_title in titles:
        normalized_input = _normalize_page_title(input_title)
        if not normalized_input:
            continue
        final_title = _resolve_title_alias(normalized_input, alias_map)
        qid = page_qids.get(final_title) or page_qids.get(normalized_input)
        if qid is not None:
            resolved[normalized_input] = qid

    return resolved
