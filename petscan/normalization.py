"""Shared normalization helpers for titles and Wikidata QIDs."""

import re
from typing import Optional

_QID_RE = re.compile(r"Q([1-9][0-9]*)", re.IGNORECASE)

__all__ = [
    "normalize_page_title",
    "normalize_qid",
]


def normalize_page_title(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # Some inputs use a leading ":" for main-namespace titles.
    return text.lstrip(":").replace(" ", "_")


def normalize_qid(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _QID_RE.search(text)
    if not match:
        return None
    return "Q{}".format(match.group(1))
