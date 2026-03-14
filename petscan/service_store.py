"""Filesystem-backed Oxigraph store path and metadata helpers."""

import json
import threading
from pathlib import Path
from typing import Any, Dict

from django.conf import settings

_LOCK_STRIPE_COUNT = 256
_LOCK_STRIPES = tuple(threading.Lock() for _ in range(_LOCK_STRIPE_COUNT))
__all__ = [
    "get_psid_lock",
    "has_existing_store",
    "meta_path",
    "read_meta",
    "store_path",
]


def get_psid_lock(psid: int) -> threading.Lock:
    return _LOCK_STRIPES[psid % _LOCK_STRIPE_COUNT]


def _store_root() -> Path:
    path = Path(settings.OXIGRAPH_BASE_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path


def store_path(psid: int) -> Path:
    return _store_root() / str(psid)


def meta_path(psid: int) -> Path:
    return store_path(psid) / "meta.json"


def read_meta(psid: int) -> Dict[str, Any]:
    path = meta_path(psid)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def has_existing_store(psid: int) -> bool:
    return meta_path(psid).exists()
