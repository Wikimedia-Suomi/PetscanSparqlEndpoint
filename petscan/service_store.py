"""Filesystem-backed Oxigraph store path and metadata helpers."""

import json
import shutil
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Set

from django.conf import settings

_LOCK_STRIPE_COUNT = 256
_LOCK_STRIPES = tuple(threading.Lock() for _ in range(_LOCK_STRIPE_COUNT))
_PRUNE_LOCK = threading.Lock()
_STORE_RETENTION_AGE = timedelta(days=1)
__all__ = [
    "get_psid_lock",
    "has_existing_store",
    "meta_path",
    "prune_expired_stores",
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


def _parse_loaded_at(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = "{}+00:00".format(text[:-1])
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _directory_timestamp(store_dir: Path) -> Optional[datetime]:
    meta_file = store_dir / "meta.json"
    if meta_file.exists():
        try:
            payload = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        loaded_at = _parse_loaded_at(payload.get("loaded_at") if isinstance(payload, dict) else None)
        if loaded_at is not None:
            return loaded_at
        meta_mtime = None
        try:
            meta_mtime = datetime.fromtimestamp(meta_file.stat().st_mtime, tz=timezone.utc)
        except OSError:
            meta_mtime = None
        if meta_mtime is not None:
            return meta_mtime
    try:
        return datetime.fromtimestamp(store_dir.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return None


def _parse_psid_dir(store_dir: Path) -> Optional[int]:
    try:
        psid = int(store_dir.name)
    except (TypeError, ValueError):
        return None
    return psid if psid > 0 else None


def prune_expired_stores(
    *,
    now: Optional[datetime] = None,
    exclude_psids: Optional[Sequence[int]] = None,
) -> Set[int]:
    current_time = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    excluded = set(exclude_psids or [])
    removed = set()  # type: Set[int]
    store_root = _store_root()

    with _PRUNE_LOCK:
        for child in store_root.iterdir():
            if not child.is_dir():
                continue
            psid = _parse_psid_dir(child)
            if psid is None or psid in excluded:
                continue

            timestamp = _directory_timestamp(child)
            if timestamp is None:
                continue
            if (current_time - timestamp) <= _STORE_RETENTION_AGE:
                continue

            lock = get_psid_lock(psid)
            with lock:
                if not child.exists():
                    continue
                try:
                    shutil.rmtree(child)
                except FileNotFoundError:
                    continue
                removed.add(psid)

    return removed
