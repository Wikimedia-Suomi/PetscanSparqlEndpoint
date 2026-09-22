"""CentralAuth registration-date enrichment for PetScan file uploaders."""

from collections.abc import Mapping as RuntimeMapping
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

from django.conf import settings

from . import enrichment_api, enrichment_sql
from .service_source import HTTP_USER_AGENT

IMG_USER_REGISTRATION_FIELD = "img_user_registration"
LOOKUP_BACKEND_API = "api"
LOOKUP_BACKEND_TOOLFORGE_SQL = "toolforge_sql"

__all__ = [
    "LOOKUP_BACKEND_API",
    "LOOKUP_BACKEND_TOOLFORGE_SQL",
    "IMG_USER_REGISTRATION_FIELD",
    "build_img_user_registration_by_name",
    "file_user_name",
    "img_user_registration_for_record",
    "user_registration_lookup_backend",
]


def _normalize_user_name(value: object) -> str:
    text = str(value or "").strip().replace("_", " ")
    normalized = " ".join(text.split())
    if not normalized:
        return ""
    return normalized[0].upper() + normalized[1:]


def file_user_name(record: Mapping[str, Any]) -> str:
    user_name = _normalize_user_name(record.get("img_user_text"))
    if user_name:
        return user_name

    metadata = record.get("metadata")
    if isinstance(metadata, RuntimeMapping):
        return _normalize_user_name(metadata.get("img_user_text"))
    return ""


def user_registration_lookup_backend() -> str:
    configured = str(
        getattr(settings, "PETSCAN_USER_REGISTRATION_LOOKUP_BACKEND", "") or ""
    ).strip().lower()
    if configured in {LOOKUP_BACKEND_API, LOOKUP_BACKEND_TOOLFORGE_SQL}:
        return configured
    if bool(getattr(settings, "TOOLFORGE_USE_REPLICA", False)):
        return LOOKUP_BACKEND_TOOLFORGE_SQL
    return LOOKUP_BACKEND_API


def _registration_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    text_value = str(value).strip()
    if not text_value:
        return None

    if len(text_value) == 14 and text_value.isdigit():
        try:
            return datetime.strptime(text_value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    normalized = "{}+00:00".format(text_value[:-1]) if text_value.endswith("Z") else text_value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _unique_file_user_names(records: Sequence[Mapping[str, Any]]) -> List[str]:
    user_names: List[str] = []
    seen: set[str] = set()
    for record in records:
        user_name = file_user_name(record)
        if not user_name or user_name in seen:
            continue
        seen.add(user_name)
        user_names.append(user_name)
    return user_names


def _fetch_registrations(user_names: Sequence[str]) -> Dict[str, str]:
    timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))
    backend = user_registration_lookup_backend()
    if backend == LOOKUP_BACKEND_TOOLFORGE_SQL:
        return enrichment_sql.fetch_global_user_registrations_sql(
            user_names,
            timeout_seconds=timeout,
            replica_cnf=str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip(),
        )

    api_url = str(
        getattr(
            settings,
            "CENTRALAUTH_API_ENDPOINT",
            "https://meta.wikimedia.org/w/api.php",
        )
    ).strip()
    return enrichment_api.fetch_global_user_registrations_api(
        api_url,
        user_names,
        user_agent=HTTP_USER_AGENT,
        timeout_seconds=timeout,
    )


def build_img_user_registration_by_name(
    records: Sequence[Mapping[str, Any]],
) -> Dict[str, str]:
    user_names = _unique_file_user_names(records)
    if not user_names:
        return {}

    registrations = _fetch_registrations(user_names)
    registrations_by_name = {
        _normalize_user_name(user_name): registration
        for user_name, registration in registrations.items()
        if _normalize_user_name(user_name)
    }

    result: Dict[str, str] = {}
    for user_name in user_names:
        registration = _registration_datetime(registrations_by_name.get(user_name))
        if registration is not None:
            result[user_name] = registration.replace(microsecond=0).isoformat().replace(
                "+00:00", "Z"
            )
    return result


def img_user_registration_for_record(
    record: Mapping[str, Any],
    img_user_registration_by_name: Mapping[str, str],
) -> Optional[str]:
    user_name = file_user_name(record)
    if not user_name:
        return None
    return img_user_registration_by_name.get(user_name)
