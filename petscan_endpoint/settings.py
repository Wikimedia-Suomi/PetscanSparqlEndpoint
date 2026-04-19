import os
from pathlib import Path
from typing import Sequence

from django.core.exceptions import ImproperlyConfigured

BASE_DIR = Path(__file__).resolve().parent.parent


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name: str, default: Sequence[str]) -> list[str]:
    value = os.getenv(name)
    if value is None:
        return list(default)
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ImproperlyConfigured("{} environment variable is required.".format(name))
    return value


SECRET_KEY = _required_env("DJANGO_SECRET_KEY")
DEBUG = _env_bool("DJANGO_DEBUG", default=False)
ALLOWED_HOSTS = _env_list("DJANGO_ALLOWED_HOSTS", default=["127.0.0.1", "localhost", "testserver"])

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "petscan.apps.PetscanConfig",
    "incubator.apps.IncubatorConfig",
    "newpages.apps.NewpagesConfig",
    "pagepile.apps.PagepileConfig",
    "quarry.apps.QuarryConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "petscan_endpoint.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
            ],
        },
    },
]

WSGI_APPLICATION = "petscan_endpoint.wsgi.application"
ASGI_APPLICATION = "petscan_endpoint.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

PETSCAN_ENDPOINT = "https://petscan.wmcloud.org/"
PETSCAN_TIMEOUT_SECONDS = 120
OXIGRAPH_BASE_DIR = _required_env("OXIGRAPH_BASE_DIR")

WIKIDATA_LOOKUP_BACKEND = os.getenv("WIKIDATA_LOOKUP_BACKEND", "api")
INCUBATOR_API_ENDPOINT = os.getenv("INCUBATOR_API_ENDPOINT", "https://incubator.wikimedia.org/w/api.php")
PAGEPILE_API_ENDPOINT = os.getenv("PAGEPILE_API_ENDPOINT", "https://pagepile.toolforge.org/api.php")
NEWPAGES_SITEMATRIX_API_ENDPOINT = os.getenv(
    "NEWPAGES_SITEMATRIX_API_ENDPOINT",
    "https://meta.wikimedia.org/w/api.php",
)
INCUBATOR_NAMESPACE_OPTIONS = (
    {"id": 0, "label": "Main"},
    {"id": 4, "label": "Project", "url_prefix": "Incubator"},
    {"id": 10, "label": "Template"},
    {"id": 14, "label": "Category"},
    {"id": 828, "label": "Module"},
)
TOOLFORGE_USE_REPLICA = _env_bool("TOOLFORGE_USE_REPLICA", default=False)
TOOLFORGE_REPLICA_CNF = os.getenv("TOOLFORGE_REPLICA_CNF", "")
TOOLFORGE_INTEGRATION_TESTS = _env_bool("TOOLFORGE_INTEGRATION_TESTS", default=False)
LIVE_API_INTEGRATION_TESTS = _env_bool("LIVE_API_INTEGRATION_TESTS", default=False)
GRAPH_PARITY_REGRESSION_TESTS = _env_bool("GRAPH_PARITY_REGRESSION_TESTS", default=False)
PERFORMANCE_BASELINE_TESTS = _env_bool("PERFORMANCE_BASELINE_TESTS", default=False)
