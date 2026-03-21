import sys
from typing import Any, Callable, cast

from django.conf import settings
from django.core.checks import CheckMessage, Warning, register

CheckFunction = Callable[..., list[CheckMessage]]
register_check = cast(Callable[[CheckFunction], CheckFunction], register())


def _running_runserver() -> bool:
    return len(sys.argv) > 1 and sys.argv[1] == "runserver"


@register_check
def warn_if_runserver_without_debug(app_configs: Any, **kwargs: Any) -> list[CheckMessage]:
    if settings.DEBUG or not _running_runserver():
        return []

    # This is intentionally only a soft reminder for local development. We keep
    # the guard bypassable with Django's own `--skip-checks` flag so that
    # intentional DEBUG=0 experiments are still possible.
    return [
        Warning(
            "DJANGO_DEBUG should be enabled when using manage.py runserver.",
            hint=(
                "Set DJANGO_DEBUG=1 before starting the development server so Django serves "
                "the UI static files by default."
            ),
            id="petscan.W001",
        )
    ]
