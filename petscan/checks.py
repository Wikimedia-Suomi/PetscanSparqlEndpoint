import sys
from typing import Any, Callable, cast

from django.conf import settings
from django.core.checks import CheckMessage, Error, register

CheckFunction = Callable[..., list[CheckMessage]]
register_check = cast(Callable[[CheckFunction], CheckFunction], register())


def _running_runserver() -> bool:
    return len(sys.argv) > 1 and sys.argv[1] == "runserver"


@register_check
def error_if_runserver_without_debug(app_configs: Any, **kwargs: Any) -> list[CheckMessage]:
    if settings.DEBUG or not _running_runserver():
        return []

    return [
        Error(
            "DJANGO_DEBUG must be enabled when using manage.py runserver.",
            hint=(
                "Set DJANGO_DEBUG=1 before starting the development server so Django serves "
                "static files correctly."
            ),
            id="petscan.E001",
        )
    ]
