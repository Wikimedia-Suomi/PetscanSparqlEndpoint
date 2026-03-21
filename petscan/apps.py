from django.apps import AppConfig


class PetscanConfig(AppConfig):  # type: ignore[misc]
    default_auto_field = "django.db.models.BigAutoField"
    name = "petscan"
