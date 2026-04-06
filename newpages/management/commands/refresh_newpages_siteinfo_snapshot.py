import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from newpages import service_source as source


class Command(BaseCommand):  # type: ignore[misc]
    help = "Refresh the local newpages siteinfo snapshot for all currently supported Wikimedia wikis."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--output",
            default=str(source._SITEINFO_SNAPSHOT_PATH),
            help="Path to the JSON snapshot file to write.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        output_path = Path(str(options["output"])).expanduser()

        try:
            descriptors = [
                descriptor
                for descriptor in source._known_wikis_by_domain().values()
                if source._is_supported_wiki_descriptor(descriptor)
            ]
        except Exception as exc:
            raise CommandError("Failed to load supported wiki list from SiteMatrix: {}".format(exc)) from exc

        snapshot_siteinfo: dict[str, Any] = {}
        for descriptor in sorted(descriptors, key=lambda item: item.domain):
            self.stdout.write("fetching siteinfo for {}".format(descriptor.domain))
            try:
                siteinfo = source._fetch_siteinfo_from_api(descriptor.domain)
            except Exception as exc:
                raise CommandError("Failed to fetch siteinfo for {}: {}".format(descriptor.domain, exc)) from exc
            snapshot_siteinfo[descriptor.domain] = source._siteinfo_to_snapshot_entry(siteinfo)

        output_payload = {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "siteinfo": snapshot_siteinfo,
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(output_payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )

        if output_path.resolve() == source._SITEINFO_SNAPSHOT_PATH.resolve():
            source._siteinfo_snapshot_by_domain.cache_clear()
            source._siteinfo_for_domain.cache_clear()

        self.stdout.write(
            self.style.SUCCESS(
                "Wrote {} supported wiki siteinfo entries to {}.".format(
                    len(snapshot_siteinfo),
                    output_path,
                )
            )
        )
