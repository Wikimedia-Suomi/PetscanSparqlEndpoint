import io
import json
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

from django.core.management import call_command
from django.test import SimpleTestCase

from newpages import service_source


class RefreshNewpagesSiteinfoSnapshotCommandTests(SimpleTestCase):
    @patch("newpages.management.commands.refresh_newpages_siteinfo_snapshot.source._fetch_siteinfo_from_api")
    @patch("newpages.management.commands.refresh_newpages_siteinfo_snapshot.source._known_wikis_by_domain")
    def test_command_writes_supported_siteinfo_snapshot_json(
        self,
        known_wikis_by_domain_mock: Any,
        fetch_siteinfo_from_api_mock: Any,
    ) -> None:
        known_wikis_by_domain_mock.return_value = {
            "fi.wikipedia.org": service_source._WikiDescriptor(
                domain="fi.wikipedia.org",
                dbname="fiwiki",
                lang_code="fi",
                wiki_group="wikipedia",
                site_url="https://fi.wikipedia.org/",
            ),
            "meta.wikimedia.org": service_source._WikiDescriptor(
                domain="meta.wikimedia.org",
                dbname="metawiki",
                lang_code="en",
                wiki_group="wikimedia",
                site_url="https://meta.wikimedia.org/",
                site_code="meta",
            ),
            "species.wikimedia.org": service_source._WikiDescriptor(
                domain="species.wikimedia.org",
                dbname="specieswiki",
                lang_code="en",
                wiki_group="wikimedia",
                site_url="https://species.wikimedia.org/",
                site_code="species",
            ),
        }
        fetch_siteinfo_from_api_mock.side_effect = [
            service_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="fi",
                namespace_names={0: "", 2: "Käyttäjä"},
                namespace_aliases={2: ("Käyttäjä", "User")},
            ),
            service_source._SiteInfo(
                article_path="/wiki/$1",
                lang_code="en",
                namespace_names={0: "", 2: "User"},
                namespace_aliases={2: ("User",)},
            ),
        ]
        stdout = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "siteinfo_snapshot.json"

            call_command(
                "refresh_newpages_siteinfo_snapshot",
                "--output",
                str(output_path),
                stdout=stdout,
            )

            written_payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(
            [call.args[0] for call in fetch_siteinfo_from_api_mock.call_args_list],
            ["fi.wikipedia.org", "meta.wikimedia.org"],
        )
        self.assertIn("generated_at", written_payload)
        self.assertEqual(
            sorted(written_payload["siteinfo"].keys()),
            ["fi.wikipedia.org", "meta.wikimedia.org"],
        )
        self.assertNotIn("species.wikimedia.org", written_payload["siteinfo"])
        self.assertEqual(
            written_payload["siteinfo"]["fi.wikipedia.org"]["namespace_aliases"]["2"],
            ["K\u00e4ytt\u00e4j\u00e4", "User"],
        )
        self.assertIn("Wrote 2 supported wiki siteinfo entries", stdout.getvalue())
