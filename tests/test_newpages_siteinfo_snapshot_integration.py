import unittest

from django.conf import settings
from django.test import SimpleTestCase

from newpages import service_source


@unittest.skipUnless(
    bool(getattr(settings, "LIVE_API_INTEGRATION_TESTS", False)),
    "Live MediaWiki API integration tests are disabled.",
)
class LiveNewpagesSiteinfoSnapshotTests(SimpleTestCase):
    def test_local_siteinfo_snapshot_covers_all_currently_supported_wikis(self) -> None:
        service_source._known_wikis_by_domain.cache_clear()
        service_source._siteinfo_snapshot_by_domain.cache_clear()

        live_supported_domains = sorted(
            domain
            for domain, descriptor in service_source._known_wikis_by_domain().items()
            if service_source._is_supported_wiki_descriptor(descriptor)
        )
        snapshot_domains = set(service_source._siteinfo_snapshot_by_domain().keys())

        missing_domains = [domain for domain in live_supported_domains if domain not in snapshot_domains]
        self.assertEqual(
            missing_domains,
            [],
            msg=(
                "Local newpages siteinfo snapshot is missing currently supported wikis: {}. "
                "Run refresh_newpages_siteinfo_snapshot to update {}."
            ).format(
                ", ".join(missing_domains),
                service_source._SITEINFO_SNAPSHOT_PATH,
            ),
        )
