import unittest
from typing import List

from django.conf import settings
from django.test import SimpleTestCase

from petscan import enrichment_sql
from petscan import service_links as links

ENWIKI = "enwiki"
SAMPLE_TITLES = [
    "Albert_Einstein",
    "Málaga",
    "São_Paulo",
    "Łódź",
    "Beyoncé",
]


@unittest.skipUnless(
    bool(getattr(settings, "TOOLFORGE_INTEGRATION_TESTS", False)),
    "Toolforge integration tests are disabled.",
)
class ToolforgeWikidataLookupParityTests(SimpleTestCase):
    @staticmethod
    def _targets_for_titles(titles: List[str]) -> List[links.SiteLookupTarget]:
        return [
            links.SiteLookupTarget(
                namespace=0,
                api_title=links.normalize_page_title(title),
                db_title=links.normalize_page_title(title),
            )
            for title in titles
        ]

    @unittest.skipUnless(enrichment_sql.pymysql is not None, "pymysql is required for Toolforge SQL tests.")
    def test_sql_and_api_lookup_return_same_wikidata_ids(self):
        # Chosen to include non-ASCII titles so transliteration/normalization bugs are visible.
        targets = self._targets_for_titles(SAMPLE_TITLES)

        api_result = links.fetch_wikibase_items_for_site(
            ENWIKI,
            targets,
            backend=links.LOOKUP_BACKEND_API,
        )
        sql_result = links.fetch_wikibase_items_for_site(
            ENWIKI,
            targets,
            backend=links.LOOKUP_BACKEND_TOOLFORGE_SQL,
        )

        self.assertEqual(
            set(api_result.keys()),
            {links.normalize_page_title(title) for title in SAMPLE_TITLES},
            "API lookup did not return all expected sample titles.",
        )
        self.assertEqual(sql_result, api_result)
