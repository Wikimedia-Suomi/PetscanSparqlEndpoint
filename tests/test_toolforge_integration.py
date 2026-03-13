import unittest

from django.conf import settings
from django.test import SimpleTestCase

from petscan import service


@unittest.skipUnless(
    bool(getattr(settings, "TOOLFORGE_INTEGRATION_TESTS", False)),
    "Toolforge integration tests are disabled.",
)
class ToolforgeWikidataLookupParityTests(SimpleTestCase):
    @unittest.skipUnless(service.pymysql is not None, "pymysql is required for Toolforge SQL tests.")
    def test_sql_and_api_lookup_return_same_wikidata_ids(self):
        # Chosen to include non-ASCII titles so transliteration/normalization bugs are visible.
        sample_titles = [
            "Albert_Einstein",
            "Málaga",
            "São_Paulo",
            "Łódź",
            "Beyoncé",
        ]
        targets = [(0, service._normalize_page_title(title), service._normalize_page_title(title)) for title in sample_titles]

        api_result = service._fetch_wikibase_items_for_site(
            "enwiki",
            targets,
            backend=service._LOOKUP_BACKEND_API,
        )
        sql_result = service._fetch_wikibase_items_for_site(
            "enwiki",
            targets,
            backend=service._LOOKUP_BACKEND_TOOLFORGE_SQL,
        )

        self.assertEqual(
            set(api_result.keys()),
            {service._normalize_page_title(title) for title in sample_titles},
            "API lookup did not return all expected sample titles.",
        )
        self.assertEqual(sql_result, api_result)
