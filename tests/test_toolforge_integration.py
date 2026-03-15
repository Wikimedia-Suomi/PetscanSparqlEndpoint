import unittest
from typing import Any, Dict, List, Mapping, Tuple

from django.conf import settings
from django.test import SimpleTestCase

from petscan import enrichment_sql, normalization, service_source
from petscan import service_links as links

ENWIKI = "enwiki"
PETSCAN_PARITY_PSID = 43641756
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
                api_title=normalization.normalize_page_title(title),
                db_title=normalization.normalize_page_title(title),
            )
            for title in titles
        ]

    @unittest.skipUnless(enrichment_sql.pymysql is not None, "pymysql is required for Toolforge SQL tests.")
    def test_sql_and_api_lookup_return_same_wikidata_ids(self):
        # Chosen to include non-ASCII titles so transliteration/normalization bugs are visible.
        targets = self._targets_for_titles(SAMPLE_TITLES)

        api_result = links.fetch_wikibase_enrichment_for_site(
            ENWIKI,
            targets,
            backend=links.LOOKUP_BACKEND_API,
        )
        sql_result = links.fetch_wikibase_enrichment_for_site(
            ENWIKI,
            targets,
            backend=links.LOOKUP_BACKEND_TOOLFORGE_SQL,
        )

        api_qids = {
            title: payload["wikidata_id"]
            for title, payload in api_result.items()
            if isinstance(payload, Mapping) and payload.get("wikidata_id") is not None
        }
        sql_qids = {
            title: payload["wikidata_id"]
            for title, payload in sql_result.items()
            if isinstance(payload, Mapping) and payload.get("wikidata_id") is not None
        }

        self.assertEqual(
            set(api_qids.keys()),
            {normalization.normalize_page_title(title) for title in SAMPLE_TITLES},
            "API lookup did not return all expected sample titles.",
        )
        self.assertEqual(sql_qids, api_qids)

    @staticmethod
    def _enrichment_diff_summary(
        api_result: Mapping[str, Mapping[str, Any]],
        sql_result: Mapping[str, Mapping[str, Any]],
        sample_size: int = 10,
    ) -> str:
        api_keys = set(api_result.keys())
        sql_keys = set(sql_result.keys())
        only_in_api = sorted(api_keys - sql_keys)
        only_in_sql = sorted(sql_keys - api_keys)

        payload_mismatches: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = []
        payload_mismatch_count = 0
        for link_uri in sorted(api_keys & sql_keys):
            if dict(api_result[link_uri]) != dict(sql_result[link_uri]):
                payload_mismatch_count += 1
                if len(payload_mismatches) < sample_size:
                    payload_mismatches.append(
                        (
                            link_uri,
                            dict(api_result[link_uri]),
                            dict(sql_result[link_uri]),
                        )
                    )

        return (
            "API keys: {}, SQL keys: {}, only_in_api: {} (sample: {}), "
            "only_in_sql: {} (sample: {}), payload_mismatches: {} (sample: {})"
        ).format(
            len(api_keys),
            len(sql_keys),
            len(only_in_api),
            only_in_api[:sample_size],
            len(only_in_sql),
            only_in_sql[:sample_size],
            payload_mismatch_count,
            payload_mismatches[:sample_size],
        )

    @unittest.skipUnless(enrichment_sql.pymysql is not None, "pymysql is required for Toolforge SQL tests.")
    def test_sql_and_api_enrichment_return_same_values_for_petscan_43641756(self):
        payload, source_url = service_source.fetch_petscan_json(PETSCAN_PARITY_PSID)
        records = service_source.extract_records(payload)
        self.assertTrue(records, "PetScan returned no records for {}".format(source_url))

        api_result = links.build_gil_link_enrichment_map(records, backend=links.LOOKUP_BACKEND_API)
        sql_result = links.build_gil_link_enrichment_map(records, backend=links.LOOKUP_BACKEND_TOOLFORGE_SQL)

        self.assertEqual(
            api_result,
            sql_result,
            msg=self._enrichment_diff_summary(api_result, sql_result),
        )
