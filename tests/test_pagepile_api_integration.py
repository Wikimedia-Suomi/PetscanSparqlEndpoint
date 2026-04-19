import os
import re
import unittest
from typing import Mapping

from django.conf import settings
from django.test import SimpleTestCase

from pagepile import service_source

QID_RE = re.compile(r"^Q[1-9][0-9]*$")
HTTPS_URL_RE = re.compile(r"^https://")


def _live_pagepile_id() -> int:
    configured = str(os.getenv("PAGEPILE_LIVE_ID", "112306")).strip()
    return int(configured or "112306")


def _live_pagepile_limit() -> int:
    configured = str(os.getenv("PAGEPILE_LIVE_LIMIT", "5")).strip()
    return int(configured or "5")


def _live_pagepile_commons_id() -> int:
    configured = str(os.getenv("PAGEPILE_LIVE_COMMONS_ID", "112301")).strip()
    return int(configured or "112301")


def _live_pagepile_commons_limit() -> int:
    configured = str(os.getenv("PAGEPILE_LIVE_COMMONS_LIMIT", "10")).strip()
    return int(configured or "10")


@unittest.skipUnless(
    bool(getattr(settings, "LIVE_API_INTEGRATION_TESTS", False)),
    "Live MediaWiki API integration tests are disabled.",
)
class LivePagepileApiTests(SimpleTestCase):
    def test_live_pagepile_json_returns_real_payload(self) -> None:
        pagepile_id = _live_pagepile_id()

        payload, source_url = service_source.fetch_pagepile_json(pagepile_id)

        self.assertIsInstance(payload, Mapping)
        self.assertEqual(int(payload["id"]), pagepile_id)
        self.assertIsInstance(payload.get("wiki"), str)
        self.assertTrue(str(payload.get("wiki", "")).strip())
        self.assertIsInstance(payload.get("pages"), list)
        self.assertGreater(len(payload["pages"]), 0)
        self.assertEqual(source_url, service_source.build_pagepile_json_url(pagepile_id))

    def test_live_pagepile_records_api_return_real_sitelink_rows(self) -> None:
        pagepile_id = _live_pagepile_id()
        limit = _live_pagepile_limit()

        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            records, source_url = service_source.fetch_pagepile_records(pagepile_id, limit=limit)

        self.assertEqual(source_url, service_source.build_pagepile_json_url(pagepile_id, limit=limit))
        self.assertGreater(len(records), 0)
        self.assertLessEqual(len(records), limit)

        qid_record_count = 0
        for record in records:
            self.assertIsInstance(record, Mapping)

            page_url = record.get("page_url")
            self.assertIsInstance(page_url, str)
            self.assertRegex(str(page_url), HTTPS_URL_RE)

            page_id = record.get("page_id")
            self.assertIsInstance(page_id, int)
            assert isinstance(page_id, int)
            self.assertGreater(page_id, 0)

            page_title = record.get("page_title")
            self.assertIsInstance(page_title, str)
            self.assertTrue(str(page_title).strip())

            page_label = record.get("page_label")
            self.assertIsInstance(page_label, str)
            self.assertTrue(str(page_label).strip())

            namespace = record.get("namespace")
            self.assertIsInstance(namespace, int)
            assert isinstance(namespace, int)
            self.assertGreaterEqual(namespace, 0)

            site_url = record.get("site_url")
            self.assertIsInstance(site_url, str)
            self.assertRegex(str(site_url), HTTPS_URL_RE)

            wiki_domain = record.get("wiki_domain")
            self.assertIsInstance(wiki_domain, str)
            self.assertTrue(str(wiki_domain).strip())

            wiki_dbname = record.get("wiki_dbname")
            self.assertIsInstance(wiki_dbname, str)
            self.assertTrue(str(wiki_dbname).strip())

            wiki_group = record.get("wiki_group")
            self.assertIsInstance(wiki_group, str)
            self.assertTrue(str(wiki_group).strip())

            lang_code = record.get("lang_code")
            if lang_code is not None:
                self.assertIsInstance(lang_code, str)
                self.assertTrue(str(lang_code).strip())

            qid = record.get("wikidata_id")
            entity = record.get("wikidata_entity")
            if qid is not None:
                qid_record_count += 1
                self.assertIsInstance(qid, str)
                self.assertRegex(str(qid), QID_RE)
                self.assertEqual(entity, "http://www.wikidata.org/entity/{}".format(qid))

        self.assertGreater(
            qid_record_count,
            0,
            msg="Live PagePile sample did not resolve any Wikidata-linked rows.",
        )

    def test_live_pagepile_records_api_adds_commons_mediaitem_entity_for_file_pages(self) -> None:
        pagepile_id = _live_pagepile_commons_id()
        limit = _live_pagepile_commons_limit()

        with self.settings(WIKIDATA_LOOKUP_BACKEND="api"):
            records, source_url = service_source.fetch_pagepile_records(pagepile_id, limit=limit)

        self.assertEqual(source_url, service_source.build_pagepile_json_url(pagepile_id, limit=limit))
        self.assertGreater(len(records), 0)
        self.assertLessEqual(len(records), limit)

        commons_file_records = [
            record
            for record in records
            if record.get("wiki_dbname") == "commonswiki" and record.get("namespace") == 6
        ]
        self.assertGreater(
            len(commons_file_records),
            0,
            msg="Live Commons PagePile sample did not return any namespace-6 file rows.",
        )

        for record in commons_file_records:
            page_id = record.get("page_id")
            self.assertIsInstance(page_id, int)
            assert isinstance(page_id, int)
            self.assertGreater(page_id, 0)

            self.assertEqual(record.get("wiki_domain"), "commons.wikimedia.org")
            self.assertEqual(record.get("site_url"), "https://commons.wikimedia.org/")
            self.assertEqual(record.get("wiki_group"), "commons")
            self.assertIsNone(record.get("wikidata_id"))
            self.assertEqual(
                record.get("wikidata_entity"),
                "https://commons.wikimedia.org/entity/M{}".format(page_id),
            )
