import json
import os
import unittest
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from django.conf import settings
from django.test import SimpleTestCase

from incubator import service_source as incubator_source
from petscan import enrichment_sql

TEST_USER_AGENT = "PetscanSparqlEndpoint incubator namespace integration tests"
_IGNORED_NON_TALK_NAMESPACE_IDS = {
    2,     # User
    6,     # File
    8,     # MediaWiki
    12,    # Help
    710,   # TimedText
    1198,  # Translations
}
_HIDDEN_INCUBATOR_NAMESPACE_IDS = (6, 8, 12)


@unittest.skipUnless(
    bool(getattr(settings, "LIVE_API_INTEGRATION_TESTS", False)),
    "Live MediaWiki API integration tests are disabled.",
)
class LiveIncubatorNamespaceOptionsTests(SimpleTestCase):
    def test_configured_namespace_options_match_live_siteinfo(self) -> None:
        request_url = "{}?{}".format(
            str(getattr(settings, "INCUBATOR_API_ENDPOINT")),
            urlencode(
                {
                    "action": "query",
                    "meta": "siteinfo",
                    "siprop": "namespaces",
                    "format": "json",
                }
            ),
        )
        request = Request(
            request_url,
            headers={
                "Accept": "application/json",
                "User-Agent": TEST_USER_AGENT,
            },
        )

        with urlopen(request, timeout=30) as response:  # nosec B310
            payload = json.loads(response.read().decode("utf-8"))

        self.assertIsInstance(payload, Mapping)
        query_payload = payload.get("query")
        self.assertIsInstance(query_payload, Mapping)
        namespaces_payload = query_payload.get("namespaces")
        self.assertIsInstance(namespaces_payload, Mapping)

        expected = incubator_source.available_incubator_namespace_options()
        expected_ids = {int(str(option["id"])) for option in expected}

        live_non_talk_namespaces: list[dict[str, object]] = []
        live_non_talk_ids = set()
        for raw_id, raw_namespace_payload in namespaces_payload.items():
            self.assertIsInstance(raw_namespace_payload, Mapping, msg="namespace {}".format(raw_id))
            namespace_id = int(str(raw_id))
            if namespace_id < 0 or namespace_id % 2 != 0:
                continue
            live_non_talk_ids.add(namespace_id)
            if namespace_id in _IGNORED_NON_TALK_NAMESPACE_IDS:
                continue

            label = str(raw_namespace_payload.get("canonical", "")).strip()
            if not label:
                label = str(raw_namespace_payload.get("*", "")).strip() or "Main"
            live_non_talk_namespaces.append({"id": namespace_id, "label": label})

        live_non_talk_namespaces.sort(key=lambda option: int(str(option["id"])))

        unclassified_ids = sorted(
            namespace_id
            for namespace_id in live_non_talk_ids
            if namespace_id not in expected_ids and namespace_id not in _IGNORED_NON_TALK_NAMESPACE_IDS
        )
        self.assertEqual(
            unclassified_ids,
            [],
            msg=(
                "Live siteinfo contains new non-talk namespaces not covered by the static "
                "Incubator namespace config or ignored-ID list: {}"
            ).format(unclassified_ids),
        )
        self.assertEqual(live_non_talk_namespaces, expected)


@unittest.skipUnless(
    bool(getattr(settings, "TOOLFORGE_INTEGRATION_TESTS", False)),
    "Toolforge integration tests are disabled.",
)
@unittest.skipUnless(enrichment_sql.pymysql is not None, "pymysql is required for Toolforge SQL tests.")
class HiddenIncubatorNamespaceReplicaTests(SimpleTestCase):
    def test_hidden_namespaces_still_have_no_wikidata_linked_pages(self) -> None:
        replica_cnf = str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip()
        timeout = int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30))

        connect_kwargs: dict[str, Any] = {
            "host": incubator_source._INCUBATOR_REPLICA_HOST,
            "database": incubator_source._INCUBATOR_REPLICA_DB,
            "charset": "utf8mb4",
            "connect_timeout": timeout,
            "read_timeout": timeout,
            "write_timeout": timeout,
            "autocommit": True,
        }
        if replica_cnf:
            connect_kwargs["read_default_file"] = os.path.expanduser(os.path.expandvars(replica_cnf))

        placeholders = ", ".join(["%s"] * len(_HIDDEN_INCUBATOR_NAMESPACE_IDS))
        sql = (
            "SELECT p.page_namespace, p.page_title, cl.cl_sortkey_prefix "
            "FROM page AS p "
            "JOIN categorylinks AS cl ON cl.cl_from = p.page_id "
            "JOIN linktarget AS lt ON lt.lt_id = cl.cl_target_id "
            "WHERE lt.lt_namespace = 14 "
            "AND lt.lt_title = %s "
            "AND p.page_namespace IN ({}) "
            "AND cl.cl_sortkey_prefix REGEXP %s "
            "ORDER BY p.page_namespace ASC, p.page_title ASC "
            "LIMIT 20"
        ).format(placeholders)
        params: list[Any] = [
            incubator_source._INCUBATOR_WIKIDATA_CATEGORY_DB_TITLE,
            *_HIDDEN_INCUBATOR_NAMESPACE_IDS,
            "^Q[1-9][0-9]*$",
        ]

        connection = None
        try:
            connection = enrichment_sql.pymysql.connect(**connect_kwargs)
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        finally:
            if connection is not None:
                connection.close()

        findings = []  # type: list[str]
        for row in rows:
            if not isinstance(row, (tuple, list)) or len(row) < 3:
                continue
            findings.append(
                "ns={} title={} qid={}".format(
                    int(row[0]),
                    incubator_source._normalize_db_page_title(row[1]),
                    str(row[2]),
                )
            )

        self.assertEqual(
            findings,
            [],
            msg=(
                "Hidden Incubator namespaces now contain Wikidata-linked pages. "
                "Please report this and consider adding the namespace to the visible filter list. "
                "Sample rows: {}"
            ).format(findings),
        )
