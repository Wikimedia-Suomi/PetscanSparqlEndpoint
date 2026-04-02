import json
import unittest
from typing import Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from django.conf import settings
from django.test import SimpleTestCase

from incubator import service_source

TEST_USER_AGENT = "PetscanSparqlEndpoint incubator namespace integration tests"
_IGNORED_NON_TALK_NAMESPACE_IDS = {
    2,     # User
    710,   # TimedText
    1198,  # Translations
}


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

        expected = service_source.available_incubator_namespace_options()
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
