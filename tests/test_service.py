import json
import shutil
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from django.conf import settings
from django.test import SimpleTestCase

from petscan import service


class PetscanServiceParsingTests(SimpleTestCase):
    def _load_example_payload(self):
        example_path = Path(settings.BASE_DIR) / "data" / "examples" / "petscan-43641756.json"
        return json.loads(example_path.read_text(encoding="utf-8"))

    def _load_second_example_payload(self):
        example_path = Path(settings.BASE_DIR) / "data" / "examples" / "petscan-43642782.json"
        return json.loads(example_path.read_text(encoding="utf-8"))

    def test_extract_records_from_example_json_has_expected_count(self):
        payload = self._load_example_payload()

        records = service._extract_records(payload)

        self.assertEqual(len(records), 2638)

    def test_structure_summary_contains_title_field_and_predicate(self):
        payload = self._load_example_payload()
        records = service._extract_records(payload)

        summary = service._summarize_structure(records)
        field_map = {field["source_key"]: field for field in summary["fields"]}

        self.assertEqual(summary["row_count"], 2638)
        self.assertGreater(summary["field_count"], 0)
        self.assertIn("title", field_map)
        self.assertEqual(
            field_map["title"]["predicate"],
            "https://petscan.wmcloud.org/ontology/title",
        )
        self.assertIn("id", field_map)
        self.assertEqual(
            field_map["id"]["predicate"],
            "https://petscan.wmcloud.org/ontology/page_id",
        )
        self.assertIn("gil_link", field_map)
        self.assertIn("gil_link_count", field_map)

    def test_gil_field_is_parsed_to_individual_links(self):
        payload = self._load_example_payload()
        records = service._extract_records(payload)

        first_row = records[0]
        rdf_fields = list(service._iter_scalar_fields(first_row))

        raw_gil_values = [value for key, value in rdf_fields if key == "gil"]
        parsed_counts = [value for key, value in rdf_fields if key == "gil_link_count"]

        self.assertEqual(len(raw_gil_values), 1)
        self.assertEqual(len(parsed_counts), 1)

        raw_links = [part.strip() for part in raw_gil_values[0].split("|") if part.strip()]
        expected_links = []
        for raw_link in raw_links:
            target = service._parse_gil_link_target(raw_link)
            if target is None:
                continue
            site, _namespace, title = target
            uri = service._gil_link_uri(site, title)
            if uri is not None:
                expected_links.append(uri)

        parsed_links = service._iter_gil_link_uris(first_row)
        self.assertEqual(parsed_links, expected_links)
        self.assertEqual(parsed_counts[0], len(expected_links))

    def test_gil_link_is_converted_to_wiki_uri_format(self):
        record = {"gil": "enwiki:0:Federalist_No._42"}
        parsed_links = service._iter_gil_link_uris(record)

        self.assertEqual(parsed_links, ["https://en.wikipedia.org/wiki/Federalist_No._42"])

    def test_item_subject_for_commons_file_uses_commons_entity_iri(self):
        record = {
            "id": 574781,
            "namespace": 6,
            "nstext": "File",
            "img_media_type": "AUDIO",
        }
        subject = service._item_subject(43641756, record, 0)

        self.assertEqual(subject.value, "https://commons.wikimedia.org/entity/M574781")

    def test_item_subject_for_non_commons_record_uses_local_psid_item_iri(self):
        record = {
            "id": 42,
            "namespace": 0,
            "nstext": "",
            "title": "Example",
        }
        subject = service._item_subject(43641756, record, 0)

        self.assertEqual(subject.value, "https://petscan.wmcloud.org/psid/43641756/item/42")

    def test_item_subject_for_wikidata_id_uses_wikidata_entity_iri(self):
        record = {
            "id": 574781,
            "namespace": 6,
            "nstext": "File",
            "img_media_type": "AUDIO",
            "wikidata_id": "Q378619",
        }
        subject = service._item_subject(43641756, record, 0)

        self.assertEqual(subject.value, "https://commons.wikimedia.org/entity/M574781")

    def test_item_subject_for_non_commons_wikidata_id_uses_wikidata_entity_iri(self):
        record = {
            "id": 123,
            "namespace": 0,
            "nstext": "",
            "title": "Example city",
            "wikidata_id": "Q378619",
        }
        subject = service._item_subject(43641756, record, 0)

        self.assertEqual(subject.value, "http://www.wikidata.org/entity/Q378619")

    def test_extract_records_from_second_example_has_expected_count(self):
        payload = self._load_second_example_payload()

        records = service._extract_records(payload)

        self.assertEqual(len(records), 23)

    def test_second_example_parses_qid_thumbnail_and_coordinates(self):
        payload = self._load_second_example_payload()
        records = service._extract_records(payload)

        first_row = records[0]
        rdf_fields = list(service._iter_scalar_fields(first_row))
        fields_by_key: Dict[str, List[Any]] = {}
        for key, value in rdf_fields:
            fields_by_key.setdefault(key, []).append(value)

        self.assertIn("qid", fields_by_key)
        self.assertIn("Q38511", fields_by_key["qid"])
        self.assertIn("wikidata_entity", fields_by_key)
        self.assertIn(
            "https://www.wikidata.org/entity/Q38511",
            fields_by_key["wikidata_entity"],
        )

        self.assertIn("thumbnail_image", fields_by_key)
        self.assertTrue(
            fields_by_key["thumbnail_image"][0].startswith(
                "https://commons.wikimedia.org/wiki/Special:FilePath/Turku_postcard_2013.png?width="
            )
        )

        self.assertIn("coordinate_lat", fields_by_key)
        self.assertIn("coordinate_lon", fields_by_key)
        self.assertAlmostEqual(fields_by_key["coordinate_lat"][0], 60.45138889)
        self.assertAlmostEqual(fields_by_key["coordinate_lon"][0], 22.26666667)

    def test_second_example_structure_summary_includes_derived_fields(self):
        payload = self._load_second_example_payload()
        records = service._extract_records(payload)
        summary = service._summarize_structure(records)
        field_map = {field["source_key"]: field for field in summary["fields"]}

        self.assertIn("qid", field_map)
        self.assertIn("wikidata_entity", field_map)
        self.assertIn("thumbnail_image", field_map)
        self.assertIn("coordinate_lat", field_map)
        self.assertIn("coordinate_lon", field_map)

    def test_build_petscan_url_forwards_extra_query_params(self):
        url = service._build_petscan_url(
            43641756,
            petscan_params={
                "category": ["Turku"],
                "language": "fi",
                "psid": "999",
                "format": "xml",
            },
        )
        parsed_query = parse_qs(urlparse(url).query)

        self.assertEqual(parsed_query.get("psid"), ["43641756"])
        self.assertEqual(parsed_query.get("format"), ["json"])
        self.assertEqual(parsed_query.get("category"), ["Turku"])
        self.assertEqual(parsed_query.get("language"), ["fi"])

    def test_site_to_mediawiki_api_url_returns_valid_https_url(self):
        url = service._site_to_mediawiki_api_url("enwiki")
        parsed = urlparse(url)

        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "en.wikipedia.org")
        self.assertEqual(parsed.path, "/w/api.php")

    def test_site_to_mediawiki_api_url_rejects_malformed_site_tokens(self):
        malformed_sites = [
            "localhost/wiki",
            "a/bwiki",
            "foo..wiki",
            "evil.comwiki",
            "evil.com/wikivoyage",
            " ",
        ]
        for site in malformed_sites:
            self.assertIsNone(service._site_to_mediawiki_api_url(site), msg=site)

    def test_meta_source_params_must_match_requested_params(self):
        meta = {"source_params": {"category": ["Turku"], "language": ["fi"]}}

        self.assertTrue(
            service._meta_has_matching_source_params(meta, {"category": ["Turku"], "language": "fi"})
        )
        self.assertFalse(service._meta_has_matching_source_params(meta, {"category": ["Helsinki"]}))

    @patch("petscan.service._build_store")
    @patch("petscan.service._extract_records")
    @patch("petscan.service._fetch_petscan_json")
    @patch("petscan.service._ensure_oxigraph")
    def test_ensure_loaded_rebuilds_when_meta_json_is_corrupt(
        self,
        _ensure_oxigraph_mock,
        fetch_petscan_json_mock,
        extract_records_mock,
        build_store_mock,
    ):
        psid = 999987
        store_path = service._store_path(psid)
        meta_path = service._meta_path(psid)

        fetch_petscan_json_mock.return_value = ({"*": [{"id": 1, "title": "Example"}]}, "https://example.invalid")
        extract_records_mock.return_value = [{"id": 1, "title": "Example"}]
        build_store_mock.return_value = {
            "psid": psid,
            "records": 1,
            "source_url": "https://example.invalid",
            "source_params": {},
            "loaded_at": "2026-01-01T00:00:00+00:00",
        }

        try:
            store_path.mkdir(parents=True, exist_ok=True)
            meta_path.write_text("{invalid-json", encoding="utf-8")

            result = service.ensure_loaded(psid, refresh=False)

            self.assertEqual(result["psid"], psid)
            fetch_petscan_json_mock.assert_called_once_with(psid, petscan_params={})
            extract_records_mock.assert_called_once()
            build_store_mock.assert_called_once()
        finally:
            shutil.rmtree(store_path, ignore_errors=True)

    @patch("petscan.service._wikidata_lookup_backend", return_value=service._LOOKUP_BACKEND_API)
    @patch("petscan.service._fetch_wikibase_items_for_site_api")
    def test_gil_wikidata_lookup_batches_by_site_and_max_50_titles(self, fetch_mock, _backend_mock):
        def fake_fetch(api_url, titles):
            # Return deterministic fake QIDs without network.
            result = {}
            for index, title in enumerate(titles, start=1):
                result[title] = "Q{}".format(index)
            return result

        fetch_mock.side_effect = fake_fetch

        en_links = ["enwiki:0:Article_{}".format(i) for i in range(61)]
        de_links = ["dewiki:0:Artikel_{}".format(i) for i in range(3)]
        records = [{"gil": "|".join(en_links + de_links)}]

        link_map = service._build_gil_link_wikidata_map(records)

        self.assertEqual(len(link_map), 64)
        self.assertIn("https://en.wikipedia.org/wiki/Article_0", link_map)

        call_sizes = [len(call.args[1]) for call in fetch_mock.call_args_list]
        self.assertTrue(call_sizes)
        self.assertLessEqual(max(call_sizes), 50)

        en_calls = [call for call in fetch_mock.call_args_list if "en.wikipedia.org" in call.args[0]]
        de_calls = [call for call in fetch_mock.call_args_list if "de.wikipedia.org" in call.args[0]]
        self.assertEqual(len(en_calls), 2)
        self.assertEqual(sum(len(call.args[1]) for call in en_calls), 61)
        self.assertEqual(len(de_calls), 1)
        self.assertEqual(sum(len(call.args[1]) for call in de_calls), 3)

    @patch("petscan.service._wikidata_lookup_backend", return_value=service._LOOKUP_BACKEND_API)
    @patch("petscan.service._fetch_wikibase_items_for_site_api")
    def test_wikidata_item_gil_link_resolves_directly_without_api_lookup(self, fetch_mock, _backend_mock):
        fetch_mock.return_value = {"Albert_Einstein": "Q937"}
        records = [{"gil": "wikidatawiki:0:Q42|enwiki:0:Albert_Einstein"}]

        link_map = service._build_gil_link_wikidata_map(records)

        self.assertEqual(link_map.get("https://www.wikidata.org/wiki/Q42"), "Q42")
        self.assertEqual(link_map.get("https://en.wikipedia.org/wiki/Albert_Einstein"), "Q937")
        self.assertEqual(len(fetch_mock.call_args_list), 1)
        self.assertIn("en.wikipedia.org", fetch_mock.call_args_list[0].args[0])

    def test_gil_wikidata_fields_are_emitted_when_mapping_exists(self):
        record = {"gil": "enwiki:0:Albert_Einstein|dewiki:0:Berlin"}
        gil_map = {"https://en.wikipedia.org/wiki/Albert_Einstein": "Q937"}

        enriched_links = service._iter_gil_link_enrichment(record, gil_link_wikidata_map=gil_map)
        self.assertEqual(
            enriched_links,
            [
                ("https://en.wikipedia.org/wiki/Albert_Einstein", "Q937"),
                ("https://de.wikipedia.org/wiki/Berlin", None),
            ],
        )

    def test_summary_includes_gil_link_relation_fields(self):
        record = {"gil": "enwiki:0:Federalist_No._42"}
        gil_map = {"https://en.wikipedia.org/wiki/Federalist_No._42": "Q5440615"}
        summary = service._summarize_structure([record], gil_link_wikidata_map=gil_map)
        field_map = {field["source_key"]: field for field in summary["fields"]}

        self.assertIn("gil_link", field_map)
        self.assertIn("gil_link_wikidata_id", field_map)
        self.assertIn("gil_link_wikidata_entity", field_map)

    @patch("petscan.service._build_gil_link_wikidata_map")
    def test_store_contains_gil_link_relation_triples(self, gil_map_mock):
        if service.Store is None:
            self.skipTest("pyoxigraph is not installed")

        link_uri = "https://en.wikipedia.org/wiki/Federalist_No._42"
        gil_map_mock.return_value = {link_uri: "Q5440615"}
        psid = 999991
        records = [{"id": 1, "title": "Example", "gil": "enwiki:0:Federalist_No._42"}]

        try:
            service._build_store(psid, records, "https://example.invalid")
            store = service.Store(str(service._store_path(psid)))
            ask_query = """
            PREFIX ps: <https://petscan.wmcloud.org/ontology/>
            ASK {
              ?item ps:gil_link <https://en.wikipedia.org/wiki/Federalist_No._42> .
              <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_wikidata_id "Q5440615" .
              <https://en.wikipedia.org/wiki/Federalist_No._42> ps:gil_link_wikidata_entity <http://www.wikidata.org/entity/Q5440615> .
            }
            """
            self.assertTrue(store.query(ask_query))
        finally:
            shutil.rmtree(service._store_path(psid), ignore_errors=True)

    def test_detects_service_clause(self):
        query = """
        SELECT * WHERE {
          SERVICE <https://query.wikidata.org/sparql> {
            ?item ?p ?o .
          }
        }
        """
        self.assertTrue(service._contains_service_clause(query))

    def test_does_not_flag_plain_iri_containing_service_word(self):
        query = """
        SELECT * WHERE {
          <https://example.org/service> ?p ?o .
        }
        """
        self.assertFalse(service._contains_service_clause(query))

    def test_detects_service_clause_with_prefixed_name(self):
        query = """
        PREFIX ex: <https://example.org/sparql>
        SELECT * WHERE {
          SERVICE ex: {
            ?item ?p ?o .
          }
        }
        """
        self.assertTrue(service._contains_service_clause(query))

    def test_detects_service_clause_with_wikibase_label_prefixed_name(self):
        query = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT * WHERE {
          SERVICE wikibase:label {
            ?item ?p ?o .
          }
        }
        """
        self.assertTrue(service._contains_service_clause(query))

    def test_query_type_ignores_prefix_name_that_matches_query_keyword(self):
        query = """
        PREFIX select: <http://example.org/ns#>
        ASK { ?s ?p ?o }
        """
        self.assertEqual(service._query_type(query), "ASK")

    def test_query_type_supports_base_and_prefix_prologue(self):
        query = """
        # comment line
        BASE <http://example.org/base/>
        PREFIX ask: <http://example.org/ns#>
        CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }
        """
        self.assertEqual(service._query_type(query), "CONSTRUCT")

    def test_execute_query_handles_prefix_name_that_matches_query_keyword(self):
        if service.Store is None:
            self.skipTest("pyoxigraph is not installed")

        psid = 999990
        records = [{"id": 1, "title": "Example"}]
        query = """
        PREFIX select: <http://example.org/ns#>
        ASK { ?s ?p ?o }
        """

        try:
            service._build_store(psid, records, "https://example.invalid")
            execution = service.execute_query(psid, query, refresh=False)
            self.assertEqual(execution["query_type"], "ASK")
            self.assertEqual(execution["result_format"], "sparql-json")
            self.assertIn("boolean", execution["sparql_json"])
        finally:
            shutil.rmtree(service._store_path(psid), ignore_errors=True)
