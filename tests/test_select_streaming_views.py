import gzip
from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase

from jsonstat import service_source


class SelectStreamingViewTests(SimpleTestCase):
    def test_all_data_source_endpoints_stream_select_results(self) -> None:
        source_url = "https://pxdata.stat.fi/PxWeb/sq/552d1f53-bdab-472b-a8e7-68b5b8c37cda"
        source_token = service_source.encode_source_token(source_url)
        cases = [
            ("petscan.views.petscan_service.execute_query", "/petscan/sparql/psid=123"),
            ("quarry.views.quarry_service.execute_query", "/quarry/sparql/quarry_id=103479"),
            ("pagepile.views.pagepile_service.execute_query", "/pagepile/sparql/pagepile_id=112306"),
            ("newpages.views.newpages_service.execute_query", "/newpages/sparql"),
            ("incubator.views.incubator_service.execute_query", "/incubator/sparql"),
            (
                "jsonstat.views.jsonstat_service.execute_query",
                "/jsonstat/sparql/source={}".format(source_token),
            ),
            ("placenames.views.service.execute_query", "/placenames/sparql/dataset=saami"),
        ]
        chunks = (
            '{"head": {"vars": ["item"]}, "results": {"bindings": [',
            '{"item": {"type": "literal", "value": "Ääkkönen"}}',
            "]}}",
        )
        expected_body = "".join(chunks).encode("utf-8")

        for patch_target, path in cases:
            with self.subTest(path=path), patch(patch_target) as execute_query:
                execute_query.return_value = {
                    "query_type": "SELECT",
                    "result_format": "sparql-json-stream",
                    "sparql_json_stream": iter(chunks),
                    "meta": {},
                }

                response = self.client.get(
                    path,
                    data={"query": "SELECT ?item WHERE { ?item ?p ?o }"},
                )

                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.streaming)
                self.assertEqual(b"".join(response.streaming_content), expected_body)
                self.assertEqual(
                    response["Content-Type"],
                    "application/sparql-results+json; charset=utf-8",
                )
                self.assertEqual(response["X-Accel-Buffering"], "no")
                self.assertEqual(response["Access-Control-Allow-Origin"], "*")
                self.assertTrue(execute_query.call_args.kwargs["stream_select_results"])
                response.close()

    @patch("petscan.views.petscan_service.execute_query")
    def test_streamed_select_is_gzip_encoded_when_client_accepts_it(
        self,
        execute_query: Any,
    ) -> None:
        chunks = (
            '{"head": {"vars": ["item"]}, "results": {"bindings": [',
            '{"item": {"type": "literal", "value": "Ääkkönen"}}',
            "]}}",
        )
        expected_body = "".join(chunks).encode("utf-8")
        execute_query.return_value = {
            "query_type": "SELECT",
            "result_format": "sparql-json-stream",
            "sparql_json_stream": iter(chunks),
            "meta": {},
        }

        response = self.client.get(
            "/petscan/sparql/psid=123",
            data={"query": "SELECT ?item WHERE { ?item ?p ?o }"},
            headers={"accept-encoding": "gzip"},
        )

        compressed_body = b"".join(response.streaming_content)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.streaming)
        self.assertEqual(response["Content-Encoding"], "gzip")
        self.assertIn("Accept-Encoding", response["Vary"])
        self.assertNotIn("Content-Length", response)
        self.assertEqual(gzip.decompress(compressed_body), expected_body)
        self.assertEqual(response["X-Accel-Buffering"], "no")
        response.close()
