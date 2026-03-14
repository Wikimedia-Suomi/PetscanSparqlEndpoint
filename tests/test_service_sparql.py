from petscan import service_sparql as sparql
from tests.service_test_support import ServiceTestCase


class ServiceSparqlTests(ServiceTestCase):
    def test_detects_service_clause(self):
        query = """
        SELECT * WHERE {
          SERVICE <https://query.wikidata.org/sparql> {
            ?item ?p ?o .
          }
        }
        """
        self.assertTrue(sparql.contains_service_clause(query))

    def test_ignores_service_clause_pattern_inside_comment_line(self):
        query = """
        # SERVICE <https://query.wikidata.org/sparql> { ?item ?p ?o . }
        SELECT * WHERE { ?item ?p ?o . }
        """
        self.assertFalse(sparql.contains_service_clause(query))

    def test_does_not_flag_plain_iri_containing_service_word(self):
        query = """
        SELECT * WHERE {
          <https://example.org/service> ?p ?o .
        }
        """
        self.assertFalse(sparql.contains_service_clause(query))

    def test_detects_service_clause_with_prefixed_name(self):
        query = """
        PREFIX ex: <https://example.org/sparql>
        SELECT * WHERE {
          SERVICE ex: {
            ?item ?p ?o .
          }
        }
        """
        self.assertTrue(sparql.contains_service_clause(query))

    def test_detects_service_clause_with_wikibase_label_prefixed_name(self):
        query = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT * WHERE {
          SERVICE wikibase:label {
            ?item ?p ?o .
          }
        }
        """
        self.assertTrue(sparql.contains_service_clause(query))

    def test_query_type_ignores_prefix_name_that_matches_query_keyword(self):
        query = """
        PREFIX select: <http://example.org/ns#>
        ASK { ?s ?p ?o }
        """
        self.assertEqual(sparql.query_type(query), "ASK")

    def test_query_type_supports_base_and_prefix_prologue(self):
        query = """
        # comment line
        BASE <http://example.org/base/>
        PREFIX ask: <http://example.org/ns#>
        CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }
        """
        self.assertEqual(sparql.query_type(query), "CONSTRUCT")
