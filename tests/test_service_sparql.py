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

    def test_detects_service_clause_with_empty_prefix(self):
        query = """
        PREFIX : <https://example.org/sparql>
        SELECT * WHERE {
          SERVICE : {
            ?item ?p ?o .
          }
        }
        """
        self.assertTrue(sparql.contains_service_clause(query))

    def test_detects_service_clause_with_dotted_prefix(self):
        query = """
        PREFIX a.b: <https://example.org/sparql#>
        SELECT * WHERE {
          SERVICE a.b:x {
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

    def test_validate_query_rejects_dataset_clause(self):
        query = """
        SELECT * FROM <https://example.org/data> WHERE {
          ?s ?p ?o .
        }
        """
        with self.assertRaisesMessage(ValueError, "Dataset clauses are not allowed in this endpoint."):
            sparql.validate_query(query)

    def test_validate_query_returns_query_form_for_allowed_query(self):
        query = """
        PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
        SELECT ?item WHERE {
          ?item a petscan:Page .
        }
        LIMIT 5
        """
        self.assertEqual(sparql.validate_query(query), "SELECT")

    def test_validate_query_rejects_invalid_syntax_as_client_error(self):
        query = "SELECT WHERE { ?s ?p ?o }"
        with self.assertRaisesMessage(ValueError, "SPARQL query is invalid:"):
            sparql.validate_query(query)
