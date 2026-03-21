from pyoxigraph import BlankNode, Literal, NamedNode

from petscan import service_sparql as sparql
from tests.service_test_support import ServiceTestCase

XSD_INTEGER_IRI = "http://www.w3.org/2001/XMLSchema#integer"
XSD_STRING_IRI = "http://www.w3.org/2001/XMLSchema#string"


class QueryBoolean:
    def __init__(self, value):
        self.value = value

    def __bool__(self):
        return bool(self.value)


class SelectResult:
    def __init__(self, variables, solutions):
        self.variables = variables
        self._solutions = solutions

    def __iter__(self):
        return iter(self._solutions)


class MappingSolution:
    def __init__(self, items):
        self._items = items

    def items(self):
        return list(self._items)


class IndexedSolution:
    def __init__(self, values):
        self._values = values

    def __getitem__(self, key):
        return self._values[key]


class Triple:
    def __init__(self, subject, predicate, object_term):
        self.subject = subject
        self.predicate = predicate
        self.object = object_term


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

    def test_serialize_select_supports_mapping_rows_and_rdf_term_shapes(self):
        result = SelectResult(
            variables=["?item", "?label", "?datatype_string", "?node"],
            solutions=[
                MappingSolution(
                    [
                        ("?item", NamedNode("https://example.org/item/1")),
                        ("?label", Literal("Turku", language="fi")),
                        ("?datatype_string", Literal("plain text", datatype=NamedNode(XSD_STRING_IRI))),
                        ("?node", BlankNode("b1")),
                    ]
                )
            ],
        )

        self.assertEqual(
            sparql.serialize_select(result),
            {
                "head": {"vars": ["item", "label", "datatype_string", "node"]},
                "results": {
                    "bindings": [
                        {
                            "item": {"type": "uri", "value": "https://example.org/item/1"},
                            "label": {
                                "type": "literal",
                                "value": "Turku",
                                "xml:lang": "fi",
                            },
                            "datatype_string": {
                                "type": "literal",
                                "value": "plain text",
                            },
                            "node": {"type": "bnode", "value": "b1"},
                        }
                    ]
                },
            },
        )

    def test_serialize_select_supports_indexed_rows_and_omits_missing_bindings(self):
        result = SelectResult(
            variables=["?item", "?count", "?missing"],
            solutions=[
                IndexedSolution(
                    {
                        "item": NamedNode("https://example.org/item/2"),
                        "count": Literal("42", datatype=NamedNode(XSD_INTEGER_IRI)),
                    }
                ),
                IndexedSolution(
                    {
                        "item": NamedNode("https://example.org/item/3"),
                        "count": 7,
                    }
                ),
            ],
        )

        self.assertEqual(
            sparql.serialize_select(result),
            {
                "head": {"vars": ["item", "count", "missing"]},
                "results": {
                    "bindings": [
                        {
                            "item": {"type": "uri", "value": "https://example.org/item/2"},
                            "count": {
                                "type": "literal",
                                "value": "42",
                                "datatype": XSD_INTEGER_IRI,
                            },
                        },
                        {
                            "item": {"type": "uri", "value": "https://example.org/item/3"},
                            "count": {"type": "literal", "value": "7"},
                        },
                    ]
                },
            },
        )

    def test_serialize_ask_accepts_query_boolean_wrapper(self):
        self.assertEqual(
            sparql.serialize_ask(QueryBoolean(True)),
            {"head": {}, "boolean": True},
        )

    def test_serialize_ask_accepts_iterable_bool_fallback(self):
        self.assertEqual(
            sparql.serialize_ask(iter([False])),
            {"head": {}, "boolean": False},
        )

    def test_serialize_ask_raises_for_unserializable_result(self):
        with self.assertRaisesMessage(sparql.PetscanServiceError, "ASK result could not be serialized."):
            sparql.serialize_ask(iter(["not-a-bool"]))

    def test_serialize_graph_supports_object_attributes_tuple_fallback_and_escaping(self):
        result = [
            Triple(
                NamedNode("https://example.org/s"),
                NamedNode("https://example.org/p"),
                Literal('Line 1\nLine "2"', language="en"),
            ),
            (
                BlankNode("node-2"),
                NamedNode("https://example.org/count"),
                Literal("42", datatype=NamedNode(XSD_INTEGER_IRI)),
            ),
            (NamedNode("https://example.org/fallback"), NamedNode("https://example.org/value"), 7),
            ("skip",),
        ]

        self.assertEqual(
            sparql.serialize_graph(result),
            (
                '<https://example.org/s> <https://example.org/p> "Line 1\\nLine \\"2\\""@en .\n'
                '_:node-2 <https://example.org/count> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
                '<https://example.org/fallback> <https://example.org/value> "7" .\n'
            ),
        )
