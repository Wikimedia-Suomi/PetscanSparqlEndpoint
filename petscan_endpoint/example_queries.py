from urllib.parse import quote

_INCUBATOR_EXAMPLE_QUERY = """PREFIX schema: <http://schema.org/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX incubator: <https://incubator.wikimedia.org/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT * WHERE {
  BIND(wd:Q11870373 AS ?qid)

  {
    ?sitelink schema:about ?qid .
    ?sitelink schema:inLanguage ?inLanguage .
    ?sitelink rdf:type ?type .
    ?sitelink schema:name ?name .
    ?sitelink schema:isPartOf ?isPartOf .
    ?isPartOf wikibase:wikiGroup ?wikiGroup .
  }
  UNION {
    SERVICE <https://sparqlbridge.toolforge.org/incubator/sparql/namespace=0&page_prefix=Wp/sms> {
      ?sitelink schema:about ?qid .
      ?sitelink schema:inLanguage ?inLanguage .
      ?sitelink rdf:type ?type .
      ?sitelink schema:name ?name .
      ?sitelink incubator:page_title ?page_title .
      ?sitelink schema:isPartOf ?isPartOf .
      ?isPartOf wikibase:wikiGroup ?wikiGroup .
    }
  }
}
"""
_QLEVER_WIKIDATA_BASE_URL = "https://qlever.wikidata.dbis.rwth-aachen.de/wikidata/?query="


def build_incubator_example_query_url() -> str:
    return "{}{}".format(_QLEVER_WIKIDATA_BASE_URL, quote(_INCUBATOR_EXAMPLE_QUERY))
