const PETSCAN_EXAMPLE_QUERY = `# This query finds spoken English Wikipedia articles whose spoken-version creation date
# differs the most from the latest revision date of the linked article.
PREFIX petscan: <https://petscan.wmcloud.org/ontology/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT * WHERE {
  SERVICE <https://sparqlbridge.toolforge.org/petscan/sparql/psid=43641756> {
    SELECT
      ?item
      ?namespace
      ?title
      ?gil_link
      ?gil_link_wikidata_entity
      ?gil_link_rev_timestamp
      ?img_timestamp
      ?days_diff
    WHERE {
      ?item a petscan:Page .
      OPTIONAL { ?item petscan:namespace ?namespace . }
      OPTIONAL { ?item petscan:title ?title . }
      OPTIONAL { ?item petscan:img_timestamp ?img_timestamp . }

      ?item petscan:gil_link ?gil_link .
      FILTER(STRSTARTS(STR(?gil_link), "https://en.wikipedia.org/wiki/"))

      ?gil_link petscan:gil_link_wikidata_entity ?gil_link_wikidata_entity .
      OPTIONAL { ?gil_link petscan:gil_link_rev_timestamp ?gil_link_rev_timestamp . }

      BIND(
        ABS(
          (YEAR(?img_timestamp) - YEAR(?gil_link_rev_timestamp)) * 365 +
          (MONTH(?img_timestamp) - MONTH(?gil_link_rev_timestamp)) * 30 +
          (DAY(?img_timestamp) - DAY(?gil_link_rev_timestamp))
        ) AS ?days_diff
      )
    }
    ORDER BY DESC(?days_diff)
  }
}
`;

const QUARRY_EXAMPLE_QUERY = `# This query finds Finnish Wikipedia biographies of women that do not yet have images.
# It then looks for matching subject images in Wikimedia Commons.
PREFIX quarrycol: <https://quarry.wmcloud.org/ontology/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT DISTINCT ?page_title ?page_id ?page_uri ?pp_value ?file WHERE {
  # 1. Fetch source data: fiwiki biographies without the "page_image_free" page property.
  # https://quarry.wmcloud.org/query/103960
  SERVICE <https://sparqlbridge.toolforge.org/quarry/sparql/quarry_id=103960> {
    SELECT ?page_id ?page_title ?page_uri ?pp_value WHERE {
      ?quarry_row_id quarrycol:page_id ?page_id .
      ?quarry_row_id quarrycol:page_title ?page_title .
      ?quarry_row_id quarrycol:page_uri ?page_uri .
      ?quarry_row_id quarrycol:pp_value ?pp_value .
    }
  }

  # 2. Filter in QLever before hitting Commons to reduce the ?pp_value set early.
  # Q5 = Human, Q6581072 = Female
  SERVICE <https://qlever.dev/api/wikidata> {
    ?pp_value wdt:P31 wd:Q5 .
    ?pp_value wdt:P21 wd:Q6581072 .
  }

  # 3. Query Commons only for the already-filtered set.
  ?file wdt:P180 ?pp_value .

  # 4. Exclude pages that already have large images using one uncorrelated subquery.
  # https://quarry.wmcloud.org/query/103966
  MINUS {
    SERVICE <https://sparqlbridge.toolforge.org/quarry/sparql/quarry_id=103966> {
      SELECT DISTINCT ?linked_page WHERE {
        ?r quarrycol:gil_page ?linked_page .
        ?r quarrycol:gil_to [] .
      }
    }
    FILTER(?linked_page = ?page_id)
  }
}
`;

const INCUBATOR_EXAMPLE_QUERY = `# This query adds Incubator language-link data to the RDF graph.
# That makes Incubator articles usable in SPARQL queries much like normal Wikipedia articles.
PREFIX schema: <http://schema.org/>
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
`;

const PAGEPILE_EXAMPLE_QUERY = `# This query exposes a PagePile as schema.org sitelinks.
# That makes the pile usable together with WDQS entity lookups and other sitelink-style sources.
PREFIX schema: <http://schema.org/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX pagepile: <https://pagepile.toolforge.org/ontology/>

SELECT * WHERE {
  SERVICE <https://sparqlbridge.toolforge.org/pagepile/sparql/pagepile_id=112306&limit=50> {
    ?page schema:about ?wikidata_entity .
    ?page schema:name ?page_label .
    ?page pagepile:page_id ?page_id .
    ?page schema:isPartOf ?site_url .
    ?site_url wikibase:wikiGroup ?wiki_group .
  }
}
LIMIT 50
`;

const NEWPAGES_EXAMPLE_QUERY = `# This query fetches newly created pages from one or more Wikimedia wikis.
# The result graph follows the same sitelink-style RDF shape as the Incubator source.
PREFIX schema: <http://schema.org/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX newpages: <https://sparqlbridge.toolforge.org/newpages/ontology/>

SELECT * WHERE {
  SERVICE <https://sparqlbridge.toolforge.org/newpages/sparql/wiki=fi&timestamp=20260401000000> {
    ?page schema:about ?wikidata_entity .
    ?page schema:name ?page_label .
    ?page newpages:created_timestamp ?created_timestamp .
    ?page schema:isPartOf ?site_url .
    ?site_url wikibase:wikiGroup ?wiki_group .
  }
}
LIMIT 50
`;

const QLEVER_WIKIDATA_BASE_URL = "https://qlever.wikidata.dbis.rwth-aachen.de/wikidata/?query=";
const QLEVER_COMMONS_BASE_URL = "https://qlever.dev/wikimedia-commons?query=";

function quoteLikePython(value) {
  return encodeURIComponent(String(value || ""))
    .replace(/[!'()*]/g, function (char) {
      return "%" + char.charCodeAt(0).toString(16).toUpperCase();
    })
    .replace(/%2F/g, "/");
}

function buildQueryUrl(baseUrl, query) {
  return baseUrl + quoteLikePython(query);
}

export function buildPetscanExampleQueryUrl() {
  return buildQueryUrl(QLEVER_WIKIDATA_BASE_URL, PETSCAN_EXAMPLE_QUERY);
}

export function buildQuarryExampleQueryUrl() {
  return buildQueryUrl(QLEVER_COMMONS_BASE_URL, QUARRY_EXAMPLE_QUERY);
}

export function buildIncubatorExampleQueryUrl() {
  return buildQueryUrl(QLEVER_WIKIDATA_BASE_URL, INCUBATOR_EXAMPLE_QUERY);
}

export function buildPagepileExampleQueryUrl() {
  return buildQueryUrl(QLEVER_WIKIDATA_BASE_URL, PAGEPILE_EXAMPLE_QUERY);
}

export function buildNewpagesExampleQueryUrl() {
  return buildQueryUrl(QLEVER_WIKIDATA_BASE_URL, NEWPAGES_EXAMPLE_QUERY);
}

export function buildExampleQueryUrl(source) {
  var normalizedSource = String(source || "").trim().toLowerCase();
  if (normalizedSource === "petscan") {
    return buildPetscanExampleQueryUrl();
  }
  if (normalizedSource === "quarry") {
    return buildQuarryExampleQueryUrl();
  }
  if (normalizedSource === "incubator") {
    return buildIncubatorExampleQueryUrl();
  }
  if (normalizedSource === "pagepile") {
    return buildPagepileExampleQueryUrl();
  }
  if (normalizedSource === "newpages") {
    return buildNewpagesExampleQueryUrl();
  }
  return "";
}
