# SPARQL Bridge

This Django application exposes several source formats and services as local, federatable SPARQL
endpoints.

## TL;DR

- Give the app a PetScan `psid`, and it turns that PetScan JSON result into a local RDF dataset.
- The dataset is stored in Oxigraph, and exposed via the shared SPARQL endpoint at
  `/sparql?dataset=petscan&...`.
- Give the JSON-stat source an allowlisted HTTPS URL, and it turns a JSON-stat 2.0 cube into RDF
  observations under `/sparql?dataset=jsonstat&...`.
- A versioned Sámi place-name dataset is bundled locally and exposed at
  `/sparql?dataset=placenames`.
- Web UI flow: load PetScan data -> inspect generated fields/structure -> run SPARQL queries.
- Optional enrichment adds Wikidata-related fields for `gil_link` targets (API or Toolforge SQL backend).

## Requirements

- Python 3.13
- `pip`
- Django
- pyoxigraph
- PyMySQL
- rdflib

Toolforge currently provides Python 3.13, and this repository targets Python 3.13 compatibility.

## Development Setup And Commands

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r constraints-dev.txt
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
```

Regenerate the tested dependency set after intentionally changing requirement ranges:

```bash
pip-compile --resolver=backtracking --strip-extras --allow-unsafe --generate-hashes --no-emit-index-url --no-header \
  --output-file=constraints-dev.txt constraints-dev.in
pip-compile --resolver=backtracking --strip-extras --allow-unsafe --generate-hashes --no-emit-index-url --no-header \
  --output-file=constraints.txt constraints.in
```

### Run tests (including lint, type-check and security scans)

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
./scripts/run_tests.sh
```

### Run browser smoke tests

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
# If you do not have a local Chrome/Chromium available, install one Playwright browser once:
# python -m playwright install chromium
./scripts/run_smoke_tests.sh
```

On macOS, the smoke tests try to use an installed Google Chrome by default.

### Run browser accessibility tests

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
# If you do not have a local Chrome/Chromium available, install one Playwright browser once:
# python -m playwright install chromium
./scripts/run_a11y_tests.sh
```

These tests use `axe-playwright-python`, so accessibility scans run fully from the Python virtual
environment without requiring `node` or `npm`. The default target set is
`wcag2a + wcag2aa + wcag21a + wcag21aa + wcag22aa + best-practice + wcag2aaa`.

### Run JavaScript helper tests

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
# If you do not have a local Chrome/Chromium available, install one Playwright browser once:
# python -m playwright install chromium
./scripts/run_js_tests.sh
```

This helper suite executes pure functions from `static/js/app_logic.js` in a real browser via Playwright Python, so no `node` or `npm` installation is required.

### Run browser E2E tests against live PetScan, Quarry, and PagePile

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
# Optional overrides:
# export PETSCAN_E2E_PSID=43641756
# export PETSCAN_E2E_OUTPUT_LIMIT=5
# export QUARRY_E2E_QUERY_ID=103479
# export QUARRY_E2E_LIMIT=5
# export PAGEPILE_E2E_ID=112306
# export PAGEPILE_E2E_LIMIT=5
# export PLAYWRIGHT_DEFAULT_TIMEOUT_MS=60000
./scripts/run_e2e_tests.sh
```

This E2E script keeps its own temporary Oxigraph store under `OXIGRAPH_BASE_DIR`, so the initial
load is not satisfied from a previous cached dataset. Unlike the smoke tests, it uses real network
requests to PetScan, Quarry, PagePile, and MediaWiki APIs, and is intentionally kept out of the
default `run_tests.sh` path.

### Run app

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export DJANGO_DEBUG=1
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
python manage.py runserver
```

Open [http://127.0.0.1:8000/petscan/](http://127.0.0.1:8000/petscan/).
The JSON-stat 2 source is available at
[http://127.0.0.1:8000/jsonstat/](http://127.0.0.1:8000/jsonstat/).
The bundled place-name source is available at
[http://127.0.0.1:8000/placenames/](http://127.0.0.1:8000/placenames/).

### Check API enrichment coverage for `gil_link`

This command validates that API enrichment returns `page_len` and `rev_timestamp`
for `gil_link` targets from a PetScan result.

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
python manage.py check_api_enrichment --petscan-url 'https://petscan.wmcloud.org/?psid=43641756'
```

By default, the command fails if any `gil_link` is missing `page_len` or `rev_timestamp`.
Use `--allow-missing` to print diagnostics without failing.

### Run opt-in graph parity regression tests

This suite compares the current refactored RDF graph build path against a
test-local legacy implementation using the bundled example datasets. It stays
out of the default test run because the large `psid=43706364` fixture is
intentionally heavy.

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
export GRAPH_PARITY_REGRESSION_TESTS=1
python manage.py test tests.test_graph_parity_regression
```

### Run lightweight performance baseline tests

This suite uses bundled example datasets and deterministic fake enrichment, so it
does not need network access. It is enabled in CI and can also be run locally
when you want a quick regression check for the `build_store()` hot path.

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
export PERFORMANCE_BASELINE_TESTS=1
python manage.py test tests.test_performance_baseline
```

### Run reproducible offline benchmark and save results

This benchmark uses the bundled PetScan and Quarry example payloads, writes each
run to a timestamped JSON file under `data/benchmarks/results/`, updates
`latest.json`, and appends a compact summary row to `history.jsonl` so later
runs are easy to compare.

```bash
./scripts/run_benchmark_example_datasets.sh --label pyoxigraph-0.5.6
```

Optional overrides:

```bash
./scripts/run_benchmark_example_datasets.sh \
  --datasets parse_only_large,quarry_large \
  --runs 3 \
  --warmup 1 \
  --output data/benchmarks/results/manual-large-run.json
```

The dataset list lives in `data/benchmarks/offline_store_build_datasets.json`,
so adding or removing benchmark fixtures does not require command code changes.

## Environment Configuration

Security-related Django settings are configured via environment variables:

- `DJANGO_SECRET_KEY` (required)
- `DJANGO_DEBUG` (`1/true/yes/on` enables debug; default: disabled)
- `DJANGO_ALLOWED_HOSTS` (comma-separated list, example: `localhost,127.0.0.1,mydomain.tld`)
- `OXIGRAPH_BASE_DIR` (required absolute path for the Oxigraph store directory;
  the place-name cache rejects symlinks in every path component)
- `PLACENAMES_SCHEMA_MODE` (`hardcoded` by default; use `dynamic` to derive the
  place-name field structure from Oxigraph after import)
- `JSONSTAT_ALLOWED_SOURCE_DOMAINS` (comma-separated source-domain allowlist; the default
  `stat.fi` also permits its subdomains, such as `pxdata.stat.fi`)
- `JSONSTAT_TIMEOUT_SECONDS` (upstream request timeout; default: `30`)
- `JSONSTAT_MAX_RESPONSE_BYTES` (maximum downloaded JSON-stat document size; default: 50 MiB)
- `JSONSTAT_MAX_CELLS` (maximum number of cube cells converted to observations; default: `300000`)
- `JSONSTAT_CLASSIFICATION_ENRICHMENT_ENABLED` (enable exact Statistics Finland classification
  enrichment; default: enabled)
- `JSONSTAT_CLASSIFICATION_LANGUAGES` (comma-separated classification API languages chosen from
  `fi`, `sv`, and `en`; default: `fi`)
- `JSONSTAT_LINKING_SNAPSHOT_PATH` (reviewed Statistics Finland runtime links; default:
  `source-data/statfi_runtime_links.json`)
- `JSONSTAT_CLASSIFICATION_SNAPSHOT_PATH` (optional classification-snapshot override; defaults to
  `JSONSTAT_LINKING_SNAPSHOT_PATH`; an empty value disables local classification enrichment)
- `JSONSTAT_CLASSIFICATION_NETWORK_FALLBACK_ENABLED` (allow the classification API as a fallback
  for classifications or languages missing from the local snapshot; default: disabled)
- `JSONSTAT_CLASSIFICATION_TIMEOUT_SECONDS` (classification API timeout; default: `10`)
- `JSONSTAT_CLASSIFICATION_MAX_RESPONSE_BYTES` (classification API response limit; default:
  20 MiB)
- `JSONSTAT_CLASSIFICATION_CACHE_SECONDS` (successful classification API response cache lifetime;
  default: `86400`)

The version-controlled runtime snapshot is a curated linking artifact rather than a complete
Statistics Finland export. It currently contains 7 useful classifications with 1,134 exact item
links, 134 statistics IDs, and 79 unit mappings. It is indexed with a memory-mapped scan, and only
classifications used by the current JSON-stat dataset are parsed into memory. The complete source
harvests, matching pipeline, review candidates, and intermediate identifier pairs are maintained
outside this repository. See `source-data/README_statfi_runtime_links.md` for the inclusion policy.

When the optional network fallback is enabled, every classification and language uses at most one
metadata request and one item collection request; enrichment never issues a sequential request for
each category code.

When using `manage.py runserver`, keep `DJANGO_DEBUG=1`. The app emits a startup warning if debug
is disabled, because Django will not serve the UI static files by default in that mode. This check
is intentionally only a reminder and can be bypassed with `python manage.py runserver --skip-checks`
for intentional local experiments.

## Example Source Files

- `data/examples/petscan-43641756.json.gz`
- `data/examples/petscan-43642782.json.gz`
- `data/examples/petscan-43706364.json.gz`
- `data/examples/quarry-103479-run-1084300.json.gz`
- `data/examples/quarry-103514-run-1084648.json.gz`
- `data/examples/jsonstat-552d1f53.json`

## Endpoint Output Regression Snapshots

The repository can also store full offline endpoint-output baselines generated
from the bundled source snapshots. The command below rebuilds each dataset from
local JSON, runs one `CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }` query through
the service layer, canonicalizes the N-Triples result, and compares it against
the committed snapshot files in `data/endpoint_snapshots/`.

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
python manage.py check_endpoint_snapshot_regression
```

To update the stored baselines after an intentional graph change:

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
python manage.py check_endpoint_snapshot_regression --write
```

## SPARQL Endpoint

### URL

`/sparql?dataset=petscan&<service-parameters>`

All sources use the fixed `/sparql` path so Wikimedia Query Service can allowlist one endpoint.
Select the source with `dataset`: `petscan`, `quarry`, `pagepile`, `incubator`, `newpages`,
`jsonstat`, or `placenames`. Existing source-specific paths such as
`/petscan/sparql/psid=43641756` remain supported for backwards compatibility, but new links and
queries should use the shared endpoint.

### Parameters

- `psid` (required): PetScan ID whose Oxigraph dataset should be queried
- `query` (required): SPARQL query (for `GET`) or in the request body (for `POST`)
- `refresh` (optional): `1/true` to force reloading PetScan data before query
- any additional URL query parameters are forwarded to PetScan JSON fetch (except reserved keys `psid`, `format`, `query`, `refresh`)
- `POST /sparql?dataset=petscan&...` supports `Content-Type: application/sparql-query` and `application/x-www-form-urlencoded`
- In the web UI, use the **PetScan extra GET params** field (example: `category=Turku&language=fi`) to simulate `SERVICE` URI parameters.

### Example `GET`

```bash
curl --get 'http://127.0.0.1:8000/sparql?dataset=petscan&psid=43641756' \
  --data-urlencode 'query=SELECT ?item ?title WHERE { ?item a <https://petscan.wmcloud.org/ontology/Page> . OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title } } LIMIT 5'
```

### Example `SERVICE` usage

You can include this endpoint in a federated query by encoding `psid` in the endpoint URL:

```sparql
SELECT ?item ?title WHERE {
  SERVICE <http://127.0.0.1:8000/sparql?dataset=petscan&psid=43641756> {
    ?item a <https://petscan.wmcloud.org/ontology/Page> .
    OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title }
  }
}
LIMIT 20
```

## JSON-stat 2 Endpoint

Open `/jsonstat/` and enter an allowlisted HTTPS URL that returns one JSON-stat 2.0 `dataset`
response. Initially only `stat.fi` and its subdomains are allowed.
The structure endpoint accepts the URL directly:

```bash
curl --get 'http://127.0.0.1:8000/jsonstat/api/structure' \
  --data-urlencode 'url=https://pxdata.stat.fi/PxWeb/sq/552d1f53-bdab-472b-a8e7-68b5b8c37cda' \
  --data-urlencode 'refresh=1'
```

The response includes `source_token`, a URL-safe token used as a SPARQL endpoint parameter. Statistics
Finland PxWeb saved-query URLs use the readable `<hostname>_<UUID>` format. The endpoint accepts only
canonical `https://<allowed-host>/PxWeb/sq/<UUID>` sources. For the example above, the endpoint is:

`/sparql?dataset=jsonstat&source=pxdata.stat.fi_552d1f53-bdab-472b-a8e7-68b5b8c37cda`

```sparql
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX jsonstat: <https://sparqlbridge.toolforge.org/ontology/jsonstat/>

SELECT ?year ?ageGroup ?indicator ?value WHERE {
  SERVICE <https://sparqlbridge.toolforge.org/sparql?dataset=jsonstat&source=pxdata.stat.fi_552d1f53-bdab-472b-a8e7-68b5b8c37cda> {
    ?observation a qb:Observation ;
      jsonstat:timeperiod_y ?year ;
      jsonstat:ikaryhma_10_20180101_label ?ageGroup ;
      jsonstat:contentscode_label ?indicator ;
      jsonstat:value ?value .
  }
}
ORDER BY ?year
```

The same SERVICE IRI works with Blazegraph even though Blazegraph appends its protocol query after
a second `?`; the dispatcher normalizes that request form. Write parameter separators as literal
`&` characters in SPARQL source. `&amp;` is HTML encoding and is not a valid replacement inside the
IRI.

The loader supports dense and sparse `value` objects, plus string, dense-array, sparse-object, and
deprecated one-item-array forms of `status`. Cube cells use JSON-stat row-major ordering. A cell
whose value is `null` is preserved as a `jsonstat:Observation` compatibility resource with its
dimensions and possible status, but it is not typed as `qb:Observation` or linked with
`qb:dataSet`. This keeps the Data Cube graph conformant with the requirement that every Data Cube
observation has a value for each declared measure.

Only HTTPS source URLs whose host belongs to `JSONSTAT_ALLOWED_SOURCE_DOMAINS` are accepted. A
listed domain also allows its subdomains, but matching is label-aware: `stat.fi` permits
`pxdata.stat.fi`, not `evilstat.fi` or `stat.fi.example.com`. The loader also rejects credentials,
fragments, local host names, and hosts resolving to non-public IP addresses, and repeats these
checks across redirects.

For Statistics Finland sources, the loader first compares JSON-stat dimension and category codes
against the curated local snapshot. It accepts an item only when its classification `localId`, item
`localId`, and code all exactly match the source dimension and category. If the separately enabled
network fallback is used, the same exact-match rules apply to the public classification API.
Successful responses are cached. Missing classifications, missing item codes, and classification
API failures do not prevent the original JSON-stat dataset from loading.

Every JSON-stat dimension has a local `skos:ConceptScheme`, and every category is represented as a
`skos:Concept`. An exact Statistics Finland match enriches those resources with localized metadata,
hierarchy, notes, API links, and reviewed ontology links such as YSO and Wikidata. The snapshot also
links known Statistics Finland statistics IDs to their statistics pages and accepted subject
concepts, and maps known units to UCUM metadata through `sdmx-attribute:unitMeasure`. Statistics
Finland concept IDs are intentionally
not duplicated: Wikidata property P14864 is their canonical integration path. The original code
predicate remains unchanged for backwards-compatible queries, while a generated
`{dimension}_concept` predicate links every cell directly to its concept:

```sparql
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX jsonstat: <https://sparqlbridge.toolforge.org/ontology/jsonstat/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?code ?classificationLabel ?apiResource WHERE {
  ?observation a qb:Observation ;
    jsonstat:ikaryhma_10_20180101 ?code ;
    jsonstat:ikaryhma_10_20180101_concept ?concept .
  ?concept skos:prefLabel ?classificationLabel ;
    skos:exactMatch ?apiResource .
  FILTER(LANG(?classificationLabel) = "fi")
}
LIMIT 20
```

## Sámi Place Names Endpoint

The repository stores the immutable source distribution as
`source-data/placenames_simple_saami_2026-05.nq.gz`. It contains North Sámi,
Inari Sámi, and Skolt Sámi names derived from the National Land Survey of
Finland Geographic Names dataset, version 2026-05. The matching JSON manifest
contains file sizes, SHA-256 checksums, graph IRI, counts, source, licence, and
attribution. The `source-data` directory, manifest, and RDF asset must be real
files and directories; symlinks are rejected.

On the first request, the application verifies the gzip asset and builds an
Oxigraph index in the dataset's fixed directory below
`OXIGRAPH_BASE_DIR/_static/placenames/` (currently `saami/`).
During that import it selects the field structure according to
`PLACENAMES_SCHEMA_MODE`. The default `hardcoded` mode uses the checked-in field
definition and avoids an additional graph scan; `dynamic` derives the fields,
RDF value types, coverage, and cardinality directly from the loaded graph. The
selected structure is saved in the store metadata.
Later requests reuse that index and schema. Loading is local and does not
download source data. Concurrent processes serialize the initial build with a
filesystem lock. After a new version has been imported successfully, older
data in that dataset's fixed Oxigraph directory is replaced, so old versions
are not retained as separate cache directories.

### URL

`/sparql?dataset=placenames`

The endpoint accepts `GET` and `POST` using the same SPARQL query formats as the
PetScan endpoint. The dataset's named graph is also configured as the default
query graph, so both bare triple patterns and explicit `GRAPH` patterns work.

```bash
curl --get 'http://127.0.0.1:8000/sparql?dataset=placenames' \
  --data-urlencode 'query=PREFIX pn: <https://sparqlbridge.toolforge.org/ontology/placenames/> SELECT ?name ?place WHERE { ?record pn:spelling ?name ; pn:place ?place . } LIMIT 20'
```

```sparql
PREFIX pn: <https://sparqlbridge.toolforge.org/ontology/placenames/>

SELECT ?name ?place WHERE {
  SERVICE <https://sparqlbridge.toolforge.org/sparql?dataset=placenames> {
    ?record pn:spelling ?name ;
            pn:place ?place .
  }
}
LIMIT 100
```

Dataset metadata and first-load status are available from
`/placenames/api/structure?dataset=saami`. Rebuilding the source distribution
is documented in `source-data/README_placenames_simple_saami.md`; the converter
dependency `pyproj` is intentionally development-only.

## Structure Endpoint

### URL

`/petscan/api/structure`

### Parameters

- `psid` (required): PetScan ID whose loaded structure metadata should be returned
- `refresh` (optional): `1/true` to force reload before returning structure metadata
- any additional URL query parameters are forwarded to PetScan JSON fetch (except reserved keys `psid`, `format`, `query`, `refresh`)

### Example `GET`

```bash
curl --get 'http://127.0.0.1:8000/petscan/api/structure' \
  --data-urlencode 'psid=43641756' \
  --data-urlencode 'category=Turku'
```

## Data Model Notes

Each PetScan row becomes one RDF resource:

- Subject: `https://petscan.wmcloud.org/psid/{psid}/item/{row-id}`
- Class: `https://petscan.wmcloud.org/ontology/Page`
- Row fields become predicates under `https://petscan.wmcloud.org/ontology/{field}`

Example field predicate:

- PetScan key `title` -> `https://petscan.wmcloud.org/ontology/title`

JSON-stat RDF uses the [W3C RDF Data Cube Vocabulary](https://www.w3.org/TR/vocab-data-cube/) as
its primary model:

- A source dataset is a `qb:DataSet` linked with `qb:structure` to one
  `qb:DataStructureDefinition`.
- JSON-stat dimensions become ordered `qb:DimensionProperty` components. Each uses a local
  `skos:ConceptScheme`, and every category is a `skos:Concept` linked from observations through a
  generated `{dimension}_concept` predicate.
- `jsonstat:value` is the DSD's `qb:MeasureProperty`. JSON-stat status and unit fields are optional
  `qb:AttributeProperty` components.
- Cells with non-null values are `qb:Observation` resources linked to the dataset with
  `qb:dataSet`.
- Exact Statistics Finland classification matches enrich the local SKOS resources with localized
  labels, hierarchy, explanatory notes, and classification API links.
- The earlier `jsonstat:Dataset`, `jsonstat:Observation`, `jsonstat:dataset`, dimension code/label,
  coordinate, note, and unit triples remain as a compatibility layer. Null-valued cells exist only
  in this compatibility layer so Data Cube observations always carry the declared measure.

## Limitations

- The app infers row location in PetScan JSON heuristically to support common PetScan JSON structures.
- Large `psid` result sets may take time to load and index.

## Deploying To Toolforge (Kubernetes)

This is an example deployment flow based on Toolforge shell + webservice.

### 1. Connect and switch to your tool account

```bash
ssh <username>@login.toolforge.org
become <toolname>
```

### 2. Create app directories and clone source

```bash
mkdir -p ~/www/python/src
cd ~/www/python/src
git clone https://github.com/Wikimedia-Suomi/PetscanSparqlEndpoint.git .
```

### 3. Create `uwsgi.ini`

Create `~/www/python/uwsgi.ini`:

```ini
[uwsgi]
module = app:app
static-map = /static=/data/project/<toolname>/www/python/src/static
buffer-size = 62768
```

### 4. WSGI entrypoint is included in repository

`app.py` is part of this repository, so no manual creation is needed after clone.

### 5. Set Toolforge environment variables

```bash
toolforge envvars create DJANGO_SECRET_KEY "<your-secret-unique-key>"
toolforge envvars create OXIGRAPH_BASE_DIR /tmp/data
toolforge envvars create DJANGO_ALLOWED_HOSTS <toolname>.toolforge.org
toolforge envvars create TOOLFORGE_USE_REPLICA 1
toolforge envvars create TOOLFORGE_REPLICA_CNF "$HOME/replica.my.cnf"
toolforge envvars create WIKIDATA_LOOKUP_BACKEND toolforge_sql
```

### 6. Build virtualenv and run checks in Toolforge shell

```bash
webservice --backend=kubernetes python3.13 shell
cd ~/www/python
python3 -m venv venv
source venv/bin/activate
cd src
python -m pip install --require-hashes -r constraints.txt
python manage.py check_replica_connections
python manage.py check_api_enrichment
TOOLFORGE_INTEGRATION_TESTS=1 python manage.py test
exit
```

### 7. Start the service

```bash
webservice --backend=kubernetes python3.13 start --cpu 1 --mem 6Gi
```

## Toolforge Replica Backend (Optional)

For Toolforge, `gil_link` Wikidata ID lookups can use wiki replicas instead of MediaWiki API.

Set environment variables:

```bash
export TOOLFORGE_USE_REPLICA=1
export WIKIDATA_LOOKUP_BACKEND=toolforge_sql
export TOOLFORGE_REPLICA_CNF=$HOME/replica.my.cnf
```

Behavior:

- Links are grouped by wiki.
- SQL lookup runs one parameterized query per wiki and uses a wiki-specific replica host like `fiwiki.web.db.svc.wikimedia.cloud`.
- DB credentials are read from `TOOLFORGE_REPLICA_CNF`.
- SQL connection is closed after each query.
- API mode is still available with `WIKIDATA_LOOKUP_BACKEND=api`.
- Set `OXIGRAPH_BASE_DIR` to the tool tmp path in Toolforge.

### Toolforge-only parity test

This test compares SQL and API lookup results for sample titles (including non-ASCII titles):

```bash
export TOOLFORGE_INTEGRATION_TESTS=1
./.venv/bin/python manage.py test tests.test_toolforge_integration
```

### Live MediaWiki API enrichment test

This opt-in test uses real MediaWiki API requests instead of mocks to verify that
`fetch_wikibase_items_for_site_api()` still returns the expected payload shape for
stable sample titles. It is skipped by default and intended to be run manually.

```bash
export LIVE_API_INTEGRATION_TESTS=1
./.venv/bin/python manage.py test tests.test_enrichment_api_integration
```

### Live PagePile API integration test

This opt-in test uses a real PagePile JSON payload and real MediaWiki API lookups instead of mocks
to verify that PagePile API-mode resolution still returns usable sitelink rows for a stable sample
pile. It also includes a Commons-specific sample that verifies namespace-6 file pages get
`https://commons.wikimedia.org/entity/M{page_id}` mediaitem entities. It is skipped by default
and intended to be run manually.

```bash
export LIVE_API_INTEGRATION_TESTS=1
# Optional overrides:
# export PAGEPILE_LIVE_ID=112306
# export PAGEPILE_LIVE_LIMIT=5
# export PAGEPILE_LIVE_COMMONS_ID=112301
# export PAGEPILE_LIVE_COMMONS_LIMIT=10
./.venv/bin/python manage.py test tests.test_pagepile_api_integration
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
