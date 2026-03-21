# PetScan SPARQL Endpoint

This is a django app which works as SPARQL endpoint for PetScan query results.

## TL;DR

- Give the app a PetScan `psid`, and it turns that PetScan JSON result into a local RDF dataset.
- The dataset is stored in Oxigraph, and exposed via a SPARQL endpoint at `/petscan/sparql/...`.
- Web UI flow: load PetScan data -> inspect generated fields/structure -> run SPARQL queries.
- Optional enrichment adds Wikidata-related fields for `gil_link` targets (API or Toolforge SQL backend).

## Requirements

- Python 3.9+
- `pip`
- Django
- pyoxigraph
- PyMySQL
- rdflib

## Development Setup And Commands

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
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

### Run browser E2E tests against live PetScan

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
# Optional overrides:
# export PETSCAN_E2E_PSID=43641756
# export PETSCAN_E2E_OUTPUT_LIMIT=5
# export PLAYWRIGHT_DEFAULT_TIMEOUT_MS=60000
./scripts/run_e2e_tests.sh
```

This E2E script keeps its own temporary Oxigraph store under `OXIGRAPH_BASE_DIR`, so the initial
load is not satisfied from a previous cached dataset. Unlike the smoke tests, it uses real network
requests to PetScan and is intentionally kept out of the default `run_tests.sh` path.

### Run app

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
python manage.py runserver
```

Open [http://127.0.0.1:8000/petscan/](http://127.0.0.1:8000/petscan/).

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

## Environment Configuration

Security-related Django settings are configured via environment variables:

- `DJANGO_SECRET_KEY` (required)
- `DJANGO_DEBUG` (`1/true/yes/on` enables debug; default: disabled)
- `DJANGO_ALLOWED_HOSTS` (comma-separated list, example: `localhost,127.0.0.1,mydomain.tld`)
- `OXIGRAPH_BASE_DIR` (required absolute path for Oxigraph store directory)

## Example PetScan JSON Files

- `data/examples/petscan-43641756.json.gz`
- `data/examples/petscan-43642782.json.gz`

## SPARQL Endpoint

### URL

`/petscan/sparql/<path:service_params>`

### Parameters

- `psid` (required): PetScan ID whose Oxigraph dataset should be queried
- `query` (required): SPARQL query (for `GET`) or in the request body (for `POST`)
- `refresh` (optional): `1/true` to force reloading PetScan data before query
- any additional URL query parameters are forwarded to PetScan JSON fetch (except reserved keys `psid`, `format`, `query`, `refresh`)
- `POST /petscan/sparql` supports `Content-Type: application/sparql-query` and `application/x-www-form-urlencoded`
- In the web UI, use the **PetScan extra GET params** field (example: `category=Turku&language=fi`) to simulate `SERVICE` URI parameters.

### Example `GET`

```bash
curl --get 'http://127.0.0.1:8000/petscan/sparql/psid=43641756' \
  --data-urlencode 'query=SELECT ?item ?title WHERE { ?item a <https://petscan.wmcloud.org/ontology/Page> . OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title } } LIMIT 5'
```

### Example `SERVICE` usage

You can include this endpoint in a federated query by encoding `psid` in the endpoint URL:

```sparql
SELECT ?item ?title WHERE {
  SERVICE <http://127.0.0.1:8000/petscan/sparql/psid=43641756> {
    ?item a <https://petscan.wmcloud.org/ontology/Page> .
    OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title }
  }
}
LIMIT 20
```

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
pip install -r requirements.txt
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

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
