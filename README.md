# PetScan SPARQL Endpoint

This is a django app which works as SPARQL endpoint for PetScan query results.

## Requirements

- Python 3.9+
- `pip`
- Django
- pyoxigraph
- PyMySQL

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

### Run app

```bash
source .venv/bin/activate
export DJANGO_SECRET_KEY='dev-only-change-me'
export OXIGRAPH_BASE_DIR="$PWD/data/oxigraph"
python manage.py runserver
```

Open [http://127.0.0.1:8000/](http://127.0.0.1:8000/).

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

`/sparql`

### Parameters

- `psid` (required): PetScan ID whose Oxigraph dataset should be queried
- `query` (required): SPARQL query (for `GET`) or in the request body (for `POST`)
- `refresh` (optional): `1/true` to force reloading PetScan data before query
- any additional URL query parameters are forwarded to PetScan JSON fetch (except reserved keys `psid`, `format`, `query`, `refresh`)
- `POST /sparql` must use `Content-Type: application/sparql-query`
- In the web UI, use the **PetScan extra GET params** field (example: `category=Turku&language=fi`) to simulate `SERVICE` URI parameters.

### Example `GET`

```bash
curl --get 'http://127.0.0.1:8000/sparql' \
  --data-urlencode 'psid=43641756' \
  --data-urlencode 'query=SELECT ?item ?title WHERE { ?item a <https://petscan.wmcloud.org/ontology/Page> . OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title } } LIMIT 5'
```

### Example `SERVICE` usage

You can include this endpoint in a federated query by encoding `psid` in the endpoint URL:

```sparql
SELECT ?item ?title WHERE {
  SERVICE <http://127.0.0.1:8000/sparql?psid=43641756> {
    ?item a <https://petscan.wmcloud.org/ontology/Page> .
    OPTIONAL { ?item <https://petscan.wmcloud.org/ontology/title> ?title }
  }
}
LIMIT 20
```

## Structure Endpoint

### URL

`/api/structure`

### Parameters

- `psid` (required): PetScan ID whose loaded structure metadata should be returned
- `refresh` (optional): `1/true` to force reload before returning structure metadata
- any additional URL query parameters are forwarded to PetScan JSON fetch (except reserved keys `psid`, `format`, `query`, `refresh`)

### Example `GET`

```bash
curl --get 'http://127.0.0.1:8000/api/structure' \
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
