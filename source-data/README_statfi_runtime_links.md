# Statistics Finland runtime links

`statfi_runtime_links.json` is a deliberately small, reviewed runtime subset for linking
Statistics Finland JSON-stat data to other datasets and ontologies. It is the only Statistics
Finland enrichment snapshot that belongs in this repository.

The current snapshot contains:

- 7 classifications and 1,134 exact classification-item links;
- 134 canonical Statistics Finland statistics IDs, of which 28 have accepted subject links;
- 79 reviewed statistical-unit mappings; and
- links to resources such as Wikidata, YSO-family ontologies, Lexvo, the Ministry of Education
  and Culture classification, and Finto UCUM.

Only exact, accepted links needed at runtime are included. Review candidates, raw API responses,
complete classification and concept exports, table metadata, matching audits, and intermediate
identifier pairs are intentionally excluded.

Statistics Finland concept IDs are also excluded. They already have the Wikidata property
[`Statistics Finland concept ID` (P14864)](https://www.wikidata.org/wiki/Property:P14864), so
Wikidata is the intended future integration source for them.

The harvesting, candidate generation, matching, and Wikidata-import work is maintained outside
this application and handled as a separate task. It is intentionally **not reproducible from this
repository**. Updating the runtime file means replacing it with a separately produced and reviewed
snapshot while preserving the documented schema and contract tests.

The snapshot metadata records its selection policy, source retrieval time, counts, and license.
The file is currently under 1 MiB; tests keep it below 2 MiB and verify all published counts.
Source data is licensed under CC BY 4.0 as recorded in the snapshot metadata.
