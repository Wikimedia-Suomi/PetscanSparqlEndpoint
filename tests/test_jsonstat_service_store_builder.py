from typing import Any

from jsonstat import (
    service,
    service_classification,
    service_runtime_links,
    service_source,
    service_store_builder,
)
from petscan import service_store as store
from tests.service_test_support import ServiceTestCase

EXAMPLE_URL = "https://pxdata.stat.fi/PxWeb/sq/552d1f53-bdab-472b-a8e7-68b5b8c37cda"


class JsonstatStoreBuilderTests(ServiceTestCase):
    def test_build_store_writes_observations_and_dataset_metadata(self) -> None:
        if service_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")
        payload = self._load_payload("jsonstat-552d1f53.json")
        records = service_source.extract_records(payload)
        store_id = service.internal_store_id(EXAMPLE_URL)
        self._cleanup_store(store_id)

        meta = service_store_builder.build_store(
            store_id=store_id,
            records=records,
            payload=payload,
            source_url=EXAMPLE_URL,
            source_params={"url": [EXAMPLE_URL]},
        )
        store_instance = service_store_builder.Store(str(store.store_path(store_id)))

        self.assertTrue(
            store_instance.query(
                """
                PREFIX jsonstat: <https://sparqlbridge.toolforge.org/ontology/jsonstat/>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                ASK {
                  ?observation a qb:Observation, jsonstat:Observation ;
                    qb:dataSet ?dataset ;
                    jsonstat:timeperiod_y "1990" ;
                    jsonstat:timeperiod_y_concept ?yearConcept ;
                    jsonstat:ikaryhma_10_20180101_label "Total" ;
                    jsonstat:contentscode_unit_base "number" ;
                    jsonstat:contentscode_unit_decimals "0"^^xsd:integer ;
                    jsonstat:value "1526457"^^xsd:integer ;
                    jsonstat:dataset ?dataset .
                  ?dataset a qb:DataSet, jsonstat:Dataset ;
                    qb:structure ?structure ;
                    jsonstat:label ?label ;
                    jsonstat:sourceUrl <https://pxdata.stat.fi/PxWeb/sq/552d1f53-bdab-472b-a8e7-68b5b8c37cda> ;
                    jsonstat:dimension ?dimension .
                  ?structure a qb:DataStructureDefinition ;
                    qb:component ?dimensionComponent, ?measureComponent, ?attributeComponent .
                  ?dimensionComponent qb:dimension jsonstat:timeperiod_y_concept ;
                    qb:componentProperty jsonstat:timeperiod_y_concept ;
                    qb:order 1 .
                  ?measureComponent qb:measure jsonstat:value .
                  ?attributeComponent qb:attribute jsonstat:contentscode_unit_base ;
                    qb:componentRequired false .
                  jsonstat:timeperiod_y_concept a qb:DimensionProperty, qb:CodedProperty ;
                    qb:codeList ?yearScheme .
                  jsonstat:value a qb:MeasureProperty .
                  jsonstat:contentscode_unit_base a qb:AttributeProperty .
                  ?yearConcept a skos:Concept ;
                    skos:notation "1990" ;
                    skos:inScheme ?yearScheme .
                  ?dimension a jsonstat:Dimension ;
                    jsonstat:dimensionId "timeperiod_y" ;
                    jsonstat:category ?category .
                  ?category jsonstat:categoryCode "1990" .
                }
                """
            )
        )
        self.assertEqual(meta["records"], 7)
        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(field_map["value"]["primary_type"], "xsd:integer")
        self.assertEqual(
            field_map["timeperiod_y"]["predicate"],
            "https://sparqlbridge.toolforge.org/ontology/jsonstat/timeperiod_y",
        )
        self.assertEqual(field_map["timeperiod_y_concept"]["primary_type"], "iri")
        self.assertEqual(field_map["contentscode_note"]["row_side_cardinality"], "1")

    def test_build_store_writes_exact_classification_and_skos_links(self) -> None:
        if service_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")
        payload = self._load_payload("jsonstat-552d1f53.json")
        records = service_source.extract_records(payload)
        store_id = service.internal_store_id(EXAMPLE_URL)
        self._cleanup_store(store_id)
        classification_id = "ikaryhma_10_20180101"
        classification_url = service_classification.classification_api_url(classification_id)
        item_url = service_classification.classification_item_api_url(
            classification_id,
            "SSS",
        )
        item = service_classification.ClassificationItemEnrichment(
            code="SSS",
            api_url=item_url,
            labels={"fi": ["Yhteensä"]},
            level=0.0,
            order=10,
            notes={"generalNote": {"fi": ["Kaikki ikäryhmät yhteensä."]}},
            external_links=[
                (
                    "http://www.w3.org/2004/02/skos/core#closeMatch",
                    "http://www.wikidata.org/entity/Q159",
                )
            ],
        )
        enrichment = service_classification.ClassificationEnrichment(
            local_id=classification_id,
            api_url=classification_url,
            labels={"fi": ["Px-koodisto ikä"]},
            descriptions={"fi": ["Taulukoinnissa käytettävä ikäkoodisto."]},
            purposes={"fi": ["Ikätietojen taulukointi."]},
            series_id="ikaryhma",
            series_labels={"fi": ["Ikäryhmät"]},
            release_date="2018-01-01",
            modified_at="2026-03-12T14:21:45Z",
            national_recommendation=False,
            international_recommendation=False,
            items={"SSS": item},
        )

        payload["extension"]["px"]["subject-code"] = "KORA"
        payload["dimension"]["contentscode"]["category"]["unit"]["akuo_lkm"][
            "base"
        ] = "prosenttia"
        runtime_links = service_runtime_links.load_runtime_links()

        meta = service_store_builder.build_store(
            store_id=store_id,
            records=records,
            payload=payload,
            source_url=EXAMPLE_URL,
            source_params={"url": [EXAMPLE_URL]},
            classification_enrichments={classification_id: enrichment},
            runtime_links=runtime_links,
        )
        store_instance = service_store_builder.Store(str(store.store_path(store_id)))

        self.assertTrue(
            store_instance.query(
                """
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX jsonstat: <https://sparqlbridge.toolforge.org/ontology/jsonstat/>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                PREFIX sdmx-attribute: <http://purl.org/linked-data/sdmx/2009/attribute#>
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                ASK {
                  ?observation a qb:Observation ;
                    jsonstat:ikaryhma_10_20180101 "SSS" ;
                    jsonstat:ikaryhma_10_20180101_concept ?concept .
                  ?concept a skos:Concept ;
                    skos:notation "SSS" ;
                    skos:prefLabel "Yhteensä"@fi ;
                    skos:inScheme ?scheme ;
                    skos:exactMatch <CLASSIFICATION_ITEM_URL> ;
                    skos:closeMatch <http://www.wikidata.org/entity/Q159> ;
                    jsonstat:classificationGeneralNote "Kaikki ikäryhmät yhteensä."@fi ;
                    jsonstat:classificationLevel "0"^^xsd:integer .
                  ?dimension jsonstat:dimensionId "ikaryhma_10_20180101" ;
                    jsonstat:classification ?scheme .
                  ?scheme a skos:ConceptScheme ;
                    skos:prefLabel "Px-koodisto ikä"@fi ;
                    dct:issued "2018-01-01"^^xsd:date ;
                    jsonstat:apiResource <CLASSIFICATION_URL> .
                  ?dataset a qb:DataSet ;
                    jsonstat:statisticsId "kora" ;
                    jsonstat:statisticsResource <https://stat.fi/tilasto/kora> ;
                    dct:subject <http://www.wikidata.org/entity/Q2144402> .
                  ?measureCategory skos:notation "akuo_lkm" ;
                    jsonstat:unitMappingStatus "accepted" ;
                    jsonstat:ucumCode "%" ;
                    jsonstat:unitConcept <http://urn.fi/URN:NBN:fi:au:ucum:r40> ;
                    sdmx-attribute:unitMeasure <http://urn.fi/URN:NBN:fi:au:ucum:r40> .
                }
                """.replace("CLASSIFICATION_ITEM_URL", item_url).replace(
                    "CLASSIFICATION_URL",
                    classification_url,
                )
            )
        )
        field_map = {field["source_key"]: field for field in meta["structure"]["fields"]}
        self.assertEqual(
            field_map["ikaryhma_10_20180101_concept"]["primary_type"],
            "iri",
        )

    def test_null_cell_is_preserved_but_not_published_as_qb_observation(self) -> None:
        if service_store_builder.Store is None:
            self.skipTest("pyoxigraph is not installed")
        payload: dict[str, Any] = {
            "version": "2.0",
            "class": "dataset",
            "id": ["area"],
            "size": [2],
            "dimension": {
                "area": {
                    "label": "Area",
                    "category": {
                        "index": ["FI", "SE"],
                        "label": {"FI": "Finland", "SE": "Sweden"},
                    },
                }
            },
            "value": [1, None],
            "status": {"1": "."},
        }
        records = service_source.extract_records(payload)
        store_id = service.internal_store_id(EXAMPLE_URL + "?null-cell=1")
        self._cleanup_store(store_id)

        service_store_builder.build_store(
            store_id=store_id,
            records=records,
            payload=payload,
            source_url=EXAMPLE_URL,
        )
        store_instance = service_store_builder.Store(str(store.store_path(store_id)))

        self.assertTrue(
            store_instance.query(
                """
                PREFIX jsonstat: <https://sparqlbridge.toolforge.org/ontology/jsonstat/>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                ASK {
                  ?valuedCell a qb:Observation ;
                    qb:dataSet ?dataset ;
                    jsonstat:position 0 ;
                    jsonstat:area_concept ?finland ;
                    jsonstat:value 1 .
                  ?missingCell a jsonstat:Observation ;
                    jsonstat:dataset ?dataset ;
                    jsonstat:position 1 ;
                    jsonstat:area_concept ?sweden ;
                    jsonstat:status "." .
                  FILTER NOT EXISTS { ?missingCell a qb:Observation }
                  FILTER NOT EXISTS { ?missingCell qb:dataSet ?qbDataset }
                  FILTER NOT EXISTS { ?missingCell jsonstat:value ?missingValue }
                }
                """
            )
        )
        self.assertFalse(
            store_instance.query(
                """
                PREFIX jsonstat: <https://sparqlbridge.toolforge.org/ontology/jsonstat/>
                PREFIX qb: <http://purl.org/linked-data/cube#>
                ASK {
                  {
                    ?observation a qb:Observation .
                    FILTER NOT EXISTS { ?observation qb:dataSet ?dataset }
                  }
                  UNION
                  {
                    ?observation a qb:Observation .
                    FILTER NOT EXISTS { ?observation jsonstat:area_concept ?area }
                  }
                  UNION
                  {
                    ?observation a qb:Observation .
                    FILTER NOT EXISTS { ?observation jsonstat:value ?value }
                  }
                }
                """
            )
        )
