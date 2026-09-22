"""Typed payload models for service responses and metadata."""

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, NotRequired, Optional, TypedDict

GIL_CATEGORIES_SCHEMA_VERSION = 3
ITEM_CATEGORIES_SCHEMA_VERSION = 2
PETSCAN_STORE_SCHEMA_VERSION = 1


class StructureField(TypedDict):
    source_key: str
    predicate: str
    present_in_rows: int
    primary_type: str
    observed_types: List[str]
    row_side_cardinality: NotRequired[Literal["1", "M"]]


class StructureSummary(TypedDict):
    row_count: int
    field_count: int
    fields: List[StructureField]


class EnrichmentOptions(TypedDict, total=False):
    petscan_store_schema_version: int
    gil_categories: bool
    gil_categories_schema_version: int
    item_categories: bool
    item_categories_schema_version: int


class StoreMeta(TypedDict):
    psid: int
    records: int
    source_url: str
    source_params: Dict[str, List[str]]
    loaded_at: str
    structure: StructureSummary
    enrichment_options: NotRequired[EnrichmentOptions]


class QueryExecution(TypedDict, total=False):
    query_type: str
    result_format: str
    sparql_json: Dict[str, Any]
    ntriples: str
    meta: StoreMeta


@dataclass(frozen=True)
class StoreMetaModel:
    psid: int
    records: int
    source_url: str
    source_params: Dict[str, List[str]]
    loaded_at: str
    structure: StructureSummary
    enrichment_options: Optional[EnrichmentOptions] = None

    def to_dict(self) -> StoreMeta:
        payload: StoreMeta = {
            "psid": self.psid,
            "records": self.records,
            "source_url": self.source_url,
            "source_params": self.source_params,
            "loaded_at": self.loaded_at,
            "structure": self.structure,
        }
        if self.enrichment_options:
            options: EnrichmentOptions = {}
            raw_store_schema_version = self.enrichment_options.get(
                "petscan_store_schema_version"
            )
            if isinstance(raw_store_schema_version, int) and not isinstance(
                raw_store_schema_version, bool
            ):
                options["petscan_store_schema_version"] = raw_store_schema_version
            if "gil_categories" in self.enrichment_options:
                options["gil_categories"] = bool(
                    self.enrichment_options["gil_categories"]
                )
            raw_schema_version = self.enrichment_options.get(
                "gil_categories_schema_version"
            )
            if isinstance(raw_schema_version, int) and not isinstance(
                raw_schema_version, bool
            ):
                options["gil_categories_schema_version"] = raw_schema_version
            if "item_categories" in self.enrichment_options:
                options["item_categories"] = bool(
                    self.enrichment_options["item_categories"]
                )
            raw_item_schema_version = self.enrichment_options.get(
                "item_categories_schema_version"
            )
            if isinstance(raw_item_schema_version, int) and not isinstance(
                raw_item_schema_version, bool
            ):
                options["item_categories_schema_version"] = raw_item_schema_version
            payload["enrichment_options"] = options
        return payload


@dataclass(frozen=True)
class QueryExecutionModel:
    query_type: str
    result_format: str
    meta: StoreMeta
    sparql_json: Optional[Dict[str, Any]] = None
    ntriples: Optional[str] = None

    def to_dict(self) -> QueryExecution:
        payload: QueryExecution = {
            "query_type": self.query_type,
            "result_format": self.result_format,
            "meta": self.meta,
        }
        if self.result_format == "sparql-json":
            payload["sparql_json"] = self.sparql_json if self.sparql_json is not None else {}
        else:
            payload["ntriples"] = self.ntriples if self.ntriples is not None else ""
        return payload
