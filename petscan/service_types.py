"""Typed payload models for service responses and metadata."""

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, NotRequired, Optional, TypedDict


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


class StoreMeta(TypedDict):
    psid: int
    records: int
    source_url: str
    source_params: Dict[str, List[str]]
    loaded_at: str
    structure: StructureSummary


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

    def to_dict(self) -> StoreMeta:
        return {
            "psid": self.psid,
            "records": self.records,
            "source_url": self.source_url,
            "source_params": self.source_params,
            "loaded_at": self.loaded_at,
            "structure": self.structure,
        }


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
