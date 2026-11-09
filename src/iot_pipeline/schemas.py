"""BigQuery table definitions as code: ``schemas/bigquery/<dataset_key>/<table>.json``.

The same files are used by:
  * Terraform (10-foundation) to create the tables
  * DAGs, for load-job schemas and column lists in SQL generation
  * Unit tests, which check that every table named in config has a valid definition
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

COMPOSER_SCHEMA_DIR = Path("/home/airflow/gcs/data/schemas/bigquery")
REPO_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas" / "bigquery"

_COLUMN_RE = re.compile(r"^[a-z][a-z0-9_]{0,299}$")
BqType = Literal[
    "STRING", "BYTES", "INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOL",
    "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON", "RECORD",
]  # fmt: skip
PARTITIONABLE = {"DATE", "TIMESTAMP", "DATETIME"}
CLUSTERABLE = {"STRING", "INT64", "BOOL", "DATE", "TIMESTAMP", "DATETIME", "NUMERIC", "GEOGRAPHY"}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Column(_Strict):
    name: str
    type: BqType
    mode: Literal["NULLABLE", "REQUIRED", "REPEATED"] = "NULLABLE"
    description: str = Field(min_length=3)

    @field_validator("name")
    @classmethod
    def _snake_case(cls, v: str) -> str:
        if not _COLUMN_RE.match(v):
            raise ValueError(f"column {v!r} must be snake_case")
        return v


class Partitioning(_Strict):
    type: Literal["DAY", "HOUR", "MONTH", "YEAR"]
    field: str


class TableDefinition(_Strict):
    description: str = Field(min_length=10)
    partitioning: Partitioning | None = None
    require_partition_filter: bool = False
    clustering: list[str] = Field(default_factory=list, max_length=4)
    columns: list[Column] = Field(alias="schema", min_length=1)

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    @model_validator(mode="after")
    def _consistency(self) -> TableDefinition:
        by_name = {c.name: c for c in self.columns}
        if len(by_name) != len(self.columns):
            raise ValueError("duplicate column names")
        if self.partitioning:
            col = by_name.get(self.partitioning.field)
            if col is None or col.type not in PARTITIONABLE:
                raise ValueError(
                    f"partition field {self.partitioning.field!r} missing or not a date"
                )
            if col.mode != "REQUIRED":
                raise ValueError("partition field must be REQUIRED")
        elif self.require_partition_filter:
            raise ValueError("require_partition_filter without partitioning")
        for name in self.clustering:
            col = by_name.get(name)
            if col is None or col.type not in CLUSTERABLE or col.mode == "REPEATED":
                raise ValueError(f"clustering column {name!r} missing or not clusterable")
        return self

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_bq_schema(self) -> list[dict[str, str]]:
        """Schema in BigQuery REST/JSON form (load jobs, ``bq`` CLI)."""
        return [c.model_dump() for c in self.columns]


def schema_dir() -> Path:
    return COMPOSER_SCHEMA_DIR if COMPOSER_SCHEMA_DIR.is_dir() else REPO_SCHEMA_DIR


@lru_cache(maxsize=64)
def load_table_def(dataset_key: str, table: str, base: Path | None = None) -> TableDefinition:
    path = (base or schema_dir()) / dataset_key / f"{table}.json"
    return TableDefinition.model_validate(json.loads(path.read_text(encoding="utf-8")))


def discover(base: Path | None = None) -> dict[tuple[str, str], Path]:
    """All table definition files, keyed by ``(dataset_key, table)``."""
    root = base or schema_dir()
    return {(p.parent.name, p.stem): p for p in sorted(root.glob("*/*.json"))}
