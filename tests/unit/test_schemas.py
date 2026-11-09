"""Contract tests: table definition files, config table names and the schema model."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from iot_pipeline.config import REPO_CONFIG_DIR, load_config
from iot_pipeline.schemas import REPO_SCHEMA_DIR, TableDefinition, discover, load_table_def

CFG = load_config(REPO_CONFIG_DIR / "dev.yaml")
FILES = discover(REPO_SCHEMA_DIR)

# pipeline.tables key prefix -> dataset key (landing tables are transient, created per load).
PREFIX_TO_DATASET = {"silver_": "silver", "gold_": "gold", "ops_": "ops"}


def _config_tables() -> set[tuple[str, str]]:
    out = set()
    for key, table in CFG.pipeline.tables.model_dump().items():
        for prefix, dataset in PREFIX_TO_DATASET.items():
            if key.startswith(prefix):
                out.add((dataset, table))
    return out


@pytest.mark.parametrize("key", sorted(FILES), ids=lambda k: f"{k[0]}.{k[1]}")
def test_every_definition_is_valid(key: tuple[str, str]) -> None:
    td = load_table_def(*key, base=REPO_SCHEMA_DIR)
    assert td.columns


def test_config_tables_and_files_match_exactly() -> None:
    managed = {k for k in FILES if k[0] != "landing"}
    assert managed == _config_tables(), "config pipeline.tables and schemas/ have drifted"


def test_dataset_dirs_exist_in_config() -> None:
    configured = set(CFG.bigquery.datasets.model_dump())
    assert {k[0] for k in FILES} <= configured


def _base() -> dict:
    return json.loads((REPO_SCHEMA_DIR / "silver" / "telemetry.json").read_text("utf-8"))


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (lambda d: d["schema"].append({**d["schema"][0]}), "duplicate"),
        (lambda d: d["partitioning"].update(field="nope"), "partition field"),
        (lambda d: d.update(clustering=["dq_flags"]), "clustering"),
        (lambda d: d.update(clustering=["a", "b", "c", "d", "e"]), "clustering"),
        (lambda d: d["schema"][0].update(name="Event ID"), "snake_case"),
        (lambda d: d["schema"][0].update(type="VARCHAR"), "type"),
        (lambda d: d.update(partitioning=None), "require_partition_filter"),
    ],
)
def test_invalid_definitions_rejected(mutate, error: str) -> None:  # type: ignore[no-untyped-def]
    data = _base()
    mutate(data)
    with pytest.raises(ValidationError, match=error):
        TableDefinition.model_validate(data)


def test_bq_schema_round_trip(tmp_path: Path) -> None:
    td = load_table_def("silver", "telemetry", base=REPO_SCHEMA_DIR)
    assert td.to_bq_schema()[0] == {
        "name": "event_id",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": td.columns[0].description,
    }
    assert "event_date" in td.column_names
