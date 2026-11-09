"""Planning logic of the ephemeral Composer lifecycle (pure, no cloud)."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "composer_lifecycle.py"
_spec = importlib.util.spec_from_file_location("composer_lifecycle", _PATH)
assert _spec and _spec.loader
lc = importlib.util.module_from_spec(_spec)
sys.modules["composer_lifecycle"] = lc
_spec.loader.exec_module(lc)

H = timedelta(hours=1)
START = datetime(2026, 11, 9, tzinfo=UTC)


def t(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=UTC)


# ---- which hours to ingest ---------------------------------------------------- #
def test_resumes_after_last_published_hour() -> None:
    assert lc.plan_hours(t(9, 4), t(9, 7, 20), 6, START) == [t(9, 5), t(9, 6)]


def test_caps_catch_up_to_most_recent_hours() -> None:
    hours = lc.plan_hours(t(9, 0), t(10, 12, 5), 6, START)
    assert hours == [t(10, 6) + i * H for i in range(6)]


def test_first_ever_run_takes_latest_completed_hour() -> None:
    assert lc.plan_hours(None, t(9, 7, 59), 6, START) == [t(9, 6)]


def test_always_includes_latest_hour_even_if_already_done() -> None:
    # Re-running inside the same hour re-processes the latest hour (idempotent) rather
    # than doing nothing.
    assert lc.plan_hours(t(9, 6), t(9, 7, 30), 6, START) == [t(9, 6)]


def test_never_before_pipeline_start_date() -> None:
    assert lc.plan_hours(None, t(9, 2, 10), 6, t(9, 1)) == [t(9, 1)]


def test_last_batch_hour_from_manifest_names() -> None:
    names = [
        "manifests/iot_telemetry/dt=2026-11-09/hour=03/part-20261109T0300.json",
        "manifests/iot_telemetry/dt=2026-11-10/hour=01/part-20261110T0100.json",
        "manifests/iot_telemetry/README",
    ]
    assert lc.last_batch_hour(names) == t(10, 1)
    assert lc.last_batch_hour([]) is None


# ---- what to deploy ------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("name", "managed"),
    [
        ("dags/iot_01_ingest_raw.py", True),
        ("dags/.airflowignore", True),
        ("dags/iot_pipeline/etl/silver.py", True),
        ("data/sql/silver/02_merge_silver.sql", True),
        ("data/config/envs/dev.yaml", True),
        ("dags/airflow_monitoring.py", False),  # Composer's own file: never touched
        ("logs/dag_id=x/run.log", False),
        ("dags/iot_pipeline/__pycache__/x.pyc", False),
    ],
)
def test_managed_objects(name: str, managed: bool) -> None:
    assert lc.is_managed(name) is managed


def test_collect_local_maps_repo_layout_to_bucket_layout() -> None:
    local = lc.collect_local()
    assert {
        "dags/iot_01_ingest_raw.py",
        "dags/iot_02_raw_to_silver.py",
        "dags/iot_03_silver_to_gold.py",
        "dags/.airflowignore",
        "dags/iot_pipeline/config.py",
        "dags/iot_pipeline/etl/silver.py",
        "data/sql/silver/02_merge_silver.sql",
        "data/schemas/bigquery/silver/telemetry.json",
        "data/config/envs/dev.yaml",
    } <= set(local)
    assert all(lc.is_managed(n) for n in local)
    assert not any("__pycache__" in n for n in local)


def test_plan_sync_uploads_changes_and_deletes_only_managed_orphans() -> None:
    local = {"dags/iot_a.py": b"new", "data/sql/x.sql": b"same"}
    remote = {
        "dags/iot_a.py": lc.md5_b64(b"old"),
        "data/sql/x.sql": lc.md5_b64(b"same"),
        "data/sql/removed.sql": lc.md5_b64(b"gone"),
        "dags/airflow_monitoring.py": lc.md5_b64(b"composer"),
    }
    uploads, deletes = lc.plan_sync(local, remote)
    assert uploads == ["dags/iot_a.py"]
    assert deletes == ["data/sql/removed.sql"]
