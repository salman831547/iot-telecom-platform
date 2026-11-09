"""DAG integrity tests: run against the real Airflow version Composer runs (Linux only).

Run: ``PYTHONPATH=src pytest tests/dags`` with Airflow installed
(see cloudbuild/dag-tests.yaml). Skipped automatically when Airflow is absent.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("airflow")

from iot_pipeline.config import REPO_CONFIG_DIR, load_config  # noqa: E402

try:  # Airflow >= 3.1
    from airflow.dag_processing.dagbag import DagBag  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    from airflow.models.dagbag import DagBag  # type: ignore[import-not-found,no-redef]

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
CFG = load_config(REPO_CONFIG_DIR / "dev.yaml")
IDS = CFG.pipeline.dags


@pytest.fixture(scope="module")
def dagbag() -> Any:
    t0 = time.monotonic()
    # Examples are disabled via AIRFLOW__CORE__LOAD_EXAMPLES=False (3.3 removed the kwarg).
    bag = DagBag(dag_folder=str(DAGS_DIR))
    bag.parse_seconds = time.monotonic() - t0
    return bag


def test_no_import_errors(dagbag: Any) -> None:
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_exactly_the_configured_dags(dagbag: Any) -> None:
    assert set(dagbag.dag_ids) == {IDS.ingest, IDS.silver, IDS.gold}


def test_parsing_is_fast_and_side_effect_free(dagbag: Any) -> None:
    # No network calls or heavy work at import time: the scheduler re-parses constantly.
    assert dagbag.parse_seconds < 10


@pytest.mark.parametrize("dag_id", [IDS.ingest, IDS.silver, IDS.gold])
def test_dag_settings_come_from_config(dagbag: Any, dag_id: str) -> None:
    dag = dagbag.get_dag(dag_id)
    p = CFG.pipeline
    assert dag.catchup is p.catchup
    key = {IDS.ingest: "ingest", IDS.silver: "silver", IDS.gold: "gold"}[dag_id]
    assert dag.max_active_runs == getattr(p.max_active_runs, key)
    assert dag.dagrun_timeout.total_seconds() == p.dagrun_timeout_minutes * 60
    assert dag.is_paused_upon_creation is False  # recreated Composer resumes by itself
    assert CFG.env in set(dag.tags)
    assert dag.doc_md
    for t in dag.tasks:
        assert t.retries == p.default_task.retries, t.task_id
        assert t.execution_timeout is not None, t.task_id
        assert t.on_failure_callback, t.task_id


def test_schedules(dagbag: Any) -> None:
    def kind(dag_id: str) -> str:
        return type(dagbag.get_dag(dag_id).timetable).__name__

    # Silver and gold run only when triggered; ingest follows config (null = on-demand).
    assert kind(IDS.silver) == "NullTimetable"
    assert kind(IDS.gold) == "NullTimetable"
    assert (kind(IDS.ingest) == "NullTimetable") is (CFG.pipeline.schedule is None)


def _up(dag: Any, task_id: str) -> set[str]:
    return set(dag.get_task(task_id).upstream_task_ids)


def test_ingest_graph(dagbag: Any) -> None:
    dag = dagbag.get_dag(IDS.ingest)
    assert set(dag.task_ids) == {
        "generate_and_upload", "validate_raw_file", "publish_manifest", "audit", "trigger_silver",
    }  # fmt: skip
    assert _up(dag, "validate_raw_file") == {"generate_and_upload"}
    assert {"validate_raw_file", "generate_and_upload"} <= _up(dag, "publish_manifest")
    assert _up(dag, "trigger_silver") == {"audit"}
    assert dag.get_task("trigger_silver").trigger_dag_id == IDS.silver


def test_silver_graph(dagbag: Any) -> None:
    dag = dagbag.get_dag(IDS.silver)
    assert set(dag.task_ids) == {
        "find_new_files", "has_new_files", "transform.load_landing", "transform.merge_silver",
        "transform.check_silver_quality", "audit", "trigger_gold",
    }  # fmt: skip
    assert "has_new_files" in _up(dag, "transform.load_landing")  # short-circuit gates work
    assert "transform.load_landing" in _up(dag, "transform.merge_silver")
    assert _up(dag, "trigger_gold") == {"audit"}
    assert dag.get_task("trigger_gold").trigger_dag_id == IDS.gold


def test_gold_graph(dagbag: Any) -> None:
    dag = dagbag.get_dag(IDS.gold)
    assert set(dag.task_ids) == {"resolve_dates", "rebuild_gold", "check_gold_quality", "audit"}
    assert "rebuild_gold" in _up(dag, "check_gold_quality")
    assert {"rebuild_gold", "check_gold_quality"} <= _up(dag, "audit")


def test_trigger_targets_exist(dagbag: Any) -> None:
    for dag in dagbag.dags.values():
        for t in dag.tasks:
            target = getattr(t, "trigger_dag_id", None)
            if target:
                assert target in dagbag.dag_ids, f"{dag.dag_id}.{t.task_id} -> {target}"


def test_cron_schedule_gives_real_hourly_intervals() -> None:
    """Airflow 3's default for cron strings has zero-length intervals (start == end),
    which broke the scheduled ingest run in the first live cycle. Guard against it."""
    import pendulum  # Airflow timetables take pendulum datetimes

    from iot_pipeline.airflow_support import ingest_schedule

    hourly = CFG.model_copy(
        update={"pipeline": CFG.pipeline.model_copy(update={"schedule": "@hourly"})}
    )
    tt = ingest_schedule(hourly)
    assert type(tt).__name__ == "CronDataIntervalTimetable"
    interval = tt.infer_manual_data_interval(
        run_after=pendulum.datetime(2026, 11, 9, 8, 30, tz="UTC")
    )
    assert (interval.end - interval.start).total_seconds() == 3600
    assert ingest_schedule(CFG) is None or CFG.pipeline.schedule is not None
