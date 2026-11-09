"""Airflow-free tests for SQL rendering, ETL helpers and the Airflow glue."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from iot_pipeline import sql
from iot_pipeline.airflow_support import (
    dag_kwargs,
    default_args,
    failure_audit_callback,
    run_context,
)
from iot_pipeline.config import REPO_CONFIG_DIR, load_config
from iot_pipeline.etl import DataQualityError, RunContext, gold, silver
from iot_pipeline.schemas import REPO_SCHEMA_DIR, load_table_def

CFG = load_config(REPO_CONFIG_DIR / "dev.yaml")
IDS = {"landing_table": "p.d.l", "silver_table": "p.d.s", "watermark_table": "p.d.w"}


# ---- SQL templates ---------------------------------------------------------- #
def test_render_substitutes_only_braced_identifiers() -> None:
    text = sql.render("silver/02_merge_silver", **IDS)
    assert "${" not in text
    assert "`p.d.s`" in text
    assert r"(dbm|db|%|c)$'" in text  # regex anchor left untouched


@pytest.mark.parametrize("bad", ["p.d.t`; DROP TABLE x; --", "a b", "p.d.t.x.y"])
def test_render_rejects_unsafe_identifiers(bad: str) -> None:
    with pytest.raises(ValueError, match="unsafe"):
        sql.render("silver/02_merge_silver", **{**IDS, "silver_table": bad})


def test_render_requires_every_placeholder() -> None:
    with pytest.raises(KeyError):
        sql.render("silver/02_merge_silver", landing_table="p.d.l")


def test_all_sql_files_render(tmp_path: Path) -> None:
    gold_ids = {"silver_table": "p.d.s", "gold_device_table": "p.d.g", "gold_cell_table": "p.d.c"}
    for name, ids in {
        "silver/01_landing_from_raw": {},
        "silver/02_merge_silver": IDS,
        "silver/03_dq_silver": {"silver_table": "p.d.s"},
        "gold/01_rebuild_gold": gold_ids,
        "gold/02_dq_gold": gold_ids,
    }.items():
        assert sql.render(name, **ids).strip()


def test_merge_inserts_every_silver_column_explicitly() -> None:
    text = sql.render("silver/02_merge_silver", **IDS)
    insert = text.split("WHEN NOT MATCHED THEN INSERT (", 1)[1].split(")", 1)[0]
    cols = [c.strip() for c in insert.replace("\n", " ").split(",")]
    assert cols == load_table_def("silver", "telemetry", base=REPO_SCHEMA_DIR).column_names


# ---- ETL helpers ------------------------------------------------------------ #
def _ctx(**kw: object) -> RunContext:
    return RunContext(CFG, "d", "manual__1", "t", **kw)  # type: ignore[arg-type]


def test_landing_table_is_run_scoped_and_deterministic() -> None:
    a, b = silver.landing_table_for(_ctx()), silver.landing_table_for(_ctx())
    assert a == b and a.startswith(f"{CFG.project.id}.iot_landing.raw_telemetry__")
    other = RunContext(CFG, "d", "manual__2", "t")
    assert silver.landing_table_for(other) != a


def test_gold_dates_from_conf_or_interval() -> None:
    assert gold.resolve_dates(_ctx(), {"dates": ["2026-11-10", "2026-11-09", "2026-11-10"]}) == [
        "2026-11-09",
        "2026-11-10",
    ]
    start = datetime(2026, 11, 9, 5, tzinfo=UTC)
    assert gold.resolve_dates(_ctx(interval_start=start), {}) == ["2026-11-09"]
    with pytest.raises(ValueError):
        gold.resolve_dates(_ctx(), {})
    with pytest.raises(ValueError):
        gold.resolve_dates(_ctx(), {"dates": ["not-a-date"]})


# ---- Airflow glue (no Airflow needed) -------------------------------------- #
def test_default_args_from_config() -> None:
    args = default_args(CFG, "silver")
    t = CFG.pipeline.default_task
    assert args["retries"] == t.retries
    assert args["retry_delay"] == timedelta(seconds=t.retry_delay_seconds)
    assert args["execution_timeout"] == timedelta(minutes=t.execution_timeout_minutes)
    assert callable(args["on_failure_callback"])


def test_dag_kwargs_from_config() -> None:
    kw = dag_kwargs(CFG, "ingest", doc_md="x")
    assert kw["dag_id"] == CFG.pipeline.dags.ingest
    assert kw["catchup"] is CFG.pipeline.catchup
    assert kw["is_paused_upon_creation"] is False
    assert {"raw", CFG.env} <= set(kw["tags"])


def _airflow_context(exc: BaseException | None = None) -> dict:
    ti = SimpleNamespace(dag_id="iot_02_raw_to_silver", task_id="merge", try_number=2)
    return {"ti": ti, "run_id": "r1", "exception": exc, "data_interval_start": None}


def test_run_context_mapping() -> None:
    ctx = run_context(CFG, _airflow_context())
    assert (ctx.dag_id, ctx.task_id, ctx.run_id, ctx.try_number) == (
        "iot_02_raw_to_silver", "merge", "r1", 2,
    )  # fmt: skip


def test_failure_callback_records_dq_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple] = []
    monkeypatch.setattr(RunContext, "audit", lambda self, *a, **k: calls.append((a, k)))
    exc = DataQualityError("too many invalid", {"invalid_ratio": 0.5})
    failure_audit_callback(CFG, "silver")(_airflow_context(exc))
    (layer, status), kwargs = calls[0]
    assert (layer, status) == ("silver", "DQ_FAILED")
    assert kwargs["details"]["invalid_ratio"] == 0.5


def test_failure_callback_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("BigQuery down")

    monkeypatch.setattr(RunContext, "audit", boom)
    failure_audit_callback(CFG, "gold")(_airflow_context(ValueError("x")))  # must not raise
