"""Airflow glue shared by all DAGs: config-driven DAG/task defaults, run context and
failure auditing. Imported only by DAG files. Airflow is imported lazily so the rest
of the library stays usable without Airflow."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from iot_pipeline.config import EnvConfig
from iot_pipeline.etl.context import DataQualityError, RunContext, log

# DAG key (config pipeline.dags.<key>) -> medallion layer used in audit records.
LAYER_BY_DAG_KEY = {"ingest": "raw", "silver": "silver", "gold": "gold"}

ASSET_NAMES = {
    "raw": "iot.raw.telemetry",
    "silver": "iot.silver.telemetry",
    "gold": "iot.gold.kpis",
}


def run_context(cfg: EnvConfig, context: dict[str, Any]) -> RunContext:
    """Airflow task context -> library RunContext (identity + data interval)."""
    ti = context["ti"]
    return RunContext(
        cfg=cfg,
        dag_id=ti.dag_id,
        run_id=context["run_id"],
        task_id=ti.task_id,
        try_number=getattr(ti, "try_number", 1) or 1,
        interval_start=context.get("data_interval_start"),
        interval_end=context.get("data_interval_end"),
    )


def failure_audit_callback(cfg: EnvConfig, dag_key: str) -> Any:
    """on_failure_callback: records FAILED or DQ_FAILED in iot_ops.batch_audit.

    It must never raise: a broken callback would hide the original error."""

    def _callback(context: dict[str, Any]) -> None:
        try:
            exc = context.get("exception")
            ctx = run_context(cfg, context)
            if isinstance(exc, DataQualityError):
                status, details = "DQ_FAILED", {"error": str(exc), **exc.metrics}
            else:
                status, details = "FAILED", {"error": f"{type(exc).__name__}: {exc}"}
            ctx.audit(LAYER_BY_DAG_KEY[dag_key], status, details=details)
        except Exception:  # noqa: BLE001 - callbacks must not mask the task error
            log.exception("failure audit callback itself failed")

    return _callback


def default_args(cfg: EnvConfig, dag_key: str) -> dict[str, Any]:
    t = cfg.pipeline.default_task
    return {
        "owner": cfg.pipeline.owner,
        "retries": t.retries,
        "retry_delay": timedelta(seconds=t.retry_delay_seconds),
        "retry_exponential_backoff": t.retry_exponential_backoff,
        "max_retry_delay": timedelta(seconds=t.max_retry_delay_seconds),
        "execution_timeout": timedelta(minutes=t.execution_timeout_minutes),
        "on_failure_callback": failure_audit_callback(cfg, dag_key),
    }


def dag_kwargs(cfg: EnvConfig, dag_key: str, *, doc_md: str | None) -> dict[str, Any]:
    """Keyword arguments for ``DAG(...)``, all from config. ``schedule`` is passed
    explicitly at each DAG so it is visible where the DAG is defined."""
    p = cfg.pipeline
    return {
        "dag_id": getattr(p.dags, dag_key),
        "start_date": p.start_date,
        "catchup": p.catchup,
        "max_active_runs": p.max_active_runs,
        "dagrun_timeout": timedelta(minutes=p.dagrun_timeout_minutes),
        "default_args": default_args(cfg, dag_key),
        "tags": [*p.tags, cfg.env, LAYER_BY_DAG_KEY[dag_key]],
        "doc_md": doc_md,
        # A freshly recreated Composer must RESUME on its own, so DAGs start unpaused.
        # (The environment-wide default stays "paused" for any other, unreviewed DAG.)
        "is_paused_upon_creation": False,
        # Lets TriggerDagRunOperator pass real lists (e.g. affected dates) in conf.
        "render_template_as_native_obj": True,
    }
