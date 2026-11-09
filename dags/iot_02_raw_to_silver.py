"""
### DAG 2: raw (GCS) → silver (BigQuery), incremental

**Triggered by** DAG 1. `max_active_runs=1` serialises merges.

| Task | What it does |
|---|---|
| `find_new_files` | Manifests created after the **watermark** stored in `iot_ops.watermarks` (outside Airflow), capped per run |
| `has_new_files` | **Short-circuit:** nothing new means everything downstream is skipped |
| `transform.load_landing` | A temporary external table over the CSVs feeds a run-scoped, all-STRING landing table with lineage columns |
| `transform.merge_silver` | One BigQuery transaction: clean, rename, cast, flag DQ, hash PII, de-duplicate, `MERGE` on the deterministic `event_id`, and advance the watermark |
| `transform.check_silver_quality` | Null keys, duplicate ids, invalid ratio and per-batch row counts, judged against config. Emits Asset `iot.silver.telemetry` |
| `audit` | Appends the outcome to `iot_ops.batch_audit` |
| `trigger_gold` | Starts DAG 3 with the affected partition dates |

**Re-runs and retries are safe:**
- The file list travels by XCom, so a retried merge re-processes the same files.
- The MERGE key is deterministic, and the watermark only moves forward.
"""

from __future__ import annotations

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Asset, TaskGroup, get_current_context, task

from iot_pipeline.airflow_support import ASSET_NAMES, dag_kwargs, run_context
from iot_pipeline.config import load_config
from iot_pipeline.etl import silver

CFG = load_config()
DAG_KEY = "silver"

with DAG(schedule=None, **dag_kwargs(CFG, DAG_KEY, doc_md=__doc__)):

    @task
    def find_new_files() -> list[dict]:
        return silver.find_new_files(run_context(CFG, get_current_context()))

    @task.short_circuit
    def has_new_files(files: list[dict]) -> bool:
        return bool(files)

    @task
    def load_landing(files: list[dict]) -> str:
        return silver.load_landing(run_context(CFG, get_current_context()), files)

    @task
    def merge_silver(files: list[dict], landing: str) -> dict:
        return silver.merge_silver(run_context(CFG, get_current_context()), files, landing)

    @task(outlets=[Asset(ASSET_NAMES["silver"])])
    def check_silver_quality(stats: dict) -> dict:
        return silver.check_silver_quality(run_context(CFG, get_current_context()), stats)

    @task
    def audit(files: list[dict], stats: dict, dq: dict) -> None:
        ctx = run_context(CFG, get_current_context())
        ctx.audit(
            "silver",
            "SUCCESS",
            rows_in=stats["rows_in"],
            rows_out=stats["rows_out"],
            rows_rejected=stats["rows_rejected"],
            target=ctx.table("silver", CFG.pipeline.tables.silver_telemetry),
            details={"files": [f["uri"] for f in files], "dq": dq, **stats},
        )

    files = find_new_files()
    gate = has_new_files(files)

    with TaskGroup("transform") as transform:
        landing = load_landing(files)
        stats = merge_silver(files, landing)
        dq = check_silver_quality(stats)

    gate >> transform

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id=CFG.pipeline.dags.gold,
        trigger_run_id="from_silver__{{ run_id }}",
        reset_dag_run=True,
        wait_for_completion=False,
        # Rendered as a native list (render_template_as_native_obj=True).
        conf={
            "dates": "{{ ti.xcom_pull(task_ids='transform.merge_silver')['affected_dates'] }}",
            "source_run_id": "{{ run_id }}",
        },
    )

    audit(files, stats, dq) >> trigger_gold
