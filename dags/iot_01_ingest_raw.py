"""
### DAG 1: ingest raw IoT telemetry → GCS staging

**Schedule:** from config (`pipeline.schedule`, hourly). One run produces the batch for one
data interval.

| Task | What it does |
|---|---|
| `generate_and_upload` | Builds the deterministic batch for `[data_interval_start, data_interval_end)` and uploads it to `dt=YYYY-MM-DD/hour=HH/`. Skipped if the content is unchanged |
| `validate_raw_file` | Re-reads the object from GCS and checks the header contract, row count and MD5 |
| `publish_manifest` | Writes the manifest, which is the **commit marker** silver waits for. Emits Asset `iot.raw.telemetry` |
| `audit` | Appends the outcome to `iot_ops.batch_audit` |
| `trigger_silver` | Starts DAG 2 (fire-and-forget). DAG 2 is incremental, so it picks up every new manifest |

**Re-runs are safe:** a re-run for the same interval writes byte-identical content to the same path, so nothing changes.
"""

from __future__ import annotations

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Asset, get_current_context, task

from iot_pipeline.airflow_support import ASSET_NAMES, dag_kwargs, run_context
from iot_pipeline.config import load_config
from iot_pipeline.etl import ingest

CFG = load_config()
DAG_KEY = "ingest"

with DAG(schedule=CFG.pipeline.schedule, **dag_kwargs(CFG, DAG_KEY, doc_md=__doc__)):

    @task
    def generate_and_upload() -> dict:
        return ingest.generate_and_upload(run_context(CFG, get_current_context()))

    @task
    def validate_raw_file(info: dict) -> dict:
        return ingest.validate_raw_file(run_context(CFG, get_current_context()), info)

    @task(outlets=[Asset(ASSET_NAMES["raw"])])
    def publish_manifest(info: dict) -> str:
        return ingest.publish_manifest(run_context(CFG, get_current_context()), info)

    @task
    def audit(info: dict, metrics: dict, manifest: str) -> None:
        run_context(CFG, get_current_context()).audit(
            "raw",
            "SUCCESS",
            rows_out=info["row_count"],
            source_uri=manifest,
            target=info["uri"],
            details={**metrics, "uploaded": info["uploaded"], "batch_id": info["batch_id"]},
        )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id=CFG.pipeline.dags.silver,
        # Deterministic run id: re-running this interval resets the same silver run.
        trigger_run_id="from_ingest__{{ run_id }}",
        reset_dag_run=True,
        wait_for_completion=False,
        conf={"source_run_id": "{{ run_id }}"},
    )

    info = generate_and_upload()
    metrics = validate_raw_file(info)
    manifest = publish_manifest(info)
    metrics >> manifest  # only a validated file gets its commit marker
    audit(info, metrics, manifest) >> trigger_silver
