"""
### DAG 3: silver → gold KPIs

**Triggered by** DAG 2 with `conf={"dates": [...]}`. Can also be run manually:
- with `{"dates": ["YYYY-MM-DD", ...]}` to rebuild specific days
- or with no conf, which rebuilds the day of the data interval

| Task | What it does |
|---|---|
| `resolve_dates` | Partitions to rebuild (validated) |
| `rebuild_gold` | One transaction: delete and re-insert the affected partitions of `device_hourly_kpis` and `cell_hourly_kpis` |
| `check_gold_quality` | **Reconciliation:** gold event counts must equal valid silver events (device and cell level). Emits Asset `iot.gold.kpis` |
| `audit` | Appends the outcome to `iot_ops.batch_audit` |

**Re-runs are safe:** gold is a pure function of silver for the given partitions.
"""

from __future__ import annotations

from airflow.sdk import DAG, Asset, get_current_context, task

from iot_pipeline.airflow_support import ASSET_NAMES, dag_kwargs, run_context
from iot_pipeline.config import load_config
from iot_pipeline.etl import gold

CFG = load_config()
DAG_KEY = "gold"

with DAG(schedule=None, **dag_kwargs(CFG, DAG_KEY, doc_md=__doc__)):

    @task
    def resolve_dates() -> list[str]:
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        return gold.resolve_dates(run_context(CFG, context), conf)

    @task
    def rebuild_gold(dates: list[str]) -> dict:
        return gold.rebuild_gold(run_context(CFG, get_current_context()), dates)

    @task(outlets=[Asset(ASSET_NAMES["gold"])])
    def check_gold_quality(dates: list[str]) -> dict:
        return gold.check_gold_quality(run_context(CFG, get_current_context()), dates)

    @task
    def audit(stats: dict, dq: dict) -> None:
        run_context(CFG, get_current_context()).audit(
            "gold",
            "SUCCESS",
            rows_out=stats["device_rows"] + stats["cell_rows"],
            target=CFG.table_ref("gold", CFG.pipeline.tables.gold_device_hourly),
            details={**stats, "dq": dq},
        )

    dates = resolve_dates()
    stats = rebuild_gold(dates)
    dq = check_gold_quality(dates)
    stats >> dq
    audit(stats, dq)
