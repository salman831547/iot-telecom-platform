"""Run the full ETL chain locally (no Airflow), calling the SAME task functions the DAGs
use, against real GCS/BigQuery with your ADC credentials.

Order mirrors the DAGs: [DAG 1 per hour] -> DAG 2 (incremental silver) -> DAG 3 (gold).

Usage:
    uv run python scripts/run_etl_local.py --start 2026-11-09T03:00:00Z --hours 2
    uv run python scripts/run_etl_local.py --skip-ingest      # only silver + gold
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from iot_pipeline.config import load_config  # noqa: E402
from iot_pipeline.etl import DataQualityError, RunContext, gold, ingest, silver  # noqa: E402  # noqa: E402


def step(ctx: RunContext, layer: str, fn: Callable[[], Any]) -> Any:
    t0 = time.monotonic()
    try:
        result = fn()
    except DataQualityError as exc:
        ctx.audit(layer, "DQ_FAILED", details={"error": str(exc), **exc.metrics})
        raise
    print(f"  [{time.monotonic() - t0:5.1f}s] {ctx.dag_id}.{ctx.task_id}")
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="first hour, e.g. 2026-11-09T03:00:00Z")
    p.add_argument("--hours", type=int, default=1)
    p.add_argument("--skip-ingest", action="store_true")
    args = p.parse_args()
    cfg = load_config(REPO_ROOT / "config" / "envs" / "dev.yaml")
    dags = cfg.pipeline.dags
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")

    def ctx(dag: str, task: str, start: datetime | None = None) -> RunContext:
        end = start + timedelta(hours=1) if start else None
        return RunContext(cfg, dag, f"local__{stamp}", task, 1, start, end)

    if not args.skip_ingest:
        if not args.start:
            p.error("--start is required unless --skip-ingest")
        first = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        for h in range(args.hours):
            start = first + timedelta(hours=h)
            print(f"DAG 1 {dags.ingest} @ {start:%Y-%m-%dT%H:%M}Z")
            c = ctx(dags.ingest, "generate_and_upload", start)
            info = step(c, "raw", lambda c=c: ingest.generate_and_upload(c))
            c.task_id = "validate_raw_file"
            metrics = step(c, "raw", lambda c=c, i=info: ingest.validate_raw_file(c, i))
            c.task_id = "publish_manifest"
            manifest = step(c, "raw", lambda c=c, i=info: ingest.publish_manifest(c, i))
            c.task_id = "audit"
            details = {**metrics, "uploaded": info["uploaded"]}
            c.audit("raw", "SUCCESS", rows_out=info["row_count"], target=info["uri"],
                    source_uri=manifest, details=details)  # fmt: skip

    print(f"DAG 2 {dags.silver}")
    c = ctx(dags.silver, "find_new_files")
    files = step(c, "silver", lambda: silver.find_new_files(c))
    if not files:
        print("  no new files -> short-circuit (silver and gold skipped)")
        return 0
    c.task_id = "load_landing"
    landing = step(c, "silver", lambda: silver.load_landing(c, files))
    c.task_id = "merge_silver"
    stats = step(c, "silver", lambda: silver.merge_silver(c, files, landing))
    c.task_id = "check_silver_quality"
    dq = step(c, "silver", lambda: silver.check_silver_quality(c, stats))
    c.task_id = "audit"
    c.audit("silver", "SUCCESS", rows_in=stats["rows_in"], rows_out=stats["rows_out"],
            rows_rejected=stats["rows_rejected"], target=c.table("silver", "telemetry"),
            details={"files": len(files), **dq})  # fmt: skip
    print("  stats:", json.dumps({k: v for k, v in stats.items() if k != "batch_ids"}, default=str))
    print("  dq   :", json.dumps(dq, default=str))

    print(f"DAG 3 {dags.gold}")
    c = ctx(dags.gold, "resolve_dates")
    dates = step(c, "gold", lambda: gold.resolve_dates(c, {"dates": stats["affected_dates"]}))
    c.task_id = "rebuild_gold"
    g = step(c, "gold", lambda: gold.rebuild_gold(c, dates))
    c.task_id = "check_gold_quality"
    gdq = step(c, "gold", lambda: gold.check_gold_quality(c, dates))
    c.task_id = "audit"
    c.audit("gold", "SUCCESS", rows_out=g["device_rows"] + g["cell_rows"], details=gdq)
    print("  gold :", json.dumps(g, default=str))
    print("  dq   :", json.dumps(gdq, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
