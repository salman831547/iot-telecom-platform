"""DAG 3 tasks: rebuild affected gold partitions from silver, then reconcile."""

from __future__ import annotations

from datetime import date
from typing import Any

from iot_pipeline import bq, sql
from iot_pipeline.etl.context import DataQualityError, RunContext, log


def _first_row(job: Any) -> dict[str, Any]:
    rows = list(job.result())
    return dict(rows[0].items()) if rows else {}


def resolve_dates(ctx: RunContext, conf: dict[str, Any] | None) -> list[str]:
    """Partitions to rebuild: from the triggering silver run (``conf['dates']``),
    otherwise, for a manual or backfill run, the day of the data interval."""
    dates = (conf or {}).get("dates") or []
    if not dates and ctx.interval_start is not None:
        dates = [ctx.interval_start.date().isoformat()]
    if not dates:
        raise ValueError("no partitions to rebuild: pass conf={'dates': [...]} or a data interval")
    parsed = sorted({date.fromisoformat(d).isoformat() for d in dates})  # validates format
    log.info("gold partitions to rebuild: %s", parsed)
    return parsed


def _tables(ctx: RunContext) -> dict[str, str]:
    t = ctx.cfg.pipeline.tables
    return {
        "silver_table": ctx.table("silver", t.silver_telemetry),
        "gold_device_table": ctx.table("gold", t.gold_device_hourly),
        "gold_cell_table": ctx.table("gold", t.gold_cell_hourly),
    }


def rebuild_gold(ctx: RunContext, dates: list[str]) -> dict[str, Any]:
    script = sql.render("gold/01_rebuild_gold", **_tables(ctx))
    params = {
        "dates": [date.fromisoformat(d) for d in dates],
        "poor_rsrp_threshold": float(ctx.cfg.pipeline.data_quality.poor_coverage_rsrp_dbm),
    }
    stats = _first_row(bq.run_query(ctx.bigquery, script, params, ctx.labels))
    log.info("gold rebuilt for %s: %s", dates, stats)
    return {**stats, "dates": dates}


def check_gold_quality(ctx: RunContext, dates: list[str]) -> dict[str, Any]:
    query = sql.render("gold/02_dq_gold", **_tables(ctx))
    m = _first_row(
        bq.run_query(
            ctx.bigquery, query, {"dates": [date.fromisoformat(d) for d in dates]}, ctx.labels
        )
    )
    problems = []
    if m["silver_valid_events"] != m["gold_device_events"]:
        problems.append(
            f"device KPIs {m['gold_device_events']} != silver {m['silver_valid_events']}"
        )
    if m["silver_valid_cell_events"] != m["gold_cell_events"]:
        problems.append(
            f"cell KPIs {m['gold_cell_events']} != silver {m['silver_valid_cell_events']}"
        )
    if m["negative_counts"]:
        problems.append(f"{m['negative_counts']} rows with negative counts")
    if m["gold_device_events"] == 0:
        problems.append("gold is empty for the rebuilt partitions")
    if problems:
        raise DataQualityError("; ".join(problems), m)
    log.info("gold DQ passed (reconciled to silver): %s", m)
    return m
