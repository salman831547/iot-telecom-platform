"""DAG 2 tasks: incremental raw -> silver.

Incremental by watermark, not by data interval. The watermark is the creation
time of the newest *manifest* (commit marker) already merged, and it lives in
BigQuery (``iot_ops.watermarks``). After any outage, including a deleted and
recreated Composer environment, the next run picks up every file published since,
capped by ``max_files_per_run``.

Retries are safe:
* ``find_new_files`` result is passed downstream via XCom, so a retried merge
  re-processes exactly the same files, even though the watermark already moved.
* The MERGE key (``event_id``) is deterministic, and the watermark only moves forward.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, cast

from iot_pipeline import bq, gcs, sql
from iot_pipeline.etl.context import DataQualityError, RunContext, log
from iot_pipeline.schemas import load_table_def
from iot_pipeline.secrets import access_secret

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _first_row(job: Any) -> dict[str, Any]:
    rows = list(job.result())
    return dict(rows[0].items()) if rows else {}


def read_watermark(ctx: RunContext) -> datetime:
    table = ctx.table("ops", ctx.cfg.pipeline.tables.ops_watermarks)
    job = bq.run_query(
        ctx.bigquery,
        f"SELECT MAX(watermark_ts) AS wm FROM `{table}` WHERE pipeline = @key",  # noqa: S608 - identifier from config
        {"key": ctx.cfg.pipeline.silver.watermark_key},
        ctx.labels,
    )
    wm = _first_row(job).get("wm")
    return wm or EPOCH


def find_new_files(ctx: RunContext) -> list[dict[str, Any]]:
    """Manifests created after the watermark, oldest first, capped per run."""
    cfg = ctx.cfg
    watermark = read_watermark(ctx)
    bucket = cfg.storage.staging_bucket.name
    blobs = [
        b
        for b in ctx.storage.list_blobs(bucket, prefix=cfg.pipeline.raw.manifest_prefix + "/")
        if b.name.endswith(".json") and b.time_created > watermark
    ]
    blobs.sort(key=lambda b: (b.time_created, b.name))
    selected = blobs[: cfg.pipeline.silver.max_files_per_run]
    files = []
    for b in selected:
        manifest = gcs.read_json(ctx.storage, bucket, b.name)
        files.append(
            {
                "uri": manifest["uri"],
                "batch_id": manifest["batch_id"],
                "row_count": manifest["row_count"],
                "manifest": gcs.gs_uri(bucket, b.name),
                "created": b.time_created.isoformat(),
            }
        )
    log.info(
        "watermark=%s -> %d new file(s) (%d pending beyond cap)",
        watermark.isoformat(),
        len(files),
        max(0, len(blobs) - len(selected)),
    )
    return files


def landing_table_for(ctx: RunContext) -> str:
    suffix = hashlib.sha1(ctx.run_id.encode(), usedforsecurity=False).hexdigest()[:12]
    return ctx.table("landing", f"{ctx.cfg.pipeline.tables.landing_raw}__{suffix}")


def load_landing(ctx: RunContext, files: list[dict[str, Any]]) -> str:
    """Raw CSVs -> run-scoped all-STRING landing table, with per-row lineage."""
    from google.cloud import bigquery

    landing = landing_table_for(ctx)
    schema_def = load_table_def("landing", ctx.cfg.pipeline.tables.landing_raw)
    ext = bigquery.ExternalConfig("CSV")
    ext.source_uris = [f["uri"] for f in files]
    ext.schema = [bigquery.SchemaField(c.name, c.type, mode=c.mode) for c in schema_def.columns]
    csv_options = cast("bigquery.CSVOptions", ext.options)  # "CSV" config -> CSVOptions
    csv_options.skip_leading_rows = 1
    csv_options.allow_jagged_rows = False
    csv_options.allow_quoted_newlines = False
    ext.max_bad_records = 0  # a structurally broken file fails loudly

    job_config = bigquery.QueryJobConfig(
        table_definitions={"raw_ext": ext},
        destination=landing,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        query_parameters=bq.query_parameters(
            {
                "uris": [f["uri"] for f in files],
                "created": [datetime.fromisoformat(f["created"]) for f in files],
            }
        ),
        labels=ctx.labels,
    )
    job = ctx.bigquery.query(sql.render("silver/01_landing_from_raw"), job_config=job_config)
    job.result()
    log.info("landing %s: %s rows from %d file(s)", landing, job.num_dml_affected_rows, len(files))
    return landing


def merge_silver(ctx: RunContext, files: list[dict[str, Any]], landing: str) -> dict[str, Any]:
    """Clean, dedupe and MERGE into silver, plus advance the watermark (one transaction)."""
    cfg = ctx.cfg
    dq = cfg.pipeline.data_quality
    r, area = dq.valid_ranges, dq.service_area
    t = cfg.pipeline.tables
    script = sql.render(
        "silver/02_merge_silver",
        landing_table=landing,
        silver_table=ctx.table("silver", t.silver_telemetry),
        watermark_table=ctx.table("ops", t.ops_watermarks),
    )
    params = {
        "pii_salt": access_secret(cfg.project.id, cfg.secrets.pii_hash_salt),
        "watermark_key": cfg.pipeline.silver.watermark_key,
        "new_watermark": max(datetime.fromisoformat(f["created"]) for f in files),
        "last_batch_id": max(f["batch_id"] for f in files),
        "run_id": ctx.run_id,
        "rsrp_min": r.rsrp_dbm.low, "rsrp_max": r.rsrp_dbm.high,
        "rsrq_min": r.rsrq_db.low, "rsrq_max": r.rsrq_db.high,
        "sinr_min": r.sinr_db.low, "sinr_max": r.sinr_db.high,
        "battery_min": r.battery_pct.low, "battery_max": r.battery_pct.high,
        "temp_min": r.temperature_c.low, "temp_max": r.temperature_c.high,
        "lat_min": area.lat_min, "lat_max": area.lat_max,
        "lon_min": area.lon_min, "lon_max": area.lon_max,
    }  # fmt: skip
    stats = _first_row(bq.run_query(ctx.bigquery, script, params, ctx.labels))
    stats["affected_dates"] = list(stats.get("affected_dates") or [])
    stats["batch_ids"] = list(stats.get("batch_ids") or [])
    log.info("silver merge: %s", {k: v for k, v in stats.items() if k != "batch_ids"})
    return stats


def check_silver_quality(ctx: RunContext, stats: dict[str, Any]) -> dict[str, Any]:
    """Measure in BigQuery, judge in Python against config thresholds."""
    from datetime import date

    cfg = ctx.cfg
    query = sql.render(
        "silver/03_dq_silver",
        silver_table=ctx.table("silver", cfg.pipeline.tables.silver_telemetry),
    )
    params = {
        "dates": [date.fromisoformat(d) for d in stats["affected_dates"]],
        "batch_ids": stats["batch_ids"],
    }
    m = _first_row(bq.run_query(ctx.bigquery, query, params, ctx.labels))
    total = m.get("rows_total") or 0
    m["invalid_ratio"] = round((m.get("rows_invalid") or 0) / total, 4) if total else 0.0

    dq = cfg.pipeline.data_quality
    problems = []
    if m["null_keys"]:
        problems.append(f"{m['null_keys']} rows with null keys")
    if m["duplicate_event_ids"]:
        problems.append(f"{m['duplicate_event_ids']} duplicate event_ids")
    if m["invalid_ratio"] > dq.max_invalid_ratio:
        problems.append(f"invalid ratio {m['invalid_ratio']} > {dq.max_invalid_ratio}")
    if m["min_rows_per_batch"] < dq.min_rows_per_batch:
        problems.append(f"a batch has {m['min_rows_per_batch']} rows < {dq.min_rows_per_batch}")
    if m["batches_found"] != len(stats["batch_ids"]):
        problems.append(
            f"{m['batches_found']} batches in silver, expected {len(stats['batch_ids'])}"
        )
    if problems:
        raise DataQualityError("; ".join(problems), m)
    log.info("silver DQ passed: %s", m)
    return m
