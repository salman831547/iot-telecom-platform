"""DAG 1 tasks: generate the raw batch, validate it in place, publish its manifest.

The manifest works as a *commit marker*: silver only picks up files whose manifest
exists, and the manifest is written only after the file passed validation. A
half-written or invalid file is therefore never processed.
"""

from __future__ import annotations

import csv
import hashlib
import io
from typing import Any

from iot_pipeline import gcs
from iot_pipeline.contracts import RAW_HEADERS
from iot_pipeline.etl.context import DataQualityError, RunContext, log
from iot_pipeline.generator import generate_batch


def _require_interval(ctx: RunContext) -> None:
    if ctx.interval_start is None or ctx.interval_end is None:
        raise ValueError("ingest tasks need a data interval")


def generate_and_upload(ctx: RunContext) -> dict[str, Any]:
    """Generate the batch for the data interval and upload it (no-op if unchanged)."""
    _require_interval(ctx)
    cfg = ctx.cfg
    batch = generate_batch(cfg, ctx.interval_start, ctx.interval_end)  # type: ignore[arg-type]
    content = batch.to_csv()
    bucket = cfg.storage.staging_bucket.name
    name = batch.object_name(cfg.pipeline.raw.prefix)
    uploaded = gcs.upload_if_changed(ctx.storage, bucket, name, content, "text/csv")
    info = {
        "batch_id": batch.batch_id,
        "bucket": bucket,
        "object": name,
        "uri": gcs.gs_uri(bucket, name),
        "manifest_object": batch.manifest_name(cfg.pipeline.raw.manifest_prefix),
        "row_count": batch.row_count,
        "md5": batch.manifest("", content)["md5"],
        "uploaded": uploaded,
    }
    log.info("raw batch %s: %s rows, uploaded=%s", batch.batch_id, batch.row_count, uploaded)
    return info


def validate_raw_file(ctx: RunContext, info: dict[str, Any]) -> dict[str, Any]:
    """Re-read the object from GCS and check it against the contract."""
    blob = ctx.storage.bucket(info["bucket"]).blob(info["object"])
    content = blob.download_as_text()
    reader = csv.reader(io.StringIO(content))
    header = tuple(next(reader, ()))
    rows = sum(1 for _ in reader)
    md5_hex = hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()
    metrics = {
        "header_ok": header == RAW_HEADERS,
        "row_count": rows,
        "expected_rows": info["row_count"],
        "md5_ok": md5_hex == info["md5"],
    }
    problems = []
    if not metrics["md5_ok"]:
        problems.append("object content does not match the generated batch (md5)")
    if not metrics["header_ok"]:
        problems.append(f"header mismatch: {header[:3]}...")
    if rows < ctx.cfg.pipeline.data_quality.min_rows_per_batch:
        problems.append(f"{rows} rows < min {ctx.cfg.pipeline.data_quality.min_rows_per_batch}")
    if rows != info["row_count"]:
        problems.append(f"row count {rows} != generated {info['row_count']}")
    if problems:
        raise DataQualityError("; ".join(problems), metrics)
    return metrics


def publish_manifest(ctx: RunContext, info: dict[str, Any]) -> str:
    """Write the manifest (commit marker). Deterministic content, so re-runs are no-ops."""
    cfg = ctx.cfg
    batch = generate_batch(cfg, ctx.interval_start, ctx.interval_end)  # type: ignore[arg-type]
    manifest = batch.manifest(info["uri"], batch.to_csv())
    gcs.write_json(ctx.storage, info["bucket"], info["manifest_object"], manifest)
    return gcs.gs_uri(info["bucket"], info["manifest_object"])
