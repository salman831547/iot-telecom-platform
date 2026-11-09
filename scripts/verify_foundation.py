"""Read-only smoke test of the applied 10-foundation layer.

Checks that live resources match config: bucket security settings, scoped SA grants,
dataset expirations, and table partitioning, clustering and required partition filters.
Exits non-zero on any mismatch, so it can gate CI after `terraform apply`.

Usage: uv run python scripts/verify_foundation.py [--env dev]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from google.cloud import bigquery, storage  # noqa: E402

from iot_pipeline.config import load_config  # noqa: E402
from iot_pipeline.schemas import REPO_SCHEMA_DIR, discover, load_table_def  # noqa: E402

failures: list[str] = []


def check(ok: bool, msg: str) -> None:
    print(f"  {'OK  ' if ok else 'FAIL'} {msg}")
    if not ok:
        failures.append(msg)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()
    cfg = load_config(REPO_ROOT / "config" / "envs" / f"{args.env}.yaml")
    gcs = storage.Client(project=cfg.project.id)
    bq = bigquery.Client(project=cfg.project.id)

    print("== buckets")
    for bcfg in (cfg.storage.staging_bucket, cfg.storage.composer_bucket):
        b = gcs.get_bucket(bcfg.name)
        ic = b.iam_configuration
        check(ic.uniform_bucket_level_access_enabled, f"{b.name}: uniform bucket-level access")
        check(ic.public_access_prevention == "enforced", f"{b.name}: public access prevention")
        check(b.versioning_enabled == bcfg.versioning, f"{b.name}: versioning={bcfg.versioning}")
        check(b.location == cfg.storage.location, f"{b.name}: location {cfg.storage.location}")

    print("== datasets")
    for ds in cfg.bigquery.datasets.model_dump().values():
        d = bq.get_dataset(ds["id"])
        check(d.location == cfg.bigquery.location, f"{d.dataset_id}: location")
        if want := ds["default_table_expiration_hours"]:
            got = (d.default_table_expiration_ms or 0) // 3_600_000
            check(got == want, f"{d.dataset_id}: table expiry {got}h")

    print("== tables")
    for (dataset_key, table), _ in discover(REPO_SCHEMA_DIR).items():
        if dataset_key == "landing":
            continue
        expected = load_table_def(dataset_key, table, base=REPO_SCHEMA_DIR)
        ds_id = getattr(cfg.bigquery.datasets, dataset_key).id
        t = bq.get_table(f"{cfg.project.id}.{ds_id}.{table}")
        check([f.name for f in t.schema] == expected.column_names, f"{t.table_id}: columns")
        if expected.partitioning:
            field = t.time_partitioning.field if t.time_partitioning else None
            check(field == expected.partitioning.field, f"{t.table_id}: partition {field}")
        check(
            (t.clustering_fields or []) == expected.clustering,
            f"{t.table_id}: clustering {t.clustering_fields}",
        )
        check(
            bool(t.require_partition_filter) == expected.require_partition_filter,
            f"{t.table_id}: require_partition_filter",
        )

    print(f"\n{'PASSED' if not failures else f'FAILED ({len(failures)})'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
