"""Typed, validated access to the environment config (config/envs/<env>.yaml).

The same YAML file is read by Terraform (``yamldecode``) and by Python. This
module is the Python side: it validates the file strictly (unknown keys are
rejected) and exposes it as immutable, typed objects.

Resolution order for the config file:
    1. explicit ``path`` argument
    2. ``IOT_CONFIG_PATH`` environment variable
    3. ``IOT_CONFIG_DIR`` (default: ``<repo>/config/envs`` or the Composer data
       folder) joined with ``<IOT_ENV>.yaml`` (``IOT_ENV`` defaults to ``dev``)

CLI:
    python -m iot_pipeline.config validate config/envs/dev.yaml [...]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
ENV_VAR_CONFIG_PATH = "IOT_CONFIG_PATH"
ENV_VAR_CONFIG_DIR = "IOT_CONFIG_DIR"
ENV_VAR_ENV = "IOT_ENV"
DEFAULT_ENV = "dev"

# Composer mounts gs://<env-bucket>/data at this path on every Airflow component.
COMPOSER_CONFIG_DIR = Path("/home/airflow/gcs/data/config/envs")
REPO_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config" / "envs"

_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$")
_DATASET_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
_LABEL_RE = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
_SA_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")

Ratio = Annotated[float, Field(ge=0.0, le=1.0)]
PositiveInt = Annotated[int, Field(gt=0)]


class _Strict(BaseModel):
    """Base model: immutable and rejects unknown keys (catches YAML typos in CI)."""

    model_config = ConfigDict(extra="forbid", frozen=True)


# --------------------------------------------------------------------------- #
# Platform / infrastructure sections
# --------------------------------------------------------------------------- #
class ProjectConfig(_Strict):
    id: str
    region: str

    @field_validator("id")
    @classmethod
    def _valid_project_id(cls, v: str) -> str:
        if not _PROJECT_ID_RE.match(v):
            raise ValueError(f"invalid GCP project id: {v!r}")
        return v


class GithubConfig(_Strict):
    owner: str
    repo: str
    deploy_branch: str = "main"


class TerraformStatePrefixes(_Strict):
    bootstrap: str
    foundation: str
    composer: str


class TerraformConfig(_Strict):
    state_bucket: str
    state_prefixes: TerraformStatePrefixes

    @field_validator("state_bucket")
    @classmethod
    def _valid_bucket(cls, v: str) -> str:
        return _validate_bucket(v)


class ServiceAccounts(_Strict):
    tf_plan: str
    tf_apply: str
    cloudbuild_deployer: str
    composer_runtime: str

    @field_validator("*")
    @classmethod
    def _valid_sa_id(cls, v: str) -> str:
        if not _SA_ID_RE.match(v):
            raise ValueError(f"invalid service account id (6-30 chars, a-z0-9-): {v!r}")
        return v


class WorkloadIdentityConfig(_Strict):
    pool_id: str
    provider_id: str


class NetworkConfig(_Strict):
    vpc_name: str
    subnet_name: str
    subnet_cidr: str


class StagingBucketConfig(_Strict):
    name: str
    versioning: bool = False
    nearline_after_days: PositiveInt
    delete_after_days: PositiveInt

    @field_validator("name")
    @classmethod
    def _valid_bucket(cls, v: str) -> str:
        return _validate_bucket(v)

    @model_validator(mode="after")
    def _lifecycle_order(self) -> StagingBucketConfig:
        if self.delete_after_days <= self.nearline_after_days:
            raise ValueError("delete_after_days must be greater than nearline_after_days")
        return self


class ComposerBucketConfig(_Strict):
    name: str
    versioning: bool = True
    noncurrent_versions_to_keep: PositiveInt = 5

    @field_validator("name")
    @classmethod
    def _valid_bucket(cls, v: str) -> str:
        return _validate_bucket(v)


class StorageConfig(_Strict):
    location: str
    staging_bucket: StagingBucketConfig
    composer_bucket: ComposerBucketConfig


class DatasetConfig(_Strict):
    id: str
    default_table_expiration_hours: PositiveInt | None = None
    default_partition_expiration_days: PositiveInt | None = None

    @field_validator("id")
    @classmethod
    def _valid_dataset(cls, v: str) -> str:
        if not _DATASET_RE.match(v):
            raise ValueError(f"invalid BigQuery dataset id: {v!r}")
        return v


class Datasets(_Strict):
    landing: DatasetConfig
    silver: DatasetConfig
    gold: DatasetConfig
    ops: DatasetConfig


class BigQueryConfig(_Strict):
    location: str
    datasets: Datasets


class SecretsConfig(_Strict):
    pii_hash_salt: str


class WorkloadSpec(_Strict):
    cpu: Annotated[float, Field(gt=0)]
    memory_gb: Annotated[float, Field(gt=0)]
    storage_gb: Annotated[float, Field(gt=0)] | None = None
    count: PositiveInt | None = None


class WorkerSpec(_Strict):
    cpu: Annotated[float, Field(gt=0)]
    memory_gb: Annotated[float, Field(gt=0)]
    storage_gb: Annotated[float, Field(gt=0)]
    min_count: PositiveInt
    max_count: PositiveInt

    @model_validator(mode="after")
    def _min_le_max(self) -> WorkerSpec:
        if self.min_count > self.max_count:
            raise ValueError("worker.min_count must be <= worker.max_count")
        return self


class ComposerWorkloads(_Strict):
    scheduler: WorkloadSpec
    web_server: WorkloadSpec
    triggerer: WorkloadSpec
    worker: WorkerSpec


class SnapshotConfig(_Strict):
    save_before_destroy: bool = False


class ComposerConfig(_Strict):
    environment_name: str
    image_version: str
    environment_size: str
    private_environment: bool = True
    resilience_mode: str = "STANDARD_RESILIENCE"
    workloads: ComposerWorkloads
    airflow_config_overrides: dict[str, str] = Field(default_factory=dict)
    env_variables: dict[str, str] = Field(default_factory=dict)
    snapshot: SnapshotConfig = SnapshotConfig()

    @field_validator("image_version")
    @classmethod
    def _composer3(cls, v: str) -> str:
        if not v.startswith("composer-3-airflow-"):
            raise ValueError("this platform targets Cloud Composer 3 (composer-3-airflow-*)")
        return v


# --------------------------------------------------------------------------- #
# Pipeline / generator sections
# --------------------------------------------------------------------------- #
class DefaultTaskConfig(_Strict):
    retries: Annotated[int, Field(ge=0, le=10)]
    retry_delay_seconds: PositiveInt
    retry_exponential_backoff: bool = True
    max_retry_delay_seconds: PositiveInt
    execution_timeout_minutes: PositiveInt


class DagIds(_Strict):
    ingest: str
    silver: str
    gold: str


class RawLayout(_Strict):
    prefix: str
    manifest_prefix: str
    file_format: str = "csv"

    @field_validator("prefix", "manifest_prefix")
    @classmethod
    def _no_slashes_at_edges(cls, v: str) -> str:
        if v.startswith("/") or v.endswith("/"):
            raise ValueError("GCS prefixes must not start or end with '/'")
        return v


class TableNames(_Strict):
    landing_raw: str
    silver_telemetry: str
    gold_device_hourly: str
    gold_cell_hourly: str
    ops_watermarks: str
    ops_audit: str


class DataQualityConfig(_Strict):
    min_rows_per_batch: PositiveInt
    max_invalid_ratio: Ratio


class PipelineConfig(_Strict):
    owner: str
    timezone: str = "UTC"
    start_date: datetime
    schedule: str
    catchup: bool = False
    max_active_runs: PositiveInt = 1
    tags: list[str] = Field(default_factory=list)
    default_task: DefaultTaskConfig
    dagrun_timeout_minutes: PositiveInt
    dags: DagIds
    raw: RawLayout
    tables: TableNames
    data_quality: DataQualityConfig

    @field_validator("start_date")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("pipeline.start_date must be timezone-aware (e.g. +00:00)")
        return v


class RowRange(_Strict):
    min: PositiveInt
    max: PositiveInt

    @model_validator(mode="after")
    def _min_le_max(self) -> RowRange:
        if self.min > self.max:
            raise ValueError("rows_per_batch.min must be <= rows_per_batch.max")
        return self


class AnomalyConfig(_Strict):
    duplicate_ratio: Ratio
    null_ratio: Ratio
    out_of_range_ratio: Ratio
    unit_suffix_ratio: Ratio
    epoch_timestamp_ratio: Ratio


class GeneratorNetwork(_Strict):
    mcc: str
    mnc: list[str]


class GeneratorConfig(_Strict):
    seed_strategy: str = "logical_date"
    rows_per_batch: RowRange
    device_count: PositiveInt
    anomalies: AnomalyConfig
    network: GeneratorNetwork


# --------------------------------------------------------------------------- #
# Root
# --------------------------------------------------------------------------- #
class EnvConfig(_Strict):
    env: str
    project: ProjectConfig
    labels: dict[str, str]
    github: GithubConfig
    terraform: TerraformConfig
    apis: list[str]
    service_accounts: ServiceAccounts
    workload_identity: WorkloadIdentityConfig
    network: NetworkConfig
    storage: StorageConfig
    bigquery: BigQueryConfig
    secrets: SecretsConfig
    composer: ComposerConfig
    pipeline: PipelineConfig
    generator: GeneratorConfig

    @field_validator("labels")
    @classmethod
    def _valid_labels(cls, v: dict[str, str]) -> dict[str, str]:
        for key, value in v.items():
            if not _LABEL_RE.match(key) or (value and not _LABEL_RE.match(value)):
                raise ValueError(f"invalid GCP label {key}={value}")
        return v

    @model_validator(mode="after")
    def _env_label_matches(self) -> EnvConfig:
        if self.labels.get("env") != self.env:
            raise ValueError("labels.env must equal env")
        return self

    # ---- convenience accessors (pure, no I/O) ---------------------------- #
    def table_ref(self, dataset: str, table: str) -> str:
        """Fully-qualified BigQuery reference: ``project.dataset.table``."""
        ds: DatasetConfig = getattr(self.bigquery.datasets, dataset)
        return f"{self.project.id}.{ds.id}.{table}"

    def service_account_email(self, key: str) -> str:
        return f"{getattr(self.service_accounts, key)}@{self.project.id}.iam.gserviceaccount.com"


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _validate_bucket(v: str) -> str:
    if not _BUCKET_RE.match(v) or ".." in v or v.startswith("goog"):
        raise ValueError(f"invalid GCS bucket name: {v!r}")
    return v


def resolve_config_path(path: str | os.PathLike[str] | None = None) -> Path:
    """Return the config file location using the documented resolution order."""
    if path:
        return Path(path)
    if explicit := os.environ.get(ENV_VAR_CONFIG_PATH):
        return Path(explicit)
    env = os.environ.get(ENV_VAR_ENV, DEFAULT_ENV)
    if config_dir := os.environ.get(ENV_VAR_CONFIG_DIR):
        return Path(config_dir) / f"{env}.yaml"
    base = COMPOSER_CONFIG_DIR if COMPOSER_CONFIG_DIR.is_dir() else REPO_CONFIG_DIR
    return base / f"{env}.yaml"


def parse_config(raw: str) -> EnvConfig:
    """Validate YAML text into an :class:`EnvConfig` (no filesystem access)."""
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError("config root must be a mapping")
    return EnvConfig.model_validate(data)


@lru_cache(maxsize=8)
def _load_cached(resolved: Path) -> EnvConfig:
    return parse_config(resolved.read_text(encoding="utf-8"))


def load_config(path: str | os.PathLike[str] | None = None) -> EnvConfig:
    """Load and validate the environment config. Cached per file path.

    Cached because Airflow re-parses DAG files frequently; config is immutable
    for the life of a process.
    """
    resolved = resolve_config_path(path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"config file not found: {resolved}")
    return _load_cached(resolved)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m iot_pipeline.config")
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate", help="validate one or more env config files")
    validate.add_argument("files", nargs="+")
    args = parser.parse_args(argv)

    failed = False
    for file in args.files:
        try:
            cfg = parse_config(Path(file).read_text(encoding="utf-8"))
        except (ValidationError, ValueError, OSError) as exc:
            failed = True
            print(f"FAIL {file}\n{exc}", file=sys.stderr)
        else:
            print(f"OK   {file} (env={cfg.env}, project={cfg.project.id})")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(_cli())
