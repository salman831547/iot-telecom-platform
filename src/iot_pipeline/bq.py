"""BigQuery helpers: typed query parameters, cost-attribution labels, audit records.

Every query is parameterised. Values are never string-formatted into SQL, which rules
out SQL injection and keeps cached query plans reusable.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from google.cloud import bigquery

_LABEL_BAD = re.compile(r"[^a-z0-9_-]")


def bigquery_client(project: str, location: str) -> bigquery.Client:
    from google.cloud import bigquery

    return bigquery.Client(project=project, location=location)


def label_value(value: str) -> str:
    """GCP label values: lowercase, [a-z0-9_-], <= 63 chars."""
    return _LABEL_BAD.sub("_", value.lower())[:63]


def job_labels(env: str, dag_id: str, task_id: str, **extra: str) -> dict[str, str]:
    """Labels on every BigQuery job, so cost can be broken down by pipeline and task."""
    labels = {"app": "iot-telecom", "env": env, "dag_id": dag_id, "task_id": task_id, **extra}
    return {label_value(k): label_value(v) for k, v in labels.items()}


def _scalar_type(value: Any) -> str:
    # bool before int: bool is a subclass of int.
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, date):
        return "DATE"
    if isinstance(value, str):
        return "STRING"
    raise TypeError(f"unsupported query parameter type: {type(value).__name__}")


def query_parameters(params: Mapping[str, Any]) -> list[Any]:
    """Python values -> BigQuery query parameters (types inferred; lists become ARRAYs)."""
    from google.cloud import bigquery

    out: list[Any] = []
    for name, value in params.items():
        if isinstance(value, list | tuple):
            if not value:
                raise ValueError(f"empty array parameter {name!r}: element type is ambiguous")
            out.append(bigquery.ArrayQueryParameter(name, _scalar_type(value[0]), list(value)))
        else:
            if value is None:
                raise ValueError(f"parameter {name!r} is None; use SQL NULL explicitly")
            out.append(bigquery.ScalarQueryParameter(name, _scalar_type(value), value))
    return out


def run_query(
    client: bigquery.Client,
    sql: str,
    params: Mapping[str, Any] | None = None,
    labels: Mapping[str, str] | None = None,
) -> bigquery.QueryJob:
    """Run a parameterised query, wait for it and return the finished job."""
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters(params or {}),
        labels=dict(labels or {}),
        use_legacy_sql=False,
    )
    job = client.query(sql, job_config=job_config)
    job.result()
    return job


# --------------------------------------------------------------------------- #
# Audit
# --------------------------------------------------------------------------- #
AUDIT_STATUSES = frozenset({"SUCCESS", "SKIPPED", "FAILED", "DQ_FAILED"})


@dataclass(frozen=True, slots=True)
class AuditRecord:
    dag_id: str
    run_id: str
    task_id: str
    layer: str
    status: str
    try_number: int = 1
    data_interval_start: datetime | None = None
    data_interval_end: datetime | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    rows_rejected: int | None = None
    source_uri: str | None = None
    target: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in AUDIT_STATUSES:
            raise ValueError(f"status must be one of {sorted(AUDIT_STATUSES)}")
        if self.layer not in {"raw", "silver", "gold"}:
            raise ValueError("layer must be raw, silver or gold")

    @property
    def audit_id(self) -> str:
        return f"{self.dag_id}|{self.run_id}|{self.task_id}|{self.try_number}"


# Optional values are sent as typed sentinels (-1, '', a has_* flag) and turned back into
# NULL in SQL, so every parameter keeps a concrete type. BigQuery has no typeless NULL
# parameters.
_AUDIT_SQL = """
INSERT INTO `{table}` (
  audit_id, dag_id, run_id, task_id, layer, status,
  data_interval_start, data_interval_end, rows_in, rows_out, rows_rejected,
  source_uri, target, details, created_at
)
VALUES (
  @audit_id, @dag_id, @run_id, @task_id, @layer, @status,
  IF(@has_start, @data_interval_start, NULL), IF(@has_end, @data_interval_end, NULL),
  NULLIF(@rows_in, -1), NULLIF(@rows_out, -1), NULLIF(@rows_rejected, -1),
  NULLIF(@source_uri, ''), NULLIF(@target, ''), SAFE.PARSE_JSON(@details),
  CURRENT_TIMESTAMP()
)
"""

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def audit_params(rec: AuditRecord) -> dict[str, Any]:
    return {
        "audit_id": rec.audit_id,
        "dag_id": rec.dag_id,
        "run_id": rec.run_id,
        "task_id": rec.task_id,
        "layer": rec.layer,
        "status": rec.status,
        "has_start": rec.data_interval_start is not None,
        "data_interval_start": rec.data_interval_start or _EPOCH,
        "has_end": rec.data_interval_end is not None,
        "data_interval_end": rec.data_interval_end or _EPOCH,
        "rows_in": -1 if rec.rows_in is None else rec.rows_in,
        "rows_out": -1 if rec.rows_out is None else rec.rows_out,
        "rows_rejected": -1 if rec.rows_rejected is None else rec.rows_rejected,
        "source_uri": rec.source_uri or "",
        "target": rec.target or "",
        "details": json.dumps(rec.details, default=str, sort_keys=True),
    }


def write_audit(
    client: bigquery.Client,
    table_ref: str,
    rec: AuditRecord,
    labels: Mapping[str, str] | None = None,
) -> None:
    """Append one audit row with a DML INSERT. DML is used instead of streaming because
    streamed rows sit in a buffer that blocks later DML on the table."""
    run_query(client, _AUDIT_SQL.format(table=table_ref), audit_params(rec), labels)


def as_dict(rec: AuditRecord) -> dict[str, Any]:
    return {**asdict(rec), "audit_id": rec.audit_id}


def chunked(items: Sequence[Any], size: int) -> list[Sequence[Any]]:
    """Split a long URI list into bounded-size groups (e.g. for load-job limits)."""
    if size <= 0:
        raise ValueError("size must be > 0")
    return [items[i : i + size] for i in range(0, len(items), size)]
