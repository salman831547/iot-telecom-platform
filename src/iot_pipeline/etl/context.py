"""Run context shared by all ETL tasks: config, run identity, lazy clients, audit."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from iot_pipeline import bq, gcs
from iot_pipeline.config import EnvConfig

log = logging.getLogger("iot_pipeline")


class DataQualityError(RuntimeError):
    """A data-quality rule failed. The task fails and the audit records DQ_FAILED."""

    def __init__(self, message: str, metrics: dict[str, Any]) -> None:
        super().__init__(message)
        self.metrics = metrics


@dataclass
class RunContext:
    cfg: EnvConfig
    dag_id: str
    run_id: str
    task_id: str
    try_number: int = 1
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    _clients: dict[str, Any] = field(default_factory=dict, repr=False)

    # ---- lazy clients (created at task runtime, never during DAG parsing) ------ #
    @property
    def storage(self) -> Any:
        if "gcs" not in self._clients:
            self._clients["gcs"] = gcs.storage_client(self.cfg.project.id)
        return self._clients["gcs"]

    @property
    def bigquery(self) -> Any:
        if "bq" not in self._clients:
            self._clients["bq"] = bq.bigquery_client(
                self.cfg.project.id, self.cfg.bigquery.location
            )
        return self._clients["bq"]

    # ---- helpers ---------------------------------------------------------------- #
    @property
    def labels(self) -> dict[str, str]:
        return bq.job_labels(self.cfg.env, self.dag_id, self.task_id)

    def table(self, dataset_key: str, table: str) -> str:
        return self.cfg.table_ref(dataset_key, table)

    def audit(self, layer: str, status: str, **fields: Any) -> None:
        rec = bq.AuditRecord(
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            layer=layer,
            status=status,
            try_number=self.try_number,
            data_interval_start=self.interval_start,
            data_interval_end=self.interval_end,
            **fields,
        )
        table = self.table("ops", self.cfg.pipeline.tables.ops_audit)
        bq.write_audit(self.bigquery, table, rec, self.labels)
        log.info("audit %s %s/%s: %s", status, self.dag_id, self.task_id, fields.get("details"))
