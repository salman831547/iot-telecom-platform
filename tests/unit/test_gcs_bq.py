"""GCS / BigQuery helper tests with in-memory fakes (no network, no credentials)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import pytest

from iot_pipeline import bq, gcs


# ---- fakes ------------------------------------------------------------------- #
@dataclass
class FakeBlob:
    store: dict[str, tuple[str, str]]
    name: str
    uploads: list[str] = field(default_factory=list)

    @property
    def md5_hash(self) -> str:
        return gcs.md5_b64(self.store[self.name][0])

    def upload_from_string(self, content: str, content_type: str, checksum: str) -> None:
        assert checksum == "md5"
        self.store[self.name] = (content, content_type)
        self.uploads.append(self.name)

    def download_as_text(self) -> str:
        return self.store[self.name][0]


class FakeBucket:
    def __init__(self, store: dict[str, tuple[str, str]], uploads: list[str]) -> None:
        self.store, self.uploads = store, uploads

    def get_blob(self, name: str) -> FakeBlob | None:
        return FakeBlob(self.store, name, self.uploads) if name in self.store else None

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self.store, name, self.uploads)


class FakeStorage:
    def __init__(self) -> None:
        self.store: dict[str, tuple[str, str]] = {}
        self.uploads: list[str] = []

    def bucket(self, _name: str) -> FakeBucket:
        return FakeBucket(self.store, self.uploads)

    def list_blobs(self, _bucket: str, prefix: str) -> list[Any]:
        return [type("B", (), {"name": n}) for n in self.store if n.startswith(prefix)]


# ---- gcs --------------------------------------------------------------------- #
def test_upload_is_idempotent() -> None:
    client = FakeStorage()
    assert gcs.upload_if_changed(client, "b", "a.csv", "x,y\n", "text/csv") is True  # type: ignore[arg-type]
    assert gcs.upload_if_changed(client, "b", "a.csv", "x,y\n", "text/csv") is False  # type: ignore[arg-type]
    assert gcs.upload_if_changed(client, "b", "a.csv", "x,z\n", "text/csv") is True  # type: ignore[arg-type]
    assert client.uploads == ["a.csv", "a.csv"]


def test_json_round_trip_and_listing() -> None:
    client = FakeStorage()
    gcs.write_json(client, "b", "m/1.json", {"z": 1, "a": 2})  # type: ignore[arg-type]
    gcs.upload_if_changed(client, "b", "m/2.csv", "x", "text/csv")  # type: ignore[arg-type]
    assert gcs.read_json(client, "b", "m/1.json") == {"a": 2, "z": 1}  # type: ignore[arg-type]
    assert gcs.list_uris(client, "b", "m/", ".json") == ["gs://b/m/1.json"]  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("uri", "expected"),
    [("gs://bkt/a/b.csv", ("bkt", "a/b.csv")), ("gs://bkt/x", ("bkt", "x"))],
)
def test_parse_gs_uri(uri: str, expected: tuple[str, str]) -> None:
    assert gcs.parse_gs_uri(uri) == expected


@pytest.mark.parametrize("bad", ["s3://b/x", "gs://bucket-only", "/local/path"])
def test_parse_gs_uri_rejects(bad: str) -> None:
    with pytest.raises(ValueError):
        gcs.parse_gs_uri(bad)


# ---- bq ---------------------------------------------------------------------- #
def test_query_parameter_types() -> None:
    params = bq.query_parameters(
        {
            "s": "x",
            "i": 3,
            "f": 1.5,
            "b": True,
            "ts": datetime(2026, 1, 1, tzinfo=UTC),
            "d": date(2026, 1, 1),
            "arr": ["a", "b"],
        }
    )
    types = {p.name: getattr(p, "type_", None) or p.array_type for p in params}
    assert types == {
        "s": "STRING", "i": "INT64", "f": "FLOAT64", "b": "BOOL",
        "ts": "TIMESTAMP", "d": "DATE", "arr": "STRING",
    }  # fmt: skip


@pytest.mark.parametrize("bad", [{"x": None}, {"x": []}, {"x": object()}])
def test_query_parameter_rejects_ambiguous(bad: dict[str, Any]) -> None:
    with pytest.raises((ValueError, TypeError)):
        bq.query_parameters(bad)


def test_job_labels_are_sanitised() -> None:
    labels = bq.job_labels(
        "dev", "iot_02_raw_to_silver", "Merge.Silver", run="manual__2026-11-09T10:00"
    )
    assert labels["task_id"] == "merge_silver"
    assert all(len(v) <= 63 and v == v.lower() for v in labels.values())
    assert ":" not in labels["run"]


class FakeJob:
    def result(self) -> None:
        return None


class FakeBQ:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def query(self, sql: str, job_config: Any) -> FakeJob:
        self.calls.append((sql, job_config))
        return FakeJob()


def test_write_audit_is_parameterised() -> None:
    client = FakeBQ()
    rec = bq.AuditRecord(
        dag_id="d", run_id="r'; DROP TABLE x; --", task_id="t", layer="raw", status="SUCCESS",
        rows_out=0, details={"k": 1},
    )  # fmt: skip
    bq.write_audit(client, "p.iot_ops.batch_audit", rec, {"env": "dev"})  # type: ignore[arg-type]
    sql, cfg = client.calls[0]
    assert "DROP TABLE" not in sql  # value travels as a parameter, never inline
    values = {p.name: p.value for p in cfg.query_parameters}
    assert values["run_id"] == "r'; DROP TABLE x; --"
    assert values["rows_out"] == 0 and values["rows_in"] == -1
    assert values["has_start"] is False
    assert json.loads(values["details"]) == {"k": 1}
    assert cfg.labels == {"env": "dev"}


def test_audit_record_validation() -> None:
    with pytest.raises(ValueError, match="status"):
        bq.AuditRecord(dag_id="d", run_id="r", task_id="t", layer="raw", status="OK")
    with pytest.raises(ValueError, match="layer"):
        bq.AuditRecord(dag_id="d", run_id="r", task_id="t", layer="bronze", status="SUCCESS")
    rec = bq.AuditRecord(
        dag_id="d", run_id="r", task_id="t", layer="gold", status="SKIPPED", try_number=2
    )
    assert rec.audit_id == "d|r|t|2"


def test_chunked() -> None:
    assert bq.chunked([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    with pytest.raises(ValueError):
        bq.chunked([1], 0)
