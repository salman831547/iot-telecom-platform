"""Ephemeral Cloud Composer: create it when there is work, run the DAG chain, destroy it
when the last job has finished. Re-create it whenever needed, and it resumes on its own.

Commands
--------
  cycle    up -> deploy -> run -> down.  `down` ALWAYS runs (also on failure) unless --keep
  up       terraform apply 20-composer (blocks until the environment is RUNNING), then deploy
  deploy   sync DAGs + library + SQL + schemas + config into the persistent Composer bucket
  run      ingest every missed hour (capped) and wait until the whole chain is idle
  down     terraform destroy 20-composer (buckets, BigQuery, watermarks are untouched)
  status   environment state, Airflow URL, last raw batch, silver watermark
  plan     show what `run` would process and what `deploy` would change (read-only)

Why it can resume after a rebuild:
  * code/config live in git -> synced into OUR bucket, which outlives the environment
  * which hours are missing is derived from the published manifests in GCS
  * silver's incremental watermark lives in BigQuery, not in Airflow's metadata DB

Usage:  uv run python scripts/composer_lifecycle.py cycle [--env dev] [--keep]
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from iot_pipeline.config import EnvConfig, load_config  # noqa: E402

HOUR = timedelta(hours=1)
TERMINAL = {"success", "failed"}
ACTIVE = ("queued", "running")
_MANIFEST_HOUR = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/")


def log(msg: str) -> None:
    print(f"[{datetime.now(UTC):%H:%M:%S}] {msg}", flush=True)


# =============================================================================
# Pure planning logic (unit-tested; no cloud calls)
# =============================================================================
def floor_hour(ts: datetime) -> datetime:
    return ts.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def last_batch_hour(manifest_names: Iterable[str]) -> datetime | None:
    """Latest hour that has a published manifest (commit marker), or None."""
    hours = [
        datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}:00:00+00:00")
        for name in manifest_names
        if (m := _MANIFEST_HOUR.search(name))
    ]
    return max(hours, default=None)


def plan_hours(
    last_done: datetime | None, now: datetime, max_hours: int, start_date: datetime
) -> list[datetime]:
    """Interval starts to ingest: every completed hour after the last published batch,
    newest ``max_hours`` only, never before the pipeline start_date.

    Always includes the most recent completed hour, so each cycle produces fresh data.
    """
    latest = floor_hour(now) - HOUR  # last COMPLETED interval
    first = last_done + HOUR if last_done else latest
    first = max(first, latest - (max_hours - 1) * HOUR, floor_hour(start_date))
    first = min(first, latest)
    return [first + i * HOUR for i in range(int((latest - first) / HOUR) + 1)]


@dataclass(frozen=True)
class Artifact:
    local_root: str  # repo-relative directory
    pattern: str  # glob within it
    remote_prefix: str  # object prefix in the Composer bucket


# What is deployed where. The library sits under dags/ so it is importable on every
# Airflow component, and dags/.airflowignore keeps it out of DAG parsing.
ARTIFACTS: tuple[Artifact, ...] = (
    Artifact("dags", "iot_*.py", "dags/"),
    Artifact("dags", ".airflowignore", "dags/"),
    Artifact("src/iot_pipeline", "**/*.py", "dags/iot_pipeline/"),
    Artifact("sql", "**/*.sql", "data/sql/"),
    Artifact("schemas/bigquery", "**/*.json", "data/schemas/bigquery/"),
    Artifact("config/envs", "*.yaml", "data/config/envs/"),
)
# Objects we own (and may delete when removed from git). Everything else in the bucket,
# e.g. Composer's own dags/airflow_monitoring.py or logs/, is never touched.
_MANAGED = re.compile(
    r"^(dags/iot_[^/]+\.py|dags/\.airflowignore|dags/iot_pipeline/.+"
    r"|data/sql/.+|data/schemas/.+|data/config/.+)$"
)


def is_managed(name: str) -> bool:
    return bool(_MANAGED.match(name)) and "__pycache__" not in name


def md5_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode()


def collect_local(repo: Path = REPO_ROOT) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for a in ARTIFACTS:
        root = repo / a.local_root
        for path in sorted(root.glob(a.pattern)):
            if path.is_file() and "__pycache__" not in path.parts:
                name = a.remote_prefix + path.relative_to(root).as_posix()
                out[name] = path.read_bytes()
    return out


def plan_sync(local: dict[str, bytes], remote_md5: dict[str, str]) -> tuple[list[str], list[str]]:
    """(uploads, deletes): upload new/changed objects, delete managed objects gone from git."""
    uploads = sorted(n for n, data in local.items() if remote_md5.get(n) != md5_b64(data))
    deletes = sorted(n for n in remote_md5 if is_managed(n) and n not in local)
    return uploads, deletes


# =============================================================================
# Cloud adapters
# =============================================================================
def _session() -> Any:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(creds)


def composer_env(cfg: EnvConfig) -> dict[str, Any] | None:
    """Composer environment resource, or None if it does not exist."""
    url = (
        f"https://composer.googleapis.com/v1/projects/{cfg.project.id}/locations/"
        f"{cfg.project.region}/environments/{cfg.composer.environment_name}"
    )
    r = _session().get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


class Airflow:
    """Minimal Airflow 3 REST API (v2) client, authenticated with Google credentials."""

    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/") + "/api/v2"
        self.s = _session()

    def _req(self, method: str, path: str, ok: tuple[int, ...] = (200,), **kw: Any) -> Any:
        r = self.s.request(method, self.base + path, timeout=60, **kw)
        if r.status_code not in ok:
            raise RuntimeError(f"Airflow API {method} {path} -> {r.status_code}: {r.text[:500]}")
        return r

    def import_errors(self) -> list[dict[str, Any]]:
        return self._req("GET", "/importErrors").json().get("import_errors", [])

    def dag(self, dag_id: str) -> dict[str, Any] | None:
        r = self._req("GET", f"/dags/{dag_id}", ok=(200, 404))
        return None if r.status_code == 404 else r.json()

    def runs(self, dag_id: str, states: Iterable[str] = ()) -> list[dict[str, Any]]:
        params: list[tuple[str, str]] = [("limit", "100"), ("order_by", "-run_after")]
        params += [("state", s) for s in states]
        return self._req("GET", f"/dags/{dag_id}/dagRuns", params=params).json()["dag_runs"]

    def trigger(self, dag_id: str, start: datetime) -> str:
        """Create a run for exactly [start, start+1h). 409 = a run for that interval
        already exists (e.g. created by the scheduler), which is fine."""
        body = {
            "dag_run_id": f"lifecycle__{start:%Y%m%dT%H%M}",
            "logical_date": start.isoformat(),
            "data_interval_start": start.isoformat(),
            "data_interval_end": (start + HOUR).isoformat(),
            "conf": {"source": "composer_lifecycle"},
        }
        r = self._req("POST", f"/dags/{dag_id}/dagRuns", ok=(200, 409), json=body)
        return "created" if r.status_code == 200 else "exists"


def _tf(cfg: EnvConfig, action: str) -> None:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "tf.py"),
        action,
        "composer",
        "--env",
        cfg.env,
    ]
    cmd += ["--", "-auto-approve", "-input=false", "-no-color"]
    log(" ".join(["tf.py", action, "composer"]))
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)  # noqa: S603


# =============================================================================
# Commands
# =============================================================================
def deploy(cfg: EnvConfig, dry_run: bool = False) -> None:
    from google.cloud import storage

    bucket = storage.Client(project=cfg.project.id).bucket(cfg.storage.composer_bucket.name)
    remote = {b.name: b.md5_hash for b in bucket.list_blobs() if is_managed(b.name)}
    local = collect_local()
    uploads, deletes = plan_sync(local, remote)
    log(f"deploy -> gs://{bucket.name}: {len(uploads)} upload(s), {len(deletes)} delete(s), "
        f"{len(local) - len(uploads)} unchanged")  # fmt: skip
    for name in uploads:
        log(f"  + {name}")
        if not dry_run:
            bucket.blob(name).upload_from_string(local[name], checksum="md5")
    for name in deletes:
        log(f"  - {name}")
        if not dry_run:
            bucket.blob(name).delete()


def up(cfg: EnvConfig) -> dict[str, Any]:
    env = composer_env(cfg)
    if env and env.get("state") == "RUNNING":
        log(f"environment {cfg.composer.environment_name} already RUNNING")
    else:
        log("creating Composer (typically 15-25 min)...")
        _tf(cfg, "apply")
        env = composer_env(cfg)
    if not env or env.get("state") != "RUNNING":
        raise RuntimeError(f"environment not RUNNING: {env and env.get('state')}")
    deploy(cfg)
    log(f"Airflow UI: {env['config']['airflowUri']}")
    return env


def down(cfg: EnvConfig) -> None:
    if composer_env(cfg) is None:
        log("environment does not exist; nothing to destroy")
        return
    log("destroying Composer (persistent buckets/BigQuery are untouched)...")
    _tf(cfg, "destroy")
    log("environment destroyed")


def _wait(what: str, check: Callable[[], bool], timeout_s: float, poll_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while not check():
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {what}")
        time.sleep(poll_s)


def _manifest_names(cfg: EnvConfig) -> list[str]:
    from google.cloud import storage

    client = storage.Client(project=cfg.project.id)
    prefix = cfg.pipeline.raw.manifest_prefix + "/"
    return [b.name for b in client.list_blobs(cfg.storage.staging_bucket.name, prefix=prefix)]


def run(cfg: EnvConfig, env: dict[str, Any] | None = None) -> bool:
    """Ingest missed hours and wait until DAG1 -> DAG2 -> DAG3 have all finished."""
    lc = cfg.composer.lifecycle
    env = env or composer_env(cfg)
    if not env or env.get("state") != "RUNNING":
        raise RuntimeError("Composer is not RUNNING; use `up` or `cycle`")
    api = Airflow(env["config"]["airflowUri"])
    dags = cfg.pipeline.dags
    dag_ids = [dags.ingest, dags.silver, dags.gold]
    timeout = lc.run_timeout_minutes * 60

    # 1. Wait until the scheduler has parsed our DAGs (and fail fast on import errors).
    def parsed() -> bool:
        if errors := api.import_errors():
            raise RuntimeError(f"DAG import errors: {[e.get('filename') for e in errors]}")
        return all(api.dag(d) for d in dag_ids)

    log("waiting for DAGs to be parsed...")
    _wait("DAG parsing", parsed, 15 * 60, lc.poll_seconds)
    for d in dag_ids:
        if api.dag(d)["is_paused"]:  # type: ignore[index]
            raise RuntimeError(f"{d} is paused; expected is_paused_upon_creation=False")

    # 2. Decide which hours to ingest (derived from GCS, so it survives rebuilds).
    hours = plan_hours(last_batch_hour(_manifest_names(cfg)), datetime.now(UTC),
                       lc.max_catchup_hours, cfg.pipeline.start_date)  # fmt: skip
    log(f"hours to ingest: {[f'{h:%Y-%m-%dT%H}' for h in hours]}")
    for h in hours:
        log(f"  {dags.ingest} {h:%Y-%m-%dT%H:%M}Z: {api.trigger(dags.ingest, h)}")

    # 3. Done = every planned DAG1 interval is terminal AND no run is active in any
    #    DAG for `quiet_polls` consecutive polls. Triggers are fire-and-forget, but each
    #    downstream run is created BEFORE its upstream run ends, so quiet = finished.
    wanted = {h.isoformat() for h in hours}
    quiet = 0

    def finished() -> bool:
        nonlocal quiet
        ingest_runs = api.runs(dags.ingest)
        done = {
            datetime.fromisoformat(r["data_interval_start"]).isoformat()
            for r in ingest_runs
            if r["state"] in TERMINAL and r.get("data_interval_start")
        }
        active = sum(len(api.runs(d, ACTIVE)) for d in dag_ids)
        quiet = quiet + 1 if wanted <= done and active == 0 else 0
        log(
            f"  ingest {len(wanted & done)}/{len(wanted)} done, active runs {active}, quiet {quiet}"
        )
        return quiet >= lc.quiet_polls

    _wait("DAG chain to finish", finished, timeout, lc.poll_seconds)
    return report(cfg, api, hours)


def report(cfg: EnvConfig, api: Airflow, hours: list[datetime]) -> bool:
    ok = True
    for d in (cfg.pipeline.dags.ingest, cfg.pipeline.dags.silver, cfg.pipeline.dags.gold):
        states: dict[str, int] = {}
        for r in api.runs(d):
            states[r["state"]] = states.get(r["state"], 0) + 1
        ok &= states.get("failed", 0) == 0
        log(f"  {d}: {states}")
    published = {h.isoformat() for h in _published_hours(cfg)}
    missing = [h for h in hours if h.isoformat() not in published]
    if missing:
        ok = False
        log(f"  MISSING raw batches: {[f'{h:%Y-%m-%dT%H}' for h in missing]}")
    log("RESULT: " + ("SUCCESS" if ok else "FAILED"))
    return ok


def _published_hours(cfg: EnvConfig) -> set[datetime]:
    out = set()
    for name in _manifest_names(cfg):
        if m := _MANIFEST_HOUR.search(name):
            out.add(datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}:00:00+00:00"))
    return out


def status(cfg: EnvConfig) -> None:
    from iot_pipeline.etl import RunContext, silver

    env = composer_env(cfg)
    log(f"environment: {env.get('state') if env else 'NOT CREATED'}")
    if env:
        log(f"airflow: {env['config'].get('airflowUri')}")
    last = last_batch_hour(_manifest_names(cfg))
    log(f"last raw batch hour: {last}")
    wm = silver.read_watermark(RunContext(cfg, "lifecycle", "status", "status"))
    log(f"silver watermark: {wm.isoformat()}")


def plan(cfg: EnvConfig) -> None:
    lc = cfg.composer.lifecycle
    hours = plan_hours(last_batch_hour(_manifest_names(cfg)), datetime.now(UTC),
                       lc.max_catchup_hours, cfg.pipeline.start_date)  # fmt: skip
    log(f"`run` would ingest {len(hours)} hour(s): {[f'{h:%Y-%m-%dT%H}' for h in hours]}")
    deploy(cfg, dry_run=True)


def cycle(cfg: EnvConfig, keep: bool) -> bool:
    started = time.monotonic()
    ok = False
    try:
        env = up(cfg)
        ok = run(cfg, env)
    except Exception as exc:  # noqa: BLE001 - report, then still tear down
        log(f"cycle failed: {type(exc).__name__}: {exc}")
    finally:
        destroy = not keep and (ok or cfg.composer.lifecycle.destroy_on_failure)
        if destroy:
            down(cfg)
        else:
            log("keeping the environment (it is billed while it exists)")
        log(f"cycle finished in {(time.monotonic() - started) / 60:.1f} min: "
            f"{'SUCCESS' if ok else 'FAILED'}")  # fmt: skip
    return ok


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="scripts/composer_lifecycle.py", description=__doc__.split("\n")[0]
    )
    p.add_argument("command", choices=["cycle", "up", "deploy", "run", "down", "status", "plan"])
    p.add_argument("--env", default=os.environ.get("IOT_ENV", "dev"))
    p.add_argument("--keep", action="store_true", help="cycle: do not destroy at the end")
    args = p.parse_args(argv)
    cfg = load_config(REPO_ROOT / "config" / "envs" / f"{args.env}.yaml")

    if args.command == "cycle":
        return 0 if cycle(cfg, args.keep) else 1
    if args.command == "run":
        return 0 if run(cfg) else 1
    {"up": up, "deploy": deploy, "down": down, "status": status, "plan": plan}[args.command](cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
