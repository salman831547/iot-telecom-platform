"""Contract tests for the environment config schema."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from iot_pipeline.config import REPO_CONFIG_DIR, load_config, parse_config, resolve_config_path

ENV_FILES = sorted(REPO_CONFIG_DIR.glob("*.yaml"))


@pytest.fixture
def dev_raw() -> dict:
    return yaml.safe_load((REPO_CONFIG_DIR / "dev.yaml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", ENV_FILES, ids=lambda p: p.name)
def test_every_env_file_is_valid(path: Path) -> None:
    cfg = load_config(path)
    assert cfg.env == path.stem


def test_unknown_key_is_rejected(dev_raw: dict) -> None:
    dev_raw["pipeline"]["shedule"] = "@daily"  # typo must fail fast
    with pytest.raises(ValidationError, match="shedule"):
        parse_config(yaml.safe_dump(dev_raw))


def test_naive_start_date_is_rejected(dev_raw: dict) -> None:
    dev_raw["pipeline"]["start_date"] = "2026-11-09T00:00:00"
    with pytest.raises(ValidationError, match="timezone-aware"):
        parse_config(yaml.safe_dump(dev_raw))


def test_invalid_bucket_is_rejected(dev_raw: dict) -> None:
    dev_raw["storage"]["staging_bucket"]["name"] = "Bad_Bucket"
    with pytest.raises(ValidationError, match="bucket"):
        parse_config(yaml.safe_dump(dev_raw))


def test_worker_min_greater_than_max_is_rejected(dev_raw: dict) -> None:
    dev_raw["composer"]["workloads"]["worker"]["min_count"] = 5
    with pytest.raises(ValidationError, match="min_count"):
        parse_config(yaml.safe_dump(dev_raw))


def test_env_label_must_match_env(dev_raw: dict) -> None:
    dev_raw["labels"]["env"] = "prod"
    with pytest.raises(ValidationError, match=r"labels\.env"):
        parse_config(yaml.safe_dump(dev_raw))


@pytest.mark.parametrize("role", ["roles/owner", "roles/editor", "owner"])
def test_broad_or_malformed_roles_are_rejected(dev_raw: dict, role: str) -> None:
    dev_raw["iam"]["tf_apply_project_roles"].append(role)
    with pytest.raises(ValidationError):
        parse_config(yaml.safe_dump(dev_raw))


def test_github_owner_id_must_be_numeric(dev_raw: dict) -> None:
    dev_raw["github"]["owner_id"] = "salman"
    with pytest.raises(ValidationError, match="owner_id"):
        parse_config(yaml.safe_dump(dev_raw))


def test_config_is_immutable() -> None:
    cfg = load_config(REPO_CONFIG_DIR / "dev.yaml")
    with pytest.raises(ValidationError):
        cfg.env = "prod"  # type: ignore[misc]


def test_helpers() -> None:
    cfg = load_config(REPO_CONFIG_DIR / "dev.yaml")
    assert cfg.table_ref("silver", "telemetry") == f"{cfg.project.id}.iot_silver.telemetry"
    assert cfg.service_account_email("composer_runtime").endswith(
        f"@{cfg.project.id}.iam.gserviceaccount.com"
    )


def test_resolution_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IOT_CONFIG_PATH", str(tmp_path / "x.yaml"))
    assert resolve_config_path() == tmp_path / "x.yaml"
    assert resolve_config_path("explicit.yaml") == Path("explicit.yaml")

    monkeypatch.delenv("IOT_CONFIG_PATH")
    monkeypatch.setenv("IOT_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("IOT_ENV", "prod")
    assert resolve_config_path() == tmp_path / "prod.yaml"
