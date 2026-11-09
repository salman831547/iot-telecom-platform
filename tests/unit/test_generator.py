"""Generator tests: determinism, realism, contract shape and injected anomalies."""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from iot_pipeline.config import REPO_CONFIG_DIR, load_config
from iot_pipeline.contracts import LANDING_COLUMNS, NULL_TOKENS, RAW_HEADERS, sanitize_header
from iot_pipeline.generator import (
    build_fleet,
    generate_batch,
    luhn_check_digit,
    main,
)
from iot_pipeline.schemas import REPO_SCHEMA_DIR, load_table_def

CFG = load_config(REPO_CONFIG_DIR / "dev.yaml")
START = datetime(2026, 11, 9, 10, tzinfo=UTC)
END = START + timedelta(hours=1)


@pytest.fixture(scope="module")
def batch():  # type: ignore[no-untyped-def]
    return generate_batch(CFG, START, END)


def _rows(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text)))


def _parse_ts(value: str) -> datetime:
    if value.isdigit():
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    if "T" in value:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def _num(value: str) -> float:
    return float(value.replace("dBm", "").replace("dB", "").rstrip("%C").strip())


# ---- contract ---------------------------------------------------------------- #
def test_batch_meets_size_requirements(batch) -> None:  # type: ignore[no-untyped-def]
    assert batch.row_count >= 100
    assert len(batch.header) >= 10
    g = CFG.generator
    base = batch.row_count - batch.anomalies["duplicate_row"]
    assert g.rows_per_batch.min <= base <= g.rows_per_batch.max


def test_header_matches_contract_and_landing_schema(batch) -> None:  # type: ignore[no-untyped-def]
    assert batch.header == RAW_HEADERS
    landing = load_table_def("landing", "raw_telemetry", base=REPO_SCHEMA_DIR)
    assert tuple(landing.column_names) == LANDING_COLUMNS
    assert all(c.type == "STRING" for c in landing.columns)


def test_sanitize_header_examples() -> None:
    assert sanitize_header("Device ID") == "device_id"
    assert sanitize_header("timeStamp") == "time_stamp"
    assert sanitize_header("Battery %") == "battery"
    assert sanitize_header("Temp (C)") == "temp_c"


def test_every_row_has_every_column(batch) -> None:  # type: ignore[no-untyped-def]
    assert all(len(r) == len(RAW_HEADERS) for r in batch.rows)


# ---- determinism / idempotency ----------------------------------------------- #
def test_same_interval_is_byte_identical(batch) -> None:  # type: ignore[no-untyped-def]
    assert generate_batch(CFG, START, END).to_csv() == batch.to_csv()


def test_different_interval_differs(batch) -> None:  # type: ignore[no-untyped-def]
    other = generate_batch(CFG, END, END + timedelta(hours=1))
    assert other.to_csv() != batch.to_csv()
    assert other.batch_id != batch.batch_id


def test_object_path_is_hive_partitioned_and_deterministic(batch) -> None:  # type: ignore[no-untyped-def]
    assert (
        batch.object_name("raw/iot_telemetry")
        == "raw/iot_telemetry/dt=2026-11-09/hour=10/part-20261109T1000.csv"
    )
    assert batch.manifest_name("manifests/x").endswith("hour=10/part-20261109T1000.json")


def test_fleet_is_stable_across_batches() -> None:
    g = CFG.generator
    a = build_fleet(CFG.env, g.device_count, g.network.mcc, tuple(g.network.mnc))
    build_fleet.cache_clear()
    b = build_fleet(CFG.env, g.device_count, g.network.mcc, tuple(g.network.mnc))
    assert a == b
    assert len({d.device_id for d in a}) == g.device_count


def test_naive_or_inverted_interval_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        generate_batch(CFG, datetime(2026, 11, 9, 10))  # noqa: DTZ001
    with pytest.raises(ValueError, match="after"):
        generate_batch(CFG, END, START)


# ---- realism ----------------------------------------------------------------- #
def test_identifiers_are_realistic() -> None:
    g = CFG.generator
    for dev in build_fleet(CFG.env, g.device_count, g.network.mcc, tuple(g.network.mnc)):
        assert len(dev.imei) == 15
        assert luhn_check_digit(dev.imei[:14]) == dev.imei[14]
        assert len(dev.imsi) == 15
        assert dev.imsi.startswith(g.network.mcc + dev.mnc)
        assert dev.msisdn.startswith("+1")
        assert len(dev.msisdn) == 12


def test_luhn_known_value() -> None:
    assert luhn_check_digit("49015420323751") == "8"  # documented IMEI example


def test_timestamps_within_interval(batch) -> None:  # type: ignore[no-untyped-def]
    for row in _rows(batch.to_csv()):
        ts = _parse_ts(row["timeStamp"])
        assert START <= ts < END


def test_clean_radio_values_in_physical_range(batch) -> None:  # type: ignore[no-untyped-def]
    rsrq = [_num(r["RSRQ"]) for r in _rows(batch.to_csv()) if r["RSRQ"] not in NULL_TOKENS]
    assert rsrq
    assert all(-20 <= v <= -3 for v in rsrq)


# ---- injected messiness ------------------------------------------------------ #
def test_all_anomaly_families_present(batch) -> None:  # type: ignore[no-untyped-def]
    a = batch.anomalies
    for tag in (
        "duplicate_row",
        "null",
        "ts_epoch_ms",
        "unit_suffixdBm",
        "unit_suffix%",
        "oor_battery",
        "oor_rsrp",
        "device_id_formatting",
        "imei_dashes",
        "fw_v_prefix",
    ):
        assert a[tag] > 0, tag


def test_duplicates_are_exact_copies(batch) -> None:  # type: ignore[no-untyped-def]
    seen = [tuple(r) for r in batch.rows]
    assert len(seen) - len(set(seen)) >= batch.anomalies["duplicate_row"] * 0.9


def test_network_type_has_inconsistent_spellings(batch) -> None:  # type: ignore[no-untyped-def]
    values = {r["Network Type"] for r in _rows(batch.to_csv())}
    assert len(values) > 6  # aliases + casing variants


def test_out_of_range_battery_present(batch) -> None:  # type: ignore[no-untyped-def]
    vals = [
        _num(r["Battery %"]) for r in _rows(batch.to_csv()) if r["Battery %"] not in NULL_TOKENS
    ]
    assert any(v > 100 or v < 0 for v in vals)


def test_manifest(batch) -> None:  # type: ignore[no-untyped-def]
    content = batch.to_csv()
    m = batch.manifest("gs://b/x.csv", content)
    assert m["row_count"] == batch.row_count
    assert m["header"] == list(RAW_HEADERS)
    assert len(str(m["md5"])) == 32


# ---- CLI --------------------------------------------------------------------- #
def test_cli_writes_local_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cfg_path = str(REPO_CONFIG_DIR / "dev.yaml")
    rc = main(
        [
            "--start",
            "2026-11-09T10:00:00Z",
            "--hours",
            "2",
            "--out",
            str(tmp_path),
            "--config",
            cfg_path,
        ]
    )
    assert rc == 0
    files = sorted(tmp_path.rglob("*.csv"))
    assert [f.parent.name for f in files] == ["hour=10", "hour=11"]
    assert "rows x 22 cols" in capsys.readouterr().out


def test_cli_rejects_unaligned_start(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        main(["--start", "2026-11-09T10:30:00Z", "--out", str(tmp_path)])
