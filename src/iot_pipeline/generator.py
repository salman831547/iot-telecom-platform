"""Synthetic telecom IoT telemetry with realistic values and deliberately messy formatting.

Design goals
------------
* **Realistic.** It models a stable device fleet (Luhn-valid IMEIs, IMSIs with MCC/MNC
  prefixes, MSISDNs), LTE cells grouped into tracking areas around real metro areas,
  radio KPIs in physically plausible ranges, battery that drains over time, and traffic
  volumes that depend on device type.
* **Messy on purpose.** Header names, timestamp formats, unit suffixes, null tokens,
  casing and aliases, out-of-range values and duplicate rows all vary. Rates come from
  ``generator.anomalies`` in config.
* **Deterministic.** The same ``(env, interval)`` always produces byte-identical
  output. Re-running an Airflow task overwrites the file with identical content, which
  makes re-runs safe.
* **Standard library only.** It runs inside a private Composer environment with no
  PyPI access.

CLI:
    python -m iot_pipeline.generator --start 2026-11-09T10:00:00Z --hours 3 --out ./output
    python -m iot_pipeline.generator --start 2026-11-09T10:00:00Z --upload   # -> staging bucket
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path

from iot_pipeline.config import EnvConfig, GeneratorConfig, load_config
from iot_pipeline.contracts import CONTRACT_VERSION, NULL_TOKENS, RAW_HEADERS

# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #
# (name, lat, lon, base TAC). US metros, consistent with MCC 310.
METROS: tuple[tuple[str, float, float, int], ...] = (
    ("NYC", 40.7128, -74.0060, 21000),
    ("CHI", 41.8781, -87.6298, 22000),
    ("DAL", 32.7767, -96.7970, 23000),
    ("SEA", 47.6062, -122.3321, 24000),
    ("ATL", 33.7490, -84.3880, 25000),
    ("DEN", 39.7392, -104.9903, 26000),
)
CELLS_PER_METRO = 12  # 4 eNodeBs x 3 sectors
TRACKING_AREAS_PER_METRO = 3

# device_type -> (fleet weight, radio access techs, bytes_up range, bytes_down range, mobile?)
Profile = tuple[float, tuple[str, ...], tuple[int, int], tuple[int, int], bool]
DEVICE_PROFILES: dict[str, Profile] = {
    "SMART_METER": (0.30, ("NB-IOT", "LTE-M"), (120, 900), (40, 300), False),
    "ASSET_TRACKER": (0.20, ("LTE-M", "4G"), (300, 4_000), (100, 1_500), True),
    "VEHICLE_TELEMATICS": (0.15, ("4G", "5G"), (5_000, 250_000), (2_000, 120_000), True),
    "ENVIRONMENTAL_SENSOR": (0.15, ("NB-IOT",), (80, 600), (20, 200), False),
    "SMART_PARKING": (0.10, ("NB-IOT", "LTE-M"), (60, 400), (20, 150), False),
    "POS_TERMINAL": (0.10, ("4G", "3G", "2G"), (2_000, 40_000), (1_000, 20_000), False),
}  # fmt: skip
# Module vendor Type Allocation Codes (first 8 IMEI digits).
IMEI_TACS: tuple[str, ...] = ("35209900", "35332811", "86723104", "35473510", "01624300")
FIRMWARE: tuple[str, ...] = ("1.4.2", "1.5.0", "2.0.1", "2.1.3", "3.0.0")
EVENT_WEIGHTS: dict[str, float] = {
    "HEARTBEAT": 0.55,
    "MEASUREMENT": 0.35,
    "ALARM": 0.04,
    "ATTACH": 0.03,
    "DETACH": 0.03,
}

# Messy spellings the source system emits for each canonical value.
RAT_ALIASES: dict[str, tuple[str, ...]] = {
    "2G": ("2G", "GSM", "gsm"),
    "3G": ("3G", "UMTS", "wcdma"),
    "4G": ("4G", "4g", "LTE", "lte", " LTE "),
    "5G": ("5G", "5g", "NR", "5G-NSA"),
    "NB-IOT": ("NB-IoT", "nbiot", "NB_IOT"),
    "LTE-M": ("LTE-M", "Cat-M1", "lte-m"),
}


def _device_type_variants(canonical: str) -> tuple[str, ...]:
    words = canonical.lower().split("_")
    return (
        canonical,
        "_".join(words),
        " ".join(w.capitalize() for w in words),
        "-".join(words).upper(),
    )


# --------------------------------------------------------------------------- #
# Fleet (stable across batches)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Cell:
    metro: str
    cell_id: int
    tac: int
    lat: float
    lon: float


@dataclass(frozen=True, slots=True)
class Device:
    device_id: str
    imei: str
    imsi: str
    msisdn: str
    mnc: str
    device_type: str
    rat: str
    firmware: str
    home_metro: int
    battery_offset: float
    drain_per_hour: float


def _seed(*parts: object) -> int:
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()
    return int(digest[:16], 16)


def luhn_check_digit(body: str) -> str:
    """Check digit that makes ``body + digit`` pass the Luhn algorithm (IMEI rule)."""
    total = 0
    for i, ch in enumerate(reversed(body)):
        d = int(ch)
        if i % 2 == 0:  # these positions are doubled once the check digit is appended
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return str((10 - total % 10) % 10)


def build_cells() -> tuple[Cell, ...]:
    rng = random.Random(_seed("cells", CONTRACT_VERSION))  # noqa: S311 (synthetic data)
    cells: list[Cell] = []
    for m, (name, lat, lon, base_tac) in enumerate(METROS):
        for c in range(CELLS_PER_METRO):
            enb = 100_000 + m * 100 + c // 3
            cells.append(
                Cell(
                    metro=name,
                    cell_id=enb * 256 + c % 3,  # LTE ECI = eNB-ID * 256 + sector
                    tac=base_tac + c % TRACKING_AREAS_PER_METRO,
                    lat=round(lat + rng.uniform(-0.15, 0.15), 6),
                    lon=round(lon + rng.uniform(-0.15, 0.15), 6),
                )
            )
    return tuple(cells)


@lru_cache(maxsize=8)
def build_fleet(env: str, device_count: int, mcc: str, mncs: tuple[str, ...]) -> tuple[Device, ...]:
    rng = random.Random(_seed(env, "fleet", device_count, mcc, mncs))  # noqa: S311
    types = list(DEVICE_PROFILES)
    weights = [DEVICE_PROFILES[t][0] for t in types]
    fleet: list[Device] = []
    for i in range(device_count):
        dtype = rng.choices(types, weights)[0]
        body = rng.choice(IMEI_TACS) + f"{rng.randrange(10**6):06d}"
        mnc = rng.choice(mncs)
        fleet.append(
            Device(
                device_id=f"DEV-{i + 1:06d}",
                imei=body + luhn_check_digit(body),
                # IMSI = MCC + MNC + MSIN, always 15 digits.
                imsi=f"{mcc}{mnc}{rng.randrange(10**15):015d}"[:15],
                msisdn=f"+1{rng.randint(201, 989)}{rng.randrange(10**7):07d}",
                mnc=mnc,
                device_type=dtype,
                rat=rng.choice(DEVICE_PROFILES[dtype][1]),
                firmware=rng.choice(FIRMWARE),
                home_metro=rng.randrange(len(METROS)),
                battery_offset=rng.uniform(0, 100),
                drain_per_hour=rng.uniform(0.05, 0.8),
            )
        )
    return tuple(fleet)


# --------------------------------------------------------------------------- #
# Batch
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class Batch:
    batch_id: str
    interval_start: datetime
    interval_end: datetime
    header: tuple[str, ...]
    rows: list[list[str]]
    anomalies: Counter[str] = field(default_factory=Counter)

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def to_csv(self) -> str:
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(self.header)
        writer.writerows(self.rows)
        return buf.getvalue()

    def object_name(self, prefix: str) -> str:
        """Hive-partitioned, deterministic path, so a re-run overwrites the same object."""
        return f"{prefix}/{self.partition_path}/part-{self.batch_id}.csv"

    def manifest_name(self, prefix: str) -> str:
        return f"{prefix}/{self.partition_path}/part-{self.batch_id}.json"

    @property
    def partition_path(self) -> str:
        return f"dt={self.interval_start:%Y-%m-%d}/hour={self.interval_start:%H}"

    def manifest(self, uri: str, content: str) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "uri": uri,
            "contract_version": CONTRACT_VERSION,
            "header": list(self.header),
            "row_count": self.row_count,
            "md5": hashlib.md5(content.encode(), usedforsecurity=False).hexdigest(),
            "interval_start": self.interval_start.isoformat(),
            "interval_end": self.interval_end.isoformat(),
            "injected_anomalies": dict(sorted(self.anomalies.items())),
        }


def batch_id_for(interval_start: datetime) -> str:
    return f"{interval_start.astimezone(UTC):%Y%m%dT%H%M}"


class _Messifier:
    """Turns clean values into source-system-style messy strings (seeded RNG)."""

    def __init__(self, rng: random.Random, cfg: GeneratorConfig, counter: Counter[str]) -> None:
        self.rng, self.a, self.n = rng, cfg.anomalies, counter

    def chance(self, p: float, tag: str) -> bool:
        hit = self.rng.random() < p
        if hit:
            self.n[tag] += 1
        return hit

    def maybe_null(self, value: str) -> str:
        return self.rng.choice(NULL_TOKENS) if self.chance(self.a.null_ratio, "null") else value

    def unit(self, value: float, suffix: str, decimals: int = 1) -> str:
        text = f"{value:.{decimals}f}"
        if self.chance(self.a.unit_suffix_ratio, f"unit_suffix{suffix.strip()}"):
            text = f"{text}{suffix}"
        return self.maybe_null(text)

    def timestamp(self, ts: datetime) -> str:
        r = self.rng.random()
        if r < self.a.epoch_timestamp_ratio:
            self.n["ts_epoch_ms"] += 1
            return str(int(ts.timestamp() * 1000))
        if r < self.a.epoch_timestamp_ratio + 0.05:
            self.n["ts_space_format"] += 1
            return f"{ts:%Y-%m-%d %H:%M:%S}"
        return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def generate_batch(
    cfg: EnvConfig, interval_start: datetime, interval_end: datetime | None = None
) -> Batch:
    """Generate the raw batch for one data interval. Deterministic and pure (no I/O)."""
    if interval_start.tzinfo is None:
        raise ValueError("interval_start must be timezone-aware")
    start = interval_start.astimezone(UTC)
    end = (interval_end or start + timedelta(hours=1)).astimezone(UTC)
    if end <= start:
        raise ValueError("interval_end must be after interval_start")

    g = cfg.generator
    fleet = build_fleet(cfg.env, g.device_count, g.network.mcc, tuple(g.network.mnc))
    cells = build_cells()
    rng = random.Random(_seed(cfg.env, "batch", start.isoformat(), end.isoformat()))  # noqa: S311
    counter: Counter[str] = Counter()
    mess = _Messifier(rng, g, counter)

    n_events = rng.randint(g.rows_per_batch.min, g.rows_per_batch.max)
    span = (end - start).total_seconds()
    events = sorted(
        (start + timedelta(seconds=rng.uniform(0, span)), rng.choice(fleet))
        for _ in range(n_events)
    )

    rows: list[list[str]] = []
    for ts, dev in events:
        ts = ts.replace(microsecond=(ts.microsecond // 1000) * 1000)
        rows.append(_row(dev, ts, cells, rng, mess))

    # Exact duplicate rows (at-least-once delivery from the source).
    for _ in range(round(len(rows) * g.anomalies.duplicate_ratio)):
        rows.insert(rng.randrange(len(rows) + 1), list(rng.choice(rows)))
        counter["duplicate_row"] += 1

    return Batch(batch_id_for(start), start, end, RAW_HEADERS, rows, counter)


def _row(
    dev: Device, ts: datetime, cells: tuple[Cell, ...], rng: random.Random, mess: _Messifier
) -> list[str]:
    _, _, up_rng, down_rng, mobile = DEVICE_PROFILES[dev.device_type]
    metro_cells = cells[dev.home_metro * CELLS_PER_METRO : (dev.home_metro + 1) * CELLS_PER_METRO]
    # Static devices stick to one serving cell; mobile ones roam within the metro.
    home_cell = metro_cells[int(dev.device_id[-6:]) % CELLS_PER_METRO]  # stable across runs
    cell = rng.choice(metro_cells) if mobile else home_cell
    jitter = 0.05 if mobile else 0.002

    rsrp = min(-44.0, max(-140.0, rng.gauss(-95, 11)))
    rsrq = min(-3.0, max(-20.0, rng.gauss(-10.5, 3)))
    sinr = min(30.0, max(-20.0, rng.gauss(12, 7)))
    hours = ts.timestamp() / 3600
    battery = 100 - ((dev.battery_offset + hours * dev.drain_per_hour) % 100)
    temp = rng.gauss(24 if dev.device_type != "VEHICLE_TELEMATICS" else 38, 6)
    lat = cell.lat + rng.uniform(-jitter, jitter)
    lon = cell.lon + rng.uniform(-jitter, jitter)
    event = rng.choices(list(EVENT_WEIGHTS), list(EVENT_WEIGHTS.values()))[0]

    # Out-of-range values (sensor faults / firmware bugs).
    a = mess.a
    if mess.chance(a.out_of_range_ratio, "oor_battery"):
        battery = rng.choice([rng.uniform(101, 160), rng.uniform(-30, -1)])
    if mess.chance(a.out_of_range_ratio, "oor_rsrp"):
        rsrp = rng.choice([rng.uniform(-220, -150), rng.uniform(5, 40)])
    if mess.chance(a.out_of_range_ratio, "oor_temperature"):
        temp = rng.uniform(150, 300)
    if mess.chance(a.out_of_range_ratio / 2, "oor_coordinates"):
        lat, lon = lon, lat  # swapped lat/long: a classic integration bug

    device_id = dev.device_id
    if mess.chance(0.10, "device_id_formatting"):
        device_id = rng.choice([f"  {device_id.lower()} ", device_id.lower(), f"{device_id} "])
    imei = dev.imei
    if mess.chance(0.10, "imei_dashes"):
        imei = f"{imei[:2]}-{imei[2:8]}-{imei[8:14]}-{imei[14]}"
    firmware = f"v{dev.firmware}" if mess.chance(0.4, "fw_v_prefix") else dev.firmware

    return [
        device_id,
        mess.maybe_null(imei),
        mess.maybe_null(dev.imsi),
        mess.maybe_null(dev.msisdn),
        mess.timestamp(ts),
        dev.imsi[:3],  # MCC
        dev.mnc,
        str(cell.tac),
        mess.maybe_null(str(cell.cell_id)),
        rng.choice(RAT_ALIASES[dev.rat]),
        mess.unit(rsrp, " dBm"),
        mess.unit(rsrq, " dB"),
        mess.unit(sinr, " dB"),
        mess.unit(battery, "%"),
        mess.unit(temp, "C"),
        str(rng.randint(*up_rng)),
        str(rng.randint(*down_rng)),
        mess.maybe_null(f"{lat:.6f}"),
        mess.maybe_null(f"{lon:.6f}"),
        firmware,
        rng.choice(_device_type_variants(dev.device_type)),
        rng.choice([event, event.lower(), event.capitalize()]),
    ]


def iter_intervals(start: datetime, hours: int) -> list[tuple[datetime, datetime]]:
    return [(start + timedelta(hours=h), start + timedelta(hours=h + 1)) for h in range(hours)]


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone, e.g. 'Z'")
    if ts.minute or ts.second or ts.microsecond:
        raise argparse.ArgumentTypeError("start must be aligned to the hour")
    return ts.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m iot_pipeline.generator")
    parser.add_argument(
        "--start", type=_parse_ts, required=True, help="hour-aligned, e.g. ...T10:00:00Z"
    )
    parser.add_argument("--hours", type=int, default=1)
    parser.add_argument("--config", help="path to env YAML (default: IOT_ENV resolution)")
    dest = parser.add_mutually_exclusive_group(required=True)
    dest.add_argument("--out", type=Path, help="write files under this local directory")
    dest.add_argument("--upload", action="store_true", help="upload to the staging bucket")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    raw = cfg.pipeline.raw
    client = None
    if args.upload:
        from iot_pipeline import gcs

        client = gcs.storage_client(cfg.project.id)

    for start, end in iter_intervals(args.start, args.hours):
        batch = generate_batch(cfg, start, end)
        content = batch.to_csv()
        name = batch.object_name(raw.prefix)
        manifest_name = batch.manifest_name(raw.manifest_prefix)
        if client is not None:
            from iot_pipeline import gcs

            bucket = cfg.storage.staging_bucket.name
            uri = f"gs://{bucket}/{name}"
            wrote = gcs.upload_if_changed(client, bucket, name, content, "text/csv")
            gcs.write_json(client, bucket, manifest_name, batch.manifest(uri, content))
            status = "uploaded" if wrote else "unchanged (idempotent skip)"
        else:
            path = args.out / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8", newline="")
            uri, status = str(path), "written"
        shape = f"{batch.row_count} rows x {len(batch.header)} cols"
        print(f"{batch.batch_id}: {shape} -> {uri} [{status}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
