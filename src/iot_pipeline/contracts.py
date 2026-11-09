"""Data contract for raw IoT telemetry files (the boundary between source and platform).

Raw CSVs carry the source system's messy headers. The landing layer loads every
column as STRING, under a sanitised snake_case name. The silver SQL then renames,
casts and cleans. Keeping the header contract in one module lets the generator, the
file-validation task, the landing schema file and the tests share a single definition.
"""

from __future__ import annotations

import re

CONTRACT_VERSION = "1.0"

# Exact header row of a raw file, in order. The messy names are deliberate: they match
# what a real device-management export looks like.
RAW_HEADERS: tuple[str, ...] = (
    "Device ID",
    "IMEI",
    "IMSI",
    "MSISDN",
    "timeStamp",
    "MCC",
    "MNC",
    "TAC",
    "Cell ID",
    "Network Type",
    "RSRP",
    "RSRQ",
    "SINR",
    "Battery %",
    "Temp (C)",
    "Bytes Up",
    "Bytes Down",
    "Lat",
    "Long",
    "FW Version",
    "Device Type",
    "Event",
)

_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def sanitize_header(header: str) -> str:
    """Raw header -> landing column name: camelCase split, lowercase, non-alnum -> '_'.

    >>> sanitize_header("timeStamp"), sanitize_header("Battery %"), sanitize_header("Temp (C)")
    ('time_stamp', 'battery', 'temp_c')
    """
    snake = _CAMEL.sub("_", header.strip()).lower()
    return _NON_ALNUM.sub("_", snake).strip("_")


LANDING_COLUMNS: tuple[str, ...] = tuple(sanitize_header(h) for h in RAW_HEADERS)

# Tokens the source uses for "no value". Silver treats all of them as NULL.
NULL_TOKENS: tuple[str, ...] = ("", "N/A", "null", "NaN", "-")
