"""Cloud Storage helpers. Functions take a ``storage.Client`` so the same code runs from
the CLI, from Airflow (``GCSHook().get_conn()``) and from tests with fakes."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # keep import-time cheap for DAG parsing
    import google.cloud.storage as storage


def storage_client(project: str) -> storage.Client:
    import google.cloud.storage as storage

    return storage.Client(project=project)


def gs_uri(bucket: str, name: str) -> str:
    return f"gs://{bucket}/{name}"


def parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://") or "/" not in uri[5:]:
        raise ValueError(f"not a gs://bucket/object URI: {uri!r}")
    bucket, _, name = uri[5:].partition("/")
    return bucket, name


def md5_b64(content: str | bytes) -> str:
    data = content.encode() if isinstance(content, str) else content
    return base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode()


def upload_if_changed(
    client: storage.Client,
    bucket: str,
    name: str,
    content: str | bytes,
    content_type: str,
) -> bool:
    """Write the object only if its content differs (compared by MD5).

    Identical re-runs therefore create no new object generation. That keeps versioned
    buckets clean and makes re-runs observably no-ops. Returns True if it wrote.
    """
    blob = client.bucket(bucket).get_blob(name)
    if blob is not None and blob.md5_hash == md5_b64(content):
        return False
    target = client.bucket(bucket).blob(name)
    target.upload_from_string(content, content_type=content_type, checksum="md5")
    return True


def write_json(client: storage.Client, bucket: str, name: str, obj: Any) -> bool:
    payload = json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n"
    return upload_if_changed(client, bucket, name, payload, "application/json")


def read_json(client: storage.Client, bucket: str, name: str) -> Any:
    return json.loads(client.bucket(bucket).blob(name).download_as_text())


def list_uris(client: storage.Client, bucket: str, prefix: str, suffix: str = "") -> list[str]:
    """Sorted gs:// URIs under ``prefix`` that end with ``suffix``."""
    return sorted(
        gs_uri(bucket, b.name)
        for b in client.list_blobs(bucket, prefix=prefix)
        if b.name.endswith(suffix)
    )
