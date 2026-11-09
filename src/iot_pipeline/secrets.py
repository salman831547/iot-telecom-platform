"""Secret Manager access. Values are fetched at task runtime, never during DAG parsing,
and are never logged or returned through XCom."""

from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=16)
def access_secret(project: str, secret_id: str, version: str = "latest") -> str:
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_id}/versions/{version}"
    return client.access_secret_version(name=name).payload.data.decode("utf-8")
